from __future__ import print_function

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import textwrap
from uuid import uuid4
import gzip
from bd2k.util.exceptions import require
from toil.lib.bioio import addLoggingOptions, setLoggingFromOptions
import time
import virtualenv
import re
from itertools import chain

from shutil import copyfile

import socket

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

def call_pipeline(mount, args):

    # Activate the virtual environment where the toil rnaseq pipeline is installed
    activate_this = '/opt/rnaseq-pipeline/toil_venv/bin/activate_this.py'
    execfile(activate_this, dict(__file__=activate_this))

    if args.auto_scale:
        # Run the meoso-master process
        # Run it in the background by using Popen instead of check_call 
        # https://stackoverflow.com/questions/32577071/execute-subprocess-in-background
        command = ['mesos-master', '--log_dir=/var/lib/mesos', '--registry=in_memory', '--cluster=' + args.cluster_name]
        try:
            log.info('Executing mesos command: ' + str(command))
            p = subprocess.Popen(command)
        except OSError as e:
            print(e.message, file=sys.stderr)
            exit(e.returncode)
        else:
            log.info('mesos-master process running in background')


    unique_toil_dir = 'Toil-RNAseq-' + str(uuid4())
    work_dir = os.path.join(mount, unique_toil_dir)

    os.makedirs(work_dir)
    log.info('Temporary directory created: {}'.format(work_dir))
    config_path = os.path.join(work_dir, 'toil-rnaseq.config')

    output_dir = mount
    if args.auto_scale:
        #set job store to be clould URL, e.g. S3 bucket
        job_store = os.path.join(args.resume, 'jobStore') if args.resume else args.job_store
        output_dir = args.output_location
    else:
        job_store = os.path.join(args.resume, 'jobStore') if args.resume else os.path.join(work_dir, 'jobStore')
   

    with open(config_path, 'w') as f:
        f.write(generate_config(args.star, args.rsem, args.kallisto, args.hera, output_dir,
                                args.disable_cutadapt, args.save_bam, args.save_wiggle,
                                args.bamqc, args.max_sample_size))
    loglevel = log.getEffectiveLevel()

    command = ['toil-rnaseq', 'run',
           job_store,
           '--config', config_path,
           args.toilLoggingOption,
           '--retryCount', '1']

    if args.auto_scale:
        # Pick the working directory carefully; it must exist on the workers
        # Do not let Toil pick the directory because this will not work when
        # launching the workflow with Dockstore because dockstore uses cwltool
        # which makes the file system read only which will prevent Toil from 
        # creating the directory. We cannot use a directory like
        # /home/ubunut/... becuase that does not exist on coreos which is what
        # the worker nodes use for an OS; '/tmp' will usually already be 
        # created on most Linux distributions
        command.extend(['--workDir', '/tmp'])
    else:
        command.extend(['--workDir', work_dir])


    if args.resume:
        command.append('--restart')
    if args.cores:
        command.append('--maxCores={}'.format(args.cores))
    path_to_manifest = generate_manifest(args.sample_tar, args.sample_single, \
        args.sample_paired, work_dir, args.output_basenames)
    command.append('--manifest=' + path_to_manifest)

    if args.auto_scale:
        # get the private ip address of the VM on which the container is running
        # this probably requires the --net=host option with the docker run
        # commmand. This will be where the mesos master is running.
        private_ip_address = socket.gethostbyname(socket.gethostname())
        # add the extra options to run autoscaling to the command to run the pipeline
        autoscale_options = ['--provisioner', args.provisioner, '--nodeType',
            args.node_type, '--batchSystem', 'mesos', '--maxNodes', str(args.max_nodes),
            '--mesosMaster', private_ip_address + ':5050', '--logLevel', 'DEBUG']
        command.extend(autoscale_options)

        if args.provisioner == 'aws':
            os.environ['AWS_ACCESS_KEY_ID'] = args.credentials_id
            os.environ['AWS_SECRET_ACCESS_KEY'] = args.credentials_secret_key

    try:
        log.info('Docker Command: ' + str(command))
        subprocess.check_call(command)
    except subprocess.CalledProcessError as e:
        print(e.message, file=sys.stderr)
        exit(e.returncode)
    else:
        log.info('Pipeline terminated, changing ownership of output files from root to user.')
        stat = os.stat(mount)
        subprocess.check_call(['chown', '-R', '{}:{}'.format(stat.st_uid, stat.st_gid), mount])
        if not args.no_clean:
            log.info('Cleaning up temporary directory: {}'.format(work_dir))
            shutil.rmtree(work_dir)
        else:
            log.info('Flag "--no-clean" was used, therefore {} was not deleted.'.format(work_dir))

    log.info("output dir is {} and files are:\n{}:".format(mount,'\n'.join(os.listdir(mount))))
    fail_files = [x for x in os.listdir(mount) if x.startswith('FAIL.')]
    log.info("fail files are:\n{}:".format('\n'.join(fail_files)))
    for fail_file in fail_files:
        new_file = fail_file[len('FAIL.'):]
        fail_file_path = os.path.join(mount, fail_file)
        new_file_path = os.path.join(mount, new_file)
        cmd = ["mv", fail_file_path, new_file_path]
        log.info("moving " + fail_file_path + " to " + new_file_path)
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError as e:
            print(e.message, file=sys.stderr)
        except Exception as e:
            print("\nERROR: FAIL file mv exception information:" + str(e), file=sys.stderr)

def generate_manifest(sample_tars, sample_singles, sample_pairs, workdir, output_basenames):
    path = os.path.join(workdir, 'manifest-toil-rnaseq.tsv')
    if sample_tars:
        sample_tars = map(fileURL, sample_tars)
    if sample_pairs:

        print('generate manifest sample pairs:{}'.format(sample_pairs))
        log.info('generate manifest sample pairs:{}'.format(sample_pairs))
        print('generate manifest output base names:{}'.format(output_basenames))
        log.info('generate manifest ouput base names:{}'.format(output_basenames))

        sample_pairs = map(lambda sample: formatPairs(sample, workdir), sample_pairs)
    if sample_singles:
        sample_singles = map(fileURL, sample_singles)
    log.info('Path to manifest: ' + workdir)
    with open(path, 'w') as f:
        for samples in (sample_pairs, sample_tars, sample_singles):
            if not samples:
                continue
            type = 'fq' if samples != sample_tars else 'tar'
            pairing = 'paired' if samples != sample_singles else 'single'
            f.write('\n'.join('\t'.join([type, pairing, getSampleName(sample, \
                output_basename), sample]) for sample, output_basename in zip(samples, output_basenames)))
            f.write('\n')
    return path


def fileURL(sample):
    if sample.startswith('/') or sample.startswith('.'):
        sample = 'file://' + sample
    return sample


def getSampleName(sample, output_basename):
    if output_basename:
        name = output_basename
    else:
        name = os.path.basename(sample).split('.')[0]
        if name.endswith('R1') or name.endswith('R2'):
            return name[:-2]
    return name


def formatPairs(sample_pairs, work_mount):
    r1, r2 = [], []
    
    print('sample pairs:{}'.format(sample_pairs))
    log.info('sample pairs:{}'.format(sample_pairs))
    fastqs = sample_pairs.split(',')
    # Pattern convention: Look for "R1" / "R2" in the filename, or "_1" / "_2" before the extension
    pattern = re.compile('(?:^|[._-])(R[12]|[12]\.f)')
    for fastq in sorted(fastqs):
        match = pattern.search(os.path.basename(fastq))
        fastq = fileURL(fastq)
        if not match:
            log.info('FASTQ file name fails to meet required convention for paired reads '
                            '(see documentation). ' + fastq)
            exit(1)
        elif '1' in match.group():
            r1.append(fastq)
        elif '2' in match.group():
            r2.append(fastq)
        else:
            assert False, match.group()
    require(len(r1) == len(r2), 'Check fastq names, uneven number of pairs found.\nr1: {}\nr2: {}'.format(r1, r2))
    interleaved_samples = zip(r1, r2)
    # flatten the list of tuples and join them into a comma delimited string
    # https://stackoverflow.com/questions/40993966/python-convert-tuple-to-comma-separated-string
    comma_delimited_samples = ','.join(map(str,chain.from_iterable(interleaved_samples)))
    log.info('comma delimited samples:{}'.format(comma_delimited_samples))
    return comma_delimited_samples


def generate_config(star_path, rsem_path, kallisto_path, hera_path, output_dir, disable_cutadapt, save_bam,
                    save_wiggle, bamqc, max_sample_size):
    cutadapt = True if not disable_cutadapt else False
    bamqc = bool(bamqc)

    
    if star_path:
        star_path = fileURL(star_path)
    if kallisto_path:
        kallisto_path = fileURL(kallisto_path)
    if rsem_path:
        rsem_path = fileURL(rsem_path)
    if hera_path:
        hera_path = fileURL(hera_path)
   
    return textwrap.dedent("""
        star-index: {star_path}
        kallisto-index: {kallisto_path}
        hera-index: {hera_path}
        rsem-ref: {rsem_path}
        output-dir: {output_dir}
        cutadapt: {cutadapt}
        fastqc: true
        bamqc: {bamqc}
        fwd-3pr-adapter: AGATCGGAAGAG
        rev-3pr-adapter: AGATCGGAAGAG
        ssec:
        gtkey:
        wiggle: {save_wiggle}
        save-bam: {save_bam}
        ci-test:
        max-sample-size: {max_sample_size}
    """[1:].format(**locals()))

def main():
    """
    Computational Genomics Lab, Genomics Institute, UC Santa Cruz
    Dockerized Toil RNA-seq pipeline

    RNA-seq fastqs are combined, aligned, and quantified with 2 different methods (RSEM and Kallisto)

    General Usage:
    docker run -v $(pwd):$(pwd) -v /var/run/docker.sock:/var/run/docker.sock \
    quay.io/ucsc_cgl/rnaseq-cgl-pipeline --samples sample1.tar

    Please see the complete documentation located at:
    https://github.com/BD2KGenomics/cgl-docker-lib/tree/master/rnaseq-cgl-pipeline
    or inside the container at: /opt/rnaseq-pipeline/README.md


    Structure of RNA-Seq Pipeline (per sample)

                  3 -- 4 -- 5
                 /          |
      0 -- 1 -- 2 ---- 6 -- 8
                 \          |
                  7 ---------

    0 = Download sample
    1 = Unpack/Merge fastqs
    2 = CutAdapt (adapter trimming)
    3 = STAR Alignment
    4 = RSEM Quantification
    5 = RSEM Post-processing
    6 = Kallisto
    7 = FastQC
    8 = Consoliate output and upload to S3
    =======================================
    Dependencies
    Docker
    """
    # Define argument parser for
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--sample-tar', default=[], action="append",
                        help='Absolute path to sample tarball.')
    parser.add_argument('--sample-single', default=[], action="append",
                        help='Absolute path to sample single-ended FASTQ.')
    parser.add_argument('--sample-paired', nargs='*', default=[],
                        help='Absolute path to sample paired FASTQs, in the form `read1,read2,read1,read2`.')
    parser.add_argument('--output-basenames', nargs='*', default=[],
                        help='Base names to use for naming the output files ')


    parser.add_argument('--star', type=str, default="",
                        help='Absolute path to STAR index tarball.')
    parser.add_argument('--rsem', type=str, default="",
                        help='Absolute path to rsem reference tarball.')
    parser.add_argument('--kallisto', type=str, default="",
                        help='Absolute path to kallisto index (.idx) file.')
    parser.add_argument('--hera', type=str, default="",
                        help='Absolute path to hera index (.idx) file.')
    parser.add_argument('--disable-cutadapt', action='store_true', default=False,
                        help='Cutadapt fails if samples are improperly paired. Use this flag to disable cutadapt.')
    parser.add_argument('--save-bam', action='store_true', default='false',
                        help='If this flag is used, genome-aligned bam is written to output.')
    parser.add_argument('--save-wiggle', action='store_true', default='false',
                        help='If this flag is used, wiggle files (.bg) are written to output.')
    parser.add_argument('--no-clean', action='store_true',
                        help='If this flag is used, temporary work directory is not cleaned.')
    parser.add_argument('--resume', type=str, default=None,
                        help='Pass the working directory that contains a job store to be resumed.')
    parser.add_argument('--cores', type=int, default=None,
                        help='Will set a cap on number of cores to use, default is all available cores.')
    parser.add_argument('--bamqc', action='store_true', default=None,
                        help='Enable BAM QC step. Disabled by default')
    parser.add_argument('--work_mount', required=True,
                        help='Mount where intermediate files should be written. This directory '
                             'should be mirror mounted into the container.')
    parser.add_argument('--max-sample-size', default="20G",
                        help='Maximum size of sample file using Toil resource requirements '
                        "syntax, e.g '20G'. Standard suffixes like K, Ki, M, Mi, G or Gi are supported.")

    auto_scale_options  = parser.add_argument_group('Auto-scaling options')
    auto_scale_options.add_argument('--auto-scale', action='store_true', default=False,
                        help='Enable Toil autoscaling. Disabled by default')
    auto_scale_options.add_argument('--cluster-name', default="",
                        help='Name of the Toil cluster. Usually the security group name')
    auto_scale_options.add_argument('--job-store', default="aws:us-west-2:autoscaling-toil-rnaseq-jobstore-2",
                        help='Directory in cloud where working files will be put; '
                        'e.g. aws:us-west-2:autoscaling-toil-rnaseq-jobstore')
    auto_scale_options.add_argument('--output-location', default="s3://toil-rnaseq-cloud-staging-area",
                        help='Directory in cloud where  output files will be put; '
                        'e.g. s3://toil-rnaseq-cloud-staging-area')
    auto_scale_options.add_argument('--provisioner', default="aws",
                        help='Cloud provisioner to use. E.g aws')
    auto_scale_options.add_argument('--node-type', default="c3.8xlarge",
                        help='Cloud worker VM type; e.g. c3.8xlarge')
    auto_scale_options.add_argument('--max-nodes', type=int, default=2,
                        help='Maximum worker nodes to launch. E.g. 2')
    auto_scale_options.add_argument('--credentials-id', default="",
                        help='Credentials id')
    auto_scale_options.add_argument('--credentials-secret-key', default="",
                        help='Credentials secret key')

    # although we don't actually set the log level in this module, the option is propagated to toil. For this reason
    # we want the logging options to show up with we run --help
    addLoggingOptions(parser)
    toilLoggingOption = '--logDebug'
    for arg in sys.argv:
        if 'log' in arg:
            toilLoggingOption = arg
            sys.argv.remove(toilLoggingOption)
            break
    args = parser.parse_args()
    args.toilLoggingOption = toilLoggingOption
    # If no arguments provided, print full help menu
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    if args.auto_scale:
        if not args.cluster_name:
           log.info('Auto-scaling requires a cluster name to be input with the --cluster-name option')
           parser.error('Auto-scaling requires a cluster name to be input with the --cluster-name option')
        if not args.credentials_id or not args.credentials_secret_key:
           log.info('Auto-scaling requires provisioner credentials id and secret key')
           parser.error('Auto-scaling requires provisioner credentials id and secret key')

    # Get name of most recent running container. If socket is mounted, should be this one.
    try:
        name = subprocess.check_output(['docker', 'ps', '--format', '{{.Names}}']).split('\n')[0]
    except subprocess.CalledProcessError as e:
        raise RuntimeError('No container detected, ensure Docker is being run with: '
                           '"-v /var/run/docker.sock:/var/run/docker.sock" as an argument. \n\n{}'.format(e.message))
    # Get name of mounted volume
    blob = json.loads(subprocess.check_output(['docker', 'inspect', name]))
    mounts = blob[0]['Mounts']
    # Ensure docker.sock is mounted correctly
    sock_mount = [x['Source'] == x['Destination'] for x in mounts if 'docker.sock' in x['Source']]
    require(len(sock_mount) == 1, 'Missing socket mount. Requires the following: '
                                  'docker run -v /var/run/docker.sock:/var/run/docker.sock')
    work_mount = args.work_mount
    for samples in [args.sample_tar, args.sample_paired, args.sample_single]:
        if not samples:
            continue

        # Enforce file input standards
        if args.auto_scale:
            require(len(args.output_basenames) == len(samples), "There must be a "
            "unique output filename for each sample. You provided {}".format(args.output_basenames))

            require(all( ((x.lower().startswith('http://') or x.lower().startswith('s3://') \
                or x.lower().startswith('ftp://')) or not x) for x in samples),
            "Sample inputs must point to a file's full path, "
            "e.g. 's3://full/path/to/sample_R1.fastq.gz', and should start with "
            " file://, http://, s3://, or ftp://.  You provided %s", str(samples))
        else:
            # If sample is given as relative path, assume it's in the work directory
            if not all(x.startswith('/') for x in samples):
                samples = [os.path.join(work_mount, x) for x in samples if not x.startswith('/')]
                log.info('\nSample given as relative path, assuming sample is in work directory: {}'.format(work_mount[0]))

            require(all(x.startswith('/') for x in samples),
                "Sample inputs must point to a file's full path, "
                "e.g. '/full/path/to/sample1.tar'. You provided %s", str(samples))
        if samples == args.sample_tar:
            log.info('TARs to run: {}'.format('\t'.join(args.sample_tar)))
        if samples == args.sample_paired:
            log.info('Paired FASTQS to run: {}'.format('\t'.join(args.sample_paired)))
        if samples == args.sample_single:
            log.info('Single FASTQS to run: {}'.format('\t'.join(args.sample_single)))


    #file paths should start with /, file://, http://, s3://, or ftp://
    if args.auto_scale:
        require(all( ((x.lower().startswith('http://') or x.lower().startswith('s3://') \
                or x.lower().startswith('ftp://')) or not x) for x in [args.star, \
                             args.kallisto, args.rsem, args.hera]),
            "Sample inputs must point to a file's full path, "
            "e.g. 's3://full/path/to/kallisto_hg38.idx', and should start with file://, http://, s3://, or ftp://.")
    else:
        #Input for star and rsem will be empty if user wants to run kallisto only so test for not x
        require(all( (x.startswith('/') or not x) for x in [args.star, 
                             args.kallisto, args.rsem, args.hera]),
            "Sample inputs must point to a file's full path, "
            "e.g. '/full/path/to/kallisto_hg38.idx'")

    # Output log information
    log.info('The work mount is: {}'.format(work_mount))
    log.info('Pipeline input locations: \n{}\n{}\n{}\n{}'.format(args.star, args.rsem, args.kallisto, args.hera))
    call_pipeline(work_mount, args)

if __name__ == '__main__':
    main()
