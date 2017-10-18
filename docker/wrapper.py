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

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()


def call_pipeline(mount, args):
    work_dir = os.path.join(mount, 'Toil-RNAseq-' + str(uuid4()))
    os.makedirs(work_dir)
    log.info('Temporary directory created: {}'.format(work_dir))
    config_path = os.path.join(work_dir, 'toil-rnaseq.config')
    job_store = os.path.join(args.resume, 'jobStore') if args.resume else os.path.join(work_dir, 'jobStore')
    with open(config_path, 'w') as f:
        f.write(generate_config(args.star, args.rsem, args.kallisto, mount,
                                args.disable_cutadapt, args.save_bam, args.save_wiggle,
                                args.bamqc))
    loglevel = log.getEffectiveLevel()

    command = ['toil-rnaseq', 'run',
               job_store,
               '--config', config_path,
               '--workDir', work_dir,
               args.toilLoggingOption,
               '--retryCount', '1']
    if args.resume:
        command.append('--restart')
    if args.cores:
        command.append('--maxCores={}'.format(args.cores))
    path_to_manifest = generate_manifest(args.sample_tar, args.sample_single, args.sample_paired, work_dir, args.output_basename)
    command.append('--manifest=' + path_to_manifest)
    try:
        log.info('Docker Comand: ' + str(command))
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

def generate_manifest(sample_tars, sample_singles, sample_pairs, workdir, output_basename):
    path = os.path.join(workdir, 'manifest-toil-rnaseq.tsv')
    if sample_tars:
        sample_tars = map(fileURL, sample_tars)
    if sample_pairs:
        sample_pairs = map(lambda sample: formatPairs(sample, workdir), sample_pairs)
    if sample_singles:
        sample_singles = map(lambda sample: formatSingles(sample, workdir), sample_singles)
    log.info('Path to manifest: ' + workdir)
    with open(path, 'w') as f:
        for samples in (sample_pairs, sample_tars, sample_singles):
            if not samples:
                continue
            type = 'fq' if samples != sample_tars else 'tar'
            pairing = 'paired' if samples != sample_singles else 'single'
            f.write('\n'.join('\t'.join([type, pairing, getSampleName(sample, output_basename), sample]) for sample in samples))
            f.write('\n')
    return path


def catFiles(outputFile, inputFiles):
    '''
    Routine to concatenate input files, gzipped or not using cat
    For gzipped files this is much faster than using python gunzip
    see https://www.biostars.org/p/136025/ and https://www.biostars.org/p/81924/
    '''
    command = 'cat'
    with open(outputFile, 'w') as outfile:
        subprocess.check_call([command] + inputFiles, stdout=outfile)
    return outputFile

def fileURL(sample):
    return 'file://' + sample


def getSampleName(sample, output_basename):
    if output_basename:
        name = output_basename
    else:
        name = os.path.basename(sample).split('.')[0]
        if name.endswith('R1') or name.endswith('R2'):
            return name[:-2]
    return name


def formatPairs(sample_pairs, work_mount):
    def formatPair(name):
        pairList = [name, name]
        for index in range(0, len(pairList)):
            for ending in ('.fastq.gz', '.fastq', '.fq.gz', '.fq'):
                if pairList[index].endswith(ending):
                    baseName = pairList[index].split(ending)[0] # TODO: RENAME FILE NOT THE STRING
                    baseName += 'merged'
                    if index % 2 == 0:
                        pairList[index] = baseName + 'R1' + ending
                        break
                    elif index % 2 == 1:
                        pairList[index] = baseName + 'R2' + ending
                        break
        return pairList

    sample_pairs = sample_pairs.split(',')
    assert len(sample_pairs) % 2 == 0
    outputName = os.path.join(work_mount, os.path.basename(sample_pairs[0]))
    outputFiles = formatPair(outputName)
    catFiles(outputFiles[0], sample_pairs[::2])
    catFiles(outputFiles[1], sample_pairs[1::2])
    return fileURL(outputFiles[0]) + ',' + fileURL(outputFiles[1])

def formatSingles(sample_singles, work_mount):
    def formatSingle(single):
        for ending in ('.fastq.gz', '.fastq', '.fq.gz', '.fq'):
            if single.endswith(ending):
                baseName = single.split(ending)[0]  # TODO: RENAME FILE NOT THE STRING
                baseName += 'merged'
                return baseName + ending
    sample_singles = sample_singles.split(',')
    output = formatSingle(os.path.join(work_mount, os.path.basename(sample_singles[0])))
    catFiles(output, sample_singles)
    return fileURL(output)

def generate_config(star_path, rsem_path, kallisto_path, output_dir, disable_cutadapt, save_bam,
                    save_wiggle, bamqc):
    cutadapt = True if not disable_cutadapt else False
    bamqc = bool(bamqc)

    if star_path:
        star_path = "file://" + star_path
    if kallisto_path:
        kallisto_path = "file://" + kallisto_path
    if rsem_path:
        rsem_path = "file://" + rsem_path

    return textwrap.dedent("""
        star-index: {star_path}
        kallisto-index: {kallisto_path}
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
    parser.add_argument('--sample-paired', default=[], action="append",
                        help='Absolute path to sample paired FASTQs, in the form `read1,read2,read1,read2`.')
    parser.add_argument('--star', type=str, required=True,
                        help='Absolute path to STAR index tarball.')
    parser.add_argument('--rsem', type=str, required=True,
                        help='Absolute path to rsem reference tarball.')
    parser.add_argument('--kallisto', type=str, required=True,
                        help='Absolute path to kallisto index (.idx) file.')
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
    parser.add_argument('--output-basename', default="",
                        help='Base name to use for naming the output files ')
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
        # If sample is given as relative path, assume it's in the work directory
        if not all(x.startswith('/') for x in samples):
            samples = [os.path.join(work_mount, x) for x in samples if not x.startswith('/')]
            log.info('\nSample given as relative path, assuming sample is in work directory: {}'.format(work_mount[0]))
        # Enforce file input standards
        require(all(x.startswith('/') for x in samples),
                "Sample inputs must point to a file's full path, "
                "e.g. '/full/path/to/sample1.tar'. You provided %s", str(samples))
        if samples == args.sample_tar:
            log.info('TARs to run: {}'.format('\t'.join(args.sample_tar)))
        if samples == args.sample_paired:
            log.info('Paired FASTQS to run: {}'.format('\t'.join(args.sample_paired)))
        if samples == args.sample_single:
            log.info('Single FASTQS to run: {}'.format('\t'.join(args.sample_single)))

    #print("star {} kallisto {} rsem {} args".format(args.star, args.kallisto, args.rsem))
    #Input for star and rsem will be empty if user wants to run kallisto only so test for not x
    require(all( (x.startswith('/') or not x) for x in [args.star, args.kallisto, args.rsem]),
            "Sample inputs must point to a file's full path, "
            "e.g. '/full/path/to/kallisto_hg38.idx'.")

    # Output log information
    log.info('The work mount is: {}'.format(work_mount))
    log.info('Pipeline input locations: \n{}\n{}\n{}'.format(args.star, args.rsem, args.kallisto))
    call_pipeline(work_mount, args)

if __name__ == '__main__':
    main()
