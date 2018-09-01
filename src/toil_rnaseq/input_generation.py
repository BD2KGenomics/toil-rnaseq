#!/usr/bin/env python2.7
import argparse
import logging
import os
import sys
from multiprocessing import cpu_count

from toil.job import Job
from toil.lib.docker import dockerCall

from utils.files import move_files
from utils.files import tarball_files
from utils.urls import download_url

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

_move_instead_of_return = True


def root(job, args):
    # Set max cores based on system or input
    args.cores = min(args.maxCores, cpu_count())

    if args.hera:
        job.addChildJobFn(hera_index, args, cores=1, disk='5G', memory='4G')

    if args.kallisto:
        job.addChildJobFn(kallisto_index, args, cores=1, disk='5G', memory='4G')

    if args.rsem:
        job.addChildJobFn(rsem_index, args, cores=args.cores, disk='10G', memory='8G')

    if args.star:
        job.addChildJobFn(star_index, args, cores=args.cores, disk='60G', memory='30G')


def star_index(job, args):
    download_url(url=args.ref, name='ref.fa', work_dir=job.tempDir)
    download_url(url=args.gtf, name='annotation.gtf', work_dir=job.tempDir)

    # Run STAR to generate index
    star_dir = os.path.join(job.tempDir, args.star_name)
    os.mkdir(star_dir)
    parameters = ['--runThreadN', str(args.cores),
                  '--runMode', 'genomeGenerate',
                  '--genomeDir', '/data/' + args.star_name,
                  '--genomeFastaFiles', 'ref.fa',
                  '--sjdbGTFfile', 'annotation.gtf']
    dockerCall(job, tool='quay.io/ucsc_cgl/star:2.4.2a--bcbd5122b69ff6ac4ef61958e47bde94001cfe80',
               workDir=job.tempDir, parameters=parameters)

    # Compress starIndex into a tarball
    star_tar = '{}.tar.gz'.format(args.star_name)
    tarball_files(star_tar, file_paths=[star_dir], output_dir=job.tempDir)

    # Move to output dir or return
    tar_path = os.path.join(job.tempDir, star_tar)
    if _move_instead_of_return:
        move_files([tar_path], args.output_dir)
    else:
        return job.fileStore.readGlobalFile(tar_path)


def rsem_index(job, args):
    download_url(url=args.ref, name='ref.fa', work_dir=job.tempDir)
    download_url(url=args.gtf, name='annotation.gtf', work_dir=job.tempDir)

    # Run RSEM to generate reference
    rsem_dir = os.path.join(job.tempDir, args.rsem_name)
    os.mkdir(rsem_dir)
    docker_parameters = ['--entrypoint', 'rsem-prepare-reference',
                         '-v', '{}:/data'.format(job.tempDir),
                         '--rm', '--log-driver=none']
    parameters = ['-p', str(args.cores),
                  '--gtf', '/data/annotation.gtf',
                  '/data/ref.fa',
                  os.path.join('/data', args.rsem_name, args.rsem_name)]
    dockerCall(job, tool='quay.io/ucsc_cgl/rsem:1.2.25--d4275175cc8df36967db460b06337a14f40d2f21',
               parameters=parameters, dockerParameters=docker_parameters)

    # Compress rsemRef into a tarball
    rsem_tar = '{}.tar.gz'.format(args.rsem_name)
    tarball_files(rsem_tar, file_paths=[rsem_dir], output_dir=job.tempDir)

    # Move to output dir
    tar_path = os.path.join(job.tempDir, rsem_tar)
    if _move_instead_of_return:
        move_files([tar_path], args.output_dir)
    else:
        return job.fileStore.readGlobalFile(tar_path)


def kallisto_index(job, args):
    if args.transcriptome:
        download_url(url=args.transcriptome, name='transcriptome.fa', work_dir=job.tempDir)
    else:
        _create_transcriptome(job, args, job.tempDir)

    # Run Kallisto Index
    parameters = ['index', 'transcriptome.fa', '-i', '/data/{}.index'.format(args.kallisto_name)]
    dockerCall(job, tool='quay.io/ucsc_cgl/kallisto:0.43.1--355c19b1fb6fbb85f7f8293e95fb8a1e9d0da163',
               workDir=job.tempDir, parameters=parameters)

    # Move to output dir
    output_path = os.path.join(job.tempDir, args.kallisto_name + '.index')
    if _move_instead_of_return:
        move_files([output_path], args.output_dir)
    else:
        return job.fileStore.readGlobalFile(output_path)


def _create_transcriptome(job, args, work_dir):
    # Download files to generate transcriptome
    download_url(url=args.ref, name='ref.fa', work_dir=work_dir)
    download_url(url=args.gtf, name='annotation.gtf', work_dir=work_dir)

    parameters = ['gtf_to_fasta', '/data/annotation.gtf', '/data/ref.fa', '/data/transcriptome.fa']
    dockerCall(job, tool='limesbonn/tophat2', workDir=work_dir, parameters=parameters)


def hera_index(job, args):
    # Download input files
    download_url(url=args.ref, name='ref.fa', work_dir=job.tempDir)
    download_url(url=args.gtf, name='annotation.gtf', work_dir=job.tempDir)

    # Run Hera build
    hera_dir = os.path.join(job.tempDir, args.hera_name)
    os.mkdir(hera_dir)
    docker_parameters = ['--rm', '--log-driver=none', '-v', '{}:/data'.format(job.tempDir),
                         '--entrypoint=/hera/build/hera_build']
    parameters = ['--fasta', '/data/ref.fa', '--gtf', '/data/annotation.gtf', '--outdir', '/data']
    dockerCall(job, tool='jvivian/hera',
               workDir=job.tempDir, parameters=parameters, dockerParameters=docker_parameters)

    # No naming options during creation so fix here
    if args.hera_name != 'hera-index':
        os.rename(os.path.join(job.tempDir, 'hera-index'), hera_dir)

    # Compress
    hera_tar = '{}.tar.gz'.format(args.hera_name)
    tarball_files(hera_tar, file_paths=[hera_dir], output_dir=job.tempDir)

    # Move to output dir
    tar_path = os.path.join(job.tempDir, hera_tar)
    if _move_instead_of_return:
        move_files([tar_path], args.output_dir)
    else:
        return job.fileStore.readGlobalFile(tar_path)


def main():
    """
    Generate inputs / indices for tools used in the toil-rnaseq workflow

    Create inputs for:
        - STAR
        - RSEM
        - Kallisto
        - Hera

    General usage:
        toil-rnaseq-inputs --ref /mnt/hg38_no_alt.fa --gtf /mnt/gencode.v23.gtf --star --rsem --kallisto

    WARNINGS:
        - References with alternative sequences can/will produce incorrect mapping during aligment.
        - Generating the STAR index can take upwards of 30G of memory

    """
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawTextHelpFormatter)

    # Inputs
    parser.add_argument('--ref', type=str, default=None,
                        help='Path to reference fasta needed to generate RSEM and STAR input')
    parser.add_argument('--gtf', type=str, default=None,
                        help='Path to GTF annotation file needed to generate RSEM and STAR input')
    parser.add_argument('--transcriptome', type=str, default=None,
                        help='Path to transcriptome needed to generate Kallisto input. If this file is not provided,\n '
                             'the reference and GTF will be used to create a transcriptome. \nIf only this file is '
                             'provided, just Kallisto will be run.')
    parser.add_argument('--output-dir', type=str, default='.', help='Output directory')

    # Toil wrapper options
    parser.add_argument('--max-cores', type=int, default=12, help='Maximum number of cores to use')
    parser.add_argument('--work-dir', type=str, default='.', help='Directory to put temporary files')
    parser.add_argument('--resume', action='store_true', default=False, help='Restarts workflow')

    # Flags
    parser.add_argument('--star', action='store_true', default=None, help='Create input for STAR')
    parser.add_argument('--rsem', action='store_true', default=None, help='Create input for RSEM')
    parser.add_argument('--kallisto', action='store_true', default=None, help='Create input for Kallisto')
    parser.add_argument('--hera', action='store_true', default=None, help='Create input for Kallisto')

    # Naming
    parser.add_argument('--star-name', type=str, default='starIndex', help='Name for STAR dir and tarball.')
    parser.add_argument('--rsem-name', type=str, default='rsemRef', help='Name for RSEM dir and tarball.')
    parser.add_argument('--kallisto-name', type=str, default='kallistoIndex', help='Name for kallisto index.')
    parser.add_argument('--hera-name', type=str, default='heraIndex', help='Name for hera dir and tarball.')

    # If no arguments provided, print full help menu
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    # Add Toil options
    args = parser.parse_args()

    # Add default toil options to args
    opts = Job.Runner.getDefaultOptions(os.path.join(os.path.abspath(args.work_dir), 'tmp-jobStore'))
    vars(args).update(vars(opts))

    # Update Toil arguments
    args.maxCores = args.max_cores
    args.workDir = args.work_dir
    args.restart = args.resume

    # Sanity Checks
    if not all([args.ref, args.gtf]):
        if not args.transcriptome:
            raise RuntimeError('No reference, gtf, or transcriptome supplied. Nothing to do.')
        else:
            log.info('Only transcriptome provided. Only Kallisto index will be created.')
            args.kallisto = True
            args.star, args.rsem, args.hera = False, False, False

    # Convert file paths to URLs for download_url
    args.gtf = 'file://' + os.path.abspath(args.gtf) if args.gtf else None
    args.ref = 'file://' + os.path.abspath(args.ref) if args.gtf else None
    args.transcriptome = 'file://' + os.path.abspath(args.transcriptome) if args.transcriptome else None

    # Get full path of output directory
    args.output_dir = os.path.abspath(args.output_dir)

    # If no tools selected...
    if not any([args.star, args.rsem, args.kallisto, args.hera]):
        log.info('No tools were selected to create indices for')
        r = raw_input('Type Y/y to create indices for all tools or exit.')
        if r.lower() == 'y':
            args.star, args.rsem, args.kallisto, args.hera = True, True, True, True
        else:
            log.info('Exiting. Rerun with --help to see arguments.')
            sys.exit(0)

    # Start workflow
    Job.Runner.startToil(Job.wrapJobFn(root, args), args)


if __name__ == '__main__':
    main()
