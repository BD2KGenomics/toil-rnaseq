#!/usr/bin/env python2.7
from __future__ import print_function

# Standard imports
import argparse
import multiprocessing
import os
import sys

# Non-standard imports
import yaml
from toil.common import Toil
from toil.job import Job, PromisedRequirement

# Local imports
from tools.aligners import run_star
from tools.bams import sort_and_save_bam
from tools.jobs import cleanup_ids
from tools.jobs import consolidate_output
from tools.jobs import map_job
from tools.jobs import save_wiggle
from tools.preprocessing import download_and_process_bam
from tools.preprocessing import download_and_process_fastqs
from tools.preprocessing import download_and_process_tar
from tools.qc import run_bamqc
from tools.qc import run_fastqc
from tools.quantifiers import run_hera
from tools.quantifiers import run_kallisto
from tools.quantifiers import run_rsem
from tools.quantifiers import run_rsem_gene_mapping
from utils import UserError, rexpando
from utils import configuration_sanity_checks
from utils import generate_config
from utils import generate_manifest
from utils import parse_samples
from utils import require
from utils import user_input_config
from utils import user_input_manifest
from utils.files import generate_file
from utils.filesize import human2bytes


def workflow(job, sample, config):
    """
    Creates workflow graph for each sample based on configuration options

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param list(str, str, str, str) sample: Sample information - filetype, paired/unpaired, UUID, and URL
    :param Expando config: Dict-like object containing workflow options as attributes
    """
    # Create copy of config to store sample-specific information
    config = config.copy()
    config.file_type, config.paired, config.uuid, config.url = sample
    config.paired = True if config.paired == 'paired' else False
    config.cores = min(config.maxCores, multiprocessing.cpu_count())

    # Download and process input based on file type
    # `inputs` will return the FileStoreID(s) of the R1 / R2 fastq
    if config.file_type == 'bam':
        disk = '2G' if config.ci_test else config.max_sample_size
        disk = human2bytes(disk) * 5
        inputs = job.wrapJobFn(download_and_process_bam, config, disk=disk).encapsulate()

    elif config.file_type == 'tar':
        inputs = job.wrapJobFn(download_and_process_tar, config).encapsulate()

    else:
        config.gz = True if config.url.split(',')[0].endswith('gz') else None  # Check if fastqs are gzipped
        inputs = job.wrapJobFn(download_and_process_fastqs, config).encapsulate()

    # Add inputs as first child to root job
    job.addChild(inputs)

    # Define preliminary disk and create dictionary for storing output
    disk = PromisedRequirement(lambda xs: sum(x.size for x in xs if x) + human2bytes('2G'), inputs.rv())
    cores = min(16, config.cores) if config.cores >= 32 else config.cores  # Core optimization
    output = {}

    # DAG wiring for remainder of workflow
    # FASTQC
    if config.fastqc:
        fastqc = job.wrapJobFn(run_fastqc, r1_id=inputs.rv(0), r2_id=inputs.rv(1), cores=2, disk=disk)
        inputs.addChild(fastqc)
        output['QC/fastQC'] = fastqc.rv()

    # Kallisto
    if config.kallisto_index:
        kallisto = job.wrapJobFn(run_kallisto, r1_id=inputs.rv(0), r2_id=inputs.rv(1),
                                 kallisto_index_url=config.kallisto_index, cores=cores, disk=disk)
        inputs.addChild(kallisto)
        output['Kallisto'] = kallisto.rv()

    # Hera
    if config.hera_index:
        hera = job.wrapJobFn(run_hera, r1_id=inputs.rv(0), r2_id=inputs.rv(1),
                             hera_index_url=config.hera_index, cores=config.cores, disk=disk)
        inputs.addChild(hera)
        output['Hera'] = hera.rv()

    # STAR and RSEM
    if config.star_index and config.rsem_ref:
        if config.ci_test:
            disk = '2G'
            mem = '2G'
        else:
            disk = PromisedRequirement(lambda xs: sum(x.size for x in xs if x) + human2bytes('50G'), inputs.rv())
            mem = '40G'

        # STAR returns: transcriptome_id, star_id, aligned_id, wiggle_id
        sort = True if config.wiggle else False
        save_bam = any([config.save_bam, config.bamqc])
        star = job.wrapJobFn(run_star, inputs.rv(0), inputs.rv(1), star_index_url=config.star_index,
                             wiggle=config.wiggle, sort=sort, save_aligned_bam=save_bam,
                             cores=config.cores, memory=mem, disk=disk)
        inputs.addChild(star)
        output['QC/STAR'] = star.rv(1)

        # BamQC
        if config.bamqc:
            cores = min(4, config.cores)
            disk = PromisedRequirement(lambda x: x.size, star.rv(2))
            bamqc = job.wrapJobFn(run_bamqc, aligned_bam_id=star.rv(2), config=config,
                                  save_bam=config.save_bam, disk=disk, cores=cores)
            star.addChild(bamqc)
            output['QC/BamQC'] = bamqc.rv()

        # Handle optional files user can save
        # Note: if bamqc is enabled, the bam is saved within the `run_bamqc` job
        if config.save_bam and not config.bamqc:
            disk = PromisedRequirement(lambda x: x.size, star.rv(2))
            star.addChildJobFn(sort_and_save_bam, config, bam_id=star.rv(2), skip_sort=sort, disk=disk)
        if config.wiggle:
            disk = PromisedRequirement(lambda x: x.size, star.rv(3))
            star.addChildJobFn(save_wiggle, config, wiggle_id=star.rv(3), disk=disk)

        # RSEM returns: gene_id, isoform_id
        rsem = job.wrapJobFn(run_rsem, bam_id=star.rv(0), rsem_ref_url=config.rsem_ref, paired=config.paired,
                             cores=cores, disk=PromisedRequirement(lambda x: x.size + human2bytes('20G'), star.rv(0)))
        star.addChild(rsem)

        # RSEM postprocess returns: rsem_id, rsem_hugo_id
        rsem_postprocess = job.wrapJobFn(run_rsem_gene_mapping, rsem_gene_id=rsem.rv(0), rsem_isoform_id=rsem.rv(1))
        rsem.addChild(rsem_postprocess)
        output['RSEM'] = rsem_postprocess.rv(0)
        output['RSEM/Hugo'] = rsem_postprocess.rv(1)

        # Cleanup
        star.addFollowOnJobFn(cleanup_ids, ids_to_delete=[star.rv(2), star.rv(3)])
        rsem.addChildJobFn(cleanup_ids, ids_to_delete=[star.rv(0)])
        rsem_postprocess.addChildJobFn(cleanup_ids, ids_to_delete=[rsem.rv(0), rsem.rv(1)])

    # Cleanup and Consolidate
    job.addFollowOnJobFn(cleanup_ids, [inputs.rv(0), inputs.rv(1)])
    job.addFollowOnJobFn(consolidate_output, config, output)


def main():
    """
                        Toil RNA-seq Workflow
    Computational Genomics Lab, Genomics Institute, UC Santa Cruz


    RNA-seq samples are trimmed, QCed, combined, aligned, and quantified:
        - CutAdapt
        - FastQC
        - STAR -> RSEM
        - Kalisto
        - Hera


    Quickstart:
    1. Type `toil-rnaseq generate` to create an editable manifest and config.
    2. Parameterize the workflow by editing the config.
    3. Fill in the manifest with information pertaining to your samples.
    4. Type `toil-rnaseq run ./jobStore` to execute the workflow locally.

    Please read the README before use and check the github wiki for additional details:
    https://github.com/BD2KGenomics/toil-scripts/tree/master/src/toil_scripts/rnaseq_cgl
    """
    # Process command line arguments
    args = cli()

    # Parse subparsers related to config and manifest
    config_path = os.path.join(os.getcwd(), 'config-toil-rnaseq.yaml')
    manifest_path = os.path.join(os.getcwd(), 'manifest-toil-rnaseq.tsv')
    if args.command == 'generate':
        generate_file(config_path, generate_config)
        generate_file(manifest_path, generate_manifest)
    elif args.command == 'config-input':
        user_input_config(config_path)
    elif args.command == 'manifest-input':
        user_input_manifest(manifest_path)

    # Workflow execution
    elif args.command == 'run':

        # Parse manifest
        require(os.path.exists(args.manifest), '{} not found. Run "toil-rnaseq generate"'.format(args.manifest))
        samples = parse_samples(args.manifest)

        # Parse config and store as Expando object for dot attribute accession
        require(os.path.exists(args.config), '{} not found. Run "toil-rnaseq generate"'.format(args.config))
        config = rexpando(yaml.load(open(args.config).read()))
        config.maxCores = int(args.maxCores) if args.maxCores else sys.maxint  # Set maxCores from args

        # Sanity check configuration file
        config = configuration_sanity_checks(config)

        # Start the workflow, calling map_job() to run the workflow for each sample
        with Toil(args) as toil:
            if args.restart:
                toil.restart()
            else:
                toil.start(Job.wrapJobFn(map_job, workflow, samples, config))


def cli():
    """
    Command line interface for the toil-rnaseq workflow

    :returns: Command line arguments
    :rtype: Namespace
    """
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawTextHelpFormatter)
    subparsers = parser.add_subparsers(dest='command')

    # Input subparsers
    subparsers.add_parser('generate', help='Generates a config and manifest in the current working directory.')
    subparsers.add_parser('config-input', help='Allows user to configure workflow by following prompts.')
    subparsers.add_parser('manifest-input', help='Allows user to input samples to the manifest by following prompts.')

    # Run subparser
    parser_run = subparsers.add_parser('run', help='Runs the Toil RNA-seq workflow')
    group = parser_run.add_mutually_exclusive_group()

    # Run arguments
    cwd = os.getcwd()
    config_path = os.path.join(cwd, 'config-toil-rnaseq.yaml')
    parser_run.add_argument('--config', default=config_path, type=str,
                            help='Path to (filled in) config file, created with "generate" or "config-input". '
                                 '\nDefault value: "%(default)s"')

    manifest_path = os.path.join(cwd, 'manifest-toil-rnaseq.tsv')
    group.add_argument('--manifest', default=manifest_path, type=str,
                       help='Path to (filled in) manifest file, created with "generate" or "manifest-input". '
                            '\nDefault value: "%(default)s"')

    # If no arguments provided, print full help menu
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    # Add Toil options
    Job.Runner.addToilOptions(parser_run)
    return parser.parse_args()


if __name__ == '__main__':
    try:
        main()
    except UserError as e:
        print(e.message, file=sys.stderr)
        sys.exit(1)
