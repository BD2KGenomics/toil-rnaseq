from __future__ import print_function

import os
import textwrap
from urlparse import urlparse

from collections import OrderedDict, defaultdict

from toil_lib import require
from toil_lib.files import copy_files
from toil_lib.urls import s3am_upload

schemes = ('file', 'http', 's3', 'ftp', 'gdc')


def parse_samples(path_to_manifest=None, sample_urls=None):
    """
    Parses samples, specified in either a manifest or listed with --samples

    :param str path_to_manifest: Path to configuration file
    :param list[str] sample_urls: Sample URLs
    :return: Samples and their attributes as defined in the manifest
    :rtype: list[list]
    """
    samples = []
    if sample_urls:
        for url in sample_urls:
            samples.append(['tar', 'paired', os.path.basename(url.split('.')[0]), url])
    elif path_to_manifest:
        with open(path_to_manifest, 'r') as f:
            for line in f.readlines():
                if not line.isspace() and not line.startswith('#'):
                    sample = line.strip().split('\t')
                    require(len(sample) == 4, 'Bad manifest format! '
                                              'Expected 4 tab separated columns, got: {}'.format(sample))
                    file_type, paired, uuid, url = sample
                    require(file_type == 'tar' or file_type == 'fq' or file_type == 'bam',
                            '1st column must be "tar" or "fq": {}'.format(sample[0]))
                    require(paired == 'paired' or paired == 'single',
                            '2nd column must be "paired" or "single": {}'.format(sample[1]))
                    if file_type == 'fq' and paired == 'paired':
                        require(len(url.split(',')) == 2, 'Fastq pair requires two URLs separated'
                                                          ' by a comma: {}'.format(url))
                    samples.append(sample)
    return samples


def generate_config():
    return textwrap.dedent("""
        ##############################################################################################################
        #                               TOIL RNA-SEQ PIPELINE CONFIGURATION FILE                                     #
        ##############################################################################################################

        # This configuration file is formatted in YAML. Simply write the value (at least one space) after the colon.
        # Edit the values in this configuration file and then rerun the pipeline: "toil-rnaseq run"
        # Just Kallisto or STAR/RSEM can be run by supplying only the inputs to those tools
        #
        # URLs can take the form: http://, ftp://, file://, s3://, gnos://
        # Local inputs follow the URL convention: file:///full/path/to/input
        # S3 URLs follow the convention: s3://bucket/directory/file.txt
        #
        # Comments (beginning with #) do not need to be removed. Optional parameters left blank are treated as false.
        
        ##############################################################################################################
        #                                           REQUIRED OPTIONS                                                 #
        ##############################################################################################################

        # Required: Output location of sample. Can be full path to a directory or an s3:// URL
        # WARNING: S3 buckets must exist prior to upload, or it will fail.
        output-dir: 
        
        ##############################################################################################################
        #                           WORKFLOW OPTIONS (Alignment and Quantification)                                  #
        ##############################################################################################################
        
        # URL {scheme} to index tarball used by STAR
        star-index: s3://cgl-pipeline-inputs/rnaseq_cgl/starIndex_hg38_no_alt.tar.gz
        
        # URL {scheme} to reference tarball used by RSEM
        # Running RSEM requires a star-index as a well as an rsem-ref
        rsem-ref: s3://cgl-pipeline-inputs/rnaseq_cgl/rsem_ref_hg38_no_alt.tar.gz

        # URL {scheme} to kallisto index file. 
        kallisto-index: s3://cgl-pipeline-inputs/rnaseq_cgl/kallisto_hg38.idx
        
        ##############################################################################################################
        #                                   WORKFLOW OPTIONS (Quality Control)                                       #
        ##############################################################################################################
        
        # If true, will preprocess samples with cutadapt using adapter sequences.
        cutadapt: true
        
        # Adapter sequence to trim when running CutAdapt. Defaults set for Illumina
        fwd-3pr-adapter: AGATCGGAAGAG

        # Adapter sequence to trim (for reverse strand) when running CutAdapt. Defaults set for Illumina
        rev-3pr-adapter: AGATCGGAAGAG

        # If true, will run FastQC and include QC in sample output
        fastqc: true

        # Optional: If true, will run BAM QC (as specified by California Kid's Cancer Comparison)
        bamqc: 
        
        ##############################################################################################################
        #                   CREDENTIAL OPTIONS (for downloading samples from secure locations)                       #
        ##############################################################################################################        

        # Optional: Provide a full path to a 32-byte key used for SSE-C Encryption in Amazon
        ssec: 

        # Optional: Provide a full path to the token.txt used to download from the GDC
        gdc-token: 
        
        ##############################################################################################################
        #                                   ADDITIONAL FILE OUTPUT OPTIONS                                           #
        ##############################################################################################################        

        # Optional: If true, saves the wiggle file (.bg extension) output by STAR
        # WARNING: Requires STAR sorting, which has memory leak issues that can crash the pipeline. 
        wiggle: 

        # Optional: If true, saves the aligned BAM (by coordinate) produced by STAR
        # You must also specify an ssec key if you want to upload to the s3-output-dir
        # as read data is assumed to be controlled access
        save-bam: 
        
        ##############################################################################################################
        #                                           DEVELOPER OPTIONS                                                #
        ##############################################################################################################        

        # Optional: If true, uses resource requirements appropriate for continuous integration
        ci-test: 
    """.format(scheme=[x + '://' for x in schemes])[1:])


def user_input_config(config_path):
    """
    User input of pipeline configuration file

    :param str config_path: Path to configuration file
    :return: Configuration file path or None if user skips
    :rtype: str
    """
    print('\n\t\t\tUser Input of Toil-rnaseq Configuration File\n')
    start = raw_input('Type Y/y and hit enter to continue: ').lower()
    if start != 'y':
        return None
    print('User will see comments for a configuation option followed by "<OPTION>: [Default Value]"')
    print('\tN/n to skip\n\ttrue/false for boolean statements\n\tq/quit to stop\n\tEnter key to submit option\n')

    config = OrderedDict()
    comments = defaultdict(list)
    quit_flag = False
    config_template = generate_config().split('\n')
    for line in config_template:
        if not line.startswith('#') and line:
            option, default = line.split(': ')

            # Fetch comments for current option
            index = config_template.index(line) - 1
            while True:
                comments[option].insert(0, config_template[index])
                index -= 1
                if not config_template[index]:
                    break
            if quit_flag:
                config[option] = default
                continue
            print('\n'.join(comments[option]) + '\n\n')

            # Show option and get user input
            user_input = None
            while not user_input:
                user_input = raw_input('\n{}: [{}]\n\tUser Input: '.format(option, default)).lower()
            if user_input == 'q' or user_input == 'quit':
                quit_flag = True
                config[option] = default
                continue
            elif user_input == 'n':
                config[option] = default
                continue
            else:
                config[option] = user_input

    print('Writing out configuration file to: {}'.format(config_path))
    with open(config_path, 'w') as f:
        for option in config:
            f.write('\n'.join(comments[option]))
            f.write('\n{}: {}\n\n'.format(option, config[option]))
    return config_path


def generate_manifest():
    return textwrap.dedent("""
        #   Edit this manifest to include information pertaining to each sample to be run.
        #   There are 4 tab-separated columns: filetype, paired/unpaired, UUID, URL(s) to sample
        #
        #   filetype    Filetype of the sample. Options: "tar", "fq", or "bam" for tarball, fastq/fastq.gz, or BAM
        #   paired      Indicates whether the data is paired or single-ended. Options:  "paired" or "single"
        #   UUID        This should be a unique identifier for the sample to be processed
        #   URL         A URL starting with {scheme} that points to the sample
        #
        #   If sample is being submitted as a fastq or several fastqs, provide URLs separated by a comma.
        #   If providing paired fastqs, alternate the fastqs so every R1 is paired with its R2 as the next URL.
        #   Samples must have the same extension - do not mix and match gzip and non-gzipped sample pairs.
        #
        #   Samples consisting of tarballs with fastq files inside must follow the file name convention of
        #   ending in an R1/R2 or _1/_2 followed by one of the 4 extensions: .fastq.gz, .fastq, .fq.gz, .fq
        #
        #   BAMs are accepted, but must have been aligned from paired reads NOT single-end reads.
        #
        #   GDC URLs may only point to individual BAM files. No other format is accepted.
        #
        #   Examples of several combinations are provided below. Lines beginning with # are ignored.
        #
        #   tar paired  UUID_1  file:///path/to/sample.tar
        #   fq  paired  UUID_2  file:///path/to/R1.fq.gz,file:///path/to/R2.fq.gz
        #   tar single  UUID_3  http://sample-depot.com/single-end-sample.tar
        #   tar paired  UUID_4  s3://my-bucket-name/directory/paired-sample.tar.gz
        #   fq  single  UUID_5  s3://my-bucket-name/directory/single-end-file.fq
        #   bam paired  UUID_6  gdc://1a5f5e03-4219-4704-8aaf-f132f23f26c7
        #
        #   Place your samples below, one per line.
        """.format(scheme=[x + '://' for x in schemes])[1:])


def user_input_manifest(manifest_path):
    """
    User input of pipeline manifest file

    :param str manifest_path: Path to write out manifest
    :return: Path to manifest or None if user skips
    :rtype: str
    """
    print('\n\t\t\tUser Input of Toil-rnaseq Manifest')
    start = raw_input('Type Y/y and hit enter to continue: ').lower()
    if start != 'y':
        return None
    print('\n'.join(generate_manifest().split('\n')[:-1]))  # Don't print last line of manifest
    print('\n\nFollow the prompts to enter sample information, based on the information above.\n')

    samples = []
    while True:
        filetype, paired, uuid = None, None, None
        url = 'bad-url'
        while filetype not in ['tar', 'fq', 'bam']:
            filetype = raw_input('Enter the filetype of the sample: ')
        while paired not in ['paired', 'single']:
            paired = raw_input('Enter whether sample is paired or single-end: ')
        uuid = raw_input('Enter unique name (or UUID) of sample: ')
        while urlparse(url).scheme not in schemes:
            url = raw_input('Enter URL for sample: ')
        samples.append((filetype, paired, uuid, url))

        # Escape loop
        user = raw_input('\nType q/quit to exit or enter any other key to add more samples\n').lower()
        if user == 'q' or user == 'quit':
            break

    print('Writing out manifest file to: {}'.format(manifest_path))
    with open(manifest_path, 'w') as f:
        f.write(generate_manifest() + '\n')
        for sample in samples:
            assert len(sample) == 4, 'Something is wrong. Expected 4 options per sample: {}'.format(sample)
            f.write('\t'.join(sample) + '\n')
    return manifest_path


def generate_file(file_path, generate_func):
    """
    Checks file existance, generates file, and provides message

    :param str file_path: File location to generate file
    :param func generate_func: Function used to generate file
    :return: Path to file
    :rtype: str
    """
    if os.path.exists(file_path):
        print('File "{}" already exists! Doing nothing.'.format(file_path))
    else:
        with open(file_path, 'w') as f:
            f.write(generate_func())
        print('\t{} has been generated in the current working directory.'.format(os.path.basename(file_path)))
    return file_path


def move_or_upload(config, files):
    if urlparse(config.output_dir).scheme == 's3' and config.ssec:
        for f in files:
            s3am_upload(fpath=f, s3_dir=config.output_dir, s3_key_path=config.ssec)
    elif urlparse(config.output_dir).scheme != 's3':
        copy_files(file_paths=files, output_dir=config.output_dir)


def docker_path(path):
    """
    Converts a path to a file to a "docker path" which replaces the dirname with '/data'

    :param str path: Path to file
    :return: Path for use in Docker parameters
    :rtype: str
    """
    return os.path.join('/data', os.path.basename(path))
