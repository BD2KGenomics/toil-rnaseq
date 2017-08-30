import os
import textwrap
from urlparse import urlparse

from toil_lib import require
from toil_lib.files import copy_files
from toil_lib.urls import s3am_upload

schemes = ('http', 'file', 's3', 'ftp', 'gnos')


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
                    require(file_type == 'tar' or file_type == 'fq',
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
        # RNA-seq CGL Pipeline configuration file
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
        # Required: URL {scheme} to index tarball used by STAR
        star-index: s3://cgl-pipeline-inputs/rnaseq_cgl/starIndex_hg38_no_alt.tar.gz

        # Required: URL {scheme} to kallisto index file.
        kallisto-index: s3://cgl-pipeline-inputs/rnaseq_cgl/kallisto_hg38.idx

        # Required: URL {scheme} to reference tarball used by RSEM
        rsem-ref: s3://cgl-pipeline-inputs/rnaseq_cgl/rsem_ref_hg38_no_alt.tar.gz

        # Required: Output location of sample. Can be full path to a directory or an s3:// URL
        # Warning: S3 buckets must exist prior to upload or it will fail.
        output-dir:

        # Optional: If true, will preprocess samples with cutadapt using adapter sequences.
        cutadapt: true

        # Optional: If true, will run FastQC and include QC in sample output
        fastqc: true

        # Optional: If true, will run BAM QC (as specified by California Kid's Cancer Comparison)
        bamqc:

        # Adapter sequence to trim. Defaults set for Illumina
        fwd-3pr-adapter: AGATCGGAAGAG

        # Adapter sequence to trim (for reverse strand). Defaults set for Illumina
        rev-3pr-adapter: AGATCGGAAGAG

        # Optional: Provide a full path to a 32-byte key used for SSE-C Encryption in Amazon
        ssec:

        # Optional: Provide a full path to a CGHub Key used to access GNOS hosted data
        gtkey:

        # Optional: If true, saves the wiggle file (.bg extension) output by STAR
        # WARNING: Requires STAR sorting, which has memory leak issues that can crash the pipeline. 
        wiggle:

        # Optional: If true, saves the aligned bam (by coordinate) produced by STAR
        # You must also specify an ssec key if you want to upload to the s3-output-dir
        save-bam:

        # Optional: If true, uses resource requirements appropriate for continuous integration
        ci-test:
    """.format(scheme=[x + '://' for x in schemes])[1:])


def generate_manifest():
    return textwrap.dedent("""
        #   Edit this manifest to include information pertaining to each sample to be run.
        #   There are 4 tab-separated columns: filetype, paired/unpaired, UUID, URL(s) to sample
        #
        #   filetype    Filetype of the sample. Options: "tar" or "fq", for tarball/tarfile or fastq/fastq.gz
        #   paired      Indicates whether the data is paired or single-ended. Options:  "paired" or "single"
        #   UUID        This should be a unique identifier for the sample to be processed
        #   URL         A URL {scheme} pointing to the sample
        #
        #   If sample is being submitted as a fastq or several fastqs, provide URLs separated by a comma.
        #   If providing paired fastqs, alternate the fastqs so every R1 is paired with its R2 as the next URL.
        #   Samples must have the same extension - do not mix and match gzip and non-gzipped sample pairs.
        #
        #   Samples consisting of tarballs with fastq files inside must follow the file name convention of
        #   ending in an R1/R2 or _1/_2 followed by one of the 4 extensions: .fastq.gz, .fastq, .fq.gz, .fq
        #
        #   Examples of several combinations are provided below. Lines beginning with # are ignored.
        #
        #   tar paired  UUID_1  file:///path/to/sample.tar
        #   fq  paired  UUID_2  file:///path/to/R1.fq.gz,file:///path/to/R2.fq.gz
        #   tar single  UUID_3  http://sample-depot.com/single-end-sample.tar
        #   tar paired  UUID_4  s3://my-bucket-name/directory/paired-sample.tar.gz
        #   fq  single  UUID_5  s3://my-bucket-name/directory/single-end-file.fq
        #
        #   Place your samples below, one per line.
        """.format(scheme=[x + '://' for x in schemes])[1:])


def generate_file(file_path, generate_func):
    """
    Checks file existance, generates file, and provides message

    :param str file_path: File location to generate file
    :param function generate_func: Function used to generate file
    """
    require(not os.path.exists(file_path), file_path + ' already exists!')
    with open(file_path, 'w') as f:
        f.write(generate_func())
    print('\t{} has been generated in the current working directory.'.format(os.path.basename(file_path)))


def move_or_upload(config, files):
    if urlparse(config.output_dir).scheme == 's3' and config.ssec:
        for f in files:
            s3am_upload(fpath=f, s3_dir=config.output_dir, s3_key_path=config.ssec)
    elif urlparse(config.output_dir).scheme != 's3':
        copy_files(file_paths=files, output_dir=config.output_dir)


def cleanup_ids(job, ids_to_delete):
    """
    Delete fileStoreIDs for files no longer needed

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param list ids_to_delete: list of FileStoreIDs to delete
    """
    [job.fileStore.deleteGlobalFile(x) for x in ids_to_delete if x is not None]
