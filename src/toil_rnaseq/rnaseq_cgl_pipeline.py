#!/usr/bin/env python2.7
from __future__ import print_function

import argparse
import multiprocessing
import os
import re
import subprocess
import sys
import tarfile
import textwrap
from contextlib import closing
from subprocess import PIPE
from urlparse import urlparse

import yaml
from bd2k.util.files import mkdir_p
from bd2k.util.processes import which
from toil.common import Toil
from toil.job import Job
from toil.lib.docker import dockerCall
from toil_lib import flatten
from toil_lib import require, UserError
from toil_lib.files import copy_files, tarball_files
from toil_lib.jobs import map_job
from toil_lib.tools.QC import run_fastqc
from toil_lib.tools.aligners import run_star
from toil_lib.tools.preprocessing import run_cutadapt
from toil_lib.tools.quantifiers import run_kallisto, run_rsem, run_rsem_postprocess
from toil_lib.urls import download_url_job, s3am_upload

from qc import run_bam_qc

schemes = ('http', 'file', 's3', 'ftp', 'gnos')


# Start of pipeline
def download_sample(job, sample, config):
    """
    Download sample and store unique attributes

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param list(str, str, str, str) sample: Sample information: filetype, paired/unpaired, UUID, and URL
    :param Namespace config: Argparse Namespace object containing argument inputs
    """
    # Create copy of config that is sample specific
    config = argparse.Namespace(**vars(config))
    config.file_type, config.paired, config.uuid, config.url = sample
    config.paired = True if config.paired == 'paired' else False
    config.cores = min(config.maxCores, multiprocessing.cpu_count())
    disk = '2G' if config.ci_test else '20G'
    job.fileStore.logToMaster('UUID: {}\nURL: {}\nPaired: {}\nFile Type: {}\nCores: {}\nCIMode: {}'.format(
        config.uuid, config.url, config.paired, config.file_type, config.cores, config.ci_test))
    # Download or locate local file and place in the jobStore
    tar_id = None
    fastq_ids = []
    if config.file_type == 'tar':
        tar_id = job.addChildJobFn(download_url_job, config.url, cghub_key_path=config.gtkey,
                                   s3_key_path=config.ssec, disk=disk).rv()
    else:
        urls = config.url.split(',')
        if config.paired:
            require(len(urls) % 2 == 0, 'Fastq pairs must have multiples of 2 URLS separated by comma')
        config.gz = True if urls[0].endswith('gz') else None
        for url in urls:
            fastq_ids.append(job.addChildJobFn(download_url_job, url, cghub_key_path=config.gtkey,
                                               s3_key_path=config.ssec, disk=disk).rv())
    job.addFollowOnJobFn(preprocessing_declaration, config, tar_id, fastq_ids)


def preprocessing_declaration(job, config, tar_id=None, fastq_ids=None):
    """
    Define preprocessing steps

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Namespace config: Argparse Namespace object containing argument inputs
    :param FileID tar_id: FileStoreID of sample tar (or None)
    :param list[FileID] fastq_ids: FileStoreIDs of fastq files
    """
    if tar_id:
        job.fileStore.logToMaster('Processing sample tar and queueing CutAdapt for: ' + config.uuid)
        disk = 5 * tar_id.size
        preprocessing_output = job.addChildJobFn(process_sample, config, input_tar=tar_id, disk=disk).rv()
    else:
        disk = 3 * sum([x.size for x in fastq_ids])
        preprocessing_output = job.addChildJobFn(process_sample, config, fastq_ids=fastq_ids, disk=disk).rv()
    job.addFollowOnJobFn(pipeline_declaration, config, preprocessing_output)


def pipeline_declaration(job, config, preprocessing_output):
    """
    Define pipeline edges that use the fastq files

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Namespace config: Argparse Namespace object containing argument inputs
    :param tuple(FileID, FileID, bool) preprocessing_output: R1 FileStoreID, R2 FileStoreID, Improper Pairing Flag
    """
    r1_id, r2_id = preprocessing_output
    kallisto_output, rsem_star_output, fastqc_output = None, None, None
    disk = 2 * (r1_id.size + r2_id.size) if r2_id else 2 * r1_id.size
    # Define jobs based on user configuration
    if config.fastqc:
        job.fileStore.logToMaster('Queueing FastQC job for: ' + config.uuid)
        fastqc_output = job.addChildJobFn(run_fastqc, r1_id, r2_id, cores=2, disk=disk).rv()
    if config.kallisto_index:
        job.fileStore.logToMaster('Queueing Kallisto job for: ' + config.uuid)
        kallisto_output = job.addChildJobFn(run_kallisto, r1_id, r2_id, config.kallisto_index,
                                            cores=config.cores, disk=disk).rv()
    if config.star_index and config.rsem_ref:
        job.fileStore.logToMaster('Queueing STAR alignment for: ' + config.uuid)
        rsem_star_output = job.addChildJobFn(star_alignment, config, r1_id, r2_id).rv()
    job.addFollowOnJobFn(consolidate_output, config, kallisto_output, rsem_star_output, fastqc_output)
    job.addFollowOnJobFn(cleanup_ids, [r1_id, r2_id])


def star_alignment(job, config, r1_id, r2_id=None):
    """
    Logic for running STAR

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Namespace config: Argparse Namespace object containing argument inputs
    :param FileID r1_id: FileStoreID of sample read 1
    :param FileId r2_id: FileStoreID of sample read 2 (or None)
    :return: FileStoreID results from RSEM
    :rtype: FileID|tuple(FileID, FileID)
    """
    job.fileStore.logToMaster('Queueing RSEM job for: ' + config.uuid)
    # Define memory and disk requirements
    mem = '2G' if config.ci_test else '40G'
    if config.ci_test:
        disk = '2G'
    else:
        disk = r1_id.size + r2_id.size + 80530636800 if r2_id else r1_id.size + 80530636800  # 75G for STAR index / tmp
    # Define job functions for STAR and RSEM
    sort = True if config.wiggle else False
    star = job.addChildJobFn(run_star, r1_id, r2_id, star_index_url=config.star_index,
                             wiggle=config.wiggle, sort=sort, cores=config.cores, memory=mem, disk=disk).rv()
    rsem = job.addFollowOnJobFn(rsem_quantification, config, star, disk=disk).rv()
    if config.bamqc:
        return rsem, job.addFollowOnJobFn(bam_qc, config, star, disk=disk).rv()
    else:
        return rsem


def bam_qc(job, config, star_output):
    """
    Unpack STAR bam and run BAM QC

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Namespace config: Argparse Namespace object containing argument inputs
    :param tuple(FileID, FileID, FileID, FileID)|tuple(FileID, FileID, FileID) star_output: FileStoreIDs from STAR
    :return: FileStoreID results from bam_qc
    :rtype: FileID
    """
    cores = min(4, config.cores)
    if config.wiggle:
        transcriptome_id, aligned_id, wiggle_id, log_id, sj_id = star_output
    else:
        transcriptome_id, aligned_id, log_id, sj_id = star_output
    disk = 5 * aligned_id.size
    return job.addChildJobFn(run_bam_qc, aligned_id, config, cores=cores, disk=disk).rv()


def rsem_quantification(job, config, star_output):
    """
    Unpack STAR results and run RSEM, saving wiggle/bam if specified

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Namespace config: Argparse Namespace object containing argument inputs
    :param tuple(FileID, FileID, FileID, FileID)|tuple(FileID, FileID, FileID) star_output: FileStoreIDs from STAR
    :return: FileStoreID results from RSEM postprocess and STAR log
    :rtype: tuple(FileID, FileID, FileID)
    """
    work_dir = job.fileStore.getLocalTempDir()
    cores = min(16, config.cores)
    if config.wiggle:
        transcriptome_id, aligned_id, wiggle_id, log_id, sj_id = flatten(star_output)
        wiggle_path = os.path.join(work_dir, config.uuid + '.wiggle.bg')
        job.fileStore.readGlobalFile(wiggle_id, wiggle_path)
        if urlparse(config.output_dir).scheme == 's3':
            s3am_upload(fpath=wiggle_path, s3_dir=config.output_dir, s3_key_path=config.ssec)
        else:
            copy_files(file_paths=[wiggle_path], output_dir=config.output_dir)
        job.fileStore.deleteGlobalFile(wiggle_id)
    else:
        transcriptome_id, aligned_id, log_id, sj_id = flatten(star_output)
    # Save sorted bam if flag is selected
    if config.save_bam and not config.bamqc:  # if config.bamqc is selected, bam is being saved in run_bam_qc
        if config.wiggle:   # Wiggle requires STAR sorting, so bam is already sorted
            bam_path = job.fileStore.readGlobalFile(aligned_id, os.path.join(work_dir, config.uuid + '.sorted.bam'))
            move_or_upload(config, files=[bam_path])
        else:
            disk = 3 * aligned_id.size
            job.addChildJobFn(sort_and_save_bam, config, aligned_id, cores=config.cores, disk=disk)
    # Declare RSEM and RSEM post-process jobs
    disk = 5 * transcriptome_id.size
    rsem_output = job.wrapJobFn(run_rsem, transcriptome_id, config.rsem_ref, paired=config.paired,
                                cores=cores, disk=disk)
    rsem_postprocess = job.wrapJobFn(run_rsem_postprocess, rsem_output.rv(0), rsem_output.rv(1))
    job.addChild(rsem_output)
    rsem_output.addChild(rsem_postprocess)
    # Save STAR log and splice junction file
    log_path = os.path.join(work_dir, 'Log.final.out')
    job.fileStore.readGlobalFile(log_id, log_path)
    sj_path = os.path.join(work_dir, 'SJ.out.tab')
    job.fileStore.readGlobalFile(sj_id, sj_path)
    tarball_files(tar_name='star.tar.gz', file_paths=[log_path, sj_path], output_dir=work_dir)
    star_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'star.tar.gz'))

    # Delete intermediates
    ids_to_delete = [transcriptome_id, log_id, sj_id]
    if not config.bamqc:  # If BamQC isn't being run, the sorted bam is no longer needed
        ids_to_delete.append(aligned_id)
    job.addFollowOnJobFn(cleanup_ids, ids_to_delete)

    return rsem_postprocess.rv(), star_id


def process_sample(job, config, input_tar=None, fastq_ids=None):
    """
    Converts sample.tar(.gz) into a fastq pair (or single fastq if single-ended.)

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Namespace config: Argparse Namespace object containing argument inputs
    :param FileID input_tar: fileStoreID of the tarball (if applicable)
    :param list[FileID] fastq_ids: FileStoreIDs of fastq files
    :return: FileStoreID from Cutadapt or from fastqs directly if pipeline was run without Cutadapt option
    :rtype: tuple(FileID, FileID)
    """
    job.fileStore.logToMaster('Processing sample: {}'.format(config.uuid))
    work_dir = job.fileStore.getLocalTempDir()
    delete_fastqs = True
    processed_r1, processed_r2 = None, None
    # I/O
    if input_tar:
        job.fileStore.readGlobalFile(input_tar, os.path.join(work_dir, 'sample.tar'), mutable=True)
        tar_path = os.path.join(work_dir, 'sample.tar')
        # Untar sample
        subprocess.check_call(['tar', '-xvf', tar_path, '-C', work_dir], stderr=PIPE, stdout=PIPE)
        os.remove(tar_path)
    else:
        ext = '.fq.gz' if config.gz else '.fq'
        for i, fastq_id in enumerate(fastq_ids):
            if i % 2 == 0:
                job.fileStore.readGlobalFile(fastq_id, os.path.join(work_dir, 'Fastq_{}_R1{}'.format(i, ext)))
            else:
                job.fileStore.readGlobalFile(fastq_id, os.path.join(work_dir, 'Fastq_{}_R2{}'.format(i, ext)))
    fastqs = []
    for root, subdir, files in os.walk(work_dir):
        fastqs.extend([os.path.join(root, x) for x in files])
    if config.paired:
        r1, r2 = [], []
        # Pattern convention: Look for "R1" / "R2" in the filename, or "_1" / "_2" before the extension
        pattern = re.compile('(?:^|[._-])(R[12]|[12]\.f)')
        for fastq in sorted(fastqs):
            match = pattern.search(os.path.basename(fastq))
            if not match:
                raise UserError('FASTQ file name fails to meet required convention for paired reads '
                                '(see documentation). ' + fastq)
            elif '1' in match.group():
                r1.append(fastq)
            elif '2' in match.group():
                r2.append(fastq)
            else:
                assert False, match.group()
        require(len(r1) == len(r2), 'Check fastq names, uneven number of pairs found.\nr1: {}\nr2: {}'.format(r1, r2))
        # Concatenate fastqs
        command = 'zcat' if r1[0].endswith('.gz') and r2[0].endswith('.gz') else 'cat'

        # If sample is already a single R1 / R2 fastq
        if command == 'cat' and len(fastqs) == 2:
            processed_r1 = fastq_ids[0]
            processed_r2 = fastq_ids[1]
            delete_fastqs = False
        else:
            with open(os.path.join(work_dir, 'R1.fastq'), 'w') as f1:
                p1 = subprocess.Popen([command] + r1, stdout=f1)
            with open(os.path.join(work_dir, 'R2.fastq'), 'w') as f2:
                p2 = subprocess.Popen([command] + r2, stdout=f2)
            p1.wait()
            p2.wait()
            processed_r1 = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'R1.fastq'))
            processed_r2 = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'R2.fastq'))
        disk = 2 * (processed_r1.size + processed_r2.size)
    else:
        command = 'zcat' if fastqs[0].endswith('.gz') else 'cat'
        if command == 'cat' and len(fastqs) == 1:
            processed_r1 = fastq_ids[0]
            delete_fastqs = False
        else:
            with open(os.path.join(work_dir, 'R1.fastq'), 'w') as f:
                subprocess.check_call([command] + fastqs, stdout=f)
            processed_r1 = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'R1.fastq'))
        disk = 2 * processed_r1.size

    # Cleanup Intermediates
    ids_to_delete = [input_tar] + fastq_ids if delete_fastqs and fastq_ids else [input_tar]
    job.addFollowOnJobFn(cleanup_ids, ids_to_delete)

    # Start cutadapt step
    if config.cutadapt:
        return job.addChildJobFn(run_cutadapt, processed_r1, processed_r2, config.fwd_3pr_adapter,
                                 config.rev_3pr_adapter, disk=disk).rv()
    else:
        return processed_r1, processed_r2


def consolidate_output(job, config, kallisto_output, rsem_star_output, fastqc_output):
    """
    Combines the contents of the outputs into one tarball and places in output directory or s3

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Namespace config: Argparse Namespace object containing argument inputs
    :param FileID kallisto_output: FileStoreID for Kallisto output
    :param tuple(FileID, FileID, FileID)|tuple(FileID, FileID, FileID, bool, FileID) rsem_star_output:
            FileStoreIDs for RSEM and STAR output, and a flag/FileID if run with bamQC
    :param FileID fastqc_output: FileStoreID for FastQC output
    """
    job.fileStore.logToMaster('Consolidating output: {}'.format(config.uuid))
    work_dir = job.fileStore.getLocalTempDir()
    # Retrieve output file paths to consolidate
    rsem_tar, hugo_tar, kallisto_tar, fastqc_tar, bamqc_tar, star_tar = None, None, None, None, None, None
    rsem_id, hugo_id, star_id = None, None, None
    if rsem_star_output:
        if config.bamqc:
            rsem_id, hugo_id, star_id, fail_flag, bamqc_id = flatten(rsem_star_output)
            bamqc_tar = job.fileStore.readGlobalFile(bamqc_id, os.path.join(work_dir, 'bamqc.tar.gz'))
            config.uuid = 'FAIL.' + config.uuid if fail_flag else config.uuid
        else:
            rsem_id, hugo_id, star_id = flatten(rsem_star_output)
        rsem_tar = job.fileStore.readGlobalFile(rsem_id, os.path.join(work_dir, 'rsem.tar.gz'))
        hugo_tar = job.fileStore.readGlobalFile(hugo_id, os.path.join(work_dir, 'rsem_hugo.tar.gz'))
        star_tar = job.fileStore.readGlobalFile(star_id, os.path.join(work_dir, 'star.tar.gz'))
    if kallisto_output:
        kallisto_tar = job.fileStore.readGlobalFile(kallisto_output, os.path.join(work_dir, 'kallisto.tar.gz'))
    if fastqc_output:
        fastqc_tar = job.fileStore.readGlobalFile(fastqc_output, os.path.join(work_dir, 'fastqc.tar.gz'))
    # I/O
    out_tar = os.path.join(work_dir, config.uuid + '.tar.gz')
    # Consolidate separate tarballs into one as streams (avoids unnecessary untaring)
    tar_list = [x for x in [rsem_tar, hugo_tar, kallisto_tar, fastqc_tar, bamqc_tar, star_tar] if x is not None]
    with tarfile.open(out_tar, 'w:gz') as f_out:
        for tar in tar_list:
            with tarfile.open(tar, 'r') as f_in:
                for tarinfo in f_in:
                    with closing(f_in.extractfile(tarinfo)) as f_in_file:
                        if tar == rsem_tar:
                            tarinfo.name = os.path.join(config.uuid, 'RSEM', os.path.basename(tarinfo.name))
                        elif tar == hugo_tar:
                            tarinfo.name = os.path.join(config.uuid, 'RSEM', 'Hugo', os.path.basename(tarinfo.name))
                        elif tar == kallisto_tar:
                            tarinfo.name = os.path.join(config.uuid, 'Kallisto', os.path.basename(tarinfo.name))
                        elif tar == bamqc_tar:
                            tarinfo.name = os.path.join(config.uuid, 'QC', 'bamQC', os.path.basename(tarinfo.name))
                        elif tar == fastqc_tar:
                            tarinfo.name = os.path.join(config.uuid, 'QC', 'fastQC', os.path.basename(tarinfo.name))
                        elif tar == star_tar:
                            tarinfo.name = os.path.join(config.uuid, 'QC', 'STAR', os.path.basename(tarinfo.name))
                        f_out.addfile(tarinfo, fileobj=f_in_file)
    # Move to output location
    if urlparse(config.output_dir).scheme == 's3':
        job.fileStore.logToMaster('Uploading {} to S3: {}'.format(config.uuid, config.output_dir))
        s3am_upload(fpath=out_tar, s3_dir=config.output_dir, num_cores=config.cores)
    else:
        job.fileStore.logToMaster('Moving {} to output dir: {}'.format(config.uuid, config.output_dir))
        mkdir_p(config.output_dir)
        copy_files(file_paths=[os.path.join(work_dir, config.uuid + '.tar.gz')], output_dir=config.output_dir)

    # Delete intermediates
    ids_to_delete = [x for x in [rsem_id, hugo_id, star_id, kallisto_output, fastqc_output]]
    job.addChildJobFn(cleanup_ids, ids_to_delete)


def cleanup_ids(job, ids_to_delete):
    """
    Delete fileStoreIDs for files no longer needed

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param list ids_to_delete: list of FileStoreIDs to delete
    """
    [job.fileStore.deleteGlobalFile(x) for x in ids_to_delete if x is not None]


def sort_and_save_bam(job, config, bam_id):
    """
    Sorts STAR's output bam using samtools. STAR has a sporadic memory leak when sorting. 
    
    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Namespace config: Argparse Namespace object containing argument inputs
    :param FileID bam_id: FileID for STARs genome aligned bam
    """
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(bam_id, os.path.join(work_dir, 'aligned.bam'))

    parameters = ['sort',
                  '-o', '/data/{}.sorted.bam'.format(config.uuid),
                  '-O', 'bam',
                  '-T', 'temp',
                  '-@', str(job.cores),
                  '/data/aligned.bam']

    dockerCall(job, tool='quay.io/ucsc_cgl/samtools:1.3--256539928ea162949d8a65ca5c79a72ef557ce7c',
               parameters=parameters, workDir=work_dir)

    bam_path = os.path.join(work_dir, '{}.sorted.bam'.format(config.uuid))
    move_or_upload(config, files=[bam_path])


# Pipeline specific functions
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

def main():
    """
    Computational Genomics Lab, Genomics Institute, UC Santa Cruz
    Toil RNA-seq pipeline

    RNA-seq fastqs are combined, aligned, and quantified with 2 different methods (RSEM and Kallisto)

    General usage:
    1. Type "toil-rnaseq generate" to create an editable manifest and config in the current working directory.
    2. Parameterize the pipeline by editing the config.
    3. Fill in the manifest with information pertaining to your samples.
    4. Type "toil-rnaseq run [jobStore]" to execute the pipeline.

    Please read the README.md located in the source directory or at:
    https://github.com/BD2KGenomics/toil-scripts/tree/master/src/toil_scripts/rnaseq_cgl

    Structure of RNA-Seq Pipeline (per sample)

                  8
                  |
                  3 -- 4 -- 5
                 /          |
      0 -- 1 -- 2 ---- 7 -- 9
                |           |
                6 -----------

    0 = Download sample
    1 = Unpack/Merge fastqs
    2 = CutAdapt (adapter trimming)
    3 = STAR Alignment
    4 = RSEM Quantification
    5 = RSEM Post-processing
    6 = FastQC
    7 = Kallisto
    8 = BamQC (as specified by CKCC at UC Santa Cruz)
    9 = Consoliate output and upload to S3
    =======================================
    Dependencies
    Curl:       apt-get install curl
    Docker:     wget -qO- https://get.docker.com/ | sh
    Toil:       pip install toil
    Boto:       pip install boto (OPTIONAL, needed for upload to S3)
    """
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawTextHelpFormatter)
    subparsers = parser.add_subparsers(dest='command')

    # Generate subparsers
    subparsers.add_parser('generate-config', help='Generates an editable config in the current working directory.')
    subparsers.add_parser('generate-manifest', help='Generates an editable manifest in the current working directory.')
    subparsers.add_parser('generate', help='Generates a config and manifest in the current working directory.')

    # Run subparser
    parser_run = subparsers.add_parser('run', help='Runs the RNA-seq pipeline')
    group = parser_run.add_mutually_exclusive_group()
    parser_run.add_argument('--config', default='config-toil-rnaseq.yaml', type=str,
                            help='Path to the (filled in) config file, generated with "generate-config". '
                                 '\nDefault value: "%(default)s"')
    group.add_argument('--manifest', default='manifest-toil-rnaseq.tsv', type=str,
                       help='Path to the (filled in) manifest file, generated with "generate-manifest". '
                            '\nDefault value: "%(default)s"')
    group.add_argument('--samples', default=None, nargs='+', type=str,
                       help='Space delimited sample URLs (any number). Samples must be tarfiles/tarballs that contain '
                            'fastq files. URLs follow the format: http://foo.com/sample.tar, '
                            'file:///full/path/to/file.tar. The UUID for the sample will be derived from the file.'
                            'Samples passed in this way will be assumed to be paired end, if using single-end data, '
                            'please use the manifest option.')

    # If no arguments provided, print full help menu
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    # Add Toil options
    Job.Runner.addToilOptions(parser_run)
    args = parser.parse_args()

    # Parse subparsers related to generation of config and manifest
    cwd = os.getcwd()
    if args.command == 'generate-config' or args.command == 'generate':
        generate_file(os.path.join(cwd, 'config-toil-rnaseq.yaml'), generate_config)
    if args.command == 'generate-manifest' or args.command == 'generate':
        generate_file(os.path.join(cwd, 'manifest-toil-rnaseq.tsv'), generate_manifest)

    # Pipeline execution
    elif args.command == 'run':
        require(os.path.exists(args.config), '{} not found. Please run '
                                             '"toil-rnaseq generate-config"'.format(args.config))
        if not args.samples:
            require(os.path.exists(args.manifest), '{} not found and no samples provided. Please '
                                                   'run "toil-rnaseq generate-manifest"'.format(args.manifest))
            samples = parse_samples(path_to_manifest=args.manifest)
        else:
            samples = parse_samples(sample_urls=args.samples)

        # Parse config
        parsed_config = {x.replace('-', '_'): y for x, y in yaml.load(open(args.config).read()).iteritems()}
        config = argparse.Namespace(**parsed_config)
        config.maxCores = int(args.maxCores) if args.maxCores else sys.maxint

        # Config sanity checks
        require(config.kallisto_index or config.star_index,
                'URLs not provided for Kallisto or STAR, so there is nothing to do!')
        if config.star_index or config.rsem_ref:
            require(config.star_index and config.rsem_ref, 'Input provided for STAR or RSEM but not both. STAR: '
                                                           '{}, RSEM: {}'.format(config.star_index, config.rsem_ref))

        # Output dir checks and handling
        require(config.output_dir, 'No output location specified: {}'.format(config.output_dir))
        for file_input in [x for x in [config.kallisto_index, config.star_index, config.rsem_ref] if x]:
            require(urlparse(file_input).scheme in schemes,
                    'Input in config must have the appropriate URL prefix: {}'.format(schemes))

        if not config.output_dir.startswith('/'):
            if urlparse(config.output_dir).scheme == 'file':
                config.output_dir = config.output_dir.split('file://')[1]
                if not config.output_dir.startswith('/'):
                    raise UserError('Output dir neither starts with / or is an S3 URL')
            elif not urlparse(config.output_dir).scheme == 's3':
                raise UserError('Output dir neither starts with / or is an S3 URL')

        if not config.output_dir.endswith('/'):
            config.output_dir += '/'

        # Program checks
        for program in ['curl', 'docker']:
            require(next(which(program), None), program + ' must be installed on every node.'.format(program))

        # Start the workflow, calling map_job() to run the pipeline for each sample
        with Toil(args) as toil:
            if args.restart:
                toil.restart()
            else:
                toil.start(Job.wrapJobFn(map_job, download_sample, samples, config))


if __name__ == '__main__':
    try:
        main()
    except UserError as e:
        print(e.message, file=sys.stderr)
        sys.exit(1)
