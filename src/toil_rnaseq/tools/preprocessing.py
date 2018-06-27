import os
import re
import subprocess
from subprocess import PIPE
from urlparse import urlparse

from toil.job import PromisedRequirement
from toil.lib.docker import dockerCall

from bams import convert_bam_to_fastq
from bams import download_bam_from_gdc
from jobs import cleanup_ids
from toil_rnaseq.tools import cutadapt_version
from toil_rnaseq.utils import require, UserError
from toil_rnaseq.utils.urls import download_url
from toil_rnaseq.utils.urls import download_url_job


def run_cutadapt(job, r1_id, r2_id, fwd_3pr_adapter, rev_3pr_adapter):
    """
    Adapter trimming for RNA-seq data

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str r1_id: FileStoreID of fastq read 1
    :param str r2_id: FileStoreID of fastq read 2 (if paired data)
    :param str fwd_3pr_adapter: Adapter sequence for the forward 3' adapter
    :param str rev_3pr_adapter: Adapter sequence for the reverse 3' adapter (second fastq pair)
    :return: R1 and R2 FileStoreIDs
    :rtype: tuple(str, str)
    """
    # Retrieve files and define parameters
    job.fileStore.readGlobalFile(r1_id, os.path.join(job.tempDir, 'R1.fastq'))
    parameters = ['-a', fwd_3pr_adapter,
                  '-m', '35']

    # If R2 fastq is present...
    if r2_id:
        require(rev_3pr_adapter, "Paired end data requires a reverse 3' adapter sequence.")
        job.fileStore.readGlobalFile(r2_id, os.path.join(job.tempDir, 'R2.fastq'))
        parameters.extend(['-A', rev_3pr_adapter,
                           '-o', '/data/R1_cutadapt.fastq',
                           '-p', '/data/R2_cutadapt.fastq',
                           '/data/R1.fastq', '/data/R2.fastq'])
    else:
        parameters.extend(['-o', '/data/R1_cutadapt.fastq', '/data/R1.fastq'])

    # Call: CutAdapt
    dockerCall(job=job, tool=cutadapt_version, workDir=job.tempDir, parameters=parameters)

    # Write to fileStore
    r1_cut_id = job.fileStore.writeGlobalFile(os.path.join(job.tempDir, 'R1_cutadapt.fastq'))
    r2_cut_id = job.fileStore.writeGlobalFile(os.path.join(job.tempDir, 'R2_cutadapt.fastq')) if r2_id else None

    return r1_cut_id, r2_cut_id


def download_and_process_tar(job, config):
    """
    Download tarball containing fastq(s) and process

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Expando config: Dict-like object containing workflow options as attributes
    :return: Processed fastqs
    :rtype: tuple(str, str)
    """
    # Define download and process jobs
    disk = '2G' if config.ci_test else config.max_sample_size
    download = job.wrapJobFn(download_url_job, config.url, s3_key_path=config.ssec, disk=disk)
    process = job.wrapJobFn(process_sample, config, input_tar=download.rv(),
                            disk=PromisedRequirement(lambda x: x.size * 10, download.rv()))

    # Wire jobs and return processed fastqs
    job.addChild(download)
    download.addChild(process)
    return process.rv()


def download_and_process_fastqs(job, config):
    """

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Expando config: Dict-like object containing workflow options as attributes
    :return: Processed fastqs
    :rtype: tuple(str, str)
    """
    # Define download and process jobs
    disk = '2G' if config.ci_test else config.max_sample_size
    download = job.wrapJobFn(multiple_fastq_dowloading, config, sample_disk=disk).encapsulate()
    process = job.wrapJobFn(process_sample, config, fastq_ids=download.rv(),
                            disk=PromisedRequirement(lambda xs: sum(x.size for x in xs) * 5, download.rv()))

    # Wire jobs and return processed fastqs
    job.addChild(download)
    download.addChild(process)
    return process.rv()


def download_and_process_bam(job, config):
    """
    Download and process a BAM by converting it to a FASTQ pair

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Expando config: Dict-like object containing workflow options as attributes
    :return: FileStoreIDs of R1 / R2 fastq files
    :rtype: tuple(str, str)
    """
    parsed_url = urlparse(config.url)

    # Download BAM
    if parsed_url.scheme == 'gdc':
        bam_path = download_bam_from_gdc(job, job.tempDir, url=config.url, token=config.gdc_token)
    else:
        bam_path = download_url(config.url, work_dir=job.tempDir, name='input.bam', s3_key_path=config.ssec)

    # Convert to fastq pairs
    r1, r2 = convert_bam_to_fastq(job, bam_path)

    # Return fastq files
    if config.cutadapt:
        disk = 2 * (r1.size + r2.size)
        return job.addChildJobFn(run_cutadapt, r1, r2, config.fwd_3pr_adapter,
                                 config.rev_3pr_adapter, disk=disk).rv()
    return r1, r2


def process_sample(job, config, input_tar=None, fastq_ids=None):
    """
    Converts sample.tar(.gz) or collection of fastqs into a fastq pair (or single fastq if single-ended.)
    WARNING: Here be dragons. I may or may not ever get the time to clean this up.

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Expando config: Dict-like object containing workflow options as attributes
    :param str input_tar: fileStoreID of the tarball (if applicable)
    :param list(str,) fastq_ids: FileStoreIDs of fastq files
    :return: FileStoreID from Cutadapt or from fastqs directly if workflow was run without Cutadapt option
    :rtype: tuple(str, str)
    """
    job.fileStore.logToMaster('Processing sample: {}'.format(config.uuid))
    delete_fastqs = True
    processed_r1, processed_r2 = None, None
    # I/O
    if input_tar:
        job.fileStore.readGlobalFile(input_tar, os.path.join(job.tempDir, 'sample.tar'), mutable=True)
        tar_path = os.path.join(job.tempDir, 'sample.tar')
        # Untar sample
        subprocess.check_call(['tar', '-xvf', tar_path, '-C', job.tempDir], stderr=PIPE, stdout=PIPE)
        os.remove(tar_path)
    else:
        ext = '.fq.gz' if config.gz else '.fq'
        for i, fastq_id in enumerate(fastq_ids):
            if i % 2 == 0:
                job.fileStore.readGlobalFile(fastq_id, os.path.join(job.tempDir, 'Fastq_{}_R1{}'.format(i, ext)))
            else:
                job.fileStore.readGlobalFile(fastq_id, os.path.join(job.tempDir, 'Fastq_{}_R2{}'.format(i, ext)))
    fastqs = []
    for root, subdir, files in os.walk(job.tempDir):
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
            with open(os.path.join(job.tempDir, 'R1.fastq'), 'w') as f1:
                p1 = subprocess.Popen([command] + r1, stdout=f1)
            with open(os.path.join(job.tempDir, 'R2.fastq'), 'w') as f2:
                p2 = subprocess.Popen([command] + r2, stdout=f2)
            p1.wait()
            p2.wait()
            processed_r1 = job.fileStore.writeGlobalFile(os.path.join(job.tempDir, 'R1.fastq'))
            processed_r2 = job.fileStore.writeGlobalFile(os.path.join(job.tempDir, 'R2.fastq'))
        disk = 2 * (processed_r1.size + processed_r2.size)
    else:
        command = 'zcat' if fastqs[0].endswith('.gz') else 'cat'
        if command == 'cat' and len(fastqs) == 1:
            processed_r1 = fastq_ids[0]
            delete_fastqs = False
        else:
            with open(os.path.join(job.tempDir, 'R1.fastq'), 'w') as f:
                subprocess.check_call([command] + fastqs, stdout=f)
            processed_r1 = job.fileStore.writeGlobalFile(os.path.join(job.tempDir, 'R1.fastq'))
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


def multiple_fastq_dowloading(job, config, sample_disk):
    """
    Convenience function for handling the downloading of multiple fastq files

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Expando config: Dict-like object containing workflow options as attributes
    :param int sample_disk: Amount of disk space to allocate to download jobs
    :return: FileStoreIDs for all fastqs downloaded
    :rtype: list(str,)
    """
    # Spawn download job per fastq file
    fastq_ids = []
    urls = config.url.split(',')
    if config.paired:
        require(len(urls) % 2 == 0, 'Fastq pairs must have multiples of 2 URLS separated by comma')
    for url in urls:
        fastq_ids.append(job.addChildJobFn(download_url_job, url, s3_key_path=config.ssec, disk=sample_disk).rv())

    return fastq_ids
