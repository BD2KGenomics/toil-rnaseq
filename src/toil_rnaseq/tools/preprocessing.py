import os
from urlparse import urlparse

from toil.job import PromisedRequirement
from toil.lib.docker import dockerCall

from toil_rnaseq.tools import cutadapt_version
from toil_rnaseq.tools.bams import download_bam_from_gdc, convert_bam_to_fastq
from toil_rnaseq.utils import require
from toil_rnaseq.utils.jobs import multiple_fastq_dowloading
from toil_rnaseq.utils.jobs import process_sample
from toil_rnaseq.utils.urls import download_url_job, download_url


def run_cutadapt(job, r1_id, r2_id, fwd_3pr_adapter, rev_3pr_adapter):
    """
    Adapter trimming for RNA-seq data

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str r1_id: FileStoreID of fastq read 1
    :param str r2_id: FileStoreID of fastq read 2 (if paired data)
    :param str fwd_3pr_adapter: Adapter sequence for the forward 3' adapter
    :param str rev_3pr_adapter: Adapter sequence for the reverse 3' adapter (second fastq pair)
    :return: R1 and R2 FileStoreIDs
    :rtype: tuple
    """
    work_dir = job.fileStore.getLocalTempDir()
    if r2_id:
        require(rev_3pr_adapter, "Paired end data requires a reverse 3' adapter sequence.")
    # Retrieve files
    parameters = ['-a', fwd_3pr_adapter,
                  '-m', '35']
    if r1_id and r2_id:
        job.fileStore.readGlobalFile(r1_id, os.path.join(work_dir, 'R1.fastq'))
        job.fileStore.readGlobalFile(r2_id, os.path.join(work_dir, 'R2.fastq'))
        parameters.extend(['-A', rev_3pr_adapter,
                           '-o', '/data/R1_cutadapt.fastq',
                           '-p', '/data/R2_cutadapt.fastq',
                           '/data/R1.fastq', '/data/R2.fastq'])
    else:
        job.fileStore.readGlobalFile(r1_id, os.path.join(work_dir, 'R1.fastq'))
        parameters.extend(['-o', '/data/R1_cutadapt.fastq', '/data/R1.fastq'])
    # Call: CutAdapt
    dockerCall(job=job, tool=cutadapt_version, workDir=work_dir, parameters=parameters)
    # Write to fileStore
    if r1_id and r2_id:
        r1_cut_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'R1_cutadapt.fastq'))
        r2_cut_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'R2_cutadapt.fastq'))
    else:
        r1_cut_id = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'R1_cutadapt.fastq'))
        r2_cut_id = None
    return r1_cut_id, r2_cut_id


def download_and_process_tar(job, config):
    # Define jobs
    disk = '2G' if config.ci_test else config.max_sample_size
    download = job.wrapJobFn(download_url_job, config.url, s3_key_path=config.ssec, disk=disk)
    inputs = job.wrapJobFn(process_sample, config, input_tar=download.rv(),
                           disk=PromisedRequirement(lambda x: x.size * 5, download.rv()))

    # Wire jobs
    job.addChild(download)
    download.addChild(inputs)
    return inputs.rv()


def download_and_process_fastqs(job, config):
    # Define jobs
    disk = '2G' if config.ci_test else config.max_sample_size
    download = job.wrapJobFn(multiple_fastq_dowloading, config, sample_disk=disk)
    inputs = job.wrapJobFn(process_sample, config, fastq_ids=download.rv(),
                           disk=PromisedRequirement(lambda xs: sum(x.size for x in xs) * 3, download.rv()))

    # Wire jobs
    job.addChild(download)
    download.addChild(inputs)
    return inputs.rv()


def download_and_process_bam(job, config):
    """
    Download and process a BAM by converting it to a FASTQ pair

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Namespace config: Argparse Namespace object containing argument inputs
    :return: FileStoreIDs of R1 / R2 fastq files
    """
    work_dir = job.fileStore.getLocalTempDir()
    parsed_url = urlparse(config.url)

    # Download BAM
    if parsed_url.scheme == 'gdc':
        bam_path = download_bam_from_gdc(job, work_dir, url=config.url, token=config.gdc_token)
    else:
        bam_path = download_url(config.url, work_dir=work_dir, name='input.bam', s3_key_path=config.ssec)

    # Convert to fastq pairs
    r1, r2 = convert_bam_to_fastq(job, bam_path)

    # Return fastq files
    if config.cutadapt:
        disk = 2 * (r1.size + r2.size)
        return job.addChildJobFn(run_cutadapt, r1, r2, config.fwd_3pr_adapter,
                                 config.rev_3pr_adapter, disk=disk).rv()
    return r1, r2
