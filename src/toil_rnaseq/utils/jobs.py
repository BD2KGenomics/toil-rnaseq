import os
import re
import subprocess
import tarfile
from contextlib import closing
from subprocess import PIPE

from toil_rnaseq.tools.preprocessing import run_cutadapt
from toil_rnaseq.utils import UserError
from toil_rnaseq.utils import partitions, require
from toil_rnaseq.utils.urls import download_url_job, move_or_upload


def cleanup_ids(job, ids_to_delete):
    """
    Delete fileStoreIDs for files no longer needed

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param list ids_to_delete: list of FileStoreIDs to delete
    """
    [job.fileStore.deleteGlobalFile(x) for x in ids_to_delete if x is not None]


def map_job(job, func, inputs, *args):
    """
    Spawns a tree of jobs to avoid overloading the number of jobs spawned by a single parent.
    This function is appropriate to use when batching samples greater than 1,000.

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param function func: Function to spawn dynamically, passes one sample as first argument
    :param list inputs: Array of samples to be batched
    :param list args: any arguments to be passed to the function
    """
    # num_partitions isn't exposed as an argument in order to be transparent to the user.
    # The value for num_partitions is a tested value
    num_partitions = 100
    partition_size = len(inputs) / num_partitions
    if partition_size > 1:
        for partition in partitions(inputs, partition_size):
            job.addChildJobFn(map_job, func, partition, *args)
    else:
        for sample in inputs:
            job.addChildJobFn(func, sample, *args)


def multiple_fastq_dowloading(job, config, sample_disk):
    fastq_ids = []
    urls = config.url.split(',')
    if config.paired:
        require(len(urls) % 2 == 0, 'Fastq pairs must have multiples of 2 URLS separated by comma')
    config.gz = True if urls[0].endswith('gz') else None
    for url in urls:
        fastq_ids.append(job.addChildJobFn(download_url_job, url, s3_key_path=config.ssec, disk=sample_disk).rv())

    return fastq_ids


def save_wiggle(job, config, wiggle_id):
    wiggle_path = os.path.join(job.tempDir, config.uuid + '.wiggle.bg')
    job.fileStore.readGlobalFile(wiggle_id, wiggle_path)
    move_or_upload(config, [wiggle_path], enforce_ssec=False)


def consolidate_output(job, config, output):
    """
    Combines the contents of the outputs into one tarball and places in output directory or s3

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Expando config: Expando object containing workflow options as attributes
    :param dict(str, str) output:
    """
    job.log('Consolidating output: {}'.format(config.uuid))

    # Collect all tarballs from fileStore
    tars = {}
    for tool, filestore_id in output.iteritems():
        tars[os.path.join(config.uuid, tool)] = job.fileStore.readGlobalFile(filestore_id)

    # Consolidate tarballs into one output tar as streams (to avoid unnecessary decompression)
    out_tar = os.path.join(job.tempDir, config.uuid + '.tar.gz')
    with tarfile.open(out_tar, 'w:gz') as f_out:
        for name, tar in tars.iteritems():
            with tarfile.open(tar, 'r') as f_in:
                for tarinfo in f_in:
                    with closing(f_in.extractfile(tarinfo)) as f_in_file:
                        tarinfo.name = os.path.join(name, os.path.basename(tarinfo.name))
                        f_out.addfile(tarinfo, fileobj=f_in_file)

    # Move to output location
    move_or_upload(config, files=[out_tar], enforce_ssec=False)


def process_sample(job, config, input_tar=None, fastq_ids=None):
    """
    Converts sample.tar(.gz) into a fastq pair (or single fastq if single-ended.)
    WARNING: Here be dragons. I may or may not ever get the time to clean this up.

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Namespace config: Argparse Namespace object containing argument inputs
    :param FileID input_tar: fileStoreID of the tarball (if applicable)
    :param list[FileID] fastq_ids: FileStoreIDs of fastq files
    :return: FileStoreID from Cutadapt or from fastqs directly if workflow was run without Cutadapt option
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
