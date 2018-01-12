import os
import tarfile
from contextlib import closing

from toil_rnaseq.utils import partitions
from toil_rnaseq.utils.urls import move_or_upload


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


def save_wiggle(job, config, wiggle_id):
    """
    Saves wiggle file that is output from STAR

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Expando config: Dict-like object containing workflow options as attributes
    :param str wiggle_id: FileStoreID of Wiggle file
    """
    wiggle_path = os.path.join(job.tempDir, config.uuid + '.wiggle.bg')
    job.fileStore.readGlobalFile(wiggle_id, wiggle_path)
    move_or_upload(config, [wiggle_path], enforce_ssec=False)


def consolidate_output(job, config, output):
    """
    Combines the contents of the outputs into one tarball and places in output directory or s3

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Expando config: Dict-like object containing workflow options as attributes
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
