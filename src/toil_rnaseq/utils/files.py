import os
import shutil
import tarfile
from contextlib import closing

from toil_lib import require


def tarball_files(tar_name, file_paths, output_dir='.', prefix=''):
    """
    Creates a tarball from a group of files

    :param str tar_name: Name of tarball
    :param list[str] file_paths: Absolute file paths to include in the tarball
    :param str output_dir: Output destination for tarball
    :param str prefix: Optional prefix for files in tarball
    """
    with tarfile.open(os.path.join(output_dir, tar_name), 'w:gz') as f_out:
        for file_path in file_paths:
            if not file_path.startswith('/'):
                raise ValueError('Path provided is relative not absolute.')
            arcname = prefix + os.path.basename(file_path)
            f_out.add(file_path, arcname=arcname)


def __forall_files(file_paths, output_dir, op):
    """
    Applies a function to a set of files and an output directory.

    :param str output_dir: Output directory
    :param list[str] file_paths: Absolute file paths to move
    """
    for file_path in file_paths:
        if not file_path.startswith('/'):
            raise ValueError('Path provided (%s) is relative not absolute.' % file_path)
        dest = os.path.join(output_dir, os.path.basename(file_path))
        op(file_path, dest)


def copy_file_job(job, name, file_id, output_dir):
    """
    Job version of move_files for one file

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str name: Name of output file (including extension)
    :param str file_id: FileStoreID of file
    :param str output_dir: Location to place output file
    """
    work_dir = job.fileStore.getLocalTempDir()
    fpath = job.fileStore.readGlobalFile(file_id, os.path.join(work_dir, name))
    copy_files([fpath], output_dir)


def move_files(file_paths, output_dir):
    """
    Moves files from the working directory to the output directory.

    Important note: this function can couple dangerously with caching.
    Specifically, if this function is called on a file in the cache, the cache
    will contain a broken reference. This may lead to a non-existent file path
    being passed to later jobs. Don't call this function on files that are in
    the cache, unless you know for sure that the input file will not be used by
    any later jobs.

    :param str output_dir: Output directory
    :param list[str] file_paths: Absolute file paths to move
    """
    __forall_files(file_paths, output_dir, shutil.move)


def copy_files(file_paths, output_dir):
    """
    Moves files from the working directory to the output directory.

    :param str output_dir: Output directory
    :param list[str] file_paths: Absolute file paths to move
    """
    __forall_files(file_paths, output_dir, shutil.copy)


def consolidate_tarballs_job(job, fname_to_id):
    """
    Combine the contents of separate tarballs into one.
    Subdirs within the tarball will be named the keys in **fname_to_id

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param dict[str,str] fname_to_id: Dictionary of the form: file-name-prefix=FileStoreID
    :return: The file store ID of the generated tarball
    :rtype: str
    """
    work_dir = job.fileStore.getLocalTempDir()
    # Retrieve output file paths to consolidate
    tar_paths = []
    for fname, file_store_id in fname_to_id.iteritems():
        p = job.fileStore.readGlobalFile(file_store_id, os.path.join(work_dir, fname + '.tar.gz'))
        tar_paths.append((p, fname))
    # I/O
    # output_name is arbitrary as this job function returns a FileStoreId
    output_name = 'foo.tar.gz'
    out_tar = os.path.join(work_dir, output_name)
    # Consolidate separate tarballs into one
    with tarfile.open(os.path.join(work_dir, out_tar), 'w:gz') as f_out:
        for tar, fname in tar_paths:
            with tarfile.open(tar, 'r') as f_in:
                for tarinfo in f_in:
                    with closing(f_in.extractfile(tarinfo)) as f_in_file:
                        tarinfo.name = os.path.join(output_name, fname, os.path.basename(tarinfo.name))
                        f_out.addfile(tarinfo, fileobj=f_in_file)
    return job.fileStore.writeGlobalFile(out_tar)


def generate_file(file_path, generate_func):
    """
    Checks file existance, generates file, and provides message
    :param str file_path: File location to generate file
    :param func generate_func: Function used to generate file
    """
    require(not os.path.exists(file_path), file_path + ' already exists!')
    with open(file_path, 'w') as f:
        f.write(generate_func())
    print('\t{} has been generated in the current working directory.'.format(os.path.basename(file_path)))
