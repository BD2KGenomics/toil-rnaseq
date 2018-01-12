import os
import shutil
import tarfile


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


def copy_files(file_paths, output_dir):
    """
    Moves files from the working directory to the output directory.

    :param str output_dir: Output directory
    :param list[str] file_paths: Absolute file paths to move
    """
    __forall_files(file_paths, output_dir, shutil.copy)


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
