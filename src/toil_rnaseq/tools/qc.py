import os
from urlparse import urlparse

from toil.lib.docker import dockerCall

from toil_rnaseq.tools import fastqc_version
from toil_rnaseq.utils.files import copy_files
from toil_rnaseq.utils.files import tarball_files
from toil_rnaseq.utils.urls import s3am_upload


def run_fastqc(job, r1_id, r2_id):
    """
    Run Fastqc on the input reads

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str r1_id: FileStoreID of fastq read 1
    :param str r2_id: FileStoreID of fastq read 2
    :return: FileStoreID of fastQC output (tarball)
    :rtype: str
    """
    # Read in files and set parameters
    job.fileStore.readGlobalFile(r1_id, os.path.join(job.tempDir, 'R1.fastq'))
    parameters = ['/data/R1.fastq']
    output_names = ['R1_fastqc.html', 'R1_fastqc.zip']
    if r2_id:
        job.fileStore.readGlobalFile(r2_id, os.path.join(job.tempDir, 'R2.fastq'))
        parameters.extend(['-t', '2', '/data/R2.fastq'])
        output_names.extend(['R2_fastqc.html', 'R2_fastqc.zip'])

    # Call fastQC
    dockerCall(job=job, tool=fastqc_version, workDir=job.tempDir, parameters=parameters)

    # Package output files and return FileStoreID
    output_files = [os.path.join(job.tempDir, x) for x in output_names]
    tarball_files(tar_name='fastqc.tar.gz', file_paths=output_files, output_dir=job.tempDir)
    return job.fileStore.writeGlobalFile(os.path.join(job.tempDir, 'fastqc.tar.gz'))


# Deprecated until further development
def run_bam_qc(job, aligned_bam_id, config):
    """
    Run BAM QC as specified by California Kids Cancer Comparison (CKCC)

    :param JobFunctionWrappingJob job:
    :param str aligned_bam_id: FileStoreID of aligned bam from STAR
    :param Namespace config: Argparse Namespace object containing argument inputs
        Must contain:
            config.uuid str: UUID of input sample
            config.save_bam bool: True/False depending on whether to save bam
            config.output_dir str: Path to save bam
            config.ssec str: Path to encryption key for secure upload to S3
    :return: boolean flag, FileStoreID for output bam, and FileStoreID for output tar
    :rtype: tuple(bool, str, str)
    """
    work_dir = job.fileStore.getLocalTempDir()
    job.fileStore.readGlobalFile(aligned_bam_id, os.path.join(work_dir, 'rnaAligned.sortedByCoord.out.bam'))
    dockerCall(job, tool='hbeale/treehouse_bam_qc:1.0', workDir=work_dir, parameters=['runQC.sh', str(job.cores)])

    # Tar Output files
    output_names = ['readDist.txt', 'rnaAligned.out.md.sorted.geneBodyCoverage.curves.pdf',
                    'rnaAligned.out.md.sorted.geneBodyCoverage.txt']
    if os.path.exists(os.path.join(work_dir, 'readDist.txt_PASS_qc.txt')):
        output_names.append('readDist.txt_PASS_qc.txt')
        fail_flag = False
    else:
        output_names.append('readDist.txt_FAIL_qc.txt')
        fail_flag = True
    output_files = [os.path.join(work_dir, x) for x in output_names]
    tarball_files(tar_name='bam_qc.tar.gz', file_paths=output_files, output_dir=work_dir)

    # Save output BAM
    if config.save_bam:
        bam_path = os.path.join(work_dir, 'rnaAligned.sortedByCoord.md.bam')
        new_bam_path = os.path.join(work_dir, config.uuid + '.sortedByCoord.md.bam')
        os.rename(bam_path, new_bam_path)
        if urlparse(config.output_dir).scheme == 's3' and config.ssec:
            s3am_upload(fpath=new_bam_path, s3_dir=config.output_dir, s3_key_path=config.ssec)
        elif urlparse(config.output_dir).scheme != 's3':
            copy_files(file_paths=[new_bam_path], output_dir=config.output_dir)

    # Delete intermediates
    job.fileStore.deleteGlobalFile(aligned_bam_id)

    return fail_flag, job.fileStore.writeGlobalFile(os.path.join(work_dir, 'bam_qc.tar.gz'))
