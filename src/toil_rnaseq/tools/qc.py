import os

from toil.lib.docker import dockerCall

from toil_rnaseq.tools import bamqc_version
from toil_rnaseq.tools import fastqc_version
from toil_rnaseq.utils.files import tarball_files
from toil_rnaseq.utils.urls import move_or_upload


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


def run_bamqc(job, aligned_bam_id, config, save_bam=False):
    """
    Run BAMQC as specified by Treehouse (UCSC)
    https://github.com/UCSC-Treehouse/bam-umend-qc

    :param JobFunctionWrappingJob job:
    :param str aligned_bam_id: FileStoreID of aligned bam from STAR
    :param Expando config: Contains sample information
    :param bool save_bam: Option to save mark-duplicate bam from BAMQC
    :return: FileStoreID for output tar
    :rtype: str
    """
    job.fileStore.readGlobalFile(aligned_bam_id, os.path.join(job.tempDir, 'input.bam'))
    dockerCall(job, tool=bamqc_version, workDir=job.tempDir, parameters=['/data/input.bam', '/data'])

    # Tar Output files
    output_names = ['readDist.txt', 'bam_umend_qc.tsv', 'bam_umend_qc.json']
    output_files = [os.path.join(job.tempDir, x) for x in output_names]
    tarball_files(tar_name='bam_qc.tar.gz', file_paths=output_files, output_dir=job.tempDir)
    tar_path = os.path.join(job.tempDir, 'bam_qc.tar.gz')

    # Save output BAM - this step is done here instead of in its own job for efficiency
    if save_bam:
        # Tag bam with sample UUID, upload, and delete
        bam_path = os.path.join(job.tempDir, 'sortedByCoord.md.bam')
        new_bam = os.path.join(job.tempDir, config.uuid + '.sortedByCoord.md.bam')
        os.rename(bam_path, new_bam)
        move_or_upload(config, [new_bam])
        job.fileStore.deleteGlobalFile(new_bam)

    # Delete intermediates
    job.fileStore.deleteGlobalFile(aligned_bam_id)

    return job.fileStore.writeGlobalFile(tar_path)
