import os
import subprocess

from toil.lib.docker import dockerCall

from toil_rnaseq.tools import star_version
from toil_rnaseq.utils.files import tarball_files
from toil_rnaseq.utils.urls import download_url


def run_star(job, r1_id, r2_id, star_index_url, wiggle=False, sort=False, save_aligned_bam=False):
    """
    Performs alignment of fastqs to bam via STAR

    --limitBAMsortRAM step added to deal with memory explosion when sorting certain samples.
    The value was chosen to complement the recommended amount of memory to have when running STAR (60G)

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str r1_id: FileStoreID of fastq (pair 1)
    :param str r2_id: FileStoreID of fastq (pair 2 if applicable, else pass None)
    :param str star_index_url: STAR index tarball
    :param bool wiggle: If True, will output a wiggle file and return it
    :param bool sort: If True, will sort output by coordinate
    :param bool save_aligned_bam: If True, will output an aligned BAM and save it
    :return: FileStoreID from RSEM
    :rtype: str
    """
    # Download and untar STAR index file
    download_url(url=star_index_url, name='starIndex.tar.gz', work_dir=job.tempDir)
    subprocess.check_call(['tar', '-xvf', os.path.join(job.tempDir, 'starIndex.tar.gz'), '-C', job.tempDir])
    os.remove(os.path.join(job.tempDir, 'starIndex.tar.gz'))
    star_index = os.path.join('/data', os.listdir(job.tempDir)[0]) if len(os.listdir(job.tempDir)) == 1 else '/data'

    # Define parameters
    parameters = ['--runThreadN', str(job.cores),
                  '--genomeDir', star_index,
                  '--outFileNamePrefix', 'rna',
                  '--outSAMunmapped', 'Within',
                  '--quantMode', 'TranscriptomeSAM',
                  '--outSAMattributes', 'NH', 'HI', 'AS', 'NM', 'MD',
                  '--outFilterType', 'BySJout',
                  '--outFilterMultimapNmax', '20',
                  '--outFilterMismatchNmax', '999',
                  '--outFilterMismatchNoverReadLmax', '0.04',
                  '--alignIntronMin', '20',
                  '--alignIntronMax', '1000000',
                  '--alignMatesGapMax', '1000000',
                  '--alignSJoverhangMin', '8',
                  '--alignSJDBoverhangMin', '1',
                  '--sjdbScore', '1']

    # Modify parameters based on function arguments
    if sort:
        parameters.extend(['--outSAMtype', 'BAM', 'SortedByCoordinate', '--limitBAMsortRAM', '49268954168'])
        aligned_bam = 'rnaAligned.sortedByCoord.out.bam'
    else:
        parameters.extend(['--outSAMtype', 'BAM', 'Unsorted'])
        aligned_bam = 'rnaAligned.out.bam'
    if wiggle:
        parameters.extend(['--outWigType', 'bedGraph',
                           '--outWigStrand', 'Unstranded',
                           '--outWigReferencesPrefix', 'chr'])

    # Read in fastq(s) and modify parameters based on
    job.fileStore.readGlobalFile(r1_id, os.path.join(job.tempDir, 'R1.fastq'))
    if r1_id and r2_id:
        job.fileStore.readGlobalFile(r2_id, os.path.join(job.tempDir, 'R2.fastq'))
        parameters.extend(['--readFilesIn', '/data/R1.fastq', '/data/R2.fastq'])
    else:
        parameters.extend(['--readFilesIn', '/data/R1.fastq'])

    # Call: STAR
    dockerCall(job=job, tool=star_version, workDir=job.tempDir, parameters=parameters)

    # Check output bam isnt size zero if sorted
    aligned_bam_path = os.path.join(job.tempDir, aligned_bam)
    if sort:
        assert os.stat(aligned_bam_path).st_size > 0, 'Aligned bam failed to sort. Ensure sufficient memory is free.'

    # Write files to fileStore
    transcriptome_id = job.fileStore.writeGlobalFile(os.path.join(job.tempDir, 'rnaAligned.toTranscriptome.out.bam'))
    aligned_id = job.fileStore.writeGlobalFile(aligned_bam_path) if save_aligned_bam else None
    wiggle_path = os.path.join(job.tempDir, 'rnaSignal.UniqueMultiple.str1.out.bg')
    wiggle_id = job.fileStore.writeGlobalFile(wiggle_path) if wiggle else None

    # Tar output files, store in fileStore, and return FileStoreIDs
    output_files = [os.path.join(job.tempDir, x) for x in ['rnaLog.final.out', 'rnaSJ.out.tab']]
    tarball_files('star.tar.gz', file_paths=output_files, output_dir=job.tempDir)
    star_id = job.fileStore.writeGlobalFile(os.path.join(job.tempDir, 'star.tar.gz'))

    return transcriptome_id, star_id, aligned_id, wiggle_id
