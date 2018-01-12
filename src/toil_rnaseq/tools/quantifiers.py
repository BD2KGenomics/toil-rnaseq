import os
import subprocess

from toil.lib.docker import dockerCall

from toil_rnaseq.tools import hera_version
from toil_rnaseq.tools import kallisto_version
from toil_rnaseq.tools import rsem_version
from toil_rnaseq.tools import rsemgenemapping_version
from toil_rnaseq.utils.files import tarball_files
from toil_rnaseq.utils.urls import download_url


def run_kallisto(job, r1_id, r2_id, kallisto_index_url):
    """
    RNA quantification via Kallisto

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str r1_id: FileStoreID of fastq (pair 1)
    :param str r2_id: FileStoreID of fastq (pair 2 if applicable, otherwise pass None for single-end)
    :param str kallisto_index_url: FileStoreID for Kallisto index file
    :return: FileStoreID from Kallisto output
    :rtype: str
    """
    # Retrieve files and define parameters
    download_url(url=kallisto_index_url, name='kallisto_hg38.idx', work_dir=job.tempDir)
    job.fileStore.readGlobalFile(r1_id, os.path.join(job.tempDir, 'R1.fastq'))
    parameters = ['quant',
                  '-i', '/data/kallisto_hg38.idx',
                  '-t', str(job.cores),
                  '-o', '/data/',
                  '-b', '100',
                  '--fusion']

    # If R2 fastq is present...
    if r2_id:
        job.fileStore.readGlobalFile(r2_id, os.path.join(job.tempDir, 'R2.fastq'))
        parameters.extend(['/data/R1.fastq', '/data/R2.fastq'])
    else:
        parameters.extend(['--single', '-l', '200', '-s', '15', '/data/R1.fastq'])

    # Call: Kallisto
    dockerCall(job, workDir=job.tempDir, parameters=parameters, tool=kallisto_version)

    # Tar output files together, store in fileStore, and return
    output_names = ['run_info.json', 'abundance.tsv', 'abundance.h5', 'fusion.txt']
    output_files = [os.path.join(job.tempDir, x) for x in output_names]
    tarball_files(tar_name='kallisto.tar.gz', file_paths=output_files, output_dir=job.tempDir)
    return job.fileStore.writeGlobalFile(os.path.join(job.tempDir, 'kallisto.tar.gz'))


def run_rsem(job, bam_id, rsem_ref_url, paired=True):
    """
    RNA quantification with RSEM

    :param JobFunctionWrappingJob job: Passed automatically by Toil
    :param str bam_id: FileStoreID of transcriptome bam for quantification
    :param str rsem_ref_url: URL of RSEM reference (tarball)
    :param bool paired: If True, uses parameters for paired end data
    :return: FileStoreIDs for RSEM's gene and isoform output
    :rtype: str
    """
    # Retrieve RSEM reference
    download_url(url=rsem_ref_url, name='rsem_ref.tar.gz', work_dir=job.tempDir)
    subprocess.check_call(['tar', '-xvf', os.path.join(job.tempDir, 'rsem_ref.tar.gz'), '-C', job.tempDir])
    os.remove(os.path.join(job.tempDir, 'rsem_ref.tar.gz'))
    # Determine tarball structure - based on it, ascertain folder name and rsem reference prefix
    rsem_files = []
    for root, directories, files in os.walk(job.tempDir):
        rsem_files.extend([os.path.join(root, x) for x in files])
    # "grp" is a required RSEM extension that should exist in the RSEM reference
    ref_prefix = [os.path.basename(os.path.splitext(x)[0]) for x in rsem_files if 'grp' in x][0]
    ref_folder = os.path.join('/data', os.listdir(job.tempDir)[0]) if len(os.listdir(job.tempDir)) == 1 else '/data'
    # Read bam from fileStore
    job.fileStore.readGlobalFile(bam_id, os.path.join(job.tempDir, 'transcriptome.bam'))

    # Call: RSEM
    output_prefix = 'rsem'
    parameters = ['--quiet',
                  '--no-qualities',
                  '-p', str(job.cores),
                  '--forward-prob', '0.5',
                  '--seed-length', '25',
                  '--fragment-length-mean', '-1.0',
                  '--bam', '/data/transcriptome.bam',
                  os.path.join(ref_folder, ref_prefix),
                  output_prefix]
    if paired:
        parameters = ['--paired-end'] + parameters
    dockerCall(job, parameters=parameters, workDir=job.tempDir, tool=rsem_version)

    # Store output in fileStore and return
    gene_id = job.fileStore.writeGlobalFile(os.path.join(job.tempDir, output_prefix + '.genes.results'))
    isoform_id = job.fileStore.writeGlobalFile(os.path.join(job.tempDir, output_prefix + '.isoforms.results'))
    return gene_id, isoform_id


def run_rsem_gene_mapping(job, rsem_gene_id, rsem_isoform_id):
    """
    Parses RSEM output files to map ENSEMBL IDs to Gencode HUGO gene names

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str rsem_gene_id: FileStoreID of rsem_gene_ids
    :param str rsem_isoform_id: FileStoreID of rsem_isoform_ids
    :return: FileStoreID from RSEM post process tarball
    :rytpe: str
    """
    # Retrieve input files
    genes = job.fileStore.readGlobalFile(rsem_gene_id, os.path.join(job.tempDir, 'rsem_genes.results'))
    iso = job.fileStore.readGlobalFile(rsem_isoform_id, os.path.join(job.tempDir, 'rsem_isoforms.results'))

    # Perform HUGO gene / isoform name mapping
    command = ['-g', 'rsem_genes.results', '-i', 'rsem_isoforms.results']
    dockerCall(job, parameters=command, workDir=job.tempDir, tool=rsemgenemapping_version)
    hugo_files = [os.path.join(job.tempDir, x) for x in ['rsem_genes.hugo.results', 'rsem_isoforms.hugo.results']]

    # Create tarballs for output, store in fileStore, and return
    tarball_files('rsem.tar.gz', file_paths=[genes, iso], output_dir=job.tempDir)
    tarball_files('rsem_hugo.tar.gz', file_paths=hugo_files, output_dir=job.tempDir)
    rsem_id = job.fileStore.writeGlobalFile(os.path.join(job.tempDir, 'rsem.tar.gz'))
    hugo_id = job.fileStore.writeGlobalFile(os.path.join(job.tempDir, 'rsem_hugo.tar.gz'))
    return rsem_id, hugo_id


def run_hera(job, r1_id, r2_id, hera_index_url):
    """
    RNA-seq quantification using Hera

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str r1_id: FileStoreID of fastq (pair 1)
    :param str r2_id: FileStoreID of fastq (pair 2 if applicable, otherwise pass None for single-end)
    :param str hera_index_url: URL to hera index file
    :return: FileStoreID of Hera outputs
    :rytpe: str
    """
    # Download and process hera index
    download_url(url=hera_index_url, name='hera-index.tar.gz', work_dir=job.tempDir)
    subprocess.check_call(['tar', '-xvf', os.path.join(job.tempDir, 'hera-index.tar.gz'), '-C', job.tempDir])
    os.remove(os.path.join(job.tempDir, 'hera-index.tar.gz'))
    hera_index = os.path.join('/data', os.listdir(job.tempDir)[0]) if len(os.listdir(job.tempDir)) == 1 else '/data'

    # Define parameters
    parameters = ['quant',
                  '-i', hera_index,
                  '-t', str(job.cores),
                  '-b', '100',  # Bootstraps
                  '-w', '1',  # Output BAM (1 = no output)
                  '/data/R1.fastq']

    # Read in fastq(s)
    job.fileStore.readGlobalFile(r1_id, os.path.join(job.tempDir, 'R1.fastq'))
    if r1_id and r2_id:
        job.fileStore.readGlobalFile(r2_id, os.path.join(job.tempDir, 'R2.fastq'))
        parameters.append('/data/R2.fastq')

    # Call: Hera
    dockerCall(job, parameters=parameters, workDir=job.tempDir, tool=hera_version)

    # Tar output files, store in fileStore, and return FileStoreID
    output_names = ['abundance.gene.tsv', 'abundance.h5', 'abundance.tsv', 'fusion.bedpe', 'summary']
    output_files = [os.path.join(job.tempDir, x) for x in output_names]
    tarball_files(tar_name='hera.tar.gz', file_paths=output_files, output_dir=job.tempDir)
    return job.fileStore.writeGlobalFile(os.path.join(job.tempDir, 'hera.tar.gz'))
