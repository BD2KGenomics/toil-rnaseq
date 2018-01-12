import os
from urlparse import urlparse

from toil.lib.docker import dockerCall
from toil.lib.docker import dockerCheckOutput

from toil_rnaseq.tools import gdc_version
from toil_rnaseq.tools import picardtools_version
from toil_rnaseq.tools import samtools_version
from toil_rnaseq.utils import docker_path
from toil_rnaseq.utils.files import copy_files
from toil_rnaseq.utils.urls import move_or_upload


def assert_bam_is_paired_end(job, bam_path, region='chr6'):
    """
    Confirm that a BAM is paired-end and not single-end. Raises an error if not paired-end

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str region: Region of the genome to select
    :param str bam_path: Path to BAM
    """
    # Check if BAM index exists, otherwise index BAM
    bam_no_ext = os.path.splitext(bam_path)[0]
    if not os.path.exists(bam_no_ext + '.bai') and not os.path.exists(bam_no_ext + '.bam.bai'):
        index_bam(job, bam_path)

    docker_bam_path = docker_path(bam_path)
    work_dir = os.path.dirname(os.path.abspath(bam_path))

    # Check for both "chr" and no "chr" format
    results = []
    regions = [region, 'chr' + region] if 'chr' not in region else [region, region.lstrip('chr')]
    for r in regions:
        parameters = ['view', '-c', '-f', '1',
                      docker_bam_path,
                      r]  # Chr6 chosen for testing, any region with reads will work
        out = dockerCheckOutput(job, workDir=work_dir, parameters=parameters, tool=samtools_version)
        results.append(int(out.strip()))
    assert any(x for x in results if x != 0), 'BAM is not paired-end, aborting run.'


def index_bam(job, bam_path):
    """
    Creates a BAM index (.bai) in the same directory as the BAM
    Indexing is necessary for viewing slices of the BAM

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str bam_path: Path to BAM
    """
    work_dir = os.path.dirname(os.path.abspath(bam_path))
    parameters = ['index', docker_path(bam_path)]
    dockerCall(job, workDir=work_dir, parameters=parameters, tool=samtools_version)


def convert_bam_to_fastq(job, bam_path, check_paired=True, ignore_validation_errors=True):
    """
    Converts BAM to a pair of FASTQ files

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str bam_path: Path to BAM
    :param bool check_paired: If True, checks whether BAM is paired-end
    :param bool ignore_validation_errors: If True, ignores validation errors from picardTools
    :return: FileStoreIDs for R1 and R2
    :rtype: tuple
    """
    if check_paired:
        assert_bam_is_paired_end(job, bam_path)

    work_dir = os.path.dirname(os.path.abspath(bam_path))
    parameters = ['SamToFastq', 'I={}'.format(docker_path(bam_path)), 'F=/data/R1.fq', 'F2=/data/R2.fq']
    if ignore_validation_errors:
        parameters.append('VALIDATION_STRINGENCY=SILENT')
    dockerCall(job=job, workDir=work_dir, parameters=parameters, tool=picardtools_version)
    r1 = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'R1.fq'))
    r2 = job.fileStore.writeGlobalFile(os.path.join(work_dir, 'R2.fq'))
    return r1, r2


def download_bam_from_gdc(job, work_dir, url, token):
    """
    Downloads BAM file from the GDC using an url (format: "gdc://<GDC ID>") and a GDC access token

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str work_dir: Directory being mounted into Docker
    :param str url: gdc URL to be downloaded
    :param str token: Full path to token
    :return: Path to BAM
    :rtype: str
    """
    assert token, 'gdc_token is missing which is required for downloading. Check config.'
    copy_files([os.path.abspath(token)], work_dir)

    parsed_url = urlparse(url)
    parameters = ['download',
                  '-d', '/data',
                  '-t', '/data/{}'.format(os.path.basename(token)),
                  parsed_url.netloc]
    dockerCall(job, tool=gdc_version, parameters=parameters, workDir=work_dir)
    files = [x for x in os.listdir(os.path.join(work_dir, parsed_url.netloc)) if x.lower().endswith('.bam')]
    assert len(files) == 1, 'More than one BAM found from GDC URL: {}'.format(files)
    bam_path = os.path.join(work_dir, parsed_url.netloc, files[0])
    return bam_path


def sort_and_save_bam(job, config, bam_id, skip_sort=True):
    """
    Sorts STAR's output BAM using samtools

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param Namespace config: Argparse Namespace object containing argument inputs
    :param bool skip_sort: If True, skips sort step and upload BAM
    :param FileID bam_id: FileID for STARs genome aligned bam
    """
    bam_path = os.path.join(job.tempDir, 'aligned.bam')
    sorted_bam = os.path.join(job.tempDir, '{}.sorted.bam'.format(config.uuid))
    job.fileStore.readGlobalFile(bam_id, bam_path)

    parameters = ['sort',
                  '-o', '/data/{}.sorted.bam'.format(config.uuid),
                  '-O', 'bam',
                  '-T', 'temp',
                  '-@', str(job.cores),
                  '/data/aligned.bam']

    if skip_sort:
        job.log('Skipping samtools sort as STAR already sorted BAM')
        os.rename(bam_path, sorted_bam)
    else:
        dockerCall(job, tool=samtools_version, parameters=parameters, workDir=job.tempDir)

    move_or_upload(config, files=[sorted_bam])
