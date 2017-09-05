import os
from urlparse import urlparse

from toil.lib.docker import dockerCheckOutput, dockerCall
from toil_lib.files import copy_files
from toil_lib.tools.preprocessing import run_cutadapt
from toil_lib.urls import download_url

from utils import docker_path


def cleanup_ids(job, ids_to_delete):
    """
    Delete fileStoreIDs for files no longer needed

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param list ids_to_delete: list of FileStoreIDs to delete
    """
    [job.fileStore.deleteGlobalFile(x) for x in ids_to_delete if x is not None]


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
        out = dockerCheckOutput(job, workDir=work_dir, parameters=parameters,
                                tool='quay.io/ucsc_cgl/samtools:1.5--98b58ba05641ee98fa98414ed28b53ac3048bc09')
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
    dockerCall(job, workDir=work_dir, parameters=parameters,
               tool='quay.io/ucsc_cgl/samtools:1.5--98b58ba05641ee98fa98414ed28b53ac3048bc09')


def convert_bam_to_fastq(job, bam_path, check_paired=True, ignore_validation_errors=True):
    """
    Converts BAM to a pair of FASTQ files

    :param JobFunctionWrappingJob job: passed automatically by Toil
    :param str bam_path: Path to BAM
    :param bool check_paired: If True, checks whether BAM is paired-end
    :return: FileStoreIDs for R1 and R2
    :rtype: tuple
    """
    if check_paired:
        assert_bam_is_paired_end(job, bam_path)

    work_dir = os.path.dirname(os.path.abspath(bam_path))
    parameters = ['SamToFastq', 'I={}'.format(docker_path(bam_path)), 'F=/data/R1.fq', 'F2=/data/R2.fq']
    if ignore_validation_errors:
        parameters.append('VALIDATION_STRINGENCY=SILENT')
    dockerCall(job=job, workDir=work_dir, parameters=parameters,
               tool='quay.io/ucsc_cgl/picardtools:2.10.9--23fc31175415b14dbf337216f9ae14d3acc3d1eb')
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
    dockerCall(job, tool='sbamin/gdc-client:1.2.0', parameters=parameters, workDir=work_dir)
    files = [x for x in os.listdir(os.path.join(work_dir, parsed_url.netloc)) if x.lower().endswith('.bam')]
    assert len(files) == 1, 'More than one BAM found from GDC URL: {}'.format(files)
    bam_path = os.path.join(work_dir, parsed_url.netloc, files[0])
    return bam_path