from __future__ import print_function

import errno
import os
import textwrap
from collections import OrderedDict, defaultdict
from urlparse import urlparse

from toil_rnaseq.utils.expando import Expando

schemes = ('file', 'http', 's3', 'ftp', 'gdc')
_iter_types = (list, tuple, set, frozenset)


def parse_samples(path_to_manifest=None):
    """
    Parses samples from manifest

    :param str path_to_manifest: Path to manifest file containing sample information
    :return: Samples and their attributes as defined in the manifest
    :rtype: list(list(str, str, str, str))
    """
    samples = []
    with open(path_to_manifest, 'r') as f:
        for line in f.readlines():
            if not line.isspace() and not line.startswith('#'):
                sample = line.strip().split('\t')

                # Enforce number of columns
                require(len(sample) == 4, 'Bad manifest format! '
                                          'Expected 4 tab separated columns, User: "{}"'.format(sample))

                # Unpack sample information
                file_type, paired, uuid, url = sample

                # Check file_type
                file_types = ['tar', 'fq', 'bam']
                require(file_type in file_types, '1st column is not valid {}. User: "{}"'.format(file_types, file_type))

                # Check paired/unpaired
                pair_types = ['paired', 'single']
                require(paired in pair_types, '2nd column is not valid {}. User: "{}"'.format(pair_types, paired))

                # If paired fastq data, ensure correct number of URLs
                if file_type == 'fq' and paired == 'paired':
                    require(len(url.split(',')) % 2 == 0, 'Paired fastqs require an even number of URLs separated'
                                                          ' by a comma: User: "{}"'.format(url))
                samples.append(sample)
    return samples


def generate_config():
    return textwrap.dedent("""
        ##############################################################################################################
        #                               TOIL RNA-SEQ WORKFLOW CONFIGURATION FILE                                     #
        ##############################################################################################################

        # This configuration file is formatted in YAML. Simply write the value (at least one space) after the colon.
        # Edit the values in this configuration file and then rerun the pipeline: "toil-rnaseq run"
        # Just Kallisto or STAR/RSEM can be run by supplying only the inputs to those tools
        #
        # URLs can take the form: http://, ftp://, file://, s3://, gdc://
        # Local inputs follow the URL convention: file:///full/path/to/input
        # S3 URLs follow the convention: s3://bucket/directory/file.txt
        #
        # Comments (beginning with #) do not need to be removed. Optional parameters left blank are treated as false.

        ##############################################################################################################
        #                                           REQUIRED OPTIONS                                                 #
        ##############################################################################################################

        # Required: Output location of sample. Can be full path to a directory or an s3:// URL
        # WARNING: S3 buckets must exist prior to upload, or it will fail.
        output-dir: 

        ##############################################################################################################
        #                            WORKFLOW INPUTS (Alignment and Quantification)                                  #
        ##############################################################################################################

        # URL {scheme} to index tarball used by STAR
        star-index: http://courtyard.gi.ucsc.edu/~jvivian/toil-rnaseq-inputs/starIndex_hg38_no_alt.tar.gz

        # URL {scheme} to reference tarball used by RSEM
        # Running RSEM requires a star-index as a well as an rsem-ref
        rsem-ref: http://courtyard.gi.ucsc.edu/~jvivian/toil-rnaseq-inputs/rsem_ref_hg38_no_alt.tar.gz

        # URL {scheme} to kallisto index file. 
        kallisto-index: http://courtyard.gi.ucsc.edu/~jvivian/toil-rnaseq-inputs/kallisto_hg38.idx
        
        # URL {scheme} to hera index
        hera-index: http://courtyard.gi.ucsc.edu/~jvivian/toil-rnaseq-inputs/hera-index.tar.gz
        
        # Maximum file size of input sample (for resource allocation during initial download)
        max-sample-size: 20G

        ##############################################################################################################
        #                                   WORKFLOW OPTIONS (Quality Control)                                       #
        ##############################################################################################################

        # If true, will preprocess samples with cutadapt using adapter sequences
        cutadapt: true

        # Adapter sequence to trim when running CutAdapt. Defaults set for Illumina
        fwd-3pr-adapter: AGATCGGAAGAG

        # Adapter sequence to trim (for reverse strand) when running CutAdapt. Defaults set for Illumina
        rev-3pr-adapter: AGATCGGAAGAG

        # If true, will run FastQC and include QC in sample output
        fastqc: true 
        
        # If true, will run UMEND BamQC and include statistics about Uniquely Mapped Exonic Non-Duplicate (UMEND) reads
        # If bamqc and save-bam are enabled, a bam with duplicates marked (output of BAMQC) is saved
        bamqc: 

        ##############################################################################################################
        #                   CREDENTIAL OPTIONS (for downloading samples from secure locations)                       #
        ##############################################################################################################        

        # Optional: Provide a full path to a 32-byte key used for SSE-C Encryption in Amazon
        ssec: 

        # Optional: Provide a full path to the token.txt used to download from the GDC
        gdc-token: 

        ##############################################################################################################
        #                                   ADDITIONAL FILE OUTPUT OPTIONS                                           #
        ##############################################################################################################        

        # Optional: If true, saves the wiggle file (.bg extension) output by STAR
        # WARNING: Requires STAR sorting, which has memory leak issues that can crash the workflow 
        wiggle: 

        # Optional: If true, saves the aligned BAM (by coordinate) produced by STAR
        # You must also specify an ssec key if you want to upload to the s3-output-dir
        # as read data is assumed to be controlled access
        save-bam: 

        ##############################################################################################################
        #                                           DEVELOPER OPTIONS                                                #
        ##############################################################################################################        

        # Optional: If true, uses resource requirements appropriate for continuous integration
        ci-test: 
    """.format(scheme=[x + '://' for x in schemes])[1:])


def user_input_config(config_path):
    """
    User input of workflow configuration file

    :param str config_path: Path to configuration file
    :return: Configuration file path or None if user skips
    :rtype: str
    """
    print('\n\t\t\tUser Input of Toil-rnaseq Configuration File\n')
    start = raw_input('Type Y/y and hit enter to continue: ').lower()
    if start != 'y':
        return None
    print('User will see comments for a configuation option followed by "<OPTION>: [Default Value]"')
    print('\tN/n to skip\n\ttrue/false for boolean statements\n\tq/quit to stop\n\tEnter key to submit option\n')

    config = OrderedDict()
    comments = defaultdict(list)
    quit_flag = False
    config_template = generate_config().split('\n')
    for line in config_template:
        if not line.startswith('#') and line:
            option, default = line.split(': ')

            # Fetch comments for current option
            index = config_template.index(line) - 1
            while True:
                comments[option].insert(0, config_template[index])
                index -= 1
                if not config_template[index]:
                    break
            if quit_flag:
                config[option] = default
                continue
            print('\n'.join(comments[option]) + '\n\n')

            # Show option and get user input
            user_input = None
            while not user_input:
                user_input = raw_input('\n{}: [{}]\n\tUser Input: '.format(option, default)).lower()
            if user_input == 'q' or user_input == 'quit':
                quit_flag = True
                config[option] = default
                continue
            elif user_input == 'n':
                config[option] = default
                continue
            else:
                config[option] = user_input

    print('Writing out configuration file to: {}'.format(config_path))
    with open(config_path, 'w') as f:
        for option in config:
            f.write('\n'.join(comments[option]))
            f.write('\n{}: {}\n\n'.format(option, config[option]))
    return config_path


def generate_manifest():
    return textwrap.dedent("""
        ##############################################################################################################
        #                                    TOIL RNA-SEQ WORKFLOW MANIFEST FILE                                     #
        ##############################################################################################################
        #   Edit this manifest to include information pertaining to each sample to be run.
        #   There are 4 tab-separated columns: filetype, paired/unpaired, UUID, URL(s) to sample
        #
        #   filetype    Filetype of the sample. Options: "tar", "fq", or "bam" for tarball, fastq/fastq.gz, or BAM
        #   paired      Indicates whether the data is paired or single-ended. Options:  "paired" or "single"
        #   UUID        This should be a unique identifier for the sample to be processed
        #   URL         A URL starting with {scheme} that points to the sample
        #
        #   If sample is being submitted as a fastq or several fastqs, provide URLs separated by a comma.
        #   If providing paired fastqs, alternate the fastqs so every R1 is paired with its R2 as the next URL.
        #   Samples must have the same extension - do not mix and match gzip and non-gzipped sample pairs.
        #
        #   Samples consisting of tarballs with fastq files inside must follow the file name convention of
        #   ending in an R1/R2 or _1/_2 followed by one of the 4 extensions: .fastq.gz, .fastq, .fq.gz, .fq
        #
        #   BAMs are accepted, but must have been aligned from paired reads NOT single-end reads.
        #
        #   GDC URLs may only point to individual BAM files. No other format is accepted.
        #
        #   Examples of several combinations are provided below. Lines beginning with # are ignored.
        #
        #   tar paired  UUID_1  file:///path/to/sample.tar
        #   fq  paired  UUID_2  file:///path/to/R1.fq.gz,file:///path/to/R2.fq.gz
        #   tar single  UUID_3  http://sample-depot.com/single-end-sample.tar
        #   tar paired  UUID_4  s3://my-bucket-name/directory/paired-sample.tar.gz
        #   fq  single  UUID_5  s3://my-bucket-name/directory/single-end-file.fq
        #   bam paired  UUID_6  gdc://1a5f5e03-4219-4704-8aaf-f132f23f26c7
        #
        #   Place your samples below, one per line.
        """.format(scheme=[x + '://' for x in schemes])[1:])


def user_input_manifest(manifest_path):
    """
    User input of workflow manifest file

    :param str manifest_path: Path to write out manifest
    :return: Path to manifest or None if user skips
    :rtype: str
    """
    print('\n\t\t\tUser Input of Toil-rnaseq Manifest')
    start = raw_input('Type Y/y and hit enter to continue: ').lower()
    if start != 'y':
        return None
    print('\n'.join(generate_manifest().split('\n')[:-1]))  # Don't print last line of manifest
    print('\n\nFollow the prompts to enter sample information, based on the information above.\n')

    samples = []
    while True:
        filetype, paired, uuid = None, None, None
        url = 'bad-url'
        while filetype not in ['tar', 'fq', 'bam']:
            filetype = raw_input('Enter the filetype of the sample: ')
        while paired not in ['paired', 'single']:
            paired = raw_input('Enter whether sample is paired or single-end: ')
        uuid = raw_input('Enter unique name (or UUID) of sample: ')
        while urlparse(url).scheme not in schemes:
            url = raw_input('Enter URL for sample: ')
        samples.append((filetype, paired, uuid, url))

        # Escape loop
        user = raw_input('\nType q/quit to exit or enter any other key to add more samples\n').lower()
        if user == 'q' or user == 'quit':
            break

    print('Writing out manifest file to: {}'.format(manifest_path))
    with open(manifest_path, 'w') as f:
        f.write(generate_manifest() + '\n')
        for sample in samples:
            assert len(sample) == 4, 'Something is wrong. Expected 4 options per sample: {}'.format(sample)
            f.write('\t'.join(sample) + '\n')
    return manifest_path


def configuration_sanity_checks(config):
    """
    Sanity check configuration file

    :param Expando config: Dict-like object containing workflow options as attributes
    :return: `config` with appropriate changes to output_dir
    :rtype: Expando
    """
    # Ensure there are inputs to run something
    require(config.kallisto_index or config.star_index or config.hera_index,
            'URLs not provided for Kallisto, STAR, or Hera, so there is nothing to do!')

    # If running STAR or RSEM, ensure both inputs exist
    if config.star_index or config.rsem_ref:
        require(config.star_index and config.rsem_ref, 'Input provided for STAR or RSEM but not both. STAR: '
                                                       '"{}", RSEM: "{}"'.format(config.star_index, config.rsem_ref))

    # Ensure file inputs have allowed URL schemes
    for file_input in [x for x in [config.kallisto_index, config.star_index, config.rsem_ref, config.hera_index] if x]:
        require(urlparse(file_input).scheme in schemes,
                'Input "{}" in config must have the appropriate URL prefix: {}'.format(file_input, schemes))

    # Output dir checks and handling
    require(config.output_dir, 'No output location specified: {}'.format(config.output_dir))
    if not config.output_dir.startswith('/'):
        if urlparse(config.output_dir).scheme == 'file':
            config.output_dir = config.output_dir.split('file://')[1]
            if not config.output_dir.startswith('/'):
                raise UserError('Output dir neither starts with / or is an S3 URL')
        elif not urlparse(config.output_dir).scheme == 's3':
            raise UserError('Output dir neither starts with / or is an S3 URL')

    if not config.output_dir.endswith('/'):
        config.output_dir += '/'

    # Create directory if local and doesn't exist
    if config.output_dir.startswith('/'):
        if not os.path.exists(config.output_dir):
            mkdir_p(config.output_dir)

    # Program checks
    for program in ['curl', 'docker']:
        require(next(which(program), None), program + ' must be installed on every node.'.format(program))

    return config


def docker_path(path):
    """
    Converts a path to a file to a "docker path" which replaces the dirname with '/data'

    :param str path: Path to file
    :return: Path for use in Docker parameters
    :rtype: str
    """
    return os.path.join('/data', os.path.basename(path))


def rexpando(d):
    """
    Recursive Expando!

    Recursively iterate through a nested dict / list object
    to convert all dictionaries to Expando objects

    :param dict d: Dictionary to convert to nested Expando objects
    :return: Converted dictionary
    :rtype: Expando
    """
    e = Expando()
    for k, v in d.iteritems():
        k = _key_to_attribute(k)
        if isinstance(v, dict):
            e[k] = rexpando(v)
        elif isinstance(v, _iter_types):
            e[k] = _rexpando_iter_helper(v)
        else:
            e[k] = v
    return e


def _rexpando_iter_helper(input_iter):
    """
    Recursively handle iterables for rexpando

    :param iter input_iter: Iterable to process
    :return: Processed iterable
    :rtype: list
    """
    l = []
    for v in input_iter:
        if isinstance(v, dict):
            l.append(rexpando(v))
        elif isinstance(v, _iter_types):
            l.append(_rexpando_iter_helper(v))
        else:
            l.append(v)
    return l


def _key_to_attribute(key):
    """
    Processes key for attribute accession by replacing illegal chars with a single '_'

    :param str key: Dictionary key to process
    :return: Processed key
    :rtype: str
    """
    illegal_chars = ['!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '.', ',', '+', '/', '\\', ':', ';']
    for c in illegal_chars:
        key = key.replace(c, '_')
    return '_'.join(x for x in key.split('_') if x)  # Remove superfluous '_' chars


# General python functionss
def flatten(x):
    """
    Flattens a nested array into a single list

    :param list x: The nested list/tuple to be flattened.
    """
    result = []
    for el in x:
        if hasattr(el, "__iter__") and not isinstance(el, basestring):
            result.extend(flatten(el))
        else:
            result.append(el)
    return result


def partitions(l, partition_size):
    """
    >>> list(partitions([], 10))
    []
    >>> list(partitions([1,2,3,4,5], 1))
    [[1], [2], [3], [4], [5]]
    >>> list(partitions([1,2,3,4,5], 2))
    [[1, 2], [3, 4], [5]]
    >>> list(partitions([1,2,3,4,5], 5))
    [[1, 2, 3, 4, 5]]

    :param list l: List to be partitioned
    :param int partition_size: Size of partitions
    """
    for i in xrange(0, len(l), partition_size):
        yield l[i:i + partition_size]


# Pseudo-bash commands
def mkdir_p(path):
    """
    The equivalent of mkdir -p
    """
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def which(name, path=None):
    """
    Look for an executable file of the given name in the given list of directories,
    or the directories listed in the PATH variable of the current environment. Roughly the
    equivalent of the `which` program. Does not work on Windows.

    :type name: str
    :param name: the name of the program

    :type path: Iterable
    :param path: the directory paths to consider or None if the directories referenced in the
    PATH environment variable should be used instead

    :returns: an iterator yielding the full path to every occurrance of an executable file of the
    given name in a directory on the given path or the PATH environment variable if no path was
    passed

    >>> next( which('ls') )
    '/bin/ls'
    >>> list( which('asdalskhvxjvkjhsdasdnbmfiewwewe') )
    []
    >>> list( which('ls', path=()) )
    []
    """
    if path is None:
        path = os.environ.get('PATH')
        if path is None:
            return
        path = path.split(os.pathsep)
    for bin_dir in path:
        executable_path = os.path.join(bin_dir, name)
        if os.access(executable_path, os.X_OK):
            yield executable_path


# Error handling
class UserError(Exception):
    pass


def require(expression, message):
    if not expression:
        raise UserError('\n\n' + message + '\n\n')
