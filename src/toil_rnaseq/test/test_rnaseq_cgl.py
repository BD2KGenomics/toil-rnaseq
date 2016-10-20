import logging
import shlex
import shutil
import subprocess
import tempfile
import textwrap
from contextlib import closing
from unittest import TestCase
from urlparse import urlparse
from uuid import uuid4

import os
import posixpath
from bd2k.util.iterables import concat
from boto.s3.connection import S3Connection, Bucket

log = logging.getLogger(__name__)


class RNASeqCGLTest(TestCase):
    """
    These tests *can* be parameterized with the following optional environment variables:

    TOIL_SCRIPTS_TEST_TOIL_OPTIONS - a space-separated list of additional command line arguments to pass to Toil via
    the script entry point. Default is the empty string.

    TOIL_SCRIPTS_TEST_JOBSTORE - the job store locator to use for the tests. The default is a file: locator pointing
    at a local temporary directory.

    TOIL_SCRIPTS_TEST_NUM_SAMPLES - the number of sample lines to generate in the input manifest
    """

    @classmethod
    def setUpClass(cls):
        super(RNASeqCGLTest, cls).setUpClass()
        # FIXME: pull up into common base class
        logging.basicConfig(level=logging.INFO)

    def setUp(self):
        self.input_dir = urlparse('s3://cgl-pipeline-inputs/rnaseq_cgl/ci')
        self.output_dir = urlparse('s3://cgl-driver-projects/test/ci/%s' % uuid4())
        self.sample = urlparse(self.input_dir.geturl() + '/chr6_sample.tar.gz')
        self.workdir = tempfile.mkdtemp()
        jobStore = os.getenv('TOIL_SCRIPTS_TEST_JOBSTORE', os.path.join(self.workdir, 'jobstore-%s' % uuid4()))
        toilOptions = shlex.split(os.environ.get('TOIL_SCRIPTS_TEST_TOIL_OPTIONS', ''))
        self.base_command = concat('toil-rnaseq', 'run',
                                   '--config', self._generate_config(),
                                   '--retryCount', '1',
                                   toilOptions,
                                   jobStore)

    def test_samples_option(self):
        self._run(self.base_command, '--samples', self.sample.geturl())
        self._assertOutput()

    def test_manifest(self):
        num_samples = int(os.environ.get('TOIL_SCRIPTS_TEST_NUM_SAMPLES', '1'))
        self._run(self.base_command, '--manifest', self._generate_manifest(num_samples))
        self._assertOutput(num_samples)

    def _run(self, *args):
        args = list(concat(*args))
        log.info('Running %r', args)
        subprocess.check_call(args)

    def _assertOutput(self, num_samples=None):
        with closing(S3Connection()) as s3:
            bucket = Bucket(s3, self.output_dir.netloc)
            prefix = self.output_dir.path[1:]
            for i in range(1 if num_samples is None else num_samples):
                output_file = self._sample_name(None if num_samples is None else i) + '.tar.gz'
                output_file = 'FAIL.' + output_file  # This flag is added by bamQC
                key = bucket.get_key(posixpath.join(prefix, output_file), validate=True)
                # FIXME: We may want to validate the output a bit more
                self.assertTrue(key.size > 0)

    def tearDown(self):
        shutil.rmtree(self.workdir)
        with closing(S3Connection()) as s3:
            bucket = Bucket(s3, self.output_dir.netloc)
            prefix = self.output_dir.path[1:]
            for key in bucket.list(prefix=prefix):
                assert key.name.startswith(prefix)
                key.delete()

    def _generate_config(self):
        path = os.path.join(self.workdir, 'config-toil-rnaseq.yaml')
        with open(path, 'w') as f:
            f.write(textwrap.dedent("""
                    star-index: {input_dir}/starIndex_chr6.tar.gz
                    kallisto-index: s3://cgl-pipeline-inputs/rnaseq_cgl/kallisto_hg38.idx
                    rsem-ref: {input_dir}/rsem_ref_chr6.tar.gz
                    output-dir: {output_dir}
                    fastqc: true
                    cutadapt:
                    ssec:
                    gtkey:
                    wiggle:
                    save-bam:
                    fwd-3pr-adapter: AGATCGGAAGAG
                    rev-3pr-adapter: AGATCGGAAGAG
                    ci-test: true
                    bamqc: true
                    """[1:]).format(output_dir=self.output_dir.geturl(),
                                    input_dir=self.input_dir.geturl()))
        return path

    def _generate_manifest(self, num_samples):
        path = os.path.join(self.workdir, 'manifest-toil-rnaseq.tsv')
        with open(path, 'w') as f:
            f.write('\n'.join('\t'.join(['tar', 'paired', self._sample_name(i), self.sample.geturl()])
                              for i in range(num_samples)))
        return path

    def _sample_name(self, i=None):
        uuid = posixpath.basename(self.sample.path).split('.')
        while uuid[-1] in ('gz', 'tar', 'zip'):
            uuid.pop()
        uuid = '.'.join(uuid)
        if i is not None:
            uuid = '%s_%i' % (uuid, i)
        return uuid
