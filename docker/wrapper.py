from __future__ import print_function
import logging

from toil.job import Job
from toil_lib.abstractPipelineWrapper import AbstractPipelineWrapper

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()


desc = """
Computational Genomics Lab, Genomics Institute, UC Santa Cruz
Dockerized Toil RNA-seq pipeline

RNA-seq fastqs are combined, aligned, and quantified with 2 different methods (RSEM and Kallisto)

General Usage:
docker run -v $(pwd):$(pwd) -v /var/run/docker.sock:/var/run/docker.sock \
quay.io/ucsc_cgl/rnaseq-cgl-pipeline --samples sample1.tar

Please see the complete documentation located at:
https://github.com/BD2KGenomics/cgl-docker-lib/tree/master/rnaseq-cgl-pipeline
or inside the container at: /opt/rnaseq-pipeline/README.md


Structure of RNA-Seq Pipeline (per sample)

              3 -- 4 -- 5
             /          |
  0 -- 1 -- 2 ---- 6 -- 8
             \          |
              7 ---------

0 = Download sample
1 = Unpack/Merge fastqs
2 = CutAdapt (adapter trimming)
3 = STAR Alignment
4 = RSEM Quantification
5 = RSEM Post-processing
6 = Kallisto
7 = FastQC
8 = Consoliate output and upload to S3
=======================================
Dependencies
Docker
"""
class RnaseqPipelineWrapper(AbstractPipelineWrapper):
    def _extend_argument_parser(self, parser):
        parser.add_argument('--samples', nargs='+', required=True,
                            help='Absolute path(s) to sample tarballs.')

    def _extend_pipeline_command(self, command, args):
        if args.cores is not None:
            command.append('--maxCores={}'.format(args.cores))
        command.append('--samples')
        command.extend('file://' + x for x in args.samples)

    def _add_option(self, arg_parser, name, *args, **kwargs):
        if name == 'output-dir':
            del kwargs['default']
            arg_parser.add_argument('--' + name, default=str('file://' + self._get_mount_path()),
                                    *args, **kwargs)
        else:
            super(RnaseqPipelineWrapper, self)._add_option(arg_parser, name, *args, **kwargs)

if __name__ == '__main__':
    RnaseqPipelineWrapper.run('toil-rnaseq', desc)
