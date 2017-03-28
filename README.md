## University of California, Santa Cruz Genomics Institute
### Guide: Running the CGL HG38 RNA-seq Pipeline using Toil

This guide attempts to walk the user through running this pipeline from start to finish. If there are any questions
please contact [John Vivian](jtvivian@gmail.com). If you find any errors or corrections please feel free to make a 
pull request. Feedback of any kind is appreciated.

- [Dependencies](#dependencies)
- [Installation](#installation)
- [Usage](#general-usage)

For detailed information and troubleshooting, see the [Wiki](https://github.com/BD2KGenomics/toil-rnaseq/wiki)
- [Inputs](https://github.com/BD2KGenomics/toil-rnaseq/wiki/Pipeline-Inputs)
- [Methods](https://github.com/BD2KGenomics/toil-rnaseq/wiki/Methods)
- [Troubleshooting](https://github.com/BD2KGenomics/toil-rnaseq/wiki/Troubleshooting)


## Overview

RNA-seq fastqs are combined, aligned, and quantified with 2 different methods (RSEM and Kallisto)

This pipeline produces a tarball (tar.gz) file for a given sample with 3 main subdirectories: Kallisto, RSEM, and QC.
If the pipeline is run with all possible options (`fastqc`, `bamqc`, etc), the output tar
will have the following structure (once uncompressed):

```
SAMPLE
├── Kallisto
│   ├── abundance.h5
│   ├── abundance.tsv
│   ├── fusion.txt
│   └── run_info.json
├── QC
│   ├── bamQC
│   │   ├── readDist.txt
│   │   ├── readDist.txt_PASS_qc.txt
│   │   ├── rnaAligned.out.md.sorted.geneBodyCoverage.curves.pdf
│   │   └── rnaAligned.out.md.sorted.geneBodyCoverage.txt
│   ├── fastQC
│   │   ├── R1_fastqc.html
│   │   ├── R1_fastqc.zip
│   │   ├── R2_fastqc.html
│   │   └── R2_fastqc.zip
│   └── STAR
│       ├── Log.final.out
│       └── SJ.out.tab
└── RSEM
    ├── Hugo
    │   ├── rsem_genes.hugo.results
    │   └── rsem_isoforms.hugo.results
    ├── rsem_genes.results
    └── rsem_isoforms.results
```
If the user selects options such as `save-bam` or `wiggle`, additional files will appear in the output directory:

- SAMPLE.sorted.bam OR SAMPLE.sortedByCoord.md.bam if `bamQC` step is enabled.
- SAMPLE.wiggle.bg

The output tarball is prepended with the unique name for the sample (e.g. SAMPLE.tar.gz). 

# Dependencies

This pipeline has been tested on Ubuntu 14.04, 16.04 and Mac OSX, but should also run on other unix based systems.  
`apt-get` and `pip` often require `sudo` privilege, so if the below commands fail, try prepending `sudo`.  
If you do not have `sudo`  privileges you will need to build these tools from source, 
or bug a sysadmin about how to get them (they don't mind). 

#### General Dependencies

    1. Python 2.7
    2. Curl         apt-get install curl
    3. Docker       http://docs.docker.com/engine/installation/

#### Python Dependencies

    1. Toil         pip install toil
    2. S3AM         pip install s3am (optional, needed for uploading output to S3)
    
    
#### System Dependencies

This pipeline needs approximately 50G of RAM in order to run STAR alignment. 

# Installation

The CGL RNA-seq pipeline is pip installable!

For most users, the preferred installation method is inside of a virtualenv to avoid dependency conflicts: 

- `virtualenv ~/toil-rnaseq` 
- `source ~/toil-rnaseq/bin/activate`
- `pip install toil`
- `pip install toil-rnaseq`

After installation, the pipeline can be executed by typing `toil-rnaseq` into the teriminal.

If there is an existing, system-wide installation of **Toil**, as is the case when using **CGCloud**, 
the `pip install toil` step should be skipped and the virtualenv should be invoked with `--system-site-packages`. 
This way the existing Toil installation will be available inside the virtualenv.

# General Usage

First, obtain all of the necessary [inputs](https://github.com/BD2KGenomics/toil-rnaseq/wiki/Pipeline-Inputs).

Then, type `toil-rnaseq` to get basic help menu and instructions
 
1. Type `toil-rnaseq generate` to create an editable manifest and config in the current working directory.
2. Parameterize the pipeline by editing the config.
3. Fill in the manifest with information pertaining to your samples.
4. Type `toil-rnaseq run [jobStore]` to execute the pipeline.

### Example Commands

Run sample(s) locally using the manifest

1. `toil-rnaseq generate`
2. Fill in config and manifest
3. `toil-rnaseq run ./example-jobstore`

Toil options can be appended to `toil-rnaseq run`, for example:
`toil-rnaseq run ./example-jobstore --retryCount=1 --workDir=/data`

For a complete list of Toil options, just type `toil-rnaseq run -h`

Run a variety of samples locally

1. `toil-rnaseq generate-config`
2. Fill in config
3. `toil-rnaseq run ./example-jobstore --retryCount=1 --workDir=/data --samples \
    s3://example-bucket/sample_1.tar file:///full/path/to/sample_2.tar https://sample-depot.com/sample_3.tar`

### Example Config

```
star-index: s3://cgl-pipeline-inputs/rnaseq_cgl/ci/starIndex_chr6.tar.gz
kallisto-index: s3://cgl-pipeline-inputs/rnaseq_cgl/kallisto_hg38.idx
rsem-ref: s3://cgl-pipeline-inputs/rnaseq_cgl/ci/rsem_ref_chr6.tar.gz
output-dir: /data/my-toil-run
cutadapt: true
fastqc: true
bamqc:
fwd-3pr-adapter: AGATCGGAAGAG
rev-3pr-adapter: AGATCGGAAGAG
ssec:
gtkey:
wiggle:
save-bam:
ci-test:
```

Example with local input files

```
star-index: file://data/starIndex_chr6.tar.gz
kallisto-index: file://data/kallisto_hg38.idx
rsem-ref: file://data/rsem_ref_chr6.tar.gz
output-dir: /data/my-toil-run
cutadapt: true
fastqc: true
bamqc:
fwd-3pr-adapter: AGATCGGAAGAG
rev-3pr-adapter: AGATCGGAAGAG
ssec:
gtkey:
wiggle:
save-bam:
ci-test:
```

## Distributed Run

To run on a distributed AWS cluster, see [CGCloud](https://github.com/BD2KGenomics/cgcloud) for instance provisioning, 
then run `toil-rnaseq run aws:us-west-2:example-jobstore-bucket --batchSystem=mesos --mesosMaster mesos-master:5050`
to use the AWS job store and mesos batch system. 

I have written an SOP for UCSC's Core Operations group that is 
[available here](https://github.com/BD2KGenomics/core-operations/blob/master/SOPs/toil-rnaseq.sop.md). 
