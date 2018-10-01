## Computational Genomics Lab, Genomics Institute, UC Santa Cruz
## Toil RNA-Seq Pipeline
#### Scalable, reproducible, and robust RNA-seq expression quantification. 

The [Toil](https://github.com/BD2KGenomics/toil) RNA-seq workflow converts RNA sequencing data into gene- and 
transcript-level expression quantification. 

Please open [issues](https://github.com/BD2KGenomics/toil-rnaseq/issues) for any bugs, errors, corrections, 
or feature requests. 

If there are any questions not answered by this README or the [wiki](https://github.com/BD2KGenomics/toil-rnaseq/wiki), 
contact [John Vivian](jtvivian@gmail.com). 

### Appendix

- [Dependencies and Installation](#dependencies-and-installation)
- [Quickstart](#quickstart)

For detailed information and troubleshooting, see the [Wiki](https://github.com/BD2KGenomics/toil-rnaseq/wiki)
- [Workflow Inputs](https://github.com/BD2KGenomics/toil-rnaseq/wiki/Workflow-Inputs)
- [Examples](https://github.com/BD2KGenomics/toil-rnaseq/wiki/Examples)
- [Methods](https://github.com/BD2KGenomics/toil-rnaseq/wiki/Methods)
- [Troubleshooting](https://github.com/BD2KGenomics/toil-rnaseq/wiki/Troubleshooting)
- [Auto-scaling on AWS](https://github.com/BD2KGenomics/toil-rnaseq/wiki/Auto-scaling-Example-Using-AWS)


## Overview

![Toil-RNAseq DAG](/imgs/toil-rnaseq.png)

This workflow takes RNA sequencing reads (fastq / BAM) as input and outputs the following (if all options enabled):

```
<SAMPLE>
├── Kallisto
│   ├── abundance.h5
│   ├── abundance.tsv
│   ├── fusion.txt
│   └── run_info.json
├── QC
│   ├── fastQC
│   │   ├── R1_fastqc.html
│   │   ├── R1_fastqc.zip
│   │   ├── R2_fastqc.html
│   │   └── R2_fastqc.zip
│   └── STAR
│       ├── Log.final.out
│       └── SJ.out.tab
├── Hera
│   ├── abundance.h5
│   ├── abundance.tsv
│   ├── fusion.bedpe (paired-end data only)
│   └── summary
└── RSEM
    ├── Hugo
    │   ├── rsem_genes.hugo.results
    │   └── rsem_isoforms.hugo.results
    ├── rsem_genes.results
    └── rsem_isoforms.results
```
If the user selects options such as `save-bam`, or `wiggle`, additional files will appear in the output directory:

- SAMPLE.sorted.bam
- SAMPLE.wiggle.bg

The output tarball is prepended with the unique name for the sample (e.g. SAMPLE.tar.gz). 

# Dependencies and Installation

This workflow has been tested on Ubuntu 14.04, 16.04 and Mac OSX, but should also run on other unix based systems.  
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

The workflow requires approximately 50-60G of RAM in order to run STAR alignment. 

###  Installation

The Toil RNA-seq workflow is pip installable!

For most users, the preferred installation method is inside a virtualenv to avoid dependency conflicts: 

- `virtualenv ~/toil-rnaseq` 
- `source ~/toil-rnaseq/bin/activate`
- `pip install toil-rnaseq`

After installation, the workflow can be executed by typing `toil-rnaseq` into the teriminal.

# Quickstart

First, obtain all of the necessary [workflow inputs](https://github.com/BD2KGenomics/toil-rnaseq/wiki/Workflow-Inputs).

Then, type `toil-rnaseq` to get basic help menu and instructions
 
1. Type `toil-rnaseq generate` to create an editable manifest and config in the current working directory.
2. Parameterize the workflow by editing the config.
3. Fill in the manifest with information pertaining to your samples.
4. Type `toil-rnaseq run [jobStore]` to execute the workflow.

### Citation

If you use this workflow to produce data for published research please cite the Toil white paper:

```
Vivian, J. et al. 
Toil enables reproducible, open source, big biomedical data analyses. 
Nat Biotech 35, 314–316 (2017).
```
