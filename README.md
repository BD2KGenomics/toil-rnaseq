## University of California, Santa Cruz Genomics Institute
### Guide: Running the CGL HG38 RNA-seq Pipeline using Toil

This guide attempts to walk the user through running this pipeline from start to finish. If there are any questions
please contact John Vivian (jtvivian@gmail.com). If you find any errors or corrections please feel free to make a 
pull request.  Feedback of any kind is appreciated.

- [Dependencies](#dependencies)
- [Installation](#installation)
- [Inputs](#inputs)
- [Usage](#general-usage)
- [Methods](#methods)


## Overview

RNA-seq fastqs are combined, aligned, and quantified with 2 different methods (RSEM and Kallisto)

This pipeline produces a tarball (tar.gz) file for a given sample that contains 3 subdirectories:

- RSEM: TPM, FPKM, counts and raw counts (parsed from RSEM output)
- Kallisto: abundance.tsv, abundance.h5, and a JSON of run information
- QC: FastQC output HTMLs and zip file

The output tarball is prepended with the UUID for the sample (e.g. UUID.tar.gz). 

# Dependencies

This pipeline has been tested on Ubuntu 14.04, but should also run on other unix based systems.  `apt-get` and `pip`
often require `sudo` privilege, so if the below commands fail, try prepending `sudo`.  If you do not have `sudo` 
privileges you will need to build these tools from source, or bug a sysadmin about how to get them (they don't mind). 

#### General Dependencies

    1. Python 2.7
    2. Curl         apt-get install curl
    3. Docker       http://docs.docker.com/engine/installation/

#### Python Dependencies

    1. Toil         pip install toil
    2. S3AM         pip install --pre s3am (optional, needed for uploading output to S3)
    
    
#### System Dependencies

This pipeline needs approximately 50G of RAM in order to run STAR alignment. 

# Installation

The CGL RNA-seq pipeline is now pip installable! `pip install toil-rnaseq` for a stable version or
`pip install --pre toil-rnaseq` for the current development version.

If there is an existing, system-wide installation of Toil, as is the case when using CGCloud, 
the `pip install toil` step should be skipped and virtualenv should be invoked with `--system-site-packages`. 
This way the existing Toil installation will be available inside the virtualenv.

To decrease the chance of versioning conflicts, install toil-rnaseq into a virtualenv: 

- `virtualenv ~/toil-rnaseq` 
- `source ~/toil-rnaseq/bin/activate`
- `pip install toil-rnaseq`

After installation, the pipeline can be executed by typing `toil-rnaseq` into the teriminal.
 
# Inputs

The CGL RNA-seq pipeline requires input files in order to run. These files are hosted on Synapse and can 
be downloaded after creating an account which takes about 1 minute and is free. 

* Register for a [Synapse account](https://www.synapse.org/#!RegisterAccount:0)
* Either download the samples from the [website GUI](https://www.synapse.org/#!Synapse:syn5886029) or use the Python API
* `pip install synapseclient`
* `python`
    * `import synapseclient`
    * `syn = synapseclient.Synapse()`
    * `syn.login('foo@bar.com', 'password')`
    * Get the RSEM reference (1 G)
        * `syn.get('syn5889216', downloadLocation='.')`
    * Get the Kallisto index (2 G)
        * `syn.get('syn5886142', downloadLocation='.')`
    * Get the STAR index (25 G)
        * `syn.get('syn5886182', downloadLocation='.')`
        
Sample tarballs containing fastq pairs can be passed via the command line option `--samples`. 
Alternatively, many samples can be placed in a manifest file created by using the 
`toil-rnaseq --generate-manifest` option. 
All samples and inputs must be submitted as URLs with support for the following schemas: 
`http://`, `file://`, `s3://`, `ftp://`.

Samples consisting of tarballs with fastq files inside _must_ follow the file name convention of ending in an 
R1/R2 or \_1/\_2 followed by `.fastq.gz`, `.fastq`, `.fq.gz` or `.fq.`.

# General Usage

Type `toil-rnaseq` to get basic help menu and instructions
 
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
s3-dir: s3://my-bucket/test/rnaseq
ssec: 
gt-key: 
wiggle: true
save-bam: true
ci-test:
fwd-3pr-adapter: AGATCGGAAGAG
rev-3pr-adapter: AGATCGGAAGAG
```

Example with local input files

```
star-index: file://data/starIndex_chr6.tar.gz
kallisto-index: file://data/kallisto_hg38.idx
rsem-ref: file://data/rsem_ref_chr6.tar.gz
output-dir: /data/my-toil-run
s3-dir: s3://my-bucket/test/rnaseq
ssec: 
gt-key: 
wiggle: true
save-bam: true
ci-test:
fwd-3pr-adapter: AGATCGGAAGAG
rev-3pr-adapter: AGATCGGAAGAG
```

## Distributed Run

To run on a distributed AWS cluster, see [CGCloud](https://github.com/BD2KGenomics/cgcloud) for instance provisioning, 
then run `toil-rnaseq run aws:us-west-2:example-jobstore-bucket --batchSystem=mesos --mesosMaster mesos-master:5050`
to use the AWS job store and mesos batch system. 

# Methods

## Tools

| Tool     | Version | Description                                                                                                                                 |
|----------|---------|---------------------------------------------------------------------------------------------------------------------------------------------|
| FastQC   | 0.11.5  | Obtains quality metrics on each FASTQ input file.                                                                                           |
| CutAdapt | 1.9     | Adapter trimming and quality checking by enforcing fastq samples are properly paired.                                                       |
| STAR     | 2.4.2a  | Aligns fastq samples to the genome. Produces transcriptome bam for RSEM, and can optionally generate a genome-aligned bam and BigWig files. |
| RSEM     | 1.2.25  | Performs quantification of RNA-seq data to produces count values for genes and isoforms.                                                    |
| Kallisto | 0.42.4  | Performs quantification of RNA-seq data to produces counts for isoforms directly from fastq data.                                           |

All tool containers can be found on our [quay.io account](quay.io/organization/ucsc_cgl).

## Reference Data

HG38 (no alternative sequences) was downloaded from [NCBI](ftp://ftp.ncbi.nlm.nih.gov/genomes/archive/old_genbank/Eukaryotes/vertebrates_mammals/Homo_sapiens/GRCh38/seqs_for_alignment_pipelines/).
The PAR locus on the Y chromosome, which has duplicate sequences relative to the X chromosome, were removed. chrY:10,000-2,781,479
chrY:56,887,902-57,217,415. This was a requirement in order to run Kallisto. 
This locus is not removed by the pipeline, and was manually removed. To get this manually modified reference 
genome, use the `s3cmd` tool with the `requester-pays` option and download: 
`s3://cgl-pipeline-inputs/rnaseq_cgl/hg38_no_alt.fa`.

Gencode v23 annotations were downloaded from [Gencode](http://www.gencodegenes.org/releases/23.html). Comprehensive 
gene annotation (Regions=CHR) GTF was used to generate all additional reference input data.

STAR index was created using the reference genome and annotation file with the following Docker command:
`sudo docker run -v $(pwd):/data quay.io/ucsc_cgl/star --runThreadN 32 --runMode genomeGenerate --genomeDir /data/genomeDir --genomeFastaFiles hg38.fa --sjdbGTFfile gencode.v23.annotation.gtf`

RSEM reference was created using the reference genome and annotation file with the following Docker command:
`sudo docker run -v $(pwd):/data --entrypoinst=rsem-prepare-reference quay.io/ucsc_cgl/rsem -p 4 --gtf gencode.v23.annotation.gtf hg38.fa hg38`

Kallisto index was created using the transcriptome and annotation file with the following Docker command:
`sudo docker run -v $(pwd):/data quay.io/ucsc_cgl/kallisto index -i hg38.gencodeV23.transcripts.idx transcriptome_hg38_gencodev23.fasta`

## Tool Options

- FastQC is run with default options
- CutAdapt is run with default options
- Kallisto is run with `bootstraps` set to 100

#### STAR

```
'--outFileNamePrefix', 'rna',
'--outSAMtype', 'BAM', 'SortedByCoordinate',
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
'--sjdbScore', '1'
```

#### RSEM

```
'--quiet',
'--no-qualities',
'-p', str(cores),
'--forward-prob', '0.5',
'--seed-length', '25',
'--fragment-length-mean', '-1.0',
'--bam', '/data/transcriptome.bam',
```
