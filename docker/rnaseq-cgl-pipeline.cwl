#!/usr/bin/env cwl-runner

class: CommandLineTool
id: RNA-Seq-CGL
label: RNA-Seq CGL Pipeline
cwlVersion: v1.0

$namespaces:
  dct: http://purl.org/dc/terms/
  foaf: http://xmlns.com/foaf/0.1/

doc: |
    ![build_status](https://quay.io/repository/ucsc_cgl/rnaseq-cgl-pipeline/status)

    **The UCSC RNA-seq CGL Workflow**

    For more information about this workflow see the Github [repo](https://github.com/BD2KGenomics/toil-scripts/tree/releases/2.0.10/src/toil_scripts/rnaseq_cgl) for the 2.0.10 release and the new [repo](https://github.com/BD2KGenomics/toil-rnaseq) for releases in the 3.x series and beyond.

    *Inputs*

    This pipeline is designed to take one or more fastq file pairs representing RNA-Seq analysis that have been tarred in a single tar file.

    *Outputs*

    RNA-Seq fastqs are combined, aligned, and quantified with 2 different methods (RSEM and Kallisto). This pipeline produces a tarball (tar.gz) file for a given sample that contains:
    ```
    RSEM: TPM, FPKM, counts and raw counts (parsed from RSEM output)
    Kallisto: abundance.tsv, abundance.h5, and a JSON of run information
    ```

    *Feedback*

    If there are any questions please contact the workflow author John Vivian (jtvivian@gmail.com). If you find any errors or corrections please feel free to make a pull request. Feedback of any kind is appreciated.


dct:creator:
  '@id': http://orcid.org/0000-0002-7681-6415
  foaf:name: Brian O'Connor
  foaf:mbox: briandoconnor@gmail.com

requirements:
  - class: DockerRequirement
    dockerPull: "quay.io/ucsc_cgl/rnaseq-cgl-pipeline:3.0.1--1.12.3"

hints:
  - class: ResourceRequirement
    coresMin: 1
    ramMin: 64000
    outdirMin: 500000000
    description: "The process requires at least 16G of RAM and we recommend 500GB or storage."

inputs:
  samples:
    doc: "Absolute path(s) to sample tarballs or paired FASTQ files. FASTQ pairs are comma delimited and each pair is space delimited. Ex: sample1,sample2 sample3,sample4"
    type:
      type: array
      items: File
    inputBinding:
      prefix: --samples

  star:
    type: File
    doc: "Absolute path to STAR index tarball."
    inputBinding:
      prefix: --star

  rsem:
    type: File
    doc: "Absolute path to rsem reference tarball."
    inputBinding:
      prefix: --rsem

  kallisto:
    type: File
    doc: "Absolute path to kallisto index (.idx) file."
    inputBinding:
      prefix: --kallisto

  disable-cutadapt:
    type: boolean?
    default: false
    doc: "Cutadapt fails if samples are improperly paired. Use this flag to disable cutadapt."
    inputBinding:
      prefix: --disable-cutadapt

  save-bam:
    type: boolean?
    default: false
    doc: "If this flag is used, genome-aligned bam is written to output."
    inputBinding:
      prefix: --save-bam

  save-wiggle:
    type: boolean?
    default: false
    doc: "If this flag is used, wiggle files (.bg) are written to output."
    inputBinding:
      prefix: --save-wiggle

  bamqc:
    type: boolean?
    default: false
    doc: "If this flag is used, the BAMQC step will be run."
    inputBinding:
      prefix: --bamqc

  work-mount:
    type: string
    doc: "Path of the working directory to be mounted into the container"
    inputBinding:
      prefix: --work_mount

  no-clean:
    type: boolean?
    default: true
    doc: "If this flag is used, temporary work directory is not cleaned."
    inputBinding:
      prefix: --no-clean

  resume:
    type: string?
    doc: "Path of the working directory that contains a job store to be resumed."
    inputBinding:
      prefix: --resume

  cores:
    type: int?
    doc: "Will set a cap on number of cores to use, default is all available cores."
    inputBinding:
      prefix: --cores

outputs:
  output_files:
    type:
      type: array
      items: File
    outputBinding:
      glob: '*.tar.gz'
    doc: "Result files RNA-seq CGL pipeline"

  wiggle_files:
    type:
      type: array
      items: File
    outputBinding:
      glob: '*.wiggle.bg'
    doc: "Wiggle result files RNA-seq CGL pipeline"

  bam_files:
    type:
      type: array
      items: File
    outputBinding:
      glob: '*.bam'
      doc: "BAM result files RNA-seq CGL pipeline"

baseCommand: ["--logDebug"]

