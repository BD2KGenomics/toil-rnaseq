# Computational Genomics Lab, Genomics Institute, UC Santa Cruz
### Running the CGL RNA-seq Pipeline Container: Release 2.0.10

This guide will walk through running the pipeline from start to finish. If there are any questions please contact
John Vivian (jtvivian@gmail.com). If you find any errors or corrections please feel free to make a pull request.
Feedback of any kind is appreciated.

## Overview

This container runs the 
[CGL RNA-seq pipeline](https://github.com/BD2KGenomics/toil-scripts/tree/master/src/toil_scripts/rnaseq_cgl), which
is built using [Toil](https://github.com/BD2KGenomics/toil), a high-performance pipeline architecture platform for
running workflows. This container is designed to run local samples.
This pipeline expects samples to be tarballs with fastq files inside, ideally paired data tagged with
the conventional R1 and R2 standard. Samples should NOT have periods (.) in them aside from the extension
as the output is derived from the sample input name.

This pipeline **requires** a run environment with at least 40G of memory to run STAR alignment. 

This pipeline **requires** a host with Docker installed. 

Docker versions 1.6-1.12 (inclusive) are supported since we build every Docker protocol version
in those releases.

You can pull a specific version of this pipeline by using the appropriate tag.

`docker pull quay.io/ucsc_cgl/rnaseq-cgl-pipeline:<supported docker version>--2.0.10`

## Inputs

The CGL RNA-seq pipeline requires input files in order to run. These files are hosted on Synapse and can 
be downloaded after creating an account which takes about 1 minute and is free. These inputs are built using the
HG38 reference genome and Gencode v.23 annotations.

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


## Running

Type `docker run quay.io/ucsc_cgl/rnaseq-cgl-pipeline` to see a help menu and list of possible run options.

The preferred way to run this pipeline is to colocate the pipeline and sample inputs in the location from which
the pipeline will be run. This greatly simplifies the command line and complexity of running the container.
This location must have _plenty_ of storage, as Toil's job store and temp directories will be created
at this location during run time. A safe estimate for a single sample would be about 100 Gigabytes.

 `-v /var/run/docker.sock:/var/run/docker.sock` must **always** be supplied as Docker argument 
 (see bottom of README for details). 

The work directory, where temp files will be created, must be "mirror mounted". This means that the diretory
must be the same on both sides of the colon in Docker's `-v` command.  

### Example Command

`cd` to the directory where the pipeline inputs and samples are located, then type:

```
docker run \
    -v $(pwd):$(pwd) \ # Work directory
    -v /var/run/docker.sock:/var/run/docker.sock \ # Required Docker socket
    quay.io/ucsc_cgl/rnaseq-cgl-pipeline \ # Name of the pipeline
    --star $(pwd)/starIndex_hg38_no_alt.tar.gz \
    --kallisto $(pwd)/kallisto_hg38.idx \
    --rsem $(pwd)/rsem_ref_hg38_no_alt.tar.gz \
    --samples $(pwd)/sample1.tar $(pwd)/sample2.tar ... 
```

### Separate sample, input, and work directory locations

If your samples (and/or inputs) are located in a different location than where you would like
the job store and work directories to be created, use the following format:

* Mirror mount points for the work directory: e.g. `-v /foo/bar:/foo/bar`
* Use `-v /foo/bar/samples:/samples`, where `:/samples` is the preferred destination path.
* Use `-v /foo/bar/inputs:/inputs`, where `:/inputs` is the preferred destination path.

Due to the way Docker works, this will change how you supply paths to the samples and inputs. Look at the 
following example and you'll see that the path being passed to the container for samples is no longer 
**/path/to/data/sample1.tar**, but **/samples/sample1.tar**.  Likewise, the inputs are now passed in
as **/inputs/kallisto_hg38.idx**. 

```
docker run \
    -v /scratch:/scratch \ # Path to work directory, mirrored
    -v /path/to/data:/samples \ # Path to samples, not mirrored
    -v /path/to/inputs:/inputs \ # Path to inputs, not mirrored
    -v /var/run/docker.sock:/var/run/docker.sock \
    quay.io/ucsc_cgl/rnaseq-cgl-pipeline \
    --star /inputs/starIndex_hg38_no_alt.tar.gz \
    --rsem /inputs/rsem_ref_hg38_no_alt.tar.gz \
    --kallisto /inputs/kallisto_hg38.idx \
    --samples /samples/sample1.tar /samples/sample2.tar ...
```

## Core Limit and Restarting

By default, the pipeline will use all available cores on the machine in which it is run. This can be regulated
by using the `--cores` argument.

In the event of failure the pipeline can be resumed by rerunning the pipeline with the `--resume` argument. 

If you would like to inspect the contents of the temp directory, you can specify the `--no-clean` flag.

## Genomic tool containers

The individual tools in the pipeline can be pulled with the following commands:

```
docker pull quay.io/ucsc_cgl/fastqc:0.11.5--be13567d00cd4c586edf8ae47d991815c8c72a49
docker pull quay.io/ucsc_cgl/cutadapt:1.9--6bd44edd2b8f8f17e25c5a268fedaab65fa851d2
docker pull quay.io/ucsc_cgl/kallisto:0.42.4--35ac87df5b21a8e8e8d159f26864ac1e1db8cf86
docker pull quay.io/ucsc_cgl/star:2.4.2a--bcbd5122b69ff6ac4ef61958e47bde94001cfe80
docker pull quay.io/ucsc_cgl/rsem:1.2.25--d4275175cc8df36967db460b06337a14f40d2f21
docker pull jvivian/rsem_postprocess
docker pull jvivian/gencode_hugo_mapping
```

## Into the Weeds

/var/run/docker.sock needs to be mirror mounted so that the host daemon process can spawn sibling containers when
Docker is executed by the parent container as opposed to nesting Docker containers as children which is ill-advised.

The mirror mount for the work directory is required since the Docker call executed inside the parent container
is actually run by the host, meaning the "src" provided to "-v" is actually on the host, not the parent container.

When using multiple mount points, A non-mirrored destination is required because there isn't an easy way to
ascertain which of the mount points is the work path versus sample path. That's because the JSON
returned by Docker inspect isn't ordered.  

You can use whatever mount point you like for the samples and inputs _so long as they are not mirrored_ and
you are consistent about using the destination path when passing in arguments to the container.
