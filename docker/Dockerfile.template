FROM quay.io/ucsc_cgl/toil:3.14.0

# File Author / Maintainer
MAINTAINER John Vivian <jtvivian@gmail.com>

RUN apt-get update && apt-get install -y \
    git \
    python-dev \
    python-pip \
    wget \
    curl \
    apt-transport-https \
    ca-certificates

# Get the Docker binary
RUN curl https://download.docker.com/linux/static/stable/x86_64/docker-DOCKERVER-ce.tgz \
         | tar -xvzf - -C /tmp && mv /tmp/docker/* /usr/local/bin/ \
         && chmod u+x /usr/local/bin/docker

# Set up a virtual environment with the system site package option so Toil
# can zip up this virtual environment and place it on the worker nodes
# Any Toil script that is pip installed must be installed in the virtual
# environment; this is how the pipeline is placed on the worker nodes
RUN pip install virtualenv
RUN virtualenv --system-site-packages /opt/rnaseq-pipeline/toil_venv

# Install toil-rnaseq
COPY toil-rnaseq.tar.gz .
RUN bash -c 'source /opt/rnaseq-pipeline/toil_venv/bin/activate  && pip install toil-rnaseq.tar.gz && rm toil-rnaseq.tar.gz'

COPY wrapper.py /opt/rnaseq-pipeline/
COPY README.md /opt/rnaseq-pipeline/

# Mesos communicates on port 5050 so make sure this port is open
EXPOSE 5050

# Mount the root folder to an anonymous directory on the host file system
# This is done to make it writable in case Dockstore (which calls cwl-runner)
# is used to launch the container, because cwl-runner makes the container
# file system read only. We need to do this because the Toil AWS provisioner
# will try to create a key pair in /root/.ssh and create a file called
# .sshSuccess in /root. 
# Be sure to run the container with the -rm option so that the volume
# is removed when the container exits
VOLUME /root

ENTRYPOINT ["python", "/opt/rnaseq-pipeline/wrapper.py"]
CMD ["--help"]
