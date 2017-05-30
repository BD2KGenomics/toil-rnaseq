FROM ubuntu:14.04

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
RUN curl https://get.docker.com/builds/Linux/x86_64/docker-1.12.3.tgz \
         | tar -xvzf - --transform='s,[^/]*/,,g' -C /usr/local/bin/ \
         && chmod u+x /usr/local/bin/docker
# Install Toil
RUN pip install toil==3.3.5

# Install toil-rnaseq
COPY toil-rnaseq.tar.gz .
RUN pip install toil-rnaseq.tar.gz && rm toil-rnaseq.tar.gz


COPY wrapper.py /opt/rnaseq-pipeline/
COPY README.md /opt/rnaseq-pipeline/

ENTRYPOINT ["python", "/opt/rnaseq-pipeline/wrapper.py"]
CMD ["--help"]
