import argparse
import textwrap

parser = argparse.ArgumentParser()
parser.add_argument('--docker-version', required=True)

args = parser.parse_args()


dependencies = ' '.join(('git',
                         'python-dev',
                         'python-pip',
                         'wget',
                         'curl',
                         'apt-transport-https',
                         'ca-certificates'))
docker_version = args.docker_version
print textwrap.dedent("""
    FROM ubuntu:14.04

    RUN apt-get update \
        && apt-get install -y {dependencies}
    RUN curl https://get.docker.com/builds/Linux/x86_64/docker-{docker_version}.tgz \
             | tar -xvzf - --transform='s,[^/]*/,,g' -C /usr/local/bin/ \
             && chmod u+x /usr/local/bin/docker
    RUN pip install setuptools --upgrade
    RUN pip install toil==3.5.0a1.dev274

    COPY wrapper.py /opt/pipeline/
    COPY toil-rnaseq-*.tar.gz /opt/pipeline/sdist.tar.gz
    RUN pip install /opt/pipeline/sdist.tar.gz
    RUN rm /opt/pipeline/sdist.tar.gz

    ENTRYPOINT ["python", "/opt/pipeline/wrapper.py"]
    CMD ["--help"]
""").format(**locals()).lstrip()

