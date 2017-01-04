#!/usr/bin/env bash

# Create Toil venv
rm -rf .env
virtualenv --never-download .env
. .env/bin/activate

# Prepare directory for temp files
TMPDIR=/mnt/ephemeral/tmp
rm -rf $TMPDIR
mkdir $TMPDIR
export TMPDIR

make prepare
make develop
make test
make docker
make test_docker
make clean
make pypi
make push_docker

rm -rf .env $TMPDIR
