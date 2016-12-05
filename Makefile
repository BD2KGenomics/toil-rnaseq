# Copyright (C) 2015 UCSC Computational Genomics Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

define help

Supported targets: prepare, develop, sdist, clean, test, pypi.

Please note that all build targets require a virtualenv to be active.

The 'prepare' target installs RNA-seq's build requirements into the current virtualenv.

The 'develop' target creates an editable install of RNA-seq and its runtime requirements in the
current virtualenv. The install is called 'editable' because changes to the source code
immediately affect the virtualenv.

The 'sdist' target creates a source distribution of RNA-seq suitable for hot-deployment (not
implemented yet).

The 'clean' target undoes the effect of 'develop', and 'sdist'.

The 'test' target runs RNA-seq's unit tests. Set the 'tests' variable to run a particular test, e.g.

	make test tests=src/toil/test/sort/sortTest.py::SortTest::testSort

The 'pypi' target publishes the current commit of RNA-seq to PyPI after enforcing that the working
copy and the index are clean, and tagging it as an unstable .dev build.

endef
export help
help:
	@echo "$$help"


SHELL=bash
python=python2.7
pip=pip2.7
tests=src
extras=

pipeline_ver:=$(shell python -c "from __future__ import print_function; import version; print(version.version, end='')")
sdist_name:=toil-rnaseq-$(pipeline_ver).tar.gz
quay_path=quay.io/ucsc_cgl/rnaseq-cgl-pipeline
docker_versions = 1.12.3 1.11.2 1.10.3 1.9.1 1.8.3 1.7.1 1.6.2

commit_id:=$(shell git log --pretty=oneline -n 1 -- $(pwd) | cut -f1 -d " ")
dirty:=$(shell (git diff --exit-code && git diff --cached --exit-code) > /dev/null || printf "%s" -DIRTY)
pipeline_tag=$(protocol_ver)-$(pipeline_ver)
short_commit_tag=$(pipeline_tag)-$(shell echo $(commit_id) | head -c 7)$(dirty)
long_commit_tag=$(pipeline_tag)-$(commit_id)$(dirty)


green=\033[0;32m
normal=\033[0m
red=\033[0;31m


develop: check_venv
	$(pip) install -e .$(extras)
clean_develop: check_venv
	- $(pip) uninstall -y toil
	- rm -rf src/*.egg-info


docker: $(foreach ver,$(docker_versions),docker/builds/$(ver))
docker/builds/%: ver_base=$(basename $(notdir $(@)))
docker/builds/%: protocol_ver=$(ver_base).x
docker/builds/%: true_ver=$(filter $(ver_base).%, $(docker_versions))
docker/builds/%: build_path=docker/builds/$(protocol_ver)
docker/builds/%: docker/Dockerfile.py docker/wrapper.py sdist
	mkdir -p $(build_path)
	cp docker/wrapper.py $(build_path)/
	cp dist/$(sdist_name) $(build_path)/
	$(python) docker/Dockerfile.py --docker-version $(true_ver) > $(build_path)/Dockerfile
	cd $(build_path) && docker build --tag $(quay_path):$(long_commit_tag) .
	for tag in $(short_commit_tag) $(pipeline_tag) $(protocol_ver); do \
        docker tag $(quay_path):$(long_commit_tag) $(quay_path):$tag; \
    done
clean_docker:
	- rm -r docker/builds

docker_push: $(foreach ver,$(docker_versions),docker_push_$(ver))
docker_push_%: docker/builds/%
	for tag in $(long_commit_tag) $(short_commit_tag) $(pipeline_tag) $(protocol_ver); do \
        docker push $(quay_path):$tag; \
    done


sdist: check_venv dist/$(sdist_name)
dist/$(sdist_name): check_venv
	@test -f dist/$(sdist_name) && mv dist/$(sdist_name) dist/$(sdist_name).old || true
	$(python) setup.py sdist
	@test -f dist/$(sdist_name).old \
	    && ( cmp -s <(tar -xOzf dist/$(sdist_name)) <(tar -xOzf dist/$(sdist_name).old) \
	         && mv dist/$(sdist_name).old dist/$(sdist_name) \
	         && printf "$(green)No significant changes to sdist, reinstating backup.$(normal)" \
	         || rm dist/$(sdist_name).old ) \
	    || true
clean_sdist:
	- rm -rf dist


test: check_venv check_build_reqs
	PATH=$$PATH:${PWD}/bin $(python) -m pytest -vv --junitxml test-report.xml $(tests)


integration-test: check_venv check_build_reqs sdist
	TOIL_TEST_INTEGRATIVE=True $(python) run_tests.py integration-test $(tests)


pypi: check_venv check_clean_working_copy check_running_on_jenkins
	test "$$ghprbActualCommit" \
	&& echo "We're building a PR, skipping PyPI." || ( \
	set -x \
	&& tag_build=`$(python) -c 'pass;\
		from version import version as v;\
		from pkg_resources import parse_version as pv;\
		import os;\
		print "--tag-build=.dev" + os.getenv("BUILD_NUMBER") if pv(v).is_prerelease else ""'` \
	&& $(python) setup.py egg_info $$tag_build sdist bdist_egg upload )
clean_pypi:
	- rm -rf build/


clean: clean_develop clean_sdist clean_pypi clean_prepare clean_docker


check_build_reqs:
	@$(python) -c 'import pytest' \
		|| ( echo "$(red)Build requirements are missing. Run 'make prepare' to install them.$(normal)" ; false )


prepare: check_venv
	rm -rf s3am
	virtualenv s3am && s3am/bin/pip install s3am==2.0
	mkdir -p bin
	ln -snf ${PWD}/s3am/bin/s3am bin/
	$(pip) install pytest==2.8.3 toil[aws]==3.3.1
clean_prepare: check_venv
	rm -rf bin s3am
	- $(pip) uninstall -y pytest toil


check_venv:
	@$(python) -c 'import sys; sys.exit( int( not hasattr(sys, "real_prefix") ) )' \
		|| ( echo "$(red)A virtualenv must be active.$(normal)" ; false )


check_clean_working_copy:
	@echo "$(green)Checking if your working copy is clean ...$(normal)"
	@git diff --exit-code > /dev/null \
		|| ( echo "$(red)Your working copy looks dirty.$(normal)" ; false )
	@git diff --cached --exit-code > /dev/null \
		|| ( echo "$(red)Your index looks dirty.$(normal)" ; false )
	@test -z "$$(git ls-files --other --exclude-standard --directory)" \
		|| ( echo "$(red)You have are untracked files:$(normal)" \
			; git ls-files --other --exclude-standard --directory \
			; false )


check_running_on_jenkins:
	@echo "$(green)Checking if running on Jenkins ...$(normal)"
	@test -n "$$BUILD_NUMBER" \
		|| ( echo "$(red)This target should only be invoked on Jenkins.$(normal)" ; false )


.PHONY: help \
		prepare \
		develop clean_develop \
		sdist clean_sdist \
		test \
		pypi clean_pypi \
		clean \
		check_venv \
		check_clean_working_copy \
		check_running_on_jenkins \
		check_build_reqs
