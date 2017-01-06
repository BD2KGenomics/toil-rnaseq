# Definitions
docker_versions = 1.12.3 1.11.2 1.10.3 1.9.1 1.8.3 1.7.1 1.6.2
gen_directories = $(docker_versions:=.docker)
build_tool = runtime-container.DONE

build: $(build_tool)

%.docker: dockerver=$(@:.docker=)
%.docker: Dockerfile.template Makefile.template wrapper.py README.md
	mkdir -p $@
	sed -e "s:DOCKERVER:$(dockerver):g" Dockerfile.template > ${@}/Dockerfile
	sed -e "s:DOCKERVER:$(dockerver):g" Makefile.template > ${@}/Makefile
	cd $@ && ln -sf ../test.py
	cp wrapper.py ${@}/
	cp README.md ${@}/

generate: $(gen_directories)

$(build_tool): generate
	$(foreach subdir, $(gen_directories), cd $(subdir) && $(MAKE) build && cd ..;)
	touch $(build_tool)

push: build
	# Requires ~/.dockercfg
	$(foreach subdir, $(gen_directories), cd $(subdir) && $(MAKE) push && cd ..;)

test: build test.py
	$(foreach subdir, $(gen_directories), cd $(subdir) && $(MAKE) test && cd ..;)

clean:
	-rm -rf $(gen_directories) && rm $(build_tool)
