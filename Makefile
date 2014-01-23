RELEASE_DIR = release
DEBUG_DIR = debug

CMAKE = $(shell which cmake || which cmake28)
CPACK = $(shell which cpack || which cpack28)

.PHONY: build release debug clean all
all: release test

## Obsolete target, use 'make release' instead
build: release 
	@echo "*****************************************************"
	@echo "'build' target is obsolete, use 'release' instead !"
	@echo "*****************************************************"
	@ln -sf ${RELEASE_DIR} build

release: 
	mkdir -p ${RELEASE_DIR}
	cd ${RELEASE_DIR} && ${CMAKE} -DCMAKE_BUILD_TYPE=release `if [[ "$$PREFER_STATIC_LINK" != ""  ]]; then echo "-DPREFER_STATIC_LINK=1"; fi` ..
	(cd ${RELEASE_DIR} && make -j`nproc`)

debug: 
	@mkdir -p ${DEBUG_DIR}
	@cd ${DEBUG_DIR} && ${CMAKE} -DCMAKE_BUILD_TYPE=debug `if [[ "$$PREFER_STATIC_LINK" != ""  ]]; then echo "-DPREFER_STATIC_LINK=1"; fi` ..
	@(cd ${DEBUG_DIR} && make -j`nproc`)

test: release
	@cd ${RELEASE_DIR} && make test

cunit: release
	@cd ${RELEASE_DIR} && make cunit

install: release
	@cd ${RELEASE_DIR} && make install

clean: 
	@rm -rf ${DEBUG_DIR} ${RELEASE_DIR} build
