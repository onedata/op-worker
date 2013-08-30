BUILD_DIR = build

CMAKE = $(shell which cmake || which cmake28)
CPACK = $(shell which cpack || which cpack28)

all: build test

build: configure
	@(cd ${BUILD_DIR} && make -j`nproc`)

configure:
	@mkdir -p ${BUILD_DIR}
	@cd ${BUILD_DIR} && ${CMAKE} ..

test: build
	@cd ${BUILD_DIR} && make test

cunit: build
	@cd ${BUILD_DIR} && make cunit

clean: 
	@rm -rf ${BUILD_DIR} 
