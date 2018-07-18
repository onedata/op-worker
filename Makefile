.EXPORT_ALL_VARIABLES:

REPO	        ?= op-worker

# distro for package building (oneof: wily, fedora-23-x86_64)
DISTRIBUTION    ?= none
export DISTRIBUTION

PKG_REVISION    ?= $(shell git describe --tags --always)
PKG_VERSION     ?= $(shell git describe --tags --always | tr - .)
PKG_ID           = op-worker-$(PKG_VERSION)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= $(BASE_DIR)/rebar3
BUILD_DIR        = _build
LIB_DIR          = $(BUILD_DIR)/default/lib
REL_DIR          = $(BUILD_DIR)/default/rel
PKG_VARS_CONFIG  = pkg.vars.config
OVERLAY_VARS    ?= --overlay_vars=rel/vars.config
TEMPLATE_SCRIPT := ./rel/overlay.escript

GIT_URL := $(shell git config --get remote.origin.url | sed -e 's/\(\/[^/]*\)$$//g')
GIT_URL := $(shell if [ "${GIT_URL}" = "file:/" ]; then echo 'ssh://git@git.plgrid.pl:7999/vfs'; else echo ${GIT_URL}; fi)
ONEDATA_GIT_URL := $(shell if [ "${ONEDATA_GIT_URL}" = "" ]; then echo ${GIT_URL}; else echo ${ONEDATA_GIT_URL}; fi)
export ONEDATA_GIT_URL

BUILD_VERSION := $(subst $(shell git describe --tags --abbrev=0)-,,$(shell git describe --tags --long))

.PHONY: deps package test

all: test_rel

##
## Rebar targets
##

compile:
	$(REBAR) compile

deps:
	$(REBAR) get-deps
	make -C _build/default/lib/helpers submodules submodule=clproto
	$(LIB_DIR)/gui/pull-gui.sh gui-config.sh

upgrade:
	$(REBAR) upgrade

## Generates a production release
generate: deps compile template
	$(REBAR) release $(OVERLAY_VARS)

clean: relclean pkgclean
	$(REBAR) clean

clean_all: clean distclean
	rm -rf $(BUILD_DIR)
	rm -rf test_distributed/logs/*
	rm -rf priv/*
	rm -rf rebar.lock

distclean:
	$(REBAR) clean --all

.PHONY: template
template:
	sed "s/{build_version, \".*\"}/{build_version, \"${BUILD_VERSION}\"}/" ./rel/vars.config.template > ./rel/vars.config
	$(TEMPLATE_SCRIPT) rel/vars.config ./rel/files/vm.args.template

##
## Submodules
##

submodules:
	git submodule sync --recursive ${submodule}
	git submodule update --init --recursive ${submodule}


##
## Release targets
##

rel: generate

test_rel: generate cm_rel 

cm_rel:
	make -C $(LIB_DIR)/cluster_manager/ submodules
	mkdir -p cluster_manager/bamboos/gen_dev
	cp -rf $(LIB_DIR)/cluster_manager/bamboos/gen_dev cluster_manager/bamboos
	printf "\n{base_dir, \"$(BASE_DIR)/cluster_manager/_build\"}." >> $(LIB_DIR)/cluster_manager/rebar.config
	make -C $(LIB_DIR)/cluster_manager/ rel
	sed -i "s@{base_dir, \"$(PWD)/cluster_manager/_build\"}\.@@" $(LIB_DIR)/cluster_manager/rebar.config

relclean:
	rm -rf $(REL_DIR)/test_cluster
	rm -rf $(REL_DIR)/op_worker
	rm -rf cluster_manager/$(REL_DIR)/cluster_manager

##
## Testing targets
##

eunit:
	$(REBAR) do eunit skip_deps=true --suite=${SUITES}, cover
## Rename all tests in order to remove duplicated names (add _(++i) suffix to each test)
	@for tout in `find test -name "TEST-*.xml"`; do awk '/testcase/{gsub("_[0-9]+\"", "_" ++i "\"")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done

coverage:
## Set on_bamboo=true so that coverage.escript will collect coverdata from all ct_logs directories
	$(BASE_DIR)/bamboos/docker/coverage.escript $(BASE_DIR) $(on_bamboo)

##
## Dialyzer targets local
##

# Dialyzes the project.
dialyzer:
	$(REBAR) dialyzer

##
## Packaging targets
##

check_distribution:
ifeq ($(DISTRIBUTION), none)
	@echo "Please provide package distribution. Oneof: 'wily', 'fedora-23-x86_64'"
	@exit 1
else
	@echo "Building package for distribution $(DISTRIBUTION)"
endif

package/$(PKG_ID).tar.gz:
	mkdir -p package
	rm -rf package/$(PKG_ID)
	git archive --format=tar --prefix=$(PKG_ID)/ $(PKG_REVISION) | (cd package && tar -xf -)
	git submodule foreach --recursive "git archive --prefix=$(PKG_ID)/\$$path/ \$$sha1 | (cd \$$toplevel/package && tar -xf -)"
	${MAKE} -C package/$(PKG_ID) upgrade deps
	for dep in package/$(PKG_ID) package/$(PKG_ID)/$(LIB_DIR)/*; do \
	     echo "Processing dependency: `basename $${dep}`"; \
	     vsn=`git --git-dir=$${dep}/.git describe --tags 2>/dev/null`; \
	     mkdir -p $${dep}/priv; \
	     echo "$${vsn}" > $${dep}/priv/vsn.git; \
	     sed -i'' "s/{vsn,\\s*git}/{vsn, \"$${vsn}\"}/" $${dep}/src/*.app.src 2>/dev/null || true; \
	done
	tar -C package -czf package/$(PKG_ID).tar.gz $(PKG_ID)

dist: package/$(PKG_ID).tar.gz
	cp package/$(PKG_ID).tar.gz .

package: check_distribution package/$(PKG_ID).tar.gz
	${MAKE} -C package -f $(PKG_ID)/node_package/Makefile

pkgclean:
	rm -rf package
