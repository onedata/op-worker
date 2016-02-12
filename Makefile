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
REBAR           ?= $(BASE_DIR)/rebar
PKG_VARS_CONFIG  = pkg.vars.config
OVERLAY_VARS    ?=

GIT_URL := $(shell git config --get remote.origin.url | sed -e 's/\(\/[^/]*\)$$//g')
GIT_URL := $(shell if [ "${GIT_URL}" = "file:/" ]; then echo 'ssh://git@git.plgrid.pl:7999/vfs'; else echo ${GIT_URL}; fi)
ONEDATA_GIT_URL := $(shell if [ "${ONEDATA_GIT_URL}" = "" ]; then echo ${GIT_URL}; else echo ${ONEDATA_GIT_URL}; fi)
export ONEDATA_GIT_URL

.PHONY: deps package test test_gui

all: test_rel

##
## Rebar targets
##

recompile:
	./rebar compile skip_deps=true

##
## If performance is compiled in cluster_worker ten annotations do not work.
## Make sure they are not included in cluster_worker build.
## todo: find better solution
##
compile:
	sed -i "s/ \"deps\/ctool\/annotations\/performance\.erl\"/%%\"deps\/ctool\/annotations\/performance\.erl\"/" deps/cluster_worker/rebar.config
	rm deps/cluster_worker/ebin/performance.beam || true
	./rebar compile
	sed -i "s/%%\"deps\/ctool\/annotations\/performance\.erl\"/ \"deps\/ctool\/annotations\/performance\.erl\"/" deps/cluster_worker/rebar.config

deps:
	./rebar get-deps

##
## Reltool configs introduce dependency on deps directories (which do not exist)
## Also a release is not nescesary for us.
## We prevent reltool from creating a release.
## todo: find better solution
##
## Generates a dev release
generate_dev: deps compile gui_dev
	sed -i "s/{sub_dirs, \[\"rel\"\]}\./{sub_dirs, \[\]}\./" deps/cluster_worker/rebar.config
	# Move gui tmp dir away from sources, so as to prevent
	# rebar from entering it during spec generation and crashing
	mv src/http/gui/tmp /tmp/gui_tmp
	./rebar generate $(OVERLAY_VARS)
	# Bring back the tmp dir to its normal location
	mv /tmp/gui_tmp src/http/gui/tmp
	sed -i "s/{sub_dirs, \[\]}\./{sub_dirs, \[\"rel\"\]}\./" deps/cluster_worker/rebar.config

## Generates a production release
generate: deps compile gui_prod
	sed -i "s/{sub_dirs, \[\"rel\"\]}\./{sub_dirs, \[\]}\./" deps/cluster_worker/rebar.config
	# Move gui tmp dir away from sources, so as to prevent
	# rebar from entering it during spec generation and crashing
	mv src/http/gui/tmp /tmp/gui_tmp
	./rebar generate $(OVERLAY_VARS)
	# Bring back the tmp dir to its normal location
	mv /tmp/gui_tmp src/http/gui/tmp
	sed -i "s/{sub_dirs, \[\]}\./{sub_dirs, \[\"rel\"\]}\./" deps/cluster_worker/rebar.config

clean: relclean pkgclean
	./rebar clean

distclean:
	./rebar delete-deps

##
## Release targets
##

rel: generate

test_rel: generate_dev cm_rel appmock_rel

cm_rel:
	ln -sf deps/cluster_worker/cluster_manager/
	make -C cluster_manager/ rel

appmock_rel:
	make -C appmock/ rel

relclean:
	rm -rf rel/test_cluster
	rm -rf rel/op_worker
	rm -rf appmock/rel/appmock
	rm -rf cluster_manager/rel/cluster_manager

##
## Testing targets
##

eunit:
	./rebar eunit skip_deps=true suites=${SUITES}
## Rename all tests in order to remove duplicated names (add _(++i) suffix to each test)
	@for tout in `find test -name "TEST-*.xml"`; do awk '/testcase/{gsub("_[0-9]+\"", "_" ++i "\"")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done

test_gui:
	cd test_gui && ember test

coverage:
	$(BASE_DIR)/bamboos/docker/coverage.escript $(BASE_DIR)

##
## Dialyzer targets local
##

PLT ?= .dialyzer.plt

# Builds dialyzer's Persistent Lookup Table file.
.PHONY: plt
plt:
	dialyzer --check_plt --plt ${PLT}; \
	if [ $$? != 0 ]; then \
	    dialyzer --build_plt --output_plt ${PLT} --apps kernel stdlib sasl erts \
		ssl tools runtime_tools crypto inets xmerl snmp public_key eunit \
		mnesia edoc common_test test_server syntax_tools compiler ./deps/*/ebin; \
	fi; exit 0


# Dialyzes the project.
dialyzer: plt
	dialyzer ./ebin --plt ${PLT} -Werror_handling -Wrace_conditions --fullpath

##
## Packaging targets
##

export PKG_VERSION PKG_ID PKG_BUILD BASE_DIR ERLANG_BIN REBAR OVERLAY_VARS RELEASE PKG_VARS_CONFIG

check_distribution:
ifeq ($(DISTRIBUTION), none)
	@echo "Please provide package distribution. Oneof: 'wily', 'fedora-23-x86_64'"
	@exit 1
else
	@echo "Building package for distribution $(DISTRIBUTION)"
endif

package/$(PKG_ID).tar.gz: deps
	mkdir -p package
	rm -rf package/$(PKG_ID)
	git archive --format=tar --prefix=$(PKG_ID)/ $(PKG_REVISION) | (cd package && tar -xf -)
	${MAKE} -C package/$(PKG_ID) deps
	for dep in package/$(PKG_ID) package/$(PKG_ID)/deps/*; do \
	     echo "Processing dependency: `basename $${dep}`"; \
	     vsn=`git --git-dir=$${dep}/.git describe --tags 2>/dev/null`; \
	     mkdir -p $${dep}/priv; \
	     echo "$${vsn}" > $${dep}/priv/vsn.git; \
	     sed -i'' "s/{vsn,\\s*git}/{vsn, \"$${vsn}\"}/" $${dep}/src/*.app.src 2>/dev/null || true; \
	done
	#find package/$(PKG_ID) -depth -name ".git" -not -path '*/cluster_worker/*' -exec rm -rf {} \;
	tar -C package -czf package/$(PKG_ID).tar.gz $(PKG_ID)

dist: package/$(PKG_ID).tar.gz
	cp package/$(PKG_ID).tar.gz .

package: check_distribution package/$(PKG_ID).tar.gz
	${MAKE} -C package -f $(PWD)/deps/node_package/Makefile

pkgclean:
	rm -rf package
