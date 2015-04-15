REPO            ?= op-ccm

PKG_REVISION    ?= $(shell git describe --tags --always)
PKG_VERSION	    ?= $(shell git describe --tags --always | tr - .)
PKG_ID           = oneprovider-ccm-$(PKG_VERSION)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= $(BASE_DIR)/rebar
PKG_VARS_CONFIG  = pkg.vars.config
OVERLAY_VARS    ?=

.PHONY: deps test package

all: rel

##
## Rebar targets
##

compile:
	./rebar compile

deps:
	./rebar get-deps

generate: deps compile
	./rebar generate $(OVERLAY_VARS)

clean:
	./rebar clean

distclean:
	./rebar delete-deps

##
## Release targets
##

rel: generate

relclean:
	rm -rf rel/test_cluster
	rm -rf rel/op_ccm

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
	        mnesia common_test test_server syntax_tools compiler ./deps/*/ebin; \
	fi; exit 0


# Dialyzes the project.
dialyzer: plt
	dialyzer ./ebin --plt ${PLT} -Werror_handling -Wrace_conditions --fullpath

##
## Packaging targets
##

export PKG_VERSION PKG_ID PKG_BUILD BASE_DIR ERLANG_BIN REBAR OVERLAY_VARS RELEASE PKG_VARS_CONFIG

package.src: deps
	mkdir -p package
	rm -rf package/$(PKG_ID)
	git archive --format=tar --prefix=$(PKG_ID)/ $(PKG_REVISION)| (cd package && tar -xf -)
	${MAKE} -C package/$(PKG_ID) deps
	mkdir -p package/$(PKG_ID)/priv
	git --git-dir=.git describe --tags --always >package/$(PKG_ID)/priv/vsn.git
	for dep in package/$(PKG_ID)/deps/*; do \
             echo "Processing dep: $${dep}"; \
             mkdir -p $${dep}/priv; \
             git --git-dir=$${dep}/.git describe --tags >$${dep}/priv/vsn.git; \
        done
	find package/$(PKG_ID) -depth -name ".git" -exec rm -rf {} \;
	tar -C package -czf package/$(PKG_ID).tar.gz $(PKG_ID)

dist: package.src
	cp package/$(PKG_ID).tar.gz .

package: package.src
	${MAKE} -C package -f $(PKG_ID)/deps/node_package/Makefile

pkgclean: distclean
	rm -rf package