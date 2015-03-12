REPO		        ?= oneprovider_ccm

PKG_REVISION    ?= $(shell git describe --tags --always)
PKG_VERSION	    ?= $(shell git describe --tags --always | tr - .)
PKG_ID           = oneprovider-ccm-$(PKG_VERSION)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= $(BASE_DIR)/rebar
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
	./rebar generate

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
	rm -rf rel/oneprovider_ccm

##
## Packaging targets
##

export PKG_VERSION PKG_ID PKG_BUILD BASE_DIR ERLANG_BIN REBAR OVERLAY_VARS RELEASE

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