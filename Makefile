.PHONY: releases deps test

all: deps generate docs

compile:
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

distclean: clean
	./rebar delete-deps

test: deps compile
	./rebar eunit ct skip_deps=true

generate: compile
	./rebar generate

gen_config:
	./gen_dev $(ARGS)

gen_config_from_file:
	./gen_dev

start_config: gen_config generate

start_config_from_file: gen_config_from_file generate

generate_ccm: compile
	./rebar generate overlay_vars=vars/ccm_node_vars.config

docs:
	./rebar doc

upgrade:
	./rebar generate-appups previous_release=${PREV}
	./rebar generate-upgrade previous_release=${PREV}
