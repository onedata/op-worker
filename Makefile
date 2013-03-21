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
	./gen_dev $(args)

gen_config_from_file:
	./gen_dev

gen_config_cleanup:
	./gen_dev -clean_up

release_config: gen_config generate gen_config_cleanup

release_config_from_file: gen_config_from_file generate gen_config_cleanup

start_config:
	./releases/$(node)/bin/veil_cluster_node start

node_attach:
	./releases/$(node)/bin/veil_cluster_node attach

start_config_console:
	./releases/$(node)/bin/veil_cluster_node console

gen_test_env:
	./gen_test $(args)

gen_test_env_from_file:
	./gen_test

start_test_env:
	./gen_test $(args) -start

start_test_env_from_file:
	./gen_test -start

docs:
	./rebar doc

upgrade:
	./rebar generate-appups previous_release=${PREV}
	./rebar generate-upgrade previous_release=${PREV}
