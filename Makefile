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
	chmod u+x ./releases/veil_cluster_node/bin/veil_cluster
	chmod u+x ./releases/veil_cluster_node/bin/veil_cluster_node

docs:
	./rebar doc

upgrade:
	./rebar generate-appups previous_release=${PREV}
	./rebar generate-upgrade previous_release=${PREV}


##############################
## TARGETS USED FOR TESTING ##
##############################

### Single node test
gen_test_node:
	./gen_dev $(args)

gen_test_node_from_file:
	./gen_dev

### Test environment
gen_test_env:
	./gen_test $(args)

gen_test_env_from_file:
	./gen_test

gen_start_test_env:
	./gen_test $(args) -start

gen_start_test_env_from_file:
	./gen_test -start

start_test_env:
	./gen_test $(args) -start_no_generate

start_test_env_from_file:
	./gen_test -start_no_generate

### Starting a node
start_node:
	./releases/$(node)/bin/veil_cluster_node start

attach_to_node:
	./releases/$(node)/bin/veil_cluster_node attach

start_node_console:
	./releases/$(node)/bin/veil_cluster_node console