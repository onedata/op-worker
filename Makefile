.PHONY: releases deps test

all: deps generate docs

compile:
	cp -R veilprotocol/proto src
	./rebar compile
	rm -rf src/proto

deps:
	./rebar get-deps
	git submodule init
	git submodule update

clean:
	./rebar clean

distclean: clean
	./rebar delete-deps

test: deps compile
	./rebar eunit ct skip_deps=true
	chmod +x test_distributed/start_distributed_test.sh
	./test_distributed/start_distributed_test.sh

generate: compile
	./rebar generate
	chmod u+x ./releases/veil_cluster_node/bin/veil_cluster
	chmod u+x ./releases/veil_cluster_node/bin/veil_cluster_node

docs:
	./rebar doc skip_deps=true

upgrade:
	./rebar generate-appups previous_release=${PREV}
	./rebar generate-upgrade previous_release=${PREV}


# Builds .dialyzer.plt init file. This is internal target, call dialyzer_init instead
.dialyzer.plt:
	dialyzer --build_plt --output_plt .dialyzer.plt --apps kernel stdlib sasl erts ssl tools runtime_tools crypto inets xmerl snmp public_key eunit syntax_tools compiler ./deps/*/ebin


# Starts dialyzer on whole ./ebin dir. If .dialyzer.plt does not exist, will be generated
dialyzer: compile .dialyzer.plt
	dialyzer ./ebin --plt .dialyzer.plt -Werror_handling -Wrace_conditions


# Starts full initialization of .dialyzer.plt that is required by dialyzer
dialyzer_init: compile .dialyzer.plt


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
	./releases/test_cluster/$(node)/bin/veil_cluster_node start

attach_to_node:
	./releases/test_cluster/$(node)/bin/veil_cluster_node attach

start_node_console:
	./releases/test_cluster/$(node)/bin/veil_cluster_node console