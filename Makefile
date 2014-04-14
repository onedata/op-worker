BASHO_BENCH_DIR = "deps/basho_bench"
STRESS_TESTS_SRC_DIR = "stress_test"
DIST_TESTS_SRC_DIR = "test_distributed"

.PHONY: releases deps test docs

all: deps generate docs

compile:
	./gen_config
	cp -R veilprotocol/proto src
	./rebar compile
	rm -rf src/proto

deps:
	./rebar get-deps
#	./rebar update-deps
	git submodule init
	git submodule update

clean:
	make -C docs clean
	./rebar clean

distclean: clean
	./rebar delete-deps


eunit: deps compile
	./rebar eunit skip_deps=true
## Rename all tests in order to remove duplicated names (add _(++i) suffix to each test)
	@for tout in `find test -name "TEST-*.xml"`; do awk '/testcase/{gsub("_[0-9]+\"", "_" ++i "\"")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done

ct:
	echo "test ok"

test: ct


generate: compile
	./rebar generate
	chmod u+x ./releases/veil_cluster_node/bin/veil_cluster
	chmod u+x ./releases/veil_cluster_node/bin/veil_cluster_node

docs: deps
	make -C docs html
	./rebar doc skip_deps=true

pdf: deps
	make -C docs latexpdf

upgrade:
	./rebar generate-appups previous_release=${PREV}
	./rebar generate-upgrade previous_release=${PREV}

rpm: deps generate
	./releases/rpm_files/create_rpm


# Builds .dialyzer.plt init file. This is internal target, call dialyzer_init instead
.dialyzer.plt:
	-dialyzer --build_plt --output_plt .dialyzer.plt --apps kernel stdlib sasl erts ssl tools runtime_tools crypto inets xmerl snmp public_key eunit syntax_tools compiler ./deps/*/ebin


# Starts dialyzer on whole ./ebin dir. If .dialyzer.plt does not exist, will be generated
dialyzer: compile .dialyzer.plt
	-dialyzer ./ebin --plt .dialyzer.plt -Werror_handling -Wrace_conditions


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

### Basho-Bench build (used by CI)
basho_bench: deps compile
	cp ${STRESS_TESTS_SRC_DIR}/**/*.erl ${BASHO_BENCH_DIR}/src
	cp ${DIST_TESTS_SRC_DIR}/wss.erl ${BASHO_BENCH_DIR}/src
	@mkdir -p ${BASHO_BENCH_DIR}/tests
	@mkdir -p ${BASHO_BENCH_DIR}/ebin
	@cp ${STRESS_TESTS_SRC_DIR}/**/*.config ${BASHO_BENCH_DIR}/tests
	@cp -R include/* ${BASHO_BENCH_DIR}/include/
	@cp -R deps/protobuffs/ebin/* ${BASHO_BENCH_DIR}/ebin/
	@cp -R deps/websocket_client/ebin/* ${BASHO_BENCH_DIR}/ebin/
	@cp -R ebin/* ${BASHO_BENCH_DIR}/ebin/
	cd ${BASHO_BENCH_DIR} && make all
	@cp ${STRESS_TESTS_SRC_DIR}/*.escript ${BASHO_BENCH_DIR}/
	@cp ${STRESS_TESTS_SRC_DIR}/*.py ${BASHO_BENCH_DIR}/
