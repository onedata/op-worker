BASHO_BENCH_DIR = "deps/basho_bench"
STRESS_TESTS_SRC_DIR = "stress_test"
DIST_TESTS_SRC_DIR = "test_distributed"

.PHONY: releases deps test docs

all: generate docs

compile:
	-@if [ -f ebin/.test ]; then rm -rf ebin; fi 
	cp -R clproto/proto src
	cp -R rtproto/proto src
	cp -R deps/prproto/proto src
	cp -R c_src/oneproxy/proto src
	./rebar compile
	rm -rf src/proto

deps:
	./rebar get-deps
#	./rebar update-deps
	git submodule init
	git submodule update

clean:
	make -C docs clean
	make -C oneclient clean
	make -C helpers clean
	./rebar clean

distclean: clean
	./rebar delete-deps


eunit: deps compile
	./rebar eunit skip_deps=true suites=${SUITES}
## Rename all tests in order to remove duplicated names (add _(++i) suffix to each test)
	@for tout in `find test -name "TEST-*.xml"`; do awk '/testcase/{gsub("_[0-9]+\"", "_" ++i "\"")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done

ct: deps compile
	-@if [ ! -f ebin/.test ]; then rm -rf ebin; fi
	-@mkdir -p ebin ; touch ebin/.test 
	cp -R clproto/proto src
	cp -R rtproto/proto src
	cp -R deps/prproto/proto src
	cp -R c_src/oneproxy/proto src
	./rebar -D TEST compile
	rm -rf src/proto
#	./rebar ct skip_deps=true
	chmod +x test_distributed/start_distributed_test.sh
	./test_distributed/start_distributed_test.sh ${SUITE} ${CASE}
## Remove *_per_suite result from CT test results
	@for tout in `find distributed_tests_out -name "TEST-*.xml"`; do awk '/testcase/{gsub("<testcase name=\"[a-z]+_per_suite\"(([^/>]*/>)|([^>]*>[^<]*</testcase>))", "")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done

test: eunit ct


generate: deps compile
	./rebar generate
	chmod u+x ./releases/oneprovider_node/bin/oneprovider
	chmod u+x ./releases/oneprovider_node/bin/oneprovider_node

docs: deps
	make -C docs html
	./rebar doc skip_deps=true

pdf: deps
	make -C docs latexpdf

upgrade:
	./rebar generate-appups previous_release=${PREV}
	./rebar generate-upgrade previous_release=${PREV}

rpm: deps generate
	make -C onepanel rel CONFIG=config/oneprovider.config
	./releases/rpm_files/create_rpm

deb: deps generate
	make -C onepanel rel CONFIG=config/oneprovider.config
	./releases/rpm_files/create_deb

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
	./releases/test_cluster/$(node)/bin/oneprovider_node start

attach_to_node:
	./releases/test_cluster/$(node)/bin/oneprovider_node attach

start_node_console:
	./releases/test_cluster/$(node)/bin/oneprovider_node console

### Basho-Bench build (used by CI)
basho_bench: deps 
	@cp -R clproto/proto ${BASHO_BENCH_DIR}/src
	cp ${STRESS_TESTS_SRC_DIR}/**/*.erl ${BASHO_BENCH_DIR}/src
	cp ${DIST_TESTS_SRC_DIR}/wss.erl ${BASHO_BENCH_DIR}/src
	@mkdir -p ${BASHO_BENCH_DIR}/tests
	@mkdir -p ${BASHO_BENCH_DIR}/ebin
	@cp ${STRESS_TESTS_SRC_DIR}/**/*.config ${BASHO_BENCH_DIR}/tests
	@cp -R include/* ${BASHO_BENCH_DIR}/include
	cd ${BASHO_BENCH_DIR} && make all
	@cp ${STRESS_TESTS_SRC_DIR}/*.escript ${BASHO_BENCH_DIR}/
	@cp ${STRESS_TESTS_SRC_DIR}/*.py ${BASHO_BENCH_DIR}/
