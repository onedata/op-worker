.PHONY: deps test

all: rel

compile:
	cp -R c_src/oneproxy/proto src
	./rebar compile
	rm -rf src/proto

deps:
	./rebar get-deps

clean: relclean testclean
	./rebar clean

distclean: clean
	./rebar delete-deps

generate: deps compile
	./rebar generate

##
## Testing
##

ctbuild_local: deps compile generate
	./test_distributed/build_distributed_test.sh

ctbuild: deps
	./deps/bamboos/docker/make.py ctbuild_local

eunit: deps compile
	./rebar eunit skip_deps=true suites=${SUITES}
  ## Rename all tests in order to remove duplicated names (add _(++i) suffix to each test)
	@for tout in `find test -name "TEST-*.xml"`; do awk '/testcase/{gsub("_[0-9]+\"", "_" ++i "\"")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done

ct_local:
	./test_distributed/start_distributed_test.sh ${SUITE} ${CASE}
	@for tout in `find distributed_tests_out -name "TEST-*.xml"`; do awk '/testcase/{gsub("<testcase name=\"[a-z]+_per_suite\"(([^/>]*/>)|([^>]*>[^<]*</testcase>))", "")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done

ct: ctbuild
	docker run --rm -it  -w /root/oneprovider -v $(CURDIR):/root/oneprovider  -v /var/run/docker.sock:/var/run/docker.sock -h d1.local onedata/worker make ct_local

test: eunit ct

testclean:
	rm -rf distributed_tests_out

##
## Release targets
##

rel: generate

relclean:
	rm -rf rel/test_cluster
	rm -rf rel/oneprovider_node

##
## Dialyzer
##

# Builds .dialyzer.plt init file. This is internal target, call dialyzer_init instead
.dialyzer.plt:
	-dialyzer --build_plt --output_plt .dialyzer.plt --apps kernel stdlib sasl erts ssl tools runtime_tools crypto inets xmerl snmp public_key eunit syntax_tools compiler ./deps/*/ebin


# Starts dialyzer on whole ./ebin dir. If .dialyzer.plt does not exist, will be generated
dialyzer: compile .dialyzer.plt
	-dialyzer ./ebin --plt .dialyzer.plt -Werror_handling -Wrace_conditions


# Starts full initialization of .dialyzer.plt that is required by dialyzer
dialyzer_init: compile .dialyzer.plt