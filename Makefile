.PHONY: deps test

all: rel

##############################################################
## LOCAL TARGETS
##############################################################

##
## Rebar targets local
##

compile_local:
	cp -R c_src/oneproxy/proto src
	./rebar compile
	rm -rf src/proto

deps_local:
	./rebar get-deps

generate_local: deps_local compile_local
	./rebar generate

clean_local:
	./rebar clean

distclean_local:
	./rebar delete-deps

##
## Release targets local
##

rel_local: generate_local

relclean_local:
	rm -rf rel/test_cluster
	rm -rf rel/oneprovider_node

##
## Testing targets local
##

ctbuild_local: generate_local
	./test_distributed/build_distributed_test.sh

eunit_local: deps_local compile_local
	./rebar eunit skip_deps=true suites=${SUITES}
## Rename all tests in order to remove duplicated names (add _(++i) suffix to each test)
	@for tout in `find test -name "TEST-*.xml"`; do awk '/testcase/{gsub("_[0-9]+\"", "_" ++i "\"")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done

ct_local:
	./test_distributed/start_distributed_test.sh ${SUITE} ${CASE}
	@for tout in `find distributed_tests_out -name "TEST-*.xml"`; do awk '/testcase/{gsub("<testcase name=\"[a-z]+_per_suite\"(([^/>]*/>)|([^>]*>[^<]*</testcase>))", "")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done

testclean_local:
	rm -rf distributed_tests_out

##
## Dialyzer targets local
##

# Builds .dialyzer.plt init file. This is internal target, call dialyzer_init instead
.dialyzer.plt_local:
	-dialyzer --build_plt --output_plt .dialyzer.plt --apps kernel stdlib sasl erts ssl tools runtime_tools crypto inets xmerl snmp public_key eunit syntax_tools compiler ./deps/*/ebin


# Starts dialyzer on whole ./ebin dir. If .dialyzer.plt does not exist, will be generated
dialyzer_local: compile_local .dialyzer.plt_local
	-dialyzer ./ebin --plt .dialyzer.plt -Werror_handling -Wrace_conditions


# Starts full initialization of .dialyzer.plt that is required by dialyzer
dialyzer_init_local: compile_local .dialyzer.plt_local

##############################################################
## DOCKER TARGETS
##############################################################

##
## Rebar targets docker
##

compile:
	./deps/bamboos/docker/make.py compile_local

deps: deps_local

generate:
	./deps/bamboos/docker/make.py generate_local

clean: relclean testclean
	./deps/bamboos/docker/make.py clean_local

distclean:
	./deps/bamboos/docker/make.py distclean_local

##
## Release targets local
##

rel: rel_local

relclean: relclean_local

##
## Testing targets docker
##

ctbuild:
	./deps/bamboos/docker/make.py ctbuild_local

eunit:
	./deps/bamboos/docker/make.py eunit_local

ct: ctbuild
	docker run --rm -it  -w /root/oneprovider -v $(CURDIR):/root/oneprovider  -v /var/run/docker.sock:/var/run/docker.sock -h d1.local onedata/worker make ct_local

test: eunit ct

testclean: testclean_local

##
## Dialyzer targets docker
##

.dialyzer.plt:
	./deps/bamboos/docker/make.py .dialyzer.plt_local

dialyzer:
	./deps/bamboos/docker/make.py dialyzer_local

dialyzer_init:
	./deps/bamboos/docker/make.py dialyzer_init_local