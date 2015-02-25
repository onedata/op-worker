.PHONY: deps test

all: rel

##
## Rebar targets
##

compile:
	./rebar compile
	cp ebin/*SUITE* test_distributed/

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
	rm -rf rel/oneprovider_node

##
## Testing targets
##

eunit:
	./rebar eunit skip_deps=true suites=${SUITES}
## Rename all tests in order to remove duplicated names (add _(++i) suffix to each test)
	@for tout in `find test -name "TEST-*.xml"`; do awk '/testcase/{gsub("_[0-9]+\"", "_" ++i "\"")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done

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
	        mnesia syntax_tools compiler ./deps/*/ebin; \
	fi; exit 0
            

# Dialyzes the project.
dialyzer: plt
	dialyzer ./ebin --plt ${PLT} -Werror_handling -Wrace_conditions --fullpath
