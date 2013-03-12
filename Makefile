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

test:
	./rebar compile
	./rebar eunit skip_deps=true

generate: compile
	./rebar generate

docs:
	./rebar doc

upgrade:
	./rebar generate-appups previous_release=${PREV}
	./rebar generate-upgrade previous_release=${PREV}
