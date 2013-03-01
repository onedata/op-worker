.PHONY: releases deps test

all: deps compile generate docs

compile:
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

distclean: clean
	./rebar delete-deps

test:
	./rebar compile eunit

generate:
	./rebar generate

docs:
	./rebar doc
