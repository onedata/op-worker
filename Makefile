.PHONY: deps

all: rel

deps:
	./rebar get-deps

compile:
	./rebar compile

rel: deps compile
	./rebar generate

start:
	rel/appmock/bin/appmock console

clean:
	./rebar clean
	rm -rf rel/appmock

distclean: clean
	./rebar delete-deps