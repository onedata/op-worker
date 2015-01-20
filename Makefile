all: deps compile generate

deps:
	./rebar get-deps

compile:
	./rebar compile

generate:
	./rebar generate

rel: compile generate

start:
	rel/appmock/bin/appmock console