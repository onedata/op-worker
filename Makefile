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
	./rebar compile eunit skip_deps=true

ct:
	./rebar compile ct skip_deps=true

website_test:
	@echo "\n=================================================================\nThis sets up the website @ localhost:8000 \
	for 60 seconds for user testing.\n=================================================================\n"
	./rebar compile ct skip_deps=true suites=veil_modules/control_panel/website

generate: compile
	./rebar generate

docs:
	./rebar doc

upgrade:
	./rebar generate-appups previous_release=${PREV}
	./rebar generate-upgrade previous_release=${PREV}
