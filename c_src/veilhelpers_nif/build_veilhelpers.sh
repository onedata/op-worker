#!/usr/bin/env bash

cd veilhelpers

# Build has to be made in other shell in order to isolate LDFLAGS value (otherwise rebar would override it) 
(LDFLAGS="" && make -s all || exit 1) 
mkdir -p ../c_lib
cp build/*.so ../c_lib