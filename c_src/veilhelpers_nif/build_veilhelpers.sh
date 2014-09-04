#!/usr/bin/env bash

cd veilhelpers

# Build has to be made in other shell in order to isolate LDFLAGS value (otherwise rebar would override it) 
(LDFLAGS="" && make -s release || exit 1)
mkdir -p ../c_lib

if [ "`uname -s`" = "Darwin" ]; then
    cp release/*.dylib ../c_lib
else
    cp release/*.so ../c_lib
fi
