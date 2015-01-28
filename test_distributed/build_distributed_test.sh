#!/bin/bash

## ===================================================================
## @author Michal Wrzeszcz
## @copyright (C): 2013 ACK CYFRONET AGH
## This software is released under the MIT license
## cited in 'LICENSE.txt'.
## ===================================================================
## @doc: This script builds distributed tests.
## ===================================================================

## Build with "TEST"
## Uncomment when you need recompilation with "TEST" flag before execution
#if [ ! -f ebin/.test ]; then rm -rf ebin; fi
#mkdir -p ebin ; touch ebin/.test
#cp -R c_src/oneproxy/proto src
#./rebar -D TEST compile skip_deps=true
#rm -rf src/proto

## Prepre test dir
mkdir -p distributed_tests_out
cp -R test_distributed/* distributed_tests_out
cd distributed_tests_out

## Compile utils
erl -make

## Clean
rm -f Emakefile
rm -f *.sh
cd ..
