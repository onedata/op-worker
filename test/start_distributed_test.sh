#!/bin/bash

mkdir -p logs
cd test/distributed
erl -make
erl -name starter -s distributed_test_starter start
find . -name "*.beam" -exec rm -rf {} \;
cd ../..