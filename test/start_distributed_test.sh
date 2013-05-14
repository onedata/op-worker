#!/bin/bash

mkdir -p logs
cd test/distributed
erl -make
erl -name 'starter@plgsl63.local' -s distributed_test_starter start
find . -name "*.beam" -exec rm -rf {} \;
cd ../..