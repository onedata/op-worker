#!/usr/bin/env bash

set -e -v

# for each given module, find its location, compile and copy to releases
for module in $@; do
    find . -type f -name ${module}.erl -exec erlc `ls deps | awk '{ print "-I deps/" $1}'` -I include -I deps {} \;
    for rel_dir in `ls rel/op_worker/lib/ | grep op_worker`; do
        cp *.beam rel/op_worker/lib/${rel_dir}/ebin/
    done
done
rm *.beam


