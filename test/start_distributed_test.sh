#!/bin/bash

mkdir -p logs
cd test/distributed
cp dist.spec dist_tmp.spec

HOST=$HOSTNAME

if [[ "$HOST" != *\.* ]]
then
  HOST=$HOST.local
fi

sed -i "s/localhost/$HOST/g" dist_tmp.spec

erl -make
erl -name starter -s distributed_test_starter start

find . -name "*.beam" -exec rm -rf {} \;
rm dist_tmp.spec
cd ../..