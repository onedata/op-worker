#!/bin/bash

mkdir -p logs
cd test/distributed

HOST=$HOSTNAME

if [[ "$HOST" != *\.* ]]
then
    HOST=$HOST.local
fi

TESTS=$(find . -name "*.spec")
for TEST in $TESTS
do
    cp $TEST $TEST.tmp
    sed -i "s/localhost/$HOST/g" $TEST.tmp
    TESTS_TMP="$TESTS_TMP $TEST.tmp"
done

erl -make
erl -name starter -s distributed_test_starter start $TESTS_TMP

find . -name "*.beam" -exec rm -rf {} \;

for TEST in $TESTS_TMP
do
    rm -f $TEST
done

cd ../..