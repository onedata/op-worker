## ===================================================================
## @author Michal Wrzeszcz
## @copyright (C): 2013 ACK CYFRONET AGH
## This software is released under the MIT license
## cited in 'LICENSE.txt'.
## ===================================================================
## @doc: This script starts distributed tests.
## ===================================================================

#!/bin/bash
mkdir -p test/distributed_tests_out
cp -R test/distributed/* test/distributed_tests_out
cd test/distributed_tests_out

HOST=$HOSTNAME

if [[ "$HOST" != *\.* ]]
then
    HOST=$HOST.local
fi

TESTS=$(find . -name "*.spec")
for TEST in $TESTS
do
    if [[ "`cat $TEST` | grep cth_surefire" != "" ]]; then
        echo "" >> $TEST
        TEST_NAME=`basename "$TEST" ".spec"`
        echo "{ct_hooks, [{cth_surefire, [{path,\"TEST-$TEST_NAME-report.xml\"}]}]}." >> $TEST
    fi
    sed -i "s/localhost/$HOST/g" $TEST
done

erl -make
erl -noshell -name starter -s distributed_test_starter start $TESTS

find . -name "*.beam" -exec rm -rf {} \;
find . -name "*.erl" -exec rm -rf {} \;

for TEST in $TESTS
do
    rm -f $TEST
done
rm -f Emakefile

cd ../..