#!/bin/bash

## ===================================================================
## @author Michal Wrzeszcz
## @copyright (C): 2013 ACK CYFRONET AGH
## This software is released under the MIT license
## cited in 'LICENSE.txt'.
## ===================================================================
## @doc: This script starts distributed tests.
## ===================================================================

mkdir -p distributed_tests_out
cp -R test_distributed/* distributed_tests_out
cd distributed_tests_out
cp -R ../cacerts .
cp -R ../c_lib/ .
cp -R ../src/veil_modules/dao/views .

TESTS=$(find . -name "*.spec")
erl -make

IFCONFIG_LINE=`ifconfig | grep "inet addr:.*Bcast:"`
COLON_INDEX=`awk -v a="$IFCONFIG_LINE" -v b=":" 'BEGIN{print index(a, b)}'`
BCAST_INDEX=`awk -v a="$IFCONFIG_LINE" -v b="Bcast" 'BEGIN{print index(a, b)}'`
COOKIE=${IFCONFIG_LINE:COLON_INDEX:((BCAST_INDEX - COLON_INDEX - 3))}

for TEST in $TESTS
do
    if [[ "`cat $TEST` | grep cth_surefire" != "" ]]; then
        echo "" >> $TEST
        TEST_NAME=`basename "$TEST" ".spec"`
        echo "{ct_hooks, [{cth_surefire, [{path,\"TEST-$TEST_NAME-report.xml\"}]}]}." >> $TEST
    fi
    ct_run -noshell -name tester -setcookie $COOKIE -spec  $TEST
done

find . -name "*.beam" -exec rm -rf {} \;
find . -name "*.erl" -exec rm -rf {} \;
find . -name "*.hrl" -exec rm -rf {} \;

for TEST in $TESTS
do
    rm -f $TEST
done
rm -f Emakefile
rm -f *.sh
rm -rf cacerts
rm -rf c_lib
rm -rf views

cd ..