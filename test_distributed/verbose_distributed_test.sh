#!/bin/bash

## ===================================================================
## @author Michal Wrzeszcz
## @copyright (C): 2013 ACK CYFRONET AGH
## This software is released under the MIT license
## cited in 'LICENSE.txt'.
## ===================================================================
## @doc: This script starts distributed tests.
## ===================================================================

umask 0
cd distributed_tests_out

if [ $# -gt 0 ]
then
    TESTS=./$1.spec

    if [ $# -gt 1 ]
    then
        sed -i "s/suites/cases/g" $TESTS
        TEST_CASE=$1'_SUITE, '$2}.
        sed -i "s/all}./$TEST_CASE/g" $TESTS
    fi
else
    TESTS=$(find . -name "*.spec")
fi

for TEST in $TESTS
do
    if [[ "`cat $TEST` | grep cth_surefire" != "" ]]; then
        echo "" >> $TEST
        TEST_NAME=`basename "$TEST" ".spec"`
        echo "{ct_hooks, [{cth_surefire, [{path,\"TEST-$TEST_NAME-report.xml\"}]}]}." >> $TEST
    fi
    ct_run -pa ../deps/**/ebin -pa ../ebin -pa ./ -noshell -name tester -spec  $TEST
done

## Clean
find . -name "*.beam" -exec rm -rf {} \;
find . -name "*.erl" -exec rm -rf {} \;
find . -name "*.hrl" -exec rm -rf {} \;

TESTS=$(find . -name "*.spec")
for TEST in $TESTS
do
    rm -f $TEST
done
rm -f *.sh
rm -rf cacerts
rm -rf certs
rm -rf c_lib
#rm -rf gui_static
rm -f sys.config

cd ..
for tout in `find distributed_tests_out -name "TEST-*.xml"`; do awk '/testcase/{gsub("<testcase name=\"[a-z]+_per_suite\"(([^/>]*/>)|([^>]*>[^<]*</testcase>))", "")}1' $$tout > $$tout.tmp; mv $$tout.tmp $$tout; done
