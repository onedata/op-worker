#!/bin/bash

## ===================================================================
## @author Michal Wrzeszcz
## @copyright (C): 2013 ACK CYFRONET AGH
## This software is released under the MIT license
## cited in 'LICENSE.txt'.
## ===================================================================
## @doc: This script starts distributed tests.
## ===================================================================

rm -rf /tmp/veilfs
rm -rf /tmp/veilfs2
mkdir /tmp/veilfs
mkdir /tmp/veilfs2

userdel -r veilfstestuser 2>/dev/null
userdel -r veilfstestuser2 2>/dev/null
groupdel veilfstestgroup 2>/dev/null
groupdel veilfstestgroup2 2>/dev/null
groupdel veilfstestuser 2>/dev/null
groupdel veilfstestuser2 2>/dev/null
rm -rf /home/veilfstestuser
rm -rf /home/veilfstestuser2

# User 1
groupadd veilfstestgroup
useradd veilfstestuser
usermod -a -G veilfstestgroup veilfstestuser

# User 2
groupadd veilfstestgroup2
useradd veilfstestuser2
usermod -a -G veilfstestgroup2 veilfstestuser2
usermod -a -G veilfstestgroup veilfstestuser2

mkdir -p distributed_tests_out
cp -R test_distributed/* distributed_tests_out
cd distributed_tests_out
cp -R ../cacerts .
cp -R ../c_lib/ .
cp -R ../src/veil_modules/dao/views .
cp -R ../src/gui_static .
cp -R ../config/sys.config .

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
    rm -rf /tmp/veilfs
    rm -rf /tmp/veilfs2
    mkdir /tmp/veilfs
    mkdir /tmp/veilfs2
    ct_run -pa ../deps/**/ebin -pa ../ebin -pa ./ -noshell -name tester -setcookie $COOKIE -spec  $TEST
done

find . -name "*.beam" -exec rm -rf {} \;
find . -name "*.erl" -exec rm -rf {} \;
find . -name "*.hrl" -exec rm -rf {} \;

TESTS=$(find . -name "*.spec")
for TEST in $TESTS
do
    rm -f $TEST
done
rm -f Emakefile
rm -f *.sh
rm -rf cacerts
rm -rf c_lib
rm -rf views
rm -rf gui_static
rm -f sys.config

userdel -r veilfstestuser
userdel -r veilfstestuser2
groupdel veilfstestgroup
groupdel veilfstestgroup2
rm -rf /home/veilfstestuser
rm -rf /home/veilfstestuser2

cd ..