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

rm -rf /tmp/onedata
rm -rf /tmp/onedata2
rm -rf /tmp/onedata3
mkdir /tmp/onedata
mkdir /tmp/onedata2
mkdir /tmp/onedata3

userdel -r onedatatestuser 2>/dev/null
userdel -r onedatatestuser2 2>/dev/null
groupdel onedatatestgroup 2>/dev/null
groupdel onedatatestgroup2 2>/dev/null
groupdel onedatatestgroup3 2>/dev/null
groupdel onedatatestuser 2>/dev/null
groupdel onedatatestuser2 2>/dev/null
rm -rf /home/onedatatestuser
rm -rf /home/onedatatestuser2

# User 1
groupadd onedatatestgroup
useradd onedatatestuser
usermod -a -G onedatatestgroup onedatatestuser

# User 2
groupadd onedatatestgroup2
useradd onedatatestuser2
usermod -a -G onedatatestgroup2 onedatatestuser2
usermod -a -G onedatatestgroup onedatatestuser2

# Group 3
groupadd onedatatestgroup3

mkdir -p distributed_tests_out
cp -R test_distributed/* distributed_tests_out
cd distributed_tests_out
cp -R ../cacerts .
cp -R ../certs .
cp -R ../c_lib/ .
#cp -R ../src/oneprovider_modules/dao/views .
#cp -R ../src/gui_static .
cp -R ../rel/sys.config .

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

COOKIE=`hostname -f`

for TEST in $TESTS
do
    if [[ "`cat $TEST` | grep cth_surefire" != "" ]]; then
        echo "" >> $TEST
        TEST_NAME=`basename "$TEST" ".spec"`
        echo "{ct_hooks, [{cth_surefire, [{path,\"TEST-$TEST_NAME-report.xml\"}]}]}." >> $TEST
    fi
    rm -rf /tmp/onedata
    rm -rf /tmp/onedata2
    rm -rf /tmp/onedata3
    mkdir /tmp/onedata
    mkdir /tmp/onedata2
    mkdir /tmp/onedata3
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
rm -rf certs
rm -rf c_lib
rm -rf views
rm -rf gui_static
rm -f sys.config

userdel -r onedatatestuser
userdel -r onedatatestuser2
groupdel onedatatestgroup
groupdel onedatatestgroup2
groupdel onedatatestgroup3
rm -rf /home/onedatatestuser
rm -rf /home/onedatatestuser2

cd ..
