#!/bin/bash

## ===================================================================
## @author Michal Wrzeszcz
## @copyright (C): 2013 ACK CYFRONET AGH
## This software is released under the MIT license
## cited in 'LICENSE.txt'.
## ===================================================================
## @doc: This script starts all distributed tests and parse output.
## To start only one suite, execute the script with one argument
## (name of the spec file that describes suite).
## To start only one test case, execute the script with two arguments
## (name of the spec file that describes suite & name of the test case to be done).
## If you are using the script with two arguments, the name of suite must be
## 'name of spec file'_SUITE.
## ===================================================================

./test_distributed/verbose_distributed_test.sh $1 $2 | ./test_distributed/filter_output.sh
for tout in `find distributed_tests_out -name "TEST-*.xml"`; do
  awk '/testcase/{gsub("<testcase name=\"[a-z]+_per_suite\"(([^/>]*/>)|([^>]*>[^<]*</testcase>))", "")}1' $tout > $tout.tmp
  mv $tout.tmp $tout
done

