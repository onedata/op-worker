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

./test_distributed/verbose_distributed_test.sh rest_test main_test | ./test_distributed/filter_output.sh