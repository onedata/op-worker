#!/bin/bash

## ===================================================================
## @author Michal Wrzeszcz
## @copyright (C): 2013 ACK CYFRONET AGH
## This software is released under the MIT license
## cited in 'LICENSE.txt'.
## ===================================================================
## @doc: This script starts distributed tests and parse output.
## ===================================================================

./test_distributed/verbose_distributed_test.sh | ./test_distributed/filter_output.sh