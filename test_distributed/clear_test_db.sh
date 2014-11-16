#!/bin/bash

## ===================================================================
## @author Michal Wrzeszcz
## @copyright (C): 2014 ACK CYFRONET AGH
## This software is released under the MIT license
## cited in 'LICENSE.txt'.
## ===================================================================
## @doc: This script clears test database
## ===================================================================

curl -X DELETE 127.0.0.1:5984/files_test
curl -X DELETE 127.0.0.1:5984/system_data_test
curl -X DELETE 127.0.0.1:5984/file_descriptors_test
curl -X DELETE 127.0.0.1:5984/available_blockss_test
curl -X DELETE 127.0.0.1:5984/users_test
curl -X DELETE 127.0.0.1:5984/cookies_test
curl -X DELETE 127.0.0.1:5984/groups_test
