#!/usr/bin/env bash

# Path to nvm binary
NVM_BINARY=/usr/lib/nvm/nvm.sh

# Source NVM
. ${NVM_BINARY}
# Make sure NVM loads node binaries
nvm use default node
# Run the tests
cd test_gui && ember test