#!/usr/bin/env bash

# Make sure NVM loads node binaries
nvm use default node
# Run the tests
cd test_gui && ember test