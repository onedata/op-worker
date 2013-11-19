#!/bin/bash

git submodule update
cd veilprotocol
git fetch
git checkout master
git pull
cd ..
cd veilhelpers
git fetch
git checkout master
git pull
cd ..