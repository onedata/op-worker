#!/bin/bash
exit 0
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
cd bigcouchdb
git fetch
git checkout master
git pull
cd ..
