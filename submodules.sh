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
cd bigcouchdb
git fetch
git checkout master
git pull
cd ..

cd veilclient
git fetch
git checkout master
git pull
cd ..

cd onepanel
git fetch
git checkout bugfix/VFS-719-dodanie-onepanel-do-rpma
git pull
cd ..
