#!/bin/bash

git submodule init
git submodule update
cd veilprotocol
git merge master
cd ..
git submodule update