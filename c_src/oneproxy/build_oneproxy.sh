#!/usr/bin/env bash

export install_dir=$(cd $1 && pwd)
cd "$(dirname "$0")"

echo $1 $install_dir

mkdir -p build
# Build has to be made in other shell in order to isolate LDFLAGS value (otherwise rebar would override it)
( cd build &&
  LDFLAGS="" CXXFLAGS="" cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$install_dir &&
  make -j10 &&
  make install )
