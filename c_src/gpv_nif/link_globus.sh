#!/usr/bin/env bash

#####################################################################
#  @author Rafal Slota
#  @copyright (C): 2013 ACK CYFRONET AGH
#  This software is released under the MIT license
#  cited in 'LICENSE.txt'.
#####################################################################
#  This script is used to include globus dynamic libraries into
#  veil_cluster release package. This script should be removed
#  after setting globus as distro package dependency
#####################################################################

BIN_DIR="c_lib"
NIF_DRIVER="$BIN_DIR/gpv_drv.so"

rm -f $BIN_DIR/libglobus*

for file in $(ldd $NIF_DRIVER | grep '=>' | awk -F'=>' '{print $2}' | awk -F' ' '{print $1}' | grep libglobus)
do
    cp -H $file ./$BIN_DIR
    ln -s `basename $file` $BIN_DIR/`basename $(echo $file | sed 's/\.so\..*/\.so/')`
done