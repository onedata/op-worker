#!/usr/bin/env bash

#####################################################################
#  @author Rafal Slota
#  @copyright (C): 2013 ACK CYFRONET AGH
#  This software is released under the MIT license
#  cited in 'LICENSE.txt'.
#####################################################################
#  This script is used to include given as first argument
#  dynamic libraries into veil_cluster release package based on ldd 
#  output from file given as second agrument. 
#####################################################################

if [ $# -lt 2 ]
then
    echo "Usage: $0 libname file1 [file2...]" 
    exit 1
fi

LIB_NAME=$1
shift

BIN_DIR="./c_lib"

rm -f $BIN_DIR/lib${LIB_NAME}*

for libfile in $@
do
    for file in $(ldd $BIN_DIR/$libfile | grep -v "$BIN_DIR/" | grep '=>' | awk -F'=>' '{print $2}' | awk -F' ' '{print $1}' | grep lib${LIB_NAME})
    do
        cp -Hs $file ./$BIN_DIR
        ln -sf `basename $file` $BIN_DIR/`basename $(echo $file | sed 's/\.so\..*/\.so/')`
    done
done
