#!/usr/bin/env bash

#####################################################################
#  @author Rafal Slota
#  @copyright (C): 2014 ACK CYFRONET AGH
#  This software is released under the MIT license
#  cited in 'LICENSE.txt'.
#####################################################################
#  This script is used to include given as first argument
#  dynamic libraries into onepanel release package based on ldd
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
    if [ ! -e $BIN_DIR/$libfile ]; then
        continue;
    fi


    if [ "`uname -s`" = "Darwin" ]; then
        libs_list=$(otool -L $BIN_DIR/$libfile | grep -v "$BIN_DIR/" | grep -v "@loader_path" | grep -v ":" | awk -F' ' '{print $1}' | grep ${LIB_NAME})
    else
        libs_list=$(ldd $BIN_DIR/$libfile | grep -v "$BIN_DIR/" | grep '=>' | awk -F'=>' '{print $2}' | awk -F' ' '{print $1}' | grep ${LIB_NAME})
    fi

    for file in $libs_list
    do
        # Copy and link shared library
        link="`basename $file`"
        target="$BIN_DIR/`basename $(echo $file | sed 's/\.so\..*/\.so/' | sed 's/\.[0-9.]*\.dylib.*/\.dylib/')`"
        if [ ! -f "$BIN_DIR/$link" ]; then
            cp -L $file $BIN_DIR
        fi
        if [ `basename $link` != `basename $target` ]; then
            ln -sf $link $target
        fi

        chmod u+w ./$BIN_DIR/$link
        chmod u+w ./$BIN_DIR/$libfile

        # Change rpath on OSX
        if [ "`uname -s`" = "Darwin" ]; then
            install_name=`otool -D ./$BIN_DIR/$link | tail -1`
            install_name_tool -id "@loader_path/$link" ./$BIN_DIR/$link
            install_name_tool -id "@loader_path/$libfile" ./$BIN_DIR/$libfile
            install_name_tool -change "$install_name" "@loader_path/$link" ./$BIN_DIR/$libfile
        fi

    done
done
