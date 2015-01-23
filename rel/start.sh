#!/bin/bash

if [[ ! -f "$1" ]]; then
    echo "Please provide path to app description file (.erl) in the first argument."
    exit 0
fi

ABS_FILE_PATH=`readlink -e $1`

echo $ABS_FILE_PATH