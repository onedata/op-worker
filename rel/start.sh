#!/bin/bash

# This script is used to start an appmock instance with given app description.

# Check if there is a file in args
if [[ ! -f "$1" ]]; then
    echo "Please provide path to app description file (.erl) in the first argument."
    exit 0
fi

# Get the absolute path of the file
ABS_FILE_PATH=`readlink -e $1`

# Enter the script dir
RUNNER_SCRIPT_DIR=$(cd ${0%/*} && pwd)
cd $RUNNER_SCRIPT_DIR

# Get application version
START_ERL=`cat releases/start_erl.data`
APP_VSN=${START_ERL#* }

# Setup sed interactive options
if [ "`uname -s`" = "Darwin" ]; then
    SED_OPTS="-i '' -e"
else
    SED_OPTS="-i"
fi

# Create a proper entry in sys.config
echo "Starting appmock with configuration from: ${ABS_FILE_PATH}"
sed $SED_OPTS "s|{appmock,.*|{appmock, [{app_description_file, \"${ABS_FILE_PATH}\"}]}|g" releases/$APP_VSN/sys.config

# Start the application
./bin/appmock start
