#!/usr/bin/env bash

#####################################################################
# @author Lukasz Opiola
# @copyright (C): 2016 ACK CYFRONET AGH
# This software is released under the MIT license
# cited in 'LICENSE.txt'.
#####################################################################
# usage:
# ./inject_gui.sh
#
# This script copies static GUI files included in a static docker image to
# given directory. Can be used to inject the GUI during release building.
#####################################################################

# Configuration
STATIC_FILES_IMAGE='docker.onedata.org/op-gui-default:VFS-1825'
TARGET_DIR='rel/op_worker/data/gui_static'

# Fail on any command failure
set -e

echo "Copying static GUI files"
echo "    from image: ${STATIC_FILES_IMAGE}"
echo "    under path: ${TARGET_DIR}"

# Create docker volume based on given image
CONTAINER_ID=`docker create -v /var/www/html ${STATIC_FILES_IMAGE} /bin/true`

# Create required dirs
mkdir -p ${TARGET_DIR}

# Remove old files (if any)
rm -rf ${TARGET_DIR}

# Copy the files ( -L = follow symbolic links )
# @TODO until we use docker 1.9 (in onedata/builder) cp from /artefact rather
# than /var/www/html because 1.9 client does not support follow symbolic links.
docker cp ${CONTAINER_ID}:/artefact ${TARGET_DIR}

# Remove unneeded container
docker rm -f ${CONTAINER_ID}
