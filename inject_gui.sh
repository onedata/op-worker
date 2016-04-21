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
STATIC_FILES_IMAGE='docker.onedata.org/oz-gui-default:VFS-1825'
TARGET_DIR='rel/oz_worker/data/gui_static'

# Fail on any command failure
set -e

echo "Copying static GUI files"
echo "    from image: ${STATIC_FILES_IMAGE}"
echo "    under path: ${TARGET_DIR}"

# Create docker volume based on given image. Path /var/www/html is arbitrarily
# chosen, could be anything really - it must be later referenced in docker cp.
CONTAINER_ID=`docker create -v /var/www/html ${STATIC_FILES_IMAGE} /bin/true`

# Create required dirs
mkdir -p ${TARGET_DIR}

# Remove old files (if any)
rm -rf ${TARGET_DIR}

# Copy the files ( -L = follow symbolic links ) - warning:
#   this works on docker client 1.10+ !
# Use path from docker create volume
docker cp ${CONTAINER_ID}:/var/www/html ${TARGET_DIR}

# Remove unneeded container
docker rm -f ${CONTAINER_ID}
