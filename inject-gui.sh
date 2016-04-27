#!/usr/bin/env bash

#####################################################################
# @author Lukasz Opiola
# @copyright (C): 2016 ACK CYFRONET AGH
# This software is released under the MIT license
# cited in 'LICENSE.txt'.
#####################################################################
# usage:
# ./inject_gui.sh <path to gui-config>
#
# This script copies static GUI files included in a static docker.
# Can be used to inject the GUI during release building.
# Requires configuration file that defines target directory and docker
# image that should be used.
#####################################################################

if [[ ! -f "${1}" ]]; then
    echo "Usage:"
    echo "    ./inject-gui.sh <path to gui config>"
    exit 1
fi

# Source gui config which should contain following exports:
# TARGET_DIR
# PRIMARY_IMAGE
# SECONDARY_IMAGE
source ${1}

echo ${TARGET_DIR}
echo ${PRIMARY_IMAGE}
echo ${SECONDARY_IMAGE}

STATIC_FILES_IMAGE=${PRIMARY_IMAGE}
docker pull ${STATIC_FILES_IMAGE}
if [ $? -ne 0 ]; then
    STATIC_FILES_IMAGE=${SECONDARY_IMAGE}
    docker pull ${STATIC_FILES_IMAGE}
    if [ $? -ne 0 ]; then
        echo "Cannot pull primary nor secondary docker image for static GUI \
        files. Exiting."
        exit 1
    fi
fi
exit 0

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
docker cp -L ${CONTAINER_ID}:/var/www/html ${TARGET_DIR}

# Remove unneeded container
docker rm -f ${CONTAINER_ID}
