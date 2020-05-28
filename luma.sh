#!/usr/bin/env bash

#ADMIN_TOKEN=
ONEPROVIDER_HOST=https://dev-oneprovider-krakow.default.svc.cluster.local
ONEPROVIDER_PANEL_STORAGES=${ONEPROVIDER_HOST}/api/v3/onepanel/provider/storages
ONEPROVIDER_PANEL_SPACES=${ONEPROVIDER_HOST}/api/v3/onepanel/provider/spaces
#ONEZONE_HOST=dev-onezone.default.svc.cluster.local

ADMIN_USER=onepanel
USER=joe
PASSWORD=password
STORAGE_ID=f4f7678b61e9f1e236bf310eff268f63ch13ef

USER_ID=c4cbd04807eefe21c7fb5457fd951e0ech1e6f
SPACE_ID=a1f575968c061fb0ed0435ae081cf0acch90a3
# get storages

#get spaces
#curl -X GET -u "$ADMIN_USER":"$PASSWORD" ${ONEPROVIDER_PANEL_SPACES}/123


#curl -X GET -vvv -u "$ADMIN_USER":"$PASSWORD" ${ONEPROVIDER_PANEL_STORAGES}/${STORAGE_ID}/luma/db/storage_access/all/onedata_user_to_credentials/${USER_ID}

#
curl -X GET -vvv -u "$ADMIN_USER":"$PASSWORD" ${ONEPROVIDER_PANEL_STORAGES}/${STORAGE_ID}/luma/db/storage_access/posix_compatible/default_credentials/$SPACE_ID
#
#curl -X GET -vvv -u "$ADMIN_USER":"$PASSWORD" ${ONEPROVIDER_PANEL_STORAGES}/${STORAGE_ID}/luma/db/oneclient_display_credentials/all/default/$SPACE_ID

#curl -X  DELETE -vvv -u "$ADMIN_USER":"$PASSWORD" ${ONEPROVIDER_PANEL_STORAGES}/${STORAGE_ID}/luma/db/storage_access/posix_compatible/default_credentials/${SPACE_ID}

#curl -X  DELETE -vvv -u "$ADMIN_USER":"$PASSWORD" ${ONEPROVIDER_PANEL_STORAGES}/${STORAGE_ID}/luma/db/oneclient_display_credentials/all/default/${SPACE_ID}

#
# create storage
# curl -X POST -i -u "$ADMIN_USER":"$PASSWORD" -k -vvv -H "Content-Type: application/json" -d @null.json https://$ONEPROVIDER_PANEL

# create space
#curl -u "$USER":"$PASSWORD"  -k -H "Content-type: application/json" -X POST -d @space.json https://$ONEZONE_HOST/api/v3/onezone/spaces