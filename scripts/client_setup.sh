#!/bin/bash

#####################################################################
# @author Rafal Slota
# @copyright (C): 2014 ACK CYFRONET AGH
# This software is released under the MIT license
# cited in 'LICENSE.txt'.
#####################################################################
# This script is used by Bamboo agent to set up VeilClient nodes
# during deployment.
#####################################################################

#####################################################################
# Check configuration and set defaults
#####################################################################

if [[ -z "$CONFIG_PATH" ]]; then
    export CONFIG_PATH="/etc/onedata_platform.conf"
fi

if [[ -z "$SETUP_DIR" ]]; then
    export SETUP_DIR="/tmp/onedata"
fi

# Load funcion defs
source ./functions.sh || exit 1

#####################################################################
# Load platform configuration
#####################################################################

info "Fetching platform configuration from $MASTER:$CONFIG_PATH ..."
scp $MASTER:$CONFIG_PATH ./conf.sh || error "Cannot fetch platform config file."
source ./conf.sh || error "Cannot find platform config file. Please try again (redeploy)."

#####################################################################
# Setup VeilClient nodes
#####################################################################

n_count=`len "$CLIENT_NODES"`
for i in `seq 1 $n_count`; do
    node=`nth "$CLIENT_NODES" $i`
    mount=`nth "$CLIENT_MOUNTS" $i`
    cert=`nth "$CLIENT_CERTS" $i`
    
    echo "Processing VeilClient on node '$node' with mountpoint '$mount' and certificate '$cert'..."
    
    [[ 
        "$node" != "" &&  
        "$mount" != "" &&  
        "$cert" != "" 
    ]] || error "Invalid node configuration!"
    
    remove_client "$node" "$mount"
    install_client "$node" "$mount" "$cert"
done

#####################################################################
# Start VeilClient nodes
#####################################################################

for i in `seq 1 $n_count`; do
    node=`nth "$CLIENT_NODES" $i`
    mount=`nth "$CLIENT_MOUNTS" $i`
    cert=`nth "$CLIENT_CERTS" $i`
    
    [[ 
        "$node" != "" &&  
        "$mount" != "" &&  
        "$cert" != "" 
    ]] || continue
  
    start_client "$node" "$mount" "$cert" "$i"
    deploy_stamp "$node"
done

exit 0