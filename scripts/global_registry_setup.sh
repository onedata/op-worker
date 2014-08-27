#!/bin/bash

#####################################################################
# @author Krzysztof Trzepla
# @copyright (C): 2014 ACK CYFRONET AGH
# This software is released under the MIT license
# cited in 'LICENSE.txt'.
#####################################################################
# This script is used by Bamboo agent to set up Global Registry nodes
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
# Clean platform
#####################################################################

ALL_NODES="$CLUSTER_NODES ; $CLUSTER_DB_NODES ; $GLOBAL_REGISTRY_NODES ; $GLOBAL_REGISTRY_DB_NODES ; $CLIENT_NODES"
ALL_NODES=`echo $ALL_NODES | tr ";" "\n" | sed -e 's/^ *//g' -e 's/ *$//g' | sort | uniq`
n_count=`len "$ALL_NODES"`
for node in $ALL_NODES; do

    [[
        "$node" != ""
    ]] || continue

    ssh $node "mkdir -p $SETUP_DIR"

    remove_cluster            "$node"
    remove_cluster_db         "$node"
    remove_global_registry    "$node"
    remove_global_registry_db "$node"

    ssh $node "rm -rf $SETUP_DIR"
    ssh $node "killall -KILL beam 2> /dev/null"
    ssh $node "killall -KILL beam.smp 2> /dev/null"
done

#####################################################################
# Install Global Registry package
#####################################################################

ALL_NODES="$GLOBAL_REGISTRY_NODES ; $GLOBAL_REGISTRY_DB_NODES"
ALL_NODES=`echo $ALL_NODES | tr ";" "\n" | sed -e 's/^ *//g' -e 's/ *$//g' | sort | uniq`
n_count=`len "$ALL_NODES"`
for node in $ALL_NODES; do
    [[
        "$node" != ""
    ]] || continue

    install_global_registry_package $node globalregistry.rpm
done

#####################################################################
# Start Global Registry DB nodes
#####################################################################

n_count=`len "$GLOBAL_REGISTRY_DB_NODES"`
for i in `seq 1 $n_count`; do
    node=`nth "$GLOBAL_REGISTRY_DB_NODES" $i`

    [[
        "$node" != ""
    ]] || error "Invalid node configuration!"

    start_global_registry_db "$node" $i
    deploy_stamp "$node"
done

#####################################################################
# Start Global Registry nodes
#####################################################################

node=`nth "$GLOBAL_REGISTRY_DB_NODES" 1`
if [[ "$node" == "" ]]; then
    error "Invalid node configuration!"
else
    start_global_registry "$node"
fi

exit 0