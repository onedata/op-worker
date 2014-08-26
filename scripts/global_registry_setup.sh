#!/bin/bash

#####################################################################
#  @author Krzysztof Trzepla
#  @copyright (C): 2014 ACK CYFRONET AGH
#  This software is released under the MIT license
#  cited in 'LICENSE.txt'.
#####################################################################
#  This script is used by Bamboo agent to set up Global Registry nodes
#  during deployment.
#####################################################################

## Check configuration and set defaults...
if [[ -z "$CONFIG_PATH" ]]; then
    export CONFIG_PATH="/etc/onedata_platform.conf"
fi

if [[ -z "$SETUP_DIR" ]]; then
    export SETUP_DIR="/tmp/onedata"
fi

# Load funcion defs
source ./functions.sh || exit 1

########## Load Platform config ############
info "Fetching platform configuration from $MASTER:$CONFIG_PATH ..."
scp $MASTER:$CONFIG_PATH ./conf.sh || error "Cannot fetch platform config file."
source ./conf.sh || error "Cannot find platform config file. Please try again (redeploy)."


########## CleanUp Script Start ############
ALL_NODES="$CLUSTER_NODES ; $CLUSTER_DB_NODES ; $GLOBAL_REGISTRY_NODES ; $GLOBAL_REGISTRY_DB_NODES ; $CLIENT_NODES"
ALL_NODES=`echo $ALL_NODES | tr ";" "\n" | sed -e 's/^ *//g' -e 's/ *$//g' | sort | uniq`

n_count=`len "$ALL_NODES"`
for node in $ALL_NODES; do

    [[
        "$node" != ""
    ]] || continue

    ssh $node "mkdir -p $SETUP_DIR"

    remove_db              "$node"
    remove_cluster         "$node"
    remove_global_registry "$node"

    ssh $node "[ -z $CLUSTER_DIO_ROOT ] || rm -rf $CLUSTER_DIO_ROOT/users $CLUSTER_DIO_ROOT/groups"
    ssh $node "rm -rf $SETUP_DIR"
    ssh $node "rm -rf /opt/veil"
    ssh $node "killall -KILL beam 2> /dev/null"
    ssh $node "killall -KILL beam.smp 2> /dev/null"
done


########## Install Script Start ############
ALL_NODES="$GLOBAL_REGISTRY_NODES ; $GLOBAL_REGISTRY_DB_NODES"
ALL_NODES=`echo $ALL_NODES | tr ";" "\n" | sed -e 's/^ *//g' -e 's/ *$//g' | sort | uniq`

n_count=`len "$ALL_NODES"`
for node in $ALL_NODES; do
    [[
        "$node" != ""
    ]] || continue

    install_rpm $node globalregistry.rpm
done