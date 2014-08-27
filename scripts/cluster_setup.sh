#!/bin/bash

#####################################################################
# @author Rafal Slota
# @copyright (C): 2014 ACK CYFRONET AGH
# This software is released under the MIT license
# cited in 'LICENSE.txt'.
#####################################################################
# This script is used by Bamboo agent to set up VeilCluster nodes
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

if [[ -z "$CLUSTER_CREATE_USER_IN_DB" ]]; then
    export CLUSTER_CREATE_USER_IN_DB="true"
fi

#####################################################################
# Validate platform configuration
#####################################################################

if [[ `len "$CLUSTER_NODES"` == 0 ]]; then
    error "VeilCluster nodes are not configured!"
fi

if [[ `len "$CLUSTER_DB_NODES"` == 0 ]]; then
    error "VeilCluster DB nodes are not configured!"
fi

#####################################################################
# Install VeilCluster package
#####################################################################

ALL_NODES="$CLUSTER_NODES ; $CLUSTER_DB_NODES"
ALL_NODES=`echo ${ALL_NODES} | tr ";" "\n" | sed -e 's/^ *//g' -e 's/ *$//g' | sort | uniq`
for node in ${ALL_NODES}; do
    [[
        "$node" != ""
    ]] || error "Invalid VeilCluster node!"

    install_veilcluster_package ${node} veilcluster.rpm
done

#####################################################################
# Start VeilCluster nodes
#####################################################################

node=`nth "$CLUSTER_NODES" 1`
start_cluster ${node}

n_count=`len "$CLUSTER_NODES"`
for i in `seq 1 ${n_count}`; do
    node=`nth "$CLUSTER_NODES" ${i}`

    deploy_stamp ${node}
done
sleep 120

#####################################################################
# Validate VeilCluster nodes start
#####################################################################

info "Validating VeilCluster nodes start..."

n_count=`len "$CLUSTER_NODES"`
for i in `seq 1 ${n_count}`; do
    node=`nth "$CLUSTER_NODES" ${i}`
    pcount=`ssh ${node} "ps aux | grep beam | wc -l"`

    [[
        ${pcount} -ge 2
    ]] || error "Could not find VeilCluster processes on $node!"
done

#####################################################################
# Nagios health check
#####################################################################

info "Nagios health check..."

cluster=`nth "$CLUSTER_NODES" 1`
cluster=${cluster#*@}

curl -k -X GET https://${cluster}/nagios > hc.xml || error "Cannot get Nagios status from node '$cluster'"
stat=`cat hc.xml | sed -e 's/>/>\n/g' | grep -v "status=\"ok\"" | grep status`
[[ "$stat" == "" ]] || error "Cluster HealthCheck failed: \n$stat"

#####################################################################
# Register user in DB
#####################################################################

cnode=`nth "$CLUSTER_NODES" 1`
scp reg_user.erl ${cnode}:/tmp
escript_bin=`ssh ${cnode} "find /opt/veil/files/veil_cluster_node/ -name escript | head -1"`
reg_run="$escript_bin /tmp/reg_user.erl"

n_count=`len "$CLIENT_NODES"`
for i in `seq 1 ${n_count}`; do

    node=`nth "$CLIENT_NODES" ${i}`
    node_name=`node_name ${cnode}`
    cert=`nth "$CLIENT_CERTS" ${i}`

    [[
        "$node" != ""
    ]] || error "Invalid VeilClient node!"

    [[
        "$cert" != ""
    ]] || error "Invalid VeilClient certificate!"

    user_name=${node%%@*}
    if [[ "$user_name" == "" ]]; then
        user_name="root"
    fi

    ## Add user to all cluster nodes
    n_count=`len "$CLUSTER_NODES"`
    for ci in `seq 1 ${n_count}`; do
        lcnode=`nth "$CLUSTER_NODES" ${ci}`
	ssh ${lcnode} "useradd $user_name 2> /dev/null || exit 0"
    done

    if [[ "$CLUSTER_CREATE_USER_IN_DB" == "true" ]]; then
        cmm="$reg_run $node_name $user_name '$user_name@test.com' /tmp/tmp_cert.pem"

        info "Trying to register $user_name using cluster node $cnode (command: $cmm)"

        scp ${cert} tmp_cert.pem
        scp tmp_cert.pem ${cnode}:/tmp/

        ssh ${cnode} "$cmm"
    fi
done

exit 0