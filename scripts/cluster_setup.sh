#!/bin/sh
#####################################################################
#  @author Rafal Slota
#  @copyright (C): 2014 ACK CYFRONET AGH
#  This software is released under the MIT license
#  cited in 'LICENSE.txt'.
#####################################################################
#  This script is used by Bamboo agent to set up VeilCluster nodes
#  during deployment.
#####################################################################

## Check configuration and set defaults...
if [[ -z "$CONFIG_PATH" ]]; then
    export CONFIG_PATH="/etc/onedata_platform.conf"
fi

if [[ -z "$SETUP_DIR" ]]; then
    export SETUP_DIR="/tmp/onedata"
fi

info "env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

if [[ -z "$CREATE_USER_IN_DB" ]]; then
    export CREATE_USER_IN_DB="true"
fi
info "env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

# Load funcion defs
source ./functions.sh || exit 1

echo "1 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

########## Load Platform config ############
info "Fetching platform configuration from $MASTER:$CONFIG_PATH ..."
scp $MASTER:$CONFIG_PATH ./conf.sh || error "Cannot fetch platform config file."
source ./conf.sh || error "Cannot find platform config file. Please try again (redeploy)."

echo "2 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"
########## CleanUp Script Start ############


ALL_NODES="$CLUSTER_NODES ; $DB_NODES ; $CLIENT_NODES"
ALL_NODES=`echo $ALL_NODES | tr ";" "\n" | sed -e 's/^ *//g' -e 's/ *$//g' | sort | uniq`
echo "3 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

n_count=`len "$ALL_NODES"`
for node in $ALL_NODES; do
    echo "4 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

    [[
        "$node" != ""
    ]] || continue

    ssh $node "mkdir -p $SETUP_DIR"
    echo "5 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

    remove_db       "$node"
    remove_cluster  "$node"
    echo "6 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

    ssh $node "[ -z $CLUSTER_DIO_ROOT ] || rm -rf $CLUSTER_DIO_ROOT/users $CLUSTER_DIO_ROOT/groups"
    ssh $node "rm -rf $SETUP_DIR"
    ssh $node "rm -rf /opt/veil"
    ssh $node "killall -KILL beam 2> /dev/null"
    ssh $node "killall -KILL beam.smp 2> /dev/null"
    echo "7 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"
done

echo "8 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

########## Install Script Start ############
ALL_NODES="$CLUSTER_NODES ; $DB_NODES"
ALL_NODES=`echo $ALL_NODES | tr ";" "\n" | sed -e 's/^ *//g' -e 's/ *$//g' | sort | uniq`
echo "9 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

n_count=`len "$ALL_NODES"`
for node in $ALL_NODES; do
    [[
        "$node" != ""
    ]] || continue

    echo "10 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"
    install $node
done
echo "11 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"


########## SetupDB Script Start ############
n_count=`len "$DB_NODES"`
for i in `seq 1 $n_count`; do
    node=`nth "$DB_NODES" $i`
    echo "12 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

    [[
        "$node" != ""
    ]] || error "Invalid node configuration !"

    echo "13 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"
    start_db "$node" $i $n_count
    deploy_stamp "$node"
done

echo "14 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"


########## SetupCluster Script Start ############
if [[ `len "$DB_NODES"` == 0 ]]; then
    error "There are no configured DB Nodes !"
fi
echo "15 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

n_count=`len "$CLUSTER_NODES"`
for i in `seq 1 $n_count`; do
    node=`nth "$CLUSTER_NODES" $i`
echo "16 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

    [[
        "$node" != ""
    ]] || error "Invalid node configuration !"

echo "17 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"
    start_cluster "$node" $i $n_count
    deploy_stamp "$node"
echo "18 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"
    sleep 10
done
sleep 120


########## SetupValidation Script Start ############
echo "19 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

info "Setup Validation starts ..."

n_count=`len "$CLUSTER_NODES"`
for i in `seq 1 $n_count`; do

    node=`nth "$CLUSTER_NODES" $i`

echo "20 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"
    [[
        "$node" != ""
    ]] || error "Invalid node configuration !"

    pcount=`ssh $node "ps aux | grep beam | wc -l"`

    [[
        $pcount -ge 2
    ]] || error "Could not find cluster processes on the node !"

done

echo "21 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"
## Nagios health check
cluster=`nth "$CLUSTER_NODES" 1`
cluster=${cluster#*@}

echo "22 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"
curl -k -X GET https://$cluster/nagios > hc.xml || error "Cannot get Nagios status from node '$cluster'"
stat=`cat hc.xml | sed -e 's/>/>\n/g' | grep -v "status=\"ok\"" | grep status`
[[ "$stat" == "" ]] || error "Cluster HealthCheck failed: \n$stat"

echo "23 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

########## RegisterUsers Script Start ############
cnode=`nth "$CLUSTER_NODES" 1`
scp reg_user.erl $cnode:/tmp
escript_bin=`ssh $cnode "find /opt/veil/files/veil_cluster_node/ -name escript | head -1"`
reg_run="$escript_bin /tmp/reg_user.erl"

echo "24 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"
n_count=`len "$CLIENT_NODES"`
for i in `seq 1 $n_count`; do

    node=`nth "$CLIENT_NODES" $i`
    node_name=`node_name $cnode`
    cert=`nth "$CLIENT_CERTS" $i`
echo "25 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

    [[
        "$node" != ""
    ]] || error "Invalid node configuration !"

    [[
        "$cert" != ""
    ]] || continue

echo "26 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"
    user_name=${node%%@*}
    if [[ "$user_name" == "" ]]; then
        user_name="root"
    fi

    ## Add user to all cluster nodes
    n_count=`len "$CLUSTER_NODES"`
    for ci in `seq 1 $n_count`; do
        lcnode=`nth "$CLUSTER_NODES" $ci`
	ssh $lcnode "useradd $user_name 2> /dev/null || exit 0"
    done
echo "27 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

    echo "at end: env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"
    echo "is_equal_true: " `[[ "$CREATE_USER_IN_DB" == "true" ]] ; echo $?`

    if [[ "$CREATE_USER_IN_DB" == "true" ]]; then
        cmm="$reg_run $node_name $user_name '$user_name@test.com' /tmp/tmp_cert.pem"

        info "Trying to register $user_name using cluster node $cnode (command: $cmm)"

        scp $cert tmp_cert.pem
        scp tmp_cert.pem $cnode:/tmp/

        ssh $cnode "$cmm"
    fi
echo "28 env CREATE_USER_IN_DB: *$CREATE_USER_IN_DB*"

done

exit 0



