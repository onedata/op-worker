#!/bin/bash

#####################################################################
#  @author Rafal Slota
#  @copyright (C): 2014 ACK CYFRONET AGH
#  This software is released under the MIT license
#  cited in 'LICENSE.txt'.
#####################################################################
#  This script contains utility function used by Bamboo agents to
#  build and deploy VeilFS project's components. 
#####################################################################

## Check configuration and set defaults...
if [[ -z "$CONFIG_PATH" ]]; then
    export CONFIG_PATH="/etc/onedata_platform.conf"
fi

if [[ -z "$SETUP_DIR" ]]; then
    export SETUP_DIR="/tmp/onedata"
fi

if [[ -z "$STAMP_DIR" ]]; then
    export STAMP_DIR="${SETUP_DIR}-stamp"
fi

# $1 - list of items seperated with ';'
# $2 - which element has to be returned, starting with 1
function nth {
    echo "$1" | awk -F ';' "{print \$$2}" | xargs
}

function len {
    echo "$1" | awk -F ';' '{print NF}'    
}

# $1 - error string
function error {
    echo "ERROR [Node: $node]: $1" 1>&2
    exit 1
}

function get_project_name {
    echo ${PROJECT// /_}
}

# $1 - node hostname
function stamp_to_version {
    echo `ssh $1 "cat '$STAMP_DIR/$(get_project_name).stamp'"`
}

# $1 - Branch name
# $2 - Build number
# $3 - Major ver.
# $4 - Minor ver.
function set_version {
    if [[ "$1" == "master" ]]; then
        export V_MAJOR=$3
        export V_MINOR=$4
        export V_PATCH=$2
	      export V_PATCH_ORIG=$V_PATCH
    else
        export V_MAJOR=0
        export V_MINOR=0
        export V_PATCH="\\\\\"$1\\\\\""
	      export V_PATCH_ORIG="${2}_${1}"
    fi
}

# $1 - node hostname
function deploy_stamp {
    [[ 
        "$PROJECT" == ""
    ]] || ssh $1 "mkdir -p $STAMP_DIR && echo '$VERSION' > '$STAMP_DIR/`get_project_name`.stamp'"
}

# $1 - info string
function info {
    echo "---> [Node: $node] $1"
}

# $1 platform master's hostname
function get_platform_config {
    info "Fetching platform configuration from $1:$CONFIG_PATH ..."
    scp $1:$CONFIG_PATH ./conf.sh || error "Cannot fetch platform config file."
    source ./conf.sh || error "Cannot find platform config file."
}

# $1 node
# $2 relative path
function absolute_path_on_node {
    ssh $1 "cd $2 && pwd"
}

function screen_wait {
    i=$2
    while i; do
        if screen -list | grep "$1"; then
            sleep 1
        else 
            break
        fi
        
        i=$(( i-1 ))
    done
}

function nth_node_name {
    local node=$(nth "$1" $2)
    echo $(ssh $node "hostname -f")
}

function node_name {
    echo `ssh $1 "hostname -f"`
}


function start_cluster {
    info "Starting VeilCluster..."
 
    dbs=`echo $CLUSTER_DB_NODES | tr ";" "\n"`
    idb_nodes=""
    for db in $dbs; do
        idb_nodes="$idb_nodes,db@`node_name $db`"
    done
    idb_nodes=`echo $idb_nodes | sed -e 's/,//'`
    cluser_type=$(nth "$CLUSTER_TYPES" $2)
    ssh $1 "echo \"
        {what_to_do, manage_veil}.
        {db_nodes_installed, yes}.
        {retry, no}.
        {define_db_nodes, \\\"$idb_nodes\\\"}.
        {settings_ok_db, ok}.
        {settings_ok, ok}.
        {new_node_type, $cluser_type}.
    \" > $SETUP_DIR/start_cluster.batch"
    
    if [[ $2 == 1 ]]; then
        n_count=`len "$STORAGE_GROUP_NAMES"`

        ssh $1 "echo \"
            {what_to_do_veil, new_cluster}.
            {veilfs_storage_root, \\\"$CLUSTER_DIO_ROOT\\\"}.
            {want_to_create_storage$((n_count+1)), no}.
            {accept_created_storage, yes}.
        \" >> $SETUP_DIR/start_cluster.batch"
        
        for i in `seq 1 $n_count`; do
            name=`nth "$STORAGE_GROUP_NAMES" $i`
            dir=`nth "$STORAGE_GROUP_DIRS" $i`
            ssh $1 "echo \"
                {want_to_create_storage${i}, yes}.
                {storage_group_name${i}, \\\"$name\\\"}.
                {storage_group_directory${i}, \\\"$dir\\\"}.
            \" >> $SETUP_DIR/start_cluster.batch"
        done
    else 
        master_hostname=`nth_node_name "$CLUSTER_NODES" 1`
        ssh $1 "echo \"
            {what_to_do_veil, extend_cluster}.
            {ip_or_hostname, \\\"$master_hostname\\\"}.
        \" >> $SETUP_DIR/start_cluster.batch"
    fi 
    ssh -tt $1 "veil_setup -batch $SETUP_DIR/start_cluster.batch" || error "Cannot setup/start cluster"
    ssh -tt $1 "/etc/init.d/veil start" || error "Cannot setup/start cluster"
    
}

# $1 - target host
# $2 - rpm name
function install_rpm {
    info "Moving $2 package to $1..."
    ssh $1 "mkdir -p $SETUP_DIR" || error "Cannot create tmp setup dir '$SETUP_DIR' on $1"
    scp *.rpm $1:$SETUP_DIR/$2 || error "Moving $2 file failed on $1"
    
    info "Installing $2 package on $1..."
    ssh $1 "rpm -Uvh $SETUP_DIR/$2 --nodeps --force" || error "Cannot install $2 package on $1"
}

function remove_cluster {
    info "Removing VeilCluster..."
    
    ssh $1 "echo \"
        {what_to_do, manage_veil}.
        {what_to_do_veil, remove_nodes}.
        {confirm_veil_nodes_deletion, yes}.
    \" > $SETUP_DIR/remove_cluster.batch" 
    
    if [[ $(ssh $1 "which veil_setup 2> /dev/null") == 0 ]]; then 
        screen -dmS veil_setup ssh $1 "veil_setup -batch $SETUP_DIR/remove_cluster.batch"
        screen_wait veil_setup 2 
        screen -XS veil_setup quit
    fi
    
    ssh $1 "rpm -e veil 2> /dev/null"
    ssh $1 "[ -z $CLUSTER_DIO_ROOT ] || rm -rf $CLUSTER_DIO_ROOT/users $CLUSTER_DIO_ROOT/groups"
    ssh $1 "rm -rf /opt/veil"
}

# $1 - target host
function remove_cluster_db {
    info "Removing VeilCluster DB..."
    
    ssh $1 "echo \"
        {what_to_do, manage_db}.
        {what_to_do_db, remove_node}.
    \" > $SETUP_DIR/remove_db.batch" 
    
    if [[ $(ssh $1 "which veil_setup 2> /dev/null") == 0 ]]; then 
        screen -dmS veil_setup ssh $1 "veil_setup -batch $SETUP_DIR/remove_db.batch"
        i=5
        while i; do
            if screen -list | grep "veil_setup"; then
                sleep 1
            else 
                break
            fi
            
            i=$(( i-1 ))
        done 
        screen -XS veil_setup quit
    fi

    ssh $1 "rm -rf /opt/bigcouch"
}

# $1 - target host
# $2 - db node numer
# $3 - total db node count
function start_cluster_db {
    info "Starting VeilCluster DB..."

    ssh $1 -tt "sed -i -e \"s/bind_address = [0-9\.]*/bind_address = 0.0.0.0/\" /opt/veil/files/database_node/etc/default.ini" || error "Cannot change db bind address on $1."

    ssh $1 "echo \"
        {what_to_do, manage_db}.
    \" > $SETUP_DIR/start_db.batch" 
    
    if [[ $2 == 1 ]]; then
        ssh $1 "echo \"
            {what_to_do_db, new_cluster}.
            {settings_ok_db, ok}.
        \" >> $SETUP_DIR/start_db.batch"
    else 
        master_db=$(node_name $CLUSTER_DB_NODES 1)
        ssh $1 "echo \"
            {what_to_do_db, extend_cluster}.
            {define_node_to_extend, \\\"$master_db\\\"}.
            {settings_ok_extend_db, ok}.
        \" >> $SETUP_DIR/start_db.batch"
    fi
    
    ssh $1 "veil_setup -batch $SETUP_DIR/start_db.batch" || error "Cannot setup/start DB"
    sleep 5
    ssh $1 -tt "nohup /opt/veil/nodes/db/bin/bigcouch start &"
}



# $1 - target host
# $2 - target mountpoint
function remove_client {
    info "Removing VeilClient..."

    RUID=`ssh $1 "echo \\\$UID"`
    ssh $1 "killall -9 veilFuse 2> /dev/null"
    ssh $1 "fusermount -u $2 2> /dev/null"
    ssh $1 "rm -rf $2"
    ssh $1 "rm -rf ${SETUP_DIR}_${RUID}"

    ssh $1 'rm -rf ~/veilFuse'
    if [[ "$RUID" == "0" ]]; then  
        ssh $1 'yum remove veilclient -y 2> /dev/null'
    fi
}

# $1 - target host
# $2 - target mountpoint
# $3 - peer certificate path
# $4 - target cluster's hostname
function install_client {
    info "Moving files to $1..."

    RUID=`ssh $1 "echo \\\$UID"`
    S_DIR="${SETUP_DIR}_${RUID}"

    
    ssh $1 "mkdir -p $S_DIR" || error "Cannot create tmp setup dir '$S_DIR' on $1"
    ssh $1 "mkdir -p $2" || error "Cannot create mountpoint dir '$2' on $1"
    
    scp VeilClient-Linux.rpm $1:$S_DIR/veilclient.rpm || error "Moving .rpm file failed"
    scp veilFuse $1:~/veilFuse || error "Moving veilFuse binary file failed"
    ( scp $3 tmp.pem && scp tmp.pem $1:$S_DIR/peer.pem ) || error "Moving $3 file failed"
    ssh $1 "chmod 600 $S_DIR/peer.pem"
    ssh $1 "chmod +x ~/veilFuse"

    info "Installing VeilClient for user with UID: $RUID"

    if [[ "$RUID" == "0" ]]; then 
        ssh $1 "yum install $S_DIR/veilclient.rpm -y" || error "Cannot install VeilClient on $1"
    fi
}

function start_client {
    info "Starting VeilClient on $1..."
    
    RUID=`ssh $1 "echo \\\$UID"`
    S_DIR="${SETUP_DIR}_${RUID}"
    group_id=`nth "$CLIENT_GROUP" "$4"`
    cl_host_count=`len "$CLUSTER_NODES"`
    cl_host_i=$(($4 % $cl_host_count))
    cl_host_i=$(($cl_host_i + 1))
    cl_host=`nth "$CLUSTER_NODES" "$cl_host_i"`    
    cl_host=${cl_host#*@}
    no_check_certificate="--no-check-certificate"

    mount_cmd="PEER_CERTIFICATE_FILE=\"$S_DIR/peer.pem\" PATH=\$HOME:\$PATH veilFuse $no_check_certificate $2"
    if [[ "$group_id" != "" ]]; then
        mount_cmd="FUSE_OPT_GROUP_ID=\"$group_id\" $mount_cmd"
    fi
    if [[ "$cl_host" == "" ]]; then
        ssh $1 "$mount_cmd" || error "Cannot mount VeilFS on $1"
    else
        ssh $1 "CLUSTER_HOSTNAME=\"$cl_host\" $mount_cmd" || error "Cannot mount VeilFS on $1 (using cluster_hostname: $cl_host, mount_cmd: 'CLUSTER_HOSTNAME=\"$cl_host\" $mount_cmd')"
    fi
}

# $1 - target host
# $2 - db node numer
function start_global_registry_db {
    info "Starting Global Registry DB..."

    ssh $1 -tt "mkdir -p /opt/bigcouch" || error "Cannot create directory for Global Registry DB on $1."
    ssh $1 -tt "cp -R /var/lib/globalregistry/bigcouchdb/database_node/* /opt/bigcouch" || error "Cannot copy Global Registry DB files on $1."
    ssh $1 -tt "sed -i -e \"s/^-name .*/-name db@\"`node_name $1`\"/\" /opt/bigcouch/etc/vm.args" || error "Cannot set Global Registry DB hostname on $1."
    ssh $1 -tt "sed -i -e \"s/^-setcookie .*/-setcookie globalregistry/\" /opt/bigcouch/etc/vm.args" || error "Cannot set Global Registry DB cookie on $1."
    ssh $1 -tt "sed -i -e \"s/^bind_address = [0-9\.]*/bind_address = 0.0.0.0/\" /opt/bigcouch/etc/default.ini" || error "Cannot set Global Registry DB bind address on $1."
    ssh $1 -tt "ulimit -n 65535 ; ulimit -u 65535 ; nohup /opt/bigcouch/bin/bigcouch start & ; sleep 5" || error "Cannot start Global Registry DB on $1."

    if [[ $2 != 1 ]]; then
        master_db=`nth_node_name "$GLOBAL_REGISTRY_DB_NODES" 1`
        current_db_host=`nth_node_name "$GLOBAL_REGISTRY_DB_NODES" "$2"`
        ssh $1 "curl -u admin:password --connect-timeout 5 -s -X GET http://$master_db:5986/nodes/_all_docs" || error "Cannot establish connection to primary Global Registry DB on $1."
        ssh $1 "curl -u admin:password --connect-timeout 5 -s -X PUT http://$master_db:5986/nodes/db@$current_db_host -d '{}'" || error "Cannot extend Global Registry DB with node db@$current_db_host on $1."
    fi
}

# $1 - target host
function remove_global_registry_db {
    info "Removing Global Registry DB..."

    ssh $1 -tt "rm -rf /opt/bigcouch" || error "Cannot remove Global Registry DB copy on $1."
    ssh $1 -tt "rm -rf /var/lib/globalregistry" || error "Cannot remove Global Registry DB on $1."
}

function start_global_registry {
    info "Starting Global Registry..."

    dbs=`echo $GLOBAL_REGISTRY_DB_NODES | tr ";" "\n"`
    idb_nodes=""
    for db in $dbs; do
        idb_nodes="$idb_nodes,'db@`node_name $db`'"
    done
    idb_nodes=`echo $idb_nodes | sed -e 's/.$//'`

    ssh $1 -tt "sed -i -e \"s/db_nodes, .*/db_nodes, [$idb_nodes] },/\" /etc/globalregistry/app.config" || error "Cannot set Global Registry DB nodes on $1."
    ssh $1 -tt "sed -i -e \"s/rest_cert_domain, .*/rest_cert_domain, \\\"onedata.org\\\" }/\" /etc/globalregistry/app.config" || error "Cannot set Global Registry REST certificate domain on $1."
    ssh $1 -tt "sed -i -e \"s/^-name .*/-name globalregistry@\"`node_name $1`\"/\" /etc/globalregistry/vm.args" || error "Cannot set Global Registry hostname on $1."
    ssh $1 -tt "ulimit -n 65535 ; ulimit -u 65535 ; /etc/init.d/globalregistry start" || error "Cannot start Global Registry on $1."
}

# $1 - target host
function remove_global_registry {
    info "Removing Global Registry..."

    ssh $1 "rpm -e globalregistry 2> /dev/null"

    ssh $1 "rm -rf /usr/lib64/globalregistry"
    ssh $1 "rm -rf /etc/globalregistry"
    ssh $1 "rm -rf /etc/init.d/globalregistry"
}