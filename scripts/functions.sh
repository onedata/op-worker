#!/bin/bash

#####################################################################
# @author Rafal Slota
# @copyright (C): 2014 ACK CYFRONET AGH
# This software is released under the MIT license
# cited in 'LICENSE.txt'.
#####################################################################
# This script contains utility function used by Bamboo agents to
# build and deploy VeilFS project's components.
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

#####################################################################
# Utility Functions
#####################################################################

# $1 - list of items seperated with ';'
# $2 - which element has to be returned, starting with 1
function nth {
    echo "$1" | awk -F ';' "{print \$$2}" | xargs
}

# $1 - list of items seperated with ';'
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
	      export V_PATCH_ORIG=${V_PATCH}
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

# $1 - platform master's hostname
function get_platform_config {
    info "Fetching platform configuration from $1:$CONFIG_PATH ..."
    scp $1:${CONFIG_PATH} ./conf.sh || error "Cannot fetch platform config file."
    source ./conf.sh || error "Cannot find platform config file."
}

# $1 - node
# $2 - relative path
function absolute_path_on_node {
    ssh $1 "cd $2 && pwd"
}

# $1 - process name
# $2 - number of seconds
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

# $1 - list of hosts seperated with ';'
# $2 - name of which node has to be returned, starting with 1
function nth_node_name {
    local node=$(nth "$1" $2)
    echo $(ssh ${node} "hostname -f")
}

# $1 - target host
function node_name {
    echo `ssh $1 "hostname -f"`
}

# $1 - target host
function strip_login {
    echo $1 | sed 's/^[^@]*@//'
}

#####################################################################
# VeilCluster Functions
#####################################################################

# $1 - target host
# $2 - rpm name
function install_veilcluster_package {
    info "Moving $2 package to $1..."
    ssh $1 "mkdir -p $SETUP_DIR" || error "Cannot create tmp setup dir '$SETUP_DIR' on $1"
    scp *.rpm $1:${SETUP_DIR}/$2 || error "Moving $2 file failed on $1"

    info "Installing $2 package on $1..."
    multicast_address="239.255.0."`echo ${MASTER##*.}`
    ssh $1 "export ONEPANEL_MULTICAST_ADDRESS=$multicast_address ; rpm -Uvh $SETUP_DIR/$2 --nodeps --force ; sleep 5" || error "Cannot install $2 package on $1"
}

# $1 - target host
function start_cluster {
    info "Starting VeilCluster..."

    db_hosts=""
    db_nodes=`echo "$CLUSTER_DB_NODES" | tr ";" "\n"`
    for db_node in ${db_nodes}; do
        db_hosts="\\\"`node_name "$db_node"`\\\",$db_hosts"
        ssh ${db_node} "sed -i -e \"s/^bind_address = [0-9\.]*/bind_address = 0.0.0.0/\" /opt/veil/files/database_node/etc/default.ini" || error "Cannot set VeilCluster DB bind address on $db_node."
    done
    db_hosts=`echo "$db_hosts" | sed -e 's/.$//'`

    i=1
    ccm_hosts=""
    worker_hosts=""
    cluster_types=`echo "$CLUSTER_TYPES" | tr ";" "\n"`
    for cluster_type in ${cluster_types}; do
        cluster_node=`nth "$CLUSTER_NODES" ${i}`
        if [[ ${cluster_type} == "ccm_plus_worker" ]]; then
            ccm_hosts="\\\"`node_name "$cluster_node"`\\\",$ccm_hosts"
            worker_hosts="\\\"`node_name "$cluster_node"`\\\",$worker_hosts"
        else
            worker_hosts="\\\"`node_name "$cluster_node"`\\\",$worker_hosts"
        fi
        i=$(( i+1 ))
    done
    ccm_hosts=`echo "$ccm_hosts" | sed -e 's/.$//'`
    worker_hosts=`echo "$worker_hosts" | sed -e 's/.$//'`

    main_ccm_host=`echo "$ccm_hosts" | awk -F ',' '{print $1}' | xargs`

    storage_paths=""
    cluster_storage_paths=`echo "$CLUSTER_STORAGE_PATHS" | tr ";" "\n"`
    for storage_path in ${cluster_storage_paths}; do
        storage_paths="\\\"$storage_path\\\",$storage_paths"
        # Add storage paths on all cluster nodes
        cluster_nodes=`echo "$CLUSTER_NODES" | tr ";" "\n"`
        for cluster_node in ${cluster_nodes}; do
            ssh ${cluster_node} "mkdir -p $storage_path"
        done
    done
    storage_paths=`echo "$storage_paths" | sed -e 's/.$//'`

    if [[ "$CLUSTER_REGISTER_IN_GLOBAL_REGISTRY" == "yes" ]]; then
        register="yes"
    else
        register="no"
    fi

    ssh $1 "echo \"{\\\"Main CCM host\\\",       \\\"$main_ccm_host\\\"}.
    {\\\"CCM hosts\\\",                   [$ccm_hosts]}.
    {\\\"Worker hosts\\\",                [$worker_hosts]}.
    {\\\"Database hosts\\\",              [$db_hosts]}.
    {\\\"Storage paths\\\",               [$storage_paths]}.
    {\\\"Register in Global Registry\\\", $register}.\" > $SETUP_DIR/install.cfg"

    ssh -tt -q $1 "onepanel_admin --install $SETUP_DIR/install.cfg 2>&1" 2>/dev/null || error "Cannot setup and start VeilCluster."
}

# $1 - target host
function remove_cluster {
    info "Removing VeilCluster..."

    local onepanel_admin
    onepanel_admin=$(ssh $1 "which onepanel_admin 2> /dev/null")
    if [[ $? == 0 ]]; then
        ssh -tt -q $1 "onepanel_admin --uninstall 2>&1" 2>/dev/null
    fi

    cluster_storage_paths=`echo "$CLUSTER_STORAGE_PATHS" | tr ";" "\n"`
    for storage_path in ${cluster_storage_paths}; do
        ssh $1 "[ -z $storage_path ] || rm -rf $storage_path/users $storage_path/groups $storage_path/vfs_storage.info"
    done
    
    ssh $1 "rpm -e veil 2> /dev/null"
    ssh $1 "rm -rf /opt/veil" || error "Cannot remove VeilCluster directory."
}

# $1 - VeilCluster host
# $2 - Global Registry host
function register_in_global_registry {
    info "Registering VeilCluster in Global Registry..."

    # Add mappings in /etc/hosts
    global_registry_ip=`strip_login $2`
    ssh $1 "sed -i -e '/onedata.*/d' /etc/hosts" || error "Cannot remove old mappings from /etc/hosts for onedata domain on $cluster_node"
    ssh $1 "echo \"$global_registry_ip     onedata.org
    149.156.10.253          onedata.com\" >> /etc/hosts"
}

#####################################################################
# VeilClient Functions
#####################################################################

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

    scp VeilClient-Linux.rpm $1:${S_DIR}/veilclient.rpm || error "Moving .rpm file failed"
    scp veilFuse $1:~/veilFuse || error "Moving veilFuse binary file failed"
    ( scp $3 tmp.pem && scp tmp.pem $1:${S_DIR}/peer.pem ) || error "Moving $3 file failed"
    ssh $1 "chmod 600 $S_DIR/peer.pem"
    ssh $1 "chmod +x ~/veilFuse"

    info "Installing VeilClient for user with UID: $RUID"

    if [[ "$RUID" == "0" ]]; then
        ssh $1 "yum install $S_DIR/veilclient.rpm -y" || error "Cannot install VeilClient on $1"
    fi
}

# $1 - target host
# $2 - target mountpoint
# $3 - peer certificate path
# $4 - client node number
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

#####################################################################
# Global Registry Functions
#####################################################################

# $1 - target host
# $2 - rpm name
function install_global_registry_package {
    info "Moving $2 package to $1..."
    ssh $1 "mkdir -p $SETUP_DIR" || error "Cannot create tmp setup dir '$SETUP_DIR' on $1"
    scp *.rpm $1:${SETUP_DIR}/$2 || error "Moving $2 file failed on $1"

    info "Installing $2 package on $1..."
    ssh $1 "rpm -Uvh $SETUP_DIR/$2 --nodeps --force" || error "Cannot install $2 package on $1"
}

# $1 - target host
# $2 - db node numer
function start_global_registry_db {
    info "Starting Global Registry DB..."

    ssh $1 "mkdir -p /opt/bigcouch" || error "Cannot create directory for Global Registry DB on $1."
    ssh $1 "cp -R /var/lib/globalregistry/bigcouchdb/database_node/* /opt/bigcouch" || error "Cannot copy Global Registry DB files on $1."
    ssh $1 "sed -i -e \"s/^-name .*/-name db@\"`node_name $1`\"/\" /opt/bigcouch/etc/vm.args" || error "Cannot set Global Registry DB hostname on $1."
    ssh $1 "sed -i -e \"s/^-setcookie .*/-setcookie globalregistry/\" /opt/bigcouch/etc/vm.args" || error "Cannot set Global Registry DB cookie on $1."
    ssh $1 "sed -i -e \"s/^bind_address = [0-9\.]*/bind_address = 0.0.0.0/\" /opt/bigcouch/etc/default.ini" || error "Cannot set Global Registry DB bind address on $1."
    ssh -tt -q $1 "ulimit -n 65535 ; ulimit -u 65535 ; nohup /opt/bigcouch/bin/bigcouch >/dev/null 2>&1 & ; sleep 5" 2> /dev/null || error "Cannot start Global Registry DB on $1."

    if [[ $2 != 1 ]]; then
        first_db=`nth_node_name "$GLOBAL_REGISTRY_DB_NODES" 1`
        current_db_host=`nth_node_name "$GLOBAL_REGISTRY_DB_NODES" "$2"`
        ssh $1 "curl -u admin:password --connect-timeout 5 -s -X GET http://$first_db:5986/nodes/_all_docs" || error "Cannot establish connection to primary Global Registry DB on $1."
        ssh $1 "curl -u admin:password --connect-timeout 5 -s -X PUT http://$first_db:5986/nodes/db@$current_db_host -d '{}'" || error "Cannot extend Global Registry DB with node db@$current_db_host on $1."
    fi
}

# $1 - target host
function remove_global_registry_db {
    info "Removing Global Registry DB..."

    ssh $1 "rm -rf /opt/bigcouch" || error "Cannot remove Global Registry DB copy on $1."
    ssh $1 "rm -rf /var/lib/globalregistry" || error "Cannot remove Global Registry DB on $1."
}

# $1 - target host
function start_global_registry {
    info "Starting Global Registry..."

    dbs=`echo ${GLOBAL_REGISTRY_DB_NODES} | tr ";" "\n"`
    db_nodes=""
    for db in ${dbs}; do
        db_nodes="'db@`node_name ${db}`',$db_nodes"
    done
    db_nodes=`echo ${db_nodes} | sed -e 's/.$//'`

    ssh $1 "sed -i -e \"s/db_nodes, .*/db_nodes, [$db_nodes] },/\" /etc/globalregistry/app.config" || error "Cannot set Global Registry DB nodes on $1."
    ssh $1 "sed -i -e \"s/rest_cert_domain, .*/rest_cert_domain, \\\"$GLOBAL_REGISTRY_REST_CERT_DOMAIN\\\" }/\" /etc/globalregistry/app.config" || error "Cannot set Global Registry REST certificate domain on $1."
    ssh $1 "sed -i -e \"s/^-name .*/-name globalregistry@\"`node_name $1`\"/\" /etc/globalregistry/vm.args" || error "Cannot set Global Registry hostname on $1."
    ssh -tt -q $1 "ulimit -n 65535 ; ulimit -u 65535 ; /etc/init.d/globalregistry start 2>&1" 2> /dev/null || error "Cannot start Global Registry on $1."
}

# $1 - target host
function remove_global_registry {
    info "Removing Global Registry..."

    ssh $1 "rpm -e globalregistry 2> /dev/null" 2> /dev/null

    ssh $1 "rm -rf /usr/lib64/globalregistry"
    ssh $1 "rm -rf /etc/globalregistry"
    ssh $1 "rm -rf /etc/init.d/globalregistry"
}