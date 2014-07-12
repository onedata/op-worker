## Check configuration and set defaults...
if [[ -z "$CONFIG_PATH" ]]; then
    export CONFIG_PATH="/etc/onedata_platform.conf"
fi

if [[ -z "$SETUP_DIR" ]]; then
    export SETUP_DIR="/tmp/onedata"
fi

# Load funcion defs
source ./functions.sh || exit 1

########## Load Platform Config ############
scp $MASTER:$CONFIG_PATH conf || error "Cannot fetch platform config file."
source ./conf || error "Cannot find platform config file. Please try again (redeploy)."

n_count=`len "$CLIENT_NODES"`
for i in `seq 1 $n_count`; do
    node=`nth "$CLIENT_NODES" $i`
    mount=`nth "$CLIENT_MOUNTS" $i`
    cert=`nth "$CLIENT_CERTS" $i`
    
    echo "Processing veilclient on node '$node' with mountpoint '$mount' and certificate '$cert'..."
    
    [[ 
        "$node" != "" &&  
        "$mount" != "" &&  
        "$cert" != "" 
    ]] || error "Invalid node configuration !"
    
    remove_client "$node" "$mount"
    install_client "$node" "$mount" "$cert"
done

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


