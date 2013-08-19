VeilCluster
===========

VeilCluster is a part of VeilFS system that unifies access to files stored at heterogeneous data storage systems that belong to geographically distributed organizations.


Goals
-----

The main goal of VeilCluster is to provision a self-scalable cluster, which manages the VeilFS system in a single data centre, i.e. it stores meta-data about actual users' data from the data centre, decides how to distribute users' files among available storage systems, and executes data management rules, which can be defined by administrators or users.


Getting Started
---------------

This is a short tutorial how to start VeilCluster on a single machine.

#### Prerequisites

In order to compile the project, you need to have the following libraries:

* libglobus_gsi_callback
* libglobus_common
* libssl

Use the following command to install the required dependency packages:

* RHEL/CentOS/Fedora Dependencies (.rpm packages):

        yum install globus-proxy-utils globus-gsi-callback-devel

        ln -s /usr/lib64/globus/include/globus_config.h /usr/include/globus


#### VeilCluster RPM installation and configuration

After installing prerequisites, you can install the VeilCluster RPM.

        rpm -i veil-<version>.rpm


VeilCluster setup scripts can be executed with the following command:
        
        veil_setup

The script will guide you through the setup procedure of the VeilCluster. To start the work you should start at least one instance of the CCM component and one worker component instance. 

A sample session of starting VeilCluster from scratch is as follows:

        [root@plgsl63 veilcluster]# veil_setup 

        *** Veil SETUP ***
        ~ Nodes configured on this machine will use its hostname: @172.16.67.219
        (!) Make sure it is resolvable by other hosts in the network
        ==> What do you want to do?
         [1] Manage database nodes
         [2] Manage veil nodes
         [3] Exit
        > Your choice: 2
        ~ Each machine can only host a single worker or a ccm + worker pair.
        ==> What do you want to do?
         [1] Set up a new cluster
         [2] Extend existing cluster
         [3] Go back
        > Your choice: 1
        ~ Installing a new cluster beside a running one may cause unpredictable behaviour.
        ~ It is required that all database nodes are installed prior to the cluster.
        ==> Do you wish to continue?
         [1] Yes
         [2] No
        > Your choice: 1
        ==> List ALL running database nodes, delimiting them with commas (no spaces) [eg. db1@host.net,db2@host2.net,...]
        ==> The cluster will use ONLY the nodes specified now.
        > Running DB nodes: db@172.16.67.219
        ==> Connection to following database nodes has been confirmed:
         - db@172.16.67.219
        ==> Following nodes will be installed:
         - ccm@172.16.67.219
         - worker@172.16.67.219
        ==> Confirm:
         [1] Continue
         [2] Go back
        > Your choice: 1
        ~ Installing ccm@172.16.67.219...
        ~ Installing worker@172.16.67.219...
        ~ Starting node(s)...

To check if the VeilCluster is running you can attach to the CCM component and check heartbeat messages:

        [root@plgsl63 veilcluster]# /opt/veil/nodes/ccm/bin/veil_cluster_node attach
        Attaching to /tmp//opt/veil/nodes/ccm/erlang.pipe.1 (^D to exit)

        (search)`': 
        (ccm@172.16.67.219)1> 11:07:00.454 [info] Heart beat on node: ccm@172.16.67.219: sent; connection: connected
        11:07:00.454 [info] Heart beat on node: ccm@172.16.67.219: answered, new state_num: 4

        (ccm@172.16.67.219)1> 11:07:09.989 [info] Cluster state ok
 

After starting the nodes you have a complete VeilCluster running :) You can now proceed to VeilClient installation.


Note:

* To have a fully working VeilFS installation, we should also start a BigCouch instance on the same machine, with its cookie set to 'veil_cluster_node' and hostname set to 'db'. 


-------------------------------------------------------------------------------


Support
-------
For more information visit project Confluence or write to 'wrzeszcz@agh.edu.pl'.