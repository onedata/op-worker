About
=====

*oneprovider* is a part of *onedata* system that unifies access to files stored at heterogeneous data storage systems 
that belong to geographically distributed organizations.


Goals
-----

The main goal of *oneprovider* is to provision a self-scalable cluster, which manages the *onedata* system in a single
data centre, i.e. it stores meta-data about actual users' data from the data centre, decides how to distribute users'
files among available storage systems, and executes data management rules, which can be defined by administrators or
users.


Getting Started
---------------

This is a short tutorial how to start *oneprovider* on a single machine.

#### Prerequisites

In order to compile the project, you need to have the following libraries:

* libglobus_gsi_callback
* libglobus_common
* libssl
* libfuse
* libboost - filesystem, thread, random, system (version >= 1.49)

Use the following command to install the *oneprovider* software and the required dependency packages:

* RHEL/CentOS/Fedora Dependencies (.rpm packages):

        yum install oneprovider-<version>.rpm


*oneprovider* is installed in /opt/oneprovider. The setup scripts are executed with the following commands: 
        
        /opt/oneprovider/setup

The script will guide you through the setup procedure of the *oneprovider*. To start the work you should start at least
one instance of the CCM component and one worker component instance. 

A sample session of starting *oneprovider* from scratch is as follows:

  	 [root@plgsl64 ~]# /opt/oneprovider/setup 

	*** oneprovider SETUP ***
	~ Erlang nodes configured on this machine will use its hostname: @172.16.67.111
	(!) Make sure it is resolvable by other hosts in the network (i. e. by adding adequate mapping to /etc/hosts)
	==> What do you want to do?
	 [1] Manage database nodes
	 [2] Manage oneprovider nodes
	 [3] Exit
	> Your choice: 1
	==> What do you want to do?
	 [1] Set up a new db custer
	 [2] Extend existing db cluster
	 [3] Go back
	> Your choice: 1
	==> Following node will be installed:
	 - db@172.16.67.111
	==> Confirm:
	 [1] Continue
	 [2] Go back
	> Your choice: 1
	~ Installing db@172.16.67.111...
	~ installation complete
	~ Starting node...

	*** oneprovider SETUP ***
	~ Erlang nodes configured on this machine will use its hostname: @172.16.67.111
	(!) Make sure it is resolvable by other hosts in the network (i. e. by adding adequate mapping to /etc/hosts)
	==> What do you want to do?
	 [1] Manage database nodes
	 [2] Manage oneprovider nodes
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
	> Running DB nodes: db@172.16.67.111
	==> Connection to following database nodes has been confirmed:
	 - db@172.16.67.111
	==> Storage setup
	> Select path where oneprovider can store its files (default: /mnt/vfs):
	(!) IMPORTANT
	(!) Configuring direct storage (much faster than default proxy storage) for fuse client groups
	(!) If you don't create any storage now, all the data will go throught proxy
	 and it will work really slow!
	==> Do you wish to create new storage dedicated for fuse client group?
	 [1] Yes
	 [2] No
	> Your choice: 1
	==> Type following attributes:
	> Fuse clients group name (default: grp1):
	> DirectIO storage mount point (default: /mnt/grp1):
	==> Do you wish to create another storage dedicated for fuse client group?
	 [1] Yes
	 [2] No
	> Your choice: 1
	==> Type following attributes:
	> Fuse clients group name (default: grp2): my_grp2
	> DirectIO storage mount point (default: /mnt/grp2): /mnt/my_grp2
	==> Do you wish to create another storage dedicated for fuse client group?
	 [1] Yes
	 [2] No
	> Your choice: 2
	==> Is this all?
	==> group_name: grp1, root: /mnt/grp1
	==> group_name: my_grp2, root: /mnt/my_grp2

	 [1] Yes, continue instalation
	 [2] Add another
	 [3] Delete all and configure them again
	> Your choice: 1
	==> Following nodes will be installed:
	 - ccm@172.16.67.111
	 - worker@172.16.67.111
	==> Confirm:
	 [1] Continue
	 [2] Go back
	> Your choice: 1
	~ Installing ccm@172.16.67.111...
	~ Installing worker@172.16.67.111...
	~ Starting node(s)...


In order to check if the *oneprovider* is running you can attach to the CCM component and check heartbeat messages:

        [root@plgsl63 oneprovider]# /opt/oneprovider/nodes/ccm/bin/oneprovider_node attach
        Attaching to /tmp//opt/oneprovider/nodes/ccm/erlang.pipe.1 (^D to exit)

        (search)`': 
        (ccm@172.16.67.219)1> 11:07:00.454 [info] Heart beat on node: ccm@172.16.67.219: sent; connection: connected
        11:07:00.454 [info] Heart beat on node: ccm@172.16.67.219: answered, new state_num: 4

        (ccm@172.16.67.219)1> 11:07:09.989 [info] Cluster state ok
 

After starting the nodes you have a complete *oneprovider* running :) You can now proceed to oneclient installation.


Note:

* To have a fully working *onedata* installation, we should also start a BigCouch instance on the same machine, with its
cookie set to *oneprovider_node* and hostname set to 'db'. 


-------------------------------------------------------------------------------

Support
-------
For more information visit project *Confluence* or write to <wrzeszcz@agh.edu.pl>.
