Installation
============

Using current RPM version it is possible to install application's nodes, Bigcouch database and configure storage. Undermentioned instructions are valid for all Red Hat Linux distributions, especially for Scientific Linux.

Dependencies
~~~~~~~~~~~~
	
	.. sourcecode:: guess

		$ yum install js-devel libicu libicu-devel openssl openssl-devel python python-devel rpm-build

System preparation
~~~~~~~~~~~~~~~~~~

	In order to make our machine recognisable for other machines in the network it is required to set its hostname properly. It is recommended to set hostname to IP address, so that modifications of /etc/hosts file are not necessary.

	.. sourcecode:: guess

		$ vi /etc/sysconfig/network -> set HOSTNAME=172.16.67.X -> save file
		$ hostname 172.16.67.X -> (instant effect, system reboot is not necessary)

	Moreover it is required to unlock ports in iptables:

	.. sourcecode:: guess

		$ service iptables stop
		$ chkconfig --del iptables (turn off service permanently - necessary if machine will be rebooted)

RPM build and installation
~~~~~~~~~~~~~~~~~~~~~~~~~~

	After project cloning:

	.. sourcecode:: guess

		$ make rpm (create .rpm in ./releases/veil-0.0.6-1.x86_64.rpm)
		$ yum localinstall ./releases/veil-0.0.6-1.x86_64.rpm (start RPM installation)
		$ veil_setup (start installation script)

	.. warning:: 

		* database needs to be installed before cluster
		* during cluster installation it is required to give full database node name (e.g. db@172.16.67.143)
		* during cluster installation in storage definition phase:
			- give storage mount point used by cluster to store data (e.g. defined users or groups)
			- define optional storages used by fuse client groups, for each of them give mount point and name of fuse client group that is supposed to use it
		* during node addition to existing cluster it is only required to give IP address of any working node (e.g. 172.16.67.143)

Useful commands
~~~~~~~~~~~~~~~

	.. sourcecode:: guess

		$ /opt/veil/nodes/ccm/bin/veil_cluster_node attach (connect to local CCM. Warning! to terminate press Ctrl+D, not Ctrl+C!)
		$ /opt/veil/nodes/worker/bin/veil_cluster_node attach (connect to local worker. Warning! to terminate press Ctrl+D, not Ctrl+C!)
		$ yum remove veil (clean deletion of RPM)

Vocabulary
~~~~~~~~~~

	* *database node* - installed Bigcouch instance
	* *veil node* - installed CCM and worker or only worker
	* *db cluster* - connected and cooperating database nodes group 
	* *veil cluster, cluster, veil* - connected and cooperating veil nodes group
	* *storage* - it is composed of group name that use it and mount point
	* *group name* - fuse client group name that use storage
	* *storage directory* - storage mount point 

