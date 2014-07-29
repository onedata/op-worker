VeilCluster 1.0
===========

VeilCluster 1.0 is a part of VeilFS 1.0. VeilFS 1.0 is a system that provides a unified and efficient access to data stored at various storage systems in one site. VeilCluster 1.0 is management component of the system. It provides fully functional cluster of cooperating nodes and offers basic functionality (Web GUI, support for FUSE clients, rule management subsystem 1.0).

Issue Summary
-----

* VeilCluster supports multiple nodes deployment. It automatically discovers cluster structure and reconfigures it if needed.
* VeilCluster handles requests from FUSE clients to show location of needed data. 
* VeilCluster provides needed data if storage system where data is located is not connected to client.
* VeilCluster provides Web GUI for users which offers data and account management functions. Management functions include certificates management.
* VeilCluster provides Web GUI for administrators which offers monitoring and logs preview (also Fuse clients logs).
* VeilCluster provides users' authentication via OpenID and certificates.
* VeilCluster provides rule management subsystem (version 1.0).
* VeilCluster may reconfigure VeilClient using callbacks.

