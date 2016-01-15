oneprovider 1.6
===============

*oneprovider 1.6* is a part of *onedata 1.6*. This is mainly a bug-fix release. See further details below.

New features
------------

* Security mechanism against attack for atoms table added

Fixed Bugs
----------

* Invalid use of WebGUI cache fixed

oneprovider 1.5
===============

*oneprovider 1.5* is a part of *onedata 1.5*. This is mainly a bug-fix release. See further details below. *onedata 1.5*
updates only *oneclient* and *oneprovider* so it links version 1.0 of other modules.

New features
------------

* WebGUI and FUSE client handler can use different certificates.

Fixed Bugs
----------

* Xss and csrf protection mechanisms added.
* Attack with symbolic links is not possible due to security mechanism update.

oneprovider 1.0
===============

*oneprovider 1.0* is a part of *onedata 1.0*. *onedata 1.0* is a system that provides a unified and efficient access to
data stored at various storage systems in one site. *oneprovider 1.0* is management component of the system. It provides
fully functional cluster of cooperating nodes and offers basic functionality (Web GUI, support for FUSE clients, rule
management subsystem 1.0).

Issue Summary
-------------

* *oneprovider* supports multiple nodes deployment. It automatically discovers cluster structure and reconfigures 
it if needed.
* *oneprovider* handles requests from FUSE clients to show location of needed data. 
* *oneprovider* provides needed data if storage system where data is located is not connected to client.
* *oneprovider* provides Web GUI for users which offers data and account management functions. Management functions 
include certificates management.
* *oneprovider* provides Web GUI for administrators which offers monitoring and logs preview (also Fuse clients logs).
* *oneprovider* provides users' authentication via OpenID and certificates.
* *oneprovider* provides rule management subsystem (version 1.0).
* *oneprovider* may reconfigure *oneclient* using callbacks.

