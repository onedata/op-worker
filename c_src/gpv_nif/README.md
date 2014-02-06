GridProxyVerify
===============

	GridProxyVerify is an library that provides GSI proxy certificates verification based on Globus and OpenSSL libraries.

### Files

	* **grid\_proxy\_verify.c** - This file can be compiled into independent static/dynamic library that is able to:
	    - Insert any peer/peerCA/CA/CRL certificates into current verification context
	    - Proceed with verification of certificates added earlier
	    - Retrieve verification errors

	    Basically it provides simple GSI validation that can be used through very simple API

	* **gpv_nif.c** - This file contains Erlang NIF interface for grid\_proxy\_verify library. It can be loaded to Erlang VM as verify\_cert\_c/4 method.
	    For more info check _cluster\_elements/request\_dispatcher/gpv\_nif.erl_ file or just its @doc.

### Prerequisites

	* libglobus_gsi_callback
	* libglobus_common
	* libssl

	Use this command to install the required dependency packages:

	* Debian/Ubuntu Dependencies (.deb packages):

	        apt-get install libglobus-gsi-callback-dev

	* RHEL/CentOS/Fedora Dependencies (.rpm packages):

	        yum install globus-gsi-callback-devel

	Additionally if you want to use NIF API, you need to have Erlang NIF libraries and headers. Normally they are shipped with Erlang.

### Compilation

	The easiest way of compiling this lib is to use **rebar**. Rebar automatically adds required by Elang NIF API lib/headers path to compiler options.
	Unfortunately you still need to explicate specify _-lglobus\_gsi\_callback -lglobus\_common -lssl_ options. In order to do that you have to provide **port\_env** option in
	your _rebar.config_ file, like in [this](https://github.com/basho/rebar/blob/master/rebar.config.sample) example.
