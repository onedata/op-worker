Authentication
==============

	In brief authentication proceed through following steps:

		* Client opens HTTPS connection and sends his certificate (in most cases proxy certificate) and all other certificates required to associate his certificate with certificate authorities present on cluster.
		* Each HTTPS connection is accepted regardless of sent certificates. However all certificates are saved in connection session. 
		* If connection requires authentication, and in case of REST requests it does, immediately after connection is established sent certificates are validated using Grid Security Infrastructure. 
		* If sent certificate fails verification or is not not present in database, connection is closed.
		* If sent certificate passes verification its distinguished name is associated with single entry in users database and since than client can access cluster resources.

	How to create proxy certificate?

		To create proxy certificate run *grid-proxy-init* script with argument *-out path_to_proxy_cert*. This script can be found at root directory on cluster and requires presence of main PLGrid certificate in */root/.globus* directory.

		**Example**:

		.. sourcecode:: guess

			sh grid-proxy-init -out proxy_cert

	How to use proxy certificate?

		Proxy certificate can be used with any computer software that provides data transfer using HTTPS protocol and supports HTTPS certificates. In following example content of *dir* directory will be retrieved using HTTP GET request and `curl <http://curl.haxx.se/>`_ tool.

		**Example**:

		.. sourcecode:: guess

			curl -i -k --cert proxy_cert -X GET https://example.com/rest/latest/files/dir

		.. warning:: 

			VeilFS REST API doesn't work with every version of **curl** tool. It has to be linked to *OpenSSL* library. To check it use *curl --version* command.  


