Resources
=========

	REST-ish endpoint for interacting with VeilFS.


Index
-----

	* :ref:`/files` [:ref:`GET <GET /files>` ]
		* :ref:`/files/(path)` [:ref:`GET <GET /files/(path)>`, :ref:`POST <POST /files/(path)>`, :ref:`PUT <PUT /files/(path)>`, :ref:`DELETE <DELETE /files/(path)>` ]
	* :ref:`/rest/latest/attrs </attrs/(path)>`
		* :ref:`/attrs/(path)` [:ref:`GET <GET /attrs/(path)>` ]
	* :ref:`/shares` [:ref:`GET <GET /shares>`, :ref:`POST <POST /shares>` ]
		* :ref:`/shares/(guid)` [:ref:`GET <GET /shares/(guid)>`, :ref:`DELETE <DELETE /shares/(guid)>` ]

..  _`/files`:

/rest/latest/files
------------------

	**Methods**

	..  _`GET /files`:
	.. http:get:: /rest/latest/files
	
		Retrieve content of root directory and return as a list of names of files and subdirectories.

		:resheader Content-Type: application/json
		:status 200: OK
		:status 500: Internal Server Error

		**Example request**:

		.. sourcecode:: http

			GET /rest/latest/files HTTP/1.1
			Header: "content-type: application/json"
			Host: example.com

		**Example responses**:
	
		.. sourcecode:: http

			HTTP/1.1 200 OK
			connection: close
			server: Cowboy
			date: Sun, 05 Jan 2014 16:34:54 GMT
			content-length: 12
			Access-Control-Allow-Origin: *
			content-type: application/json

			["dir1","dir2","groups","file.txt"]

..  _`/files/(path)`:

/rest/latest/files/(path)
-------------------------

	**Methods**

	..  _`GET /files/(path)`:
	.. http:get:: /rest/latest/files/(path)
	
		Retrieve content of specified file or directory. For path to an existing file this request returns its content. For path to an existing directory this request returns list of names of contained files and subdirectories.

		:param path: path to file or directory
		:type path: string
		:resheader Content-Type: application/json for path to directory
		:resheader Content-Type: MIME type for path to file
		:status 200: OK
		:status 404: Not Found
		:status 500: Internal Server Error

		**Example request**:

		.. sourcecode:: http

			GET /rest/latest/files/dir1/dir2 HTTP/1.1
			Header: "content-type: application/json"
			Host: example.com

		**Example responses**:
	
		.. sourcecode:: http

			HTTP/1.1 200 OK
			connection: close
			server: Cowboy
			date: Sun, 05 Jan 2014 16:34:54 GMT
			content-length: 12
			Access-Control-Allow-Origin: *
			content-type: application/json

			["file.txt"]

	..  _`POST /files/(path)`:
	.. http:post:: /rest/latest/files/(path)
	
		Upload data to specified path using multipart method. Path has to be a valid file system path, that is it can't contain regular file as a subdirectory. Specified path can't exist and will be created automatically.

		:param path: path where file will be uploaded
		:type path: string
		:reqheader Content-Type: multipart/form-data
		:resheader Content-Type: application/octet-stream
		:status 100: Continue
		:status 204: No Content
		:status 422: Unprocessable Entity

		An example `curl <http://curl.haxx.se/>`_ request to upload file 'file.txt', that is located in current directory, to remote location */dir/file.txt* would be:

		.. sourcecode:: guess

			curl -i -k --cert proxy_cert -X POST -H "content-type: multipart/form-data" -F "file=@file.txt" https://example.com/rest/latest/files/dir/file.txt		

		**Example request**:

		.. sourcecode:: http

			POST /rest/latest/files/dir/file.txt HTTP/1.1
			Host: example.com
			Header: "content-type: multipart/form-data"
			Data: "file=@file.txt"

		**Example responses**:
	
		.. sourcecode:: http

			HTTP/1.1 100 Continue

			HTTP/1.1 204 No Content
			connection: close
			server: Cowboy
			date: Fri, 24 Jan 2014 08:43:05 GMT
			content-length: 0
			Access-Control-Allow-Origin: *
			content-type: application/octet-stream

	..  _`PUT /files/(path)`:
	.. http:put:: /rest/latest/files/(path)
	
		Upload data to specified path using multipart method. Path has to be a valid file system path, that is it can't contain regular file as a subdirectory. If specified path doesn't exist it will be created automatically. If specified path exists it will be overwritten.

		:param path: path where file will be uploaded
		:type path: string
		:reqheader Content-Type: multipart/form-data
		:resheader Content-Type: application/octet-stream
		:status 100: Continue
		:status 204: No Content
		:status 422: Unprocessable Entity

		An example `curl <http://curl.haxx.se/>`_ request to upload file 'file.txt', that is located in current directory, to remote location */dir/file.txt* would be:

		.. sourcecode:: guess

			curl -i -k --cert proxy_cert -X PUT -H "content-type: multipart/form-data" -F "file=@file.txt" https://example.com/rest/latest/files/dir/file.txt		

		**Example request**:

		.. sourcecode:: http

			PUT /rest/latest/files/dir/file.txt HTTP/1.1
			Host: example.com
			Header: "content-type: multipart/form-data"
			Data: "file=@file.txt"

		**Example responses**:
	
		.. sourcecode:: http

			HTTP/1.1 100 Continue

			HTTP/1.1 204 No Content
			connection: close
			server: Cowboy
			date: Fri, 24 Jan 2014 08:43:05 GMT
			content-length: 0
			Access-Control-Allow-Origin: *
			content-type: application/octet-stream

	..  _`DELETE /files/(path)`:
	.. http:delete:: /rest/latest/files/(path)
	
		Delete regular file at specified path if it exists.

		:param path: path to file or directory
		:type path: string
		:resheader Content-Type: application/json
		:resheader Content-Type: application/octet-stream
		:status 204: No Content
		:status 404: Not Found

		**Example request**:

		.. sourcecode:: http

			DELETE /rest/latest/files/dir/file.txt HTTP/1.1
			Header: "content-type: application/json"
			Host: example.com

		**Example responses**:
	
		.. sourcecode:: http

			HTTP/1.1 404 Not Found
			connection: close
			server: Cowboy
			date: Fri, 24 Jan 2014 08:50:18 GMT
			content-length: 0
			Access-Control-Allow-Origin: *
			content-type: application/json


..  _`/attrs/(path)`:

/rest/latest/attrs/(path)
-------------------------

	**Methods**

	..  _`GET /attrs/(path)`:
	.. http:get:: /rest/latest/attrs/(path)
	
		Retrieve attributes of specified file or directory as a record of structure *{property : value}*.

		**Fields of returned record:**

			* file protection mode
			* file owner user ID
			* file owner group ID
			* file last access time
			* file last modification time
			* file or inode last change time
			* file type
			* file owner user name
			* file owner group name

		:param path: path to file or directory
		:type path: string
		:resheader Content-Type: application/json
		:status 200: OK
		:status 404: Not Found
		:status 500: Internal Server Error

		**Example request**:

		.. sourcecode:: http

			GET /rest/latest/attrs/dir1/dir2 HTTP/1.1
			Header: "content-type: application/json"
			Host: example.com

		**Example responses**:
	
		.. sourcecode:: http

			HTTP/1.1 200 OK
			connection: close
			server: Cowboy
			date: Sun, 05 Jan 2014 17:17:39 GMT
			content-length: 157
			Access-Control-Allow-Origin: *
			content-type: application/json

			{"mode":8,"uid":20000,"gid":20000,"atime":1388937272,"mtime":1388937283,"ctime":1388937272,"type":"DIR","size":0,"uname":"user","gname":"group"}

..  _`/shares`:

/rest/latest/shares
-------------------

	**Methods**

	..  _`GET /shares`:
	.. http:get:: /rest/latest/shares
	
		Retrieve shared files as a list of globally unique identifiers.

		:resheader Content-Type: application/json
		:status 200: OK
		:status 404: Not Found
		:status 500: Internal Server Error

		**Example request**:

		.. sourcecode:: http

			GET /rest/latest/files/shares HTTP/1.1
			Header: "content-type: application/json"
			Host: example.com

		**Example responses**:
	
		.. sourcecode:: http

			HTTP/1.1 200 OK
			connection: close
			server: Cowboy
			date: Sun, 05 Jan 2014 17:47:00 GMT
			content-length: 36
			Access-Control-Allow-Origin: *
			content-type: application/json

			["04ef3c62ea0cdba9cd2ac1a860835efe"]

	..  _`POST /shares`:
	.. http:post:: /rest/latest/shares
	
		Share existing file. This request adds specified file to a list of shared files.  

		:param path: path to file to be shared
		:type path: string
		:reqheader Content-Type: application/json
		:resheader Content-Type: application/json
		:resheader Location: redirect link to shared file
		:status 303: See Other
		:status 422: Unprocessable Entity
		:status 500: Internal Server Error

		An example `curl <http://curl.haxx.se/>`_ request to share file */dir/file.txt* would be:

		.. sourcecode:: guess

			curl -i -k --cert proxy_cert -H "content-type: application/json" -X POST https://example.com/rest/latest/shares/ -d '"dir/file.txt"'		

		**Example request**:

		.. sourcecode:: http

			POST /rest/latest/files/shares HTTP/1.1
			Host: example.com
			Header: "content-type: application/json"
			Data: "dir/file.txt"

		**Example responses**:
	
		.. sourcecode:: http

			HTTP/1.1 303 See Other
			connection: close
			server: Cowboy
			date: Sun, 05 Jan 2014 18:38:17 GMT
			content-length: 0
			Access-Control-Allow-Origin: *
			content-type: application/json
			location: https://example.com/share/04ef3d726a2554f240bb15bf4cfa2a13

..  _`/shares/(guid)`:

/rest/latest/shares/(guid)
--------------------------

	**Methods**

	..  _`GET /shares/(guid)`:
	.. http:get:: /rest/latest/shares/(guid)
	
		Retrieve shared file details as a record of structure *{property : value}*.

		**Fields of returned record:**

			* shared file path
			* shared file download url

		:param guid: shared files globally unique identifier
		:type guid: string
		:resheader Content-Type: application/json
		:status 200: OK
		:status 404: Not Found
		:status 500: Internal Server Error

		**Example request**:

		.. sourcecode:: http

			GET /rest/latest/files/shares/04ef3c62ea0cdba9cd2ac1a860835efe HTTP/1.1
			Header: "content-type: application/json"
			Host: example.com

		**Example responses**:
	
		.. sourcecode:: http

			HTTP/1.1 200 OK
			connection: close
			server: Cowboy
			date: Sun, 05 Jan 2014 17:52:16 GMT
			content-length: 108
			Access-Control-Allow-Origin: *
			content-type: application/json

			{"file_path":"dir/file.txt","download_path":"https://example.com/share/04ef3c62ea0cdba9cd2ac1a860835efe"}

	..  _`DELETE /shares/(guid)`:
	.. http:delete:: /rest/latest/shares/(guid)
	
		Stop sharing existing file. This request removes specified file from a list of shared files. 

		:param guid: shared files globally unique identifier
		:type guid: string
		:resheader Content-Type: application/json
		:status 204: No Content
		:status 405: Method Not Allowed
		:status 500: Internal Server Error

		**Example request**:

		.. sourcecode:: http

			DELETE /rest/latest/files/shares/04ef3c62ea0cdba9cd2ac1a860835efe HTTP/1.1
			Header: "content-type: application/json"
			Host: example.com

		**Example responses**:
	
		.. sourcecode:: http

			HTTP/1.1 204 No Content
			connection: close
			server: Cowboy
			date: Sun, 05 Jan 2014 17:58:05 GMT
			content-length: 0
			Access-Control-Allow-Origin: *
			content-type: application/json
