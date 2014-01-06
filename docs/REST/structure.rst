Structure of the REST URIs
==========================

	VeilFS's REST APIs provide access to resources (data entities) via URI paths. To use a REST API, your application will make an HTTP request and parse the response. The VeilFS REST API uses JSON as its communication format, and the standard HTTP methods like GET, PUT, POST and DELETE. URIs for VeilFS's REST API resource have the following structure:

	.. sourcecode:: http

		http://host:port/rest/api-version/path/to/resource

	Where *api-version* can be of following:

		* number - which stands for an exact API version
		* latest - which stands for most recent, stable version

	We strongly recommend usage of the latter option especially due to introduction of new features and elimination of bugs in system. 

	Example URI that would retrieve content of *dir* directory.

	.. sourcecode:: http

		https://example.com/rest/latest/files/dir

	For more information see a full list of available :doc:`resources <resources>`.
