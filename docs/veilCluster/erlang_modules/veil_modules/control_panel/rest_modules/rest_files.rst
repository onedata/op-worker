.. _rest_files:

rest_files
==========

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module implements rest_module_behaviour and handles all REST requests directed at /rest/files/(path). Essentially, it serves user content (files) via HTTP.
	:Behaviours: :ref:`rest_module_behaviour`

Function Index
~~~~~~~~~~~~~~~

	* :ref:`allowed_methods/3 <rest_files:allowed_methods/3>`
	* :ref:`content_types_provided/2 <rest_files:content_types_provided/2>`
	* :ref:`content_types_provided/3 <rest_files:content_types_provided/3>`
	* :ref:`delete/3 <rest_files:delete/3>`
	* :ref:`exists/3 <rest_files:exists/3>`
	* :ref:`get/2 <rest_files:get/2>`
	* :ref:`get/3 <rest_files:get/3>`
	* :ref:`handle_multipart_data/4 <rest_files:handle_multipart_data/4>`
	* :ref:`post/4 <rest_files:post/4>`
	* :ref:`put/4 <rest_files:put/4>`
	* :ref:`validate/4 <rest_files:validate/4>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`rest_files:allowed_methods/3`:

	.. function:: allowed_methods(req(), binary(), binary()) -> {[binary()], req()}
		:noindex:

	Should return list of methods that are allowed and directed at specific Id. e.g.: if Id =:= undefined -> ''[<<"GET">>, <<"POST">>]'' if Id /= undefined -> ''[<<"GET">>, <<"PUT">>, <<"DELETE">>]''

	.. _`rest_files:content_types_provided/2`:

	.. function:: content_types_provided(req(), binary()) -> {[binary()], req()}
		:noindex:

	Should return list of provided content-types without specified ID (e.g. ".../rest/resource/"). Should take into account different types of methods (PUT, GET etc.), if needed. Should return empty list if method is not supported. If there is no id, only dirs can be listed -> application/json.

	.. _`rest_files:content_types_provided/3`:

	.. function:: content_types_provided(req(), binary(), binary()) -> {[binary()], req()}
		:noindex:

	Should return list of provided content-types with specified ID (e.g. ".../rest/resource/some_id"). Should take into account different types of methods (PUT, GET etc.), if needed. Should return empty list if method is not supported. Id is a dir -> application/json Id is a regular file -> '<mimetype>' Id does not exist -> []

	.. _`rest_files:delete/3`:

	.. function:: delete(req(), binary(), binary()) -> {boolean(), req()}
		:noindex:

	Will be called for DELETE request on given ID. Should try to remove specified resource and return true/false indicating the result. Should always return false if the method is not supported.

	.. _`rest_files:exists/3`:

	.. function:: exists(req(), binary(), binary()) -> {boolean(), req()}
		:noindex:

	Should return whether resource specified by given ID exists. Will be called for GET, PUT and DELETE when ID is contained in the URL.

	.. _`rest_files:get/2`:

	.. function:: get(req(), binary()) -> {term() | {stream, integer(), function()} | halt, req(), req()}
		:noindex:

	Will be called for GET request without specified ID (e.g. ".../rest/resource/"). Should return one of the following: 1. ResponseBody, of the same type as content_types_provided/1 returned for this request 2. Cowboy type stream function, serving content of the same type as content_types_provided/1 returned for this request 3. 'halt' atom if method is not supported

	.. _`rest_files:get/3`:

	.. function:: get(req(), binary(), binary()) -> {term() | {stream, integer(), function()} | halt, req(), req()}
		:noindex:

	Will be called for GET request with specified ID (e.g. ".../rest/resource/some_id"). Should return one of the following: 1. ResponseBody, of the same type as content_types_provided/2 returned for this request 2. Cowboy type stream function, serving content of the same type as content_types_provided/2 returned for this request 3. 'halt' atom if method is not supported

	.. _`rest_files:handle_multipart_data/4`:

	.. function:: handle_multipart_data(req(), binary(), binary(), term()) -> {boolean(), req()}
		:noindex:

	Optional callback to handle multipart requests. Data should be streamed in handling module with use of cowboy_multipart module. Method can be '<<"POST">> or <<"PUT">>'. Should handle the request and return true/false indicating the result. Should always return false if the method is not supported.

	.. _`rest_files:post/4`:

	.. function:: post(req(), binary(), binary(), term()) -> {boolean() | {true, binary()}, req()}
		:noindex:

	Will be called for POST request, after the request has been validated. Should handle the request and return true/false indicating the result. Should always return false if the method is not supported. Returning {true, URL} will cause the reply to contain 201 redirect to given URL.

	.. _`rest_files:put/4`:

	.. function:: put(req(), binary(), binary(), term()) -> {boolean(), req()}
		:noindex:

	Will be called for PUT request on given ID, after the request has been validated. Should handle the request and return true/false indicating the result. Should always return false if the method is not supported.

	.. _`rest_files:validate/4`:

	.. function:: validate(req(), binary(), binary(), term()) -> {boolean(), req()}
		:noindex:

	Should return true/false depending on whether the request is valid in terms of the handling module. Will be called before POST or PUT, should discard unprocessable requests.

