.. _rest_attrs:

rest_attrs
==========

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module implements rest_module_behaviour and handles all REST requests directed at /rest/attr/*. It returns file attributes, if possible.
	:Behaviours: :ref:`rest_module_behaviour`

Function Index
~~~~~~~~~~~~~~~

	* :ref:`allowed_methods/3 <rest_attrs;allowed_methods/3>`
	* :ref:`content_types_provided/2 <rest_attrs;content_types_provided/2>`
	* :ref:`content_types_provided/3 <rest_attrs;content_types_provided/3>`
	* :ref:`delete/3 <rest_attrs;delete/3>`
	* :ref:`exists/3 <rest_attrs;exists/3>`
	* :ref:`get/2 <rest_attrs;get/2>`
	* :ref:`get/3 <rest_attrs;get/3>`
	* :ref:`post/4 <rest_attrs;post/4>`
	* :ref:`put/4 <rest_attrs;put/4>`
	* :ref:`validate/4 <rest_attrs;validate/4>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: rest_attrs

	.. _`rest_attrs;allowed_methods/3`:

	.. erl:function:: allowed_methods(req(), binary(), binary()) -> {[binary()], req()}

	Should return list of methods that are allowed and directed at specific Id. e.g.: if Id =:= undefined -> `[ >, >]' if Id /= undefined -> `[ >, >, >]'

	.. _`rest_attrs;content_types_provided/2`:

	.. erl:function:: content_types_provided(req(), binary()) -> {[binary()], req()}

	Should return list of provided content-types without specified ID (e.g. ".../rest/resource/"). Should take into account different types of methods (PUT, GET etc.), if needed. Should return empty list if method is not supported.

	.. _`rest_attrs;content_types_provided/3`:

	.. erl:function:: content_types_provided(req(), binary(), binary()) -> {[binary()], req()}

	Should return list of provided content-types with specified ID (e.g. ".../rest/resource/some_id"). Should take into account different types of methods (PUT, GET etc.), if needed. Should return empty list if method is not supported.

	.. _`rest_attrs;delete/3`:

	.. erl:function:: delete(req(), binary(), binary()) -> {boolean(), req()}

	Will be called for DELETE request on given ID. Should try to remove specified resource and return true/false indicating the result. Should always return false if the method is not supported.

	.. _`rest_attrs;exists/3`:

	.. erl:function:: exists(req(), binary(), binary()) -> {boolean(), req()}

	Should return whether resource specified by given ID exists. Will be called for GET, PUT and DELETE when ID is contained in the URL.

	.. _`rest_attrs;get/2`:

	.. erl:function:: get(req(), binary()) -> {term() | {stream, integer(), function()} | halt, req(), req()}

	Will be called for GET request without specified ID (e.g. ".../rest/resource/"). Should return one of the following: 1. ResponseBody, of the same type as content_types_provided/1 returned for this request 2. Cowboy type stream function, serving content of the same type as content_types_provided/1 returned for this request 3. 'halt' atom if method is not supported

	.. _`rest_attrs;get/3`:

	.. erl:function:: get(req(), binary(), binary()) -> {term() | {stream, integer(), function()} | halt, req(), req()}

	Will be called for GET request with specified ID (e.g. ".../rest/resource/some_id"). Should return one of the following: 1. ResponseBody, of the same type as content_types_provided/2 returned for this request 2. Cowboy type stream function, serving content of the same type as content_types_provided/2 returned for this request 3. 'halt' atom if method is not supported

	.. _`rest_attrs;post/4`:

	.. erl:function:: post(req(), binary(), binary(), term()) -> {boolean() | {true, binary()}, req()}

	Will be called for POST request, after the request has been validated. Should handle the request and return true/false indicating the result. Should always return false if the method is not supported. Returning {true, URL} will cause the reply to contain 201 redirect to given URL.

	.. _`rest_attrs;put/4`:

	.. erl:function:: put(req(), binary(), binary(), term()) -> {boolean(), req()}

	Will be called for PUT request on given ID, after the request has been validated. Should handle the request and return true/false indicating the result. Should always return false if the method is not supported.

	.. _`rest_attrs;validate/4`:

	.. erl:function:: validate(req(), binary(), binary(), term()) -> {boolean(), req()}

	Should return true/false depending on whether the request is valid in terms of the handling module. Will be called before POST or PUT, should discard unprocessable requests.

