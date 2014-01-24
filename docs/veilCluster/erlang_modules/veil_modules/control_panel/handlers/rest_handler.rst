.. _rest_handler:

rest_handler
============

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This is a cowboy handler module, implementing cowboy_rest interface. It handles REST requests by routing them to proper rest module.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`allowed_methods/2 <rest_handler;allowed_methods/2>`
	* :ref:`content_types_accepted/2 <rest_handler;content_types_accepted/2>`
	* :ref:`content_types_provided/2 <rest_handler;content_types_provided/2>`
	* :ref:`delete_resource/2 <rest_handler;delete_resource/2>`
	* :ref:`get_resource/2 <rest_handler;get_resource/2>`
	* :ref:`handle_json_data/2 <rest_handler;handle_json_data/2>`
	* :ref:`handle_multipart_data/2 <rest_handler;handle_multipart_data/2>`
	* :ref:`handle_urlencoded_data/2 <rest_handler;handle_urlencoded_data/2>`
	* :ref:`init/3 <rest_handler;init/3>`
	* :ref:`resource_exists/2 <rest_handler;resource_exists/2>`
	* :ref:`rest_init/2 <rest_handler;rest_init/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: rest_handler

	.. _`rest_handler;allowed_methods/2`:

	.. erl:function:: allowed_methods(req(), #state{}) -> {[binary()], req(), #state{}}

	Cowboy callback function Returns methods that are allowed for request URL. Will call allowed_methods/2 from rest_module_behaviour.

	.. _`rest_handler;content_types_accepted/2`:

	.. erl:function:: content_types_accepted(req(), #state{}) -> {term(), req(), #state{}}

	Cowboy callback function Returns content-types that are accepted by REST handler and what functions should be used to process the requests.

	.. _`rest_handler;content_types_provided/2`:

	.. erl:function:: content_types_provided(req(), #state{}) -> {[binary()], req(), #state{}}

	Cowboy callback function Returns content types that can be provided for the request. Will call content_types_provided/1|2 from rest_module_behaviour.

	.. _`rest_handler;delete_resource/2`:

	.. erl:function:: delete_resource(req(), #state{}) -> {term(), req(), #state{}}

	Cowboy callback function Handles DELETE requests. Will call delete/2 from rest_module_behaviour.

	.. _`rest_handler;get_resource/2`:

	.. erl:function:: get_resource(req(), #state{}) -> {term(), req(), #state{}}

	Cowboy callback function Handles GET requests. Will call get/1|2 from rest_module_behaviour.

	.. _`rest_handler;handle_json_data/2`:

	.. erl:function:: handle_json_data(req(), #state{}) -> {boolean(), req(), #state{}}

	Function handling "application/json" requests.

	.. _`rest_handler;handle_multipart_data/2`:

	.. erl:function:: handle_multipart_data(req(), #state{}) -> {boolean(), req(), #state{}}

	Function handling "multipart/form-data" requests.

	.. _`rest_handler;handle_urlencoded_data/2`:

	.. erl:function:: handle_urlencoded_data(req(), #state{}) -> {boolean(), req(), #state{}}

	Function handling "application/x-www-form-urlencoded" requests.

	.. _`rest_handler;init/3`:

	.. erl:function:: init(any(), any(), any()) -> {upgrade, protocol, cowboy_rest}

	Cowboy callback function Imposes a cowboy upgrade protocol to cowboy_rest - this module is now treated as REST module by cowboy.

	.. _`rest_handler;resource_exists/2`:

	.. erl:function:: resource_exists(req(), #state{}) -> {boolean(), req(), #state{}}

	Cowboy callback function Determines if resource identified by URL exists. Will call exists/2 from rest_module_behaviour.

	.. _`rest_handler;rest_init/2`:

	.. erl:function:: rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}

	Cowboy callback function Called right after protocol upgrade to init the request context. Will shut down the connection if the peer doesn't provide a valid proxy certificate.

