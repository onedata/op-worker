.. _rest_routes:

rest_routes
===========

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module provides mapping of rest subpaths to erlang modules that will end up handling REST requests.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`route/1 <rest_routes:route/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`rest_routes:route/1`:

	.. function:: route([binary()]) -> {atom(), binary()}
		:noindex:

	 This function returns handler module and resource ID based on REST request path. The argument is a list of binaries - result of splitting request subpath on "/". Subpath is all that occurs after ''"<host>/rest/<version>/"'' in request path. Should return a tuple: - the module that will be called to handle requested REST resource (atom) - resource id or undefined if none was specified (binary or atom (undefined)) or undefined if no module was matched

