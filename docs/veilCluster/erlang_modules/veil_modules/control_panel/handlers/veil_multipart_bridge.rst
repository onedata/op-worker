.. _veil_multipart_bridge:

veil_multipart_bridge
=====================

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This is a custom multipart bridge. It checks if a request is a multipart POST requests, checks its validity and passes control to file_transfer_handler.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`parse/1 <veil_multipart_bridge:parse/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`veil_multipart_bridge:parse/1`:

	.. function:: parse(ReqBridge :: record()) -> {ok, not_multipart} | {ok, Params :: [tuple()], Files :: [#uploaded_file{}]} | {error, any()}
		:noindex:

	Try to parse the request encapsulated in request bridge if it is a multipart POST request.

