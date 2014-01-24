.. _file_transfer_handler:

file_transfer_handler
=====================

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module processes file download and upload requests. After validating them it conducts streaming to or from a socket or makes nitrogen display a proper error page.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`get_download_buffer_size/0 <file_transfer_handler;get_download_buffer_size/0>`
	* :ref:`handle_rest_upload/3 <file_transfer_handler;handle_rest_upload/3>`
	* :ref:`handle_upload_request/2 <file_transfer_handler;handle_upload_request/2>`
	* :ref:`maybe_handle_request/1 <file_transfer_handler;maybe_handle_request/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: file_transfer_handler

	.. _`file_transfer_handler;get_download_buffer_size/0`:

	.. _`file_transfer_handler;handle_rest_upload/3`:

	.. erl:function:: multipart_data(Req) -> {headers, cowboy:http_headers(), Req} | {body, binary(), Req} | {end_of_part | eof, Req}

	* **Req:** req()

	Asserts the validity of mutlipart POST request and proceeds with parsing and writing its data to a file at specified path. Returns true for successful upload anf false otherwise.

	.. _`file_transfer_handler;handle_upload_request/2`:

	.. erl:function:: handle_upload_request(Req :: req(), record()) -> {ok, Params :: [tuple()], Files :: [#uploaded_file{}]} | {error, incorrect_session}

	Asserts the validity of mutlipart POST request and proceeds with parsing or returns an error. Returns list of parsed filed values and file body.

	.. _`file_transfer_handler;maybe_handle_request/1`:

	.. erl:function:: maybe_handle_request(Req :: req()) -> {boolean(), NewReq :: req()}

	Intercepts the request if its a user content or shared file download request or doeas nothinf otherwise. Checks its validity and serves the file or redirects to error page. Should be called on a request coming directly to nitrogen_handler.

