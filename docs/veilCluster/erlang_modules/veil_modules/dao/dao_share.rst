.. _dao_share:

dao_share
=========

	:Authors: Michał Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module provides high level DB API for handling file sharing.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`get_file_share/1 <dao_share:get_file_share/1>`
	* :ref:`remove_file_share/1 <dao_share:remove_file_share/1>`
	* :ref:`save_file_share/1 <dao_share:save_file_share/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`dao_share:get_file_share/1`:

	.. function:: get_file_share(Key:: {file, File :: uuid()} | {user, User :: uuid()} | {uuid, UUID :: uuid()}) -> {ok, file_share_doc()} | {ok, [file_share_doc()]} | {error, any()} | no_return()
		:noindex:

	Gets info about file sharing from db by share_id, file name or user uuid. l Non-error return value is always {ok, #veil_document{record = #share_desc}. See :ref:`dao:save_record/1 <dao:save_record/1>` and :ref:`dao:get_record/1 <dao:get_record/1>` for more details about #veil_document{} wrapper.<br/> Should not be used directly, use :ref:`dao:handle/2 <dao:handle/2>` instead (See :ref:`dao:handle/2 <dao:handle/2>` for more details).

	.. _`dao_share:remove_file_share/1`:

	.. function:: remove_file_share(Key:: {file, File :: uuid()} | {user, User :: uuid()} | {uuid, UUID :: uuid()}) -> {error, any()} | no_return()
		:noindex:

	Removes info about file sharing from DB by share_id, file name or user uuid. Should not be used directly, use :ref:`dao:handle/2 <dao:handle/2>` instead (See :ref:`dao:handle/2 <dao:handle/2>` for more details).

	.. _`dao_share:save_file_share/1`:

	.. function:: save_file_share(Share :: file_share_info() | file_share_doc()) -> {ok, file_share()} | {error, any()} | no_return()
		:noindex:

	Saves info about file sharing to DB. Argument should be either #share_desc{} record (if you want to save it as new document) <br/> or #veil_document{} that wraps #share_desc{} if you want to update descriptor in DB. <br/> See :ref:`dao:save_record/1 <dao:save_record/1>` and :ref:`dao:get_record/1 <dao:get_record/1>` for more details about #veil_document{} wrapper.<br/> Should not be used directly, use :ref:`dao:handle/2 <dao:handle/2>` instead (See :ref:`dao:handle/2 <dao:handle/2>` for more details).

