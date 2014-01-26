.. _dao_users:

dao_users
=========

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module provides high level DB API for handling user documents.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`get_files_number/2 <dao_users:get_files_number/2>`
	* :ref:`get_user/1 <dao_users:get_user/1>`
	* :ref:`remove_user/1 <dao_users:remove_user/1>`
	* :ref:`save_user/1 <dao_users:save_user/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`dao_users:get_files_number/2`:

	.. _`dao_users:get_user/1`:

	.. function:: get_user(Key:: {login, Login :: string()} | {email, Email :: string()} | {uuid, UUID :: uuid()} | {dn, DN :: string()}) -> {ok, user_doc()} | {error, any()} | no_return()
		:noindex:

	Gets user from DB by login, e-mail, uuid or dn. Non-error return value is always {ok, #veil_document{record = #user}. See :ref:`dao:save_record/1 <dao:save_record/1>` and :ref:`dao:get_record/1 <dao:get_record/1>` for more details about #veil_document{} wrapper. Should not be used directly, use :ref:`dao:handle/2 <dao:handle/2>` instead (See :ref:`dao:handle/2 <dao:handle/2>` for more details).

	.. _`dao_users:remove_user/1`:

	.. function:: remove_user(Key:: {login, Login :: string()} | {email, Email :: string()} | {uuid, UUID :: uuid()} | {dn, DN :: string()}) -> {error, any()} | no_return()
		:noindex:

	Removes user from DB by login, e-mail, uuid or dn. Should not be used directly, use :ref:`dao:handle/2 <dao:handle/2>` instead (See :ref:`dao:handle/2 <dao:handle/2>` for more details).

	.. _`dao_users:save_user/1`:

	.. function:: save_user(User :: user_info() | user_doc()) -> {ok, user()} | {error, any()} | no_return()
		:noindex:

	Saves user to DB. Argument should be either #user{} record (if you want to save it as new document) or #veil_document{} that wraps #user{} if you want to update descriptor in DB. See :ref:`dao:save_record/1 <dao:save_record/1>` and :ref:`dao:get_record/1 <dao:get_record/1>` for more details about #veil_document{} wrapper. Should not be used directly, use :ref:`dao:handle/2 <dao:handle/2>` instead (See :ref:`dao:handle/2 <dao:handle/2>` for more details).

