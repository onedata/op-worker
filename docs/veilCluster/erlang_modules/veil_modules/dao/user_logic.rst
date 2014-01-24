.. _user_logic:

user_logic
==========

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module provides methods for managing users in the system.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`create_dirs_at_storage/2 <user_logic:create_dirs_at_storage/2>`
	* :ref:`create_user/5 <user_logic:create_user/5>`
	* :ref:`extract_dn_from_cert/1 <user_logic:extract_dn_from_cert/1>`
	* :ref:`get_dn_list/1 <user_logic:get_dn_list/1>`
	* :ref:`get_email_list/1 <user_logic:get_email_list/1>`
	* :ref:`get_login/1 <user_logic:get_login/1>`
	* :ref:`get_name/1 <user_logic:get_name/1>`
	* :ref:`get_team_names/1 <user_logic:get_team_names/1>`
	* :ref:`get_teams/1 <user_logic:get_teams/1>`
	* :ref:`get_user/1 <user_logic:get_user/1>`
	* :ref:`invert_dn_string/1 <user_logic:invert_dn_string/1>`
	* :ref:`oid_code_to_shortname/1 <user_logic:oid_code_to_shortname/1>`
	* :ref:`rdn_sequence_to_dn_string/1 <user_logic:rdn_sequence_to_dn_string/1>`
	* :ref:`remove_user/1 <user_logic:remove_user/1>`
	* :ref:`shortname_to_oid_code/1 <user_logic:shortname_to_oid_code/1>`
	* :ref:`sign_in/1 <user_logic:sign_in/1>`
	* :ref:`update_dn_list/2 <user_logic:update_dn_list/2>`
	* :ref:`update_email_list/2 <user_logic:update_email_list/2>`
	* :ref:`update_teams/2 <user_logic:update_teams/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`user_logic:create_dirs_at_storage/2`:

	.. function:: create_dirs_at_storage(Root :: string(), Teams :: [string()]) -> ok | Error
		:noindex:

	* **Error:** atom()

	Creates root dir for user and for its teams

	.. _`user_logic:create_user/5`:

	.. function:: create_user(Login, Name, Teams, Email, DnList) -> Result
		:noindex:

	* **DnList:** [string()]
	* **Email:** string()
	* **Login:** string()
	* **Name:** string()
	* **Result:** {ok, user_doc()} | {error, any()}
	* **Teams:** string()

	 Creates a user in the DB.

	.. _`user_logic:extract_dn_from_cert/1`:

	.. function:: extract_dn_from_cert(PemBin :: binary()) -> {rdnSequence, [#'AttributeTypeAndValue'{}]} | {error, Reason}
		:noindex:

	* **Reason:** proxy_ceertificate | self_signed | extraction_failed

	Processes a .pem certificate and extracts subject (DN) part, returning it as an rdnSequence. Returns an error if: - fails to extract DN - certificate is a proxy certificate -> {error, proxy_ceertificate} - certificate is self-signed -> {error, self_signed}

	.. _`user_logic:get_dn_list/1`:

	.. function:: get_dn_list(User) -> Result
		:noindex:

	* **Result:** string()
	* **User:** user_doc()

	 Convinience function to get DN list from #veil_document encapsulating #user record.

	.. _`user_logic:get_email_list/1`:

	.. function:: get_email_list(User) -> Result
		:noindex:

	* **Result:** string()
	* **User:** user_doc()

	 Convinience function to get e-mail list from #veil_document encapsulating #user record.

	.. _`user_logic:get_login/1`:

	.. function:: get_login(User) -> Result
		:noindex:

	* **Result:** string()
	* **User:** user_doc()

	 Convinience function to get user login from #veil_document encapsulating #user record.

	.. _`user_logic:get_name/1`:

	.. function:: get_name(User) -> Result
		:noindex:

	* **Result:** string()
	* **User:** user_doc()

	 Convinience function to get user name from #veil_document encapsulating #user record.

	.. _`user_logic:get_team_names/1`:

	.. function:: get_team_names(UserQuery :: term()) -> [string()] | no_return()
		:noindex:

	Returns list of group/team names for given user. UserQuery shall be either #user{} record or query compatible with user_logic:get_user/1. The method assumes that user exists therefore will fail with exception when it doesnt.

	.. _`user_logic:get_teams/1`:

	.. function:: get_teams(User) -> Result
		:noindex:

	* **Result:** string()
	* **User:** user_doc()

	 Convinience function to get user teams from #veil_document encapsulating #user record.

	.. _`user_logic:get_user/1`:

	.. function:: get_user(Key :: {login, Login :: string()} | {email, Email :: string()} | {uuid, UUID :: user()} | {dn, DN :: string()} | {rdnSequence, [#'AttributeTypeAndValue'{}]}) -> {ok, user_doc()} | {error, any()}
		:noindex:

	 Retrieves user from DB by login, email, uuid, DN or rdnSequence proplist. Returns veil_document wrapping a #user record.

	.. _`user_logic:invert_dn_string/1`:

	.. function:: invert_dn_string(string()) -> string()
		:noindex:

	Inverts the sequence of entries in a DN string.

	.. _`user_logic:oid_code_to_shortname/1`:

	.. function:: oid_code_to_shortname(term()) -> string() | no_return()
		:noindex:

	Converts erlang-like OID code to OpenSSL short name.

	.. _`user_logic:rdn_sequence_to_dn_string/1`:

	.. function:: rdn_sequence_to_dn_string([#'AttributeTypeAndValue'{}]) -> string() | no_return()
		:noindex:

	Converts rdnSequence to DN string so that it can be compared to another DN.

	.. _`user_logic:remove_user/1`:

	.. function:: remove_user(Key :: {login, Login :: string()} | {email, Email :: string()} | {uuid, UUID :: user()} | {dn, DN :: string()} | {rdnSequence, [#'AttributeTypeAndValue'{}]}) -> Result :: ok | {error, any()}
		:noindex:

	 Removes user from DB by login.

	.. _`user_logic:shortname_to_oid_code/1`:

	.. function:: shortname_to_oid_code(string()) -> term() | no_return()
		:noindex:

	Converts OpenSSL short name to erlang-like OID code.

	.. _`user_logic:sign_in/1`:

	.. function:: sign_in(Proplist) -> Result
		:noindex:

	* **Proplist:** list()
	* **Result:** {string(), user_doc()}

	 This function should be called after a user has logged in via OpenID. It looks the user up in database by login. If he is not there, it creates a proper document. If the user already exists, synchronization is made between document in the database and info received from OpenID provider.

	.. _`user_logic:update_dn_list/2`:

	.. function:: update_dn_list(User, NewDnList) -> Result
		:noindex:

	* **NewDnList:** [string()]
	* **Result:** {ok, user_doc()} | {error, any()}
	* **User:** user_doc()

	 Update #veil_document encapsulating #user record with new DN list and save it to DB.

	.. _`user_logic:update_email_list/2`:

	.. function:: update_email_list(User, NewEmailList) -> Result
		:noindex:

	* **NewEmailList:** [string()]
	* **Result:** {ok, user_doc()} | {error, any()}
	* **User:** user_doc()

	 Update #veil_document encapsulating #user record with new e-mail list and save it to DB.

	.. _`user_logic:update_teams/2`:

	.. function:: update_teams(User, NewTeams) -> Result
		:noindex:

	* **NewTeams:** string()
	* **Result:** {ok, user_doc()} | {error, any()}
	* **User:** user_doc()

	 Update #veil_document encapsulating #user record with new teams and save it to DB.

