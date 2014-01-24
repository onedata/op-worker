.. _openid_utils:

openid_utils
============

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This is a simple library used to establish an OpenID authentication. It needs nitrogen and ibrowse to run.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`get_login_url/2 <openid_utils:get_login_url/2>`
	* :ref:`nitrogen_prepare_validation_parameters/0 <openid_utils:nitrogen_prepare_validation_parameters/0>`
	* :ref:`nitrogen_retrieve_user_info/0 <openid_utils:nitrogen_retrieve_user_info/0>`
	* :ref:`validate_openid_login/1 <openid_utils:validate_openid_login/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`openid_utils:get_login_url/2`:

	.. _`openid_utils:nitrogen_prepare_validation_parameters/0`:

	.. function:: nitrogen_prepare_validation_parameters() -> Result
		:noindex:

	* **Error:** invalid_request
	* **Result:** {string(), string()} | {error, Error}

	 This function retrieves endpoint URL and parameters from redirection URL created by OpenID provider. They are later used as arguments to validate_openid_login() function. Must be called from within nitrogen page context to work, precisely from openid redirection page.

	.. _`openid_utils:nitrogen_retrieve_user_info/0`:

	.. function:: nitrogen_retrieve_user_info() -> Result
		:noindex:

	* **Error:** invalid_request
	* **Result:** {ok, list()} | {error, Error}

	 This function retrieves user info from parameters of redirection URL created by OpenID provider. They are returned as a proplist and later used to authenticate a user in the system. Must be called from within nitrogen page context to work, precisely from openid redirection page.

	.. _`openid_utils:validate_openid_login/1`:

