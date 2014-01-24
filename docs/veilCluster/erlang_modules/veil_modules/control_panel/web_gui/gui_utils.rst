.. _gui_utils:

gui_utils
=========

	:Authors: Lukasz Opiola
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This file contains useful functions commonly used in control_panel modules.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`apply_or_redirect/3 <gui_utils;apply_or_redirect/3>`
	* :ref:`apply_or_redirect/4 <gui_utils;apply_or_redirect/4>`
	* :ref:`can_view_logs/0 <gui_utils;can_view_logs/0>`
	* :ref:`dn_and_storage_defined/0 <gui_utils;dn_and_storage_defined/0>`
	* :ref:`empty_page/0 <gui_utils;empty_page/0>`
	* :ref:`get_requested_hostname/0 <gui_utils;get_requested_hostname/0>`
	* :ref:`get_user_dn/0 <gui_utils;get_user_dn/0>`
	* :ref:`logotype_footer/1 <gui_utils;logotype_footer/1>`
	* :ref:`storage_defined/0 <gui_utils;storage_defined/0>`
	* :ref:`top_menu/1 <gui_utils;top_menu/1>`
	* :ref:`top_menu/2 <gui_utils;top_menu/2>`
	* :ref:`user_logged_in/0 <gui_utils;user_logged_in/0>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: gui_utils

	.. _`gui_utils;apply_or_redirect/3`:

	.. erl:function:: apply_or_redirect(Module :: atom, Fun :: atom, boolean()) -> boolean()

	Checks if the client has right to do the operation (is logged in and possibly has a certificate DN defined). If so, it executes the code.

	.. _`gui_utils;apply_or_redirect/4`:

	.. erl:function:: apply_or_redirect(Module :: atom, Fun :: atom, Args :: list(), boolean()) -> boolean()

	Checks if the client has right to do the operation (is logged in and possibly has a certificate DN defined). If so, it executes the code.

	.. _`gui_utils;can_view_logs/0`:

	.. erl:function:: can_view_logs() -> boolean()

	Determines if current user is allowed to view cluster logs.

	.. _`gui_utils;dn_and_storage_defined/0`:

	.. erl:function:: dn_and_storage_defined() -> boolean()

	Convienience function to check both conditions.

	.. _`gui_utils;empty_page/0`:

	.. _`gui_utils;get_requested_hostname/0`:

	.. erl:function:: get_requested_hostname() -> string()

	Returns the hostname requested by the client.

	.. _`gui_utils;get_user_dn/0`:

	.. erl:function:: get_user_dn() -> string()

	Returns user's DN retrieved from his session state.

	.. _`gui_utils;logotype_footer/1`:

	.. erl:function:: logotype_footer(MarginTop :: integer()) -> list()

	Convienience function to render logotype footer, coming after page content.

	.. _`gui_utils;storage_defined/0`:

	.. erl:function:: storage_defined() -> boolean()

	Checks if any storage is defined in the database.

	.. _`gui_utils;top_menu/1`:

	.. erl:function:: top_menu(ActiveTabID :: any()) -> list()

	Convienience function to render top menu in GUI pages. Item with ActiveTabID will be highlighted as active.

	.. _`gui_utils;top_menu/2`:

	.. erl:function:: top_menu(ActiveTabID :: any(), SubMenuBody :: any()) -> list()

	Convienience function to render top menu in GUI pages. Item with ActiveTabID will be highlighted as active. Submenu body (list of nitrogen elements) will be concatenated below the main menu.

	.. _`gui_utils;user_logged_in/0`:

	.. erl:function:: user_logged_in() -> boolean()

	Checks if the client has a valid login session.

