.. _fslogic_storage:

fslogic_storage
===============

	:Authors: Rafal Slota
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module exports storage management tools for fslogic

Function Index
~~~~~~~~~~~~~~~

	* :ref:`get_sh_for_fuse/2 <fslogic_storage:get_sh_for_fuse/2>`
	* :ref:`insert_storage/2 <fslogic_storage:insert_storage/2>`
	* :ref:`insert_storage/3 <fslogic_storage:insert_storage/3>`
	* :ref:`select_storage/2 <fslogic_storage:select_storage/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. _`fslogic_storage:get_sh_for_fuse/2`:

	.. function:: get_sh_for_fuse(FuseID :: string(), Storage :: #storage_info{}) -> #storage_helper_info{}
		:noindex:

	Returns #storage_helper_info{} record which describes storage helper that is connected with given storage (described with #storage_info{} record). Each storage can have multiple storage helpers, that varies between FUSE groups, so that different FUSE clients (with different FUSE_ID) could select different storage helper.

	.. _`fslogic_storage:insert_storage/2`:

	.. function:: insert_storage(HelperName :: string(), HelperArgs :: [string()]) -> term()
		:noindex:

	Creates new mock-storage info in DB that uses default storage helper with name HelperName and argument list HelperArgs.

	.. _`fslogic_storage:insert_storage/3`:

	.. function:: insert_storage(HelperName :: string(), HelperArgs :: [string()], Fuse_groups :: list()) -> term()
		:noindex:

	Creates new mock-storage info in DB that uses default storage helper with name HelperName and argument list HelperArgs. TODO: This is mock method and should be replaced by GUI-tool form control_panel module.

	.. _`fslogic_storage:select_storage/2`:

	.. function:: select_storage(FuseID :: string(), StorageList :: [#storage_info{}]) -> #storage_info{}
		:noindex:

	Chooses and returns one storage_info from given list of #storage_info records. TODO: This method is an mock method that shall be replaced in future. Currently returns random #storage_info{}.

