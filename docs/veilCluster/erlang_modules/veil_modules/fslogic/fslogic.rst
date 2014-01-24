.. _fslogic:

fslogic
=======

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module implements worker_plugin_behaviour to provide functionality of file system logic.
	:Behaviours: :ref:`worker_plugin_behaviour`

Function Index
~~~~~~~~~~~~~~~

	* :ref:`cleanup/0 <fslogic;cleanup/0>`
	* :ref:`create_dirs/4 <fslogic;create_dirs/4>`
	* :ref:`get_file/3 <fslogic;get_file/3>`
	* :ref:`get_files_number/3 <fslogic;get_files_number/3>`
	* :ref:`get_full_file_name/1 <fslogic;get_full_file_name/1>`
	* :ref:`get_user_id/0 <fslogic;get_user_id/0>`
	* :ref:`get_user_root/1 <fslogic;get_user_root/1>`
	* :ref:`handle/2 <fslogic;handle/2>`
	* :ref:`handle_fuse_message/3 <fslogic;handle_fuse_message/3>`
	* :ref:`init/1 <fslogic;init/1>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: fslogic

	.. _`fslogic;cleanup/0`:

	.. erl:function:: cleanup() -> ok

	:ref:`worker_plugin_behaviour` callback cleanup/0

	.. _`fslogic;create_dirs/4`:

	.. erl:function:: create_dirs(Count :: integer(), CountingBase :: integer(), SHInfo :: term(), TmpAns :: string()) -> string()

	Creates dir at storage for files (if needed). Returns the path that contains created dirs.

	.. _`fslogic;get_file/3`:

	.. erl:function:: get_file(ProtocolVersion :: term(), File :: string(), FuseID :: string()) -> Result

	* **Result:** term()

	Gets file info from DB

	.. _`fslogic;get_files_number/3`:

	.. erl:function:: get_files_number(user | group, UUID :: uuid() | string(), ProtocolVersion :: integer()) -> Result

	* **Result:** {ok, Sum} | {error, any()}
	* **Sum:** integer()

	Returns number of user's or group's files

	.. _`fslogic;get_full_file_name/1`:

	.. erl:function:: get_full_file_name(FileName :: string()) -> Result

	* **ErrorDesc:** atom
	* **FullFileName:** string()
	* **Result:** {ok, FullFileName} | {error, ErrorDesc}

	Gets file's full name (user's root is added to name, but only when asking about non-group dir).

	.. _`fslogic;get_user_id/0`:

	.. erl:function:: get_user_id() -> Result

	* **ErrorDesc:** atom
	* **Result:** {ok, UserID} | {error, ErrorDesc}
	* **UserID:** term()

	Gets user's id.

	.. _`fslogic;get_user_root/1`:

	.. erl:function:: get_user_root(UserDoc :: term()) -> Result

	* **ErrorDesc:** atom
	* **Result:** {ok, RootDir} | {error, ErrorDesc}
	* **RootDir:** string()

	Gets user's root directory.

	.. _`fslogic;handle/2`:

	.. erl:function:: handle(ProtocolVersion :: term(), Request :: term()) -> Result

	* **Result:** term()

	:ref:`worker_plugin_behaviour` callback handle/1. Processes standard worker requests (e.g. ping) and requests from FUSE.

	.. _`fslogic;handle_fuse_message/3`:

	.. erl:function:: handle_fuse_message(ProtocolVersion :: term(), Record :: tuple(), FuseID :: string()) -> Result

	* **Result:** term()

	Processes requests from FUSE.

	.. _`fslogic;init/1`:

	.. erl:function:: init(Args :: term()) -> list()

	:ref:`worker_plugin_behaviour` callback init/1

