.. _storage_files_manager:

storage_files_manager
=====================

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module provides high level file system operations that operates directly on storage.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`chmod/3 <storage_files_manager;chmod/3>`
	* :ref:`chown/4 <storage_files_manager;chown/4>`
	* :ref:`create/2 <storage_files_manager;create/2>`
	* :ref:`delete/2 <storage_files_manager;delete/2>`
	* :ref:`delete_dir/2 <storage_files_manager;delete_dir/2>`
	* :ref:`get_cached_value/3 <storage_files_manager;get_cached_value/3>`
	* :ref:`ls/0 <storage_files_manager;ls/0>`
	* :ref:`mkdir/2 <storage_files_manager;mkdir/2>`
	* :ref:`mv/3 <storage_files_manager;mv/3>`
	* :ref:`read/4 <storage_files_manager;read/4>`
	* :ref:`truncate/3 <storage_files_manager;truncate/3>`
	* :ref:`write/3 <storage_files_manager;write/3>`
	* :ref:`write/4 <storage_files_manager;write/4>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: storage_files_manager

	.. _`storage_files_manager;chmod/3`:

	.. erl:function:: chmod(Storage_helper_info :: record(), Dir :: string(), Mode :: integer()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Change file mode at storage

	.. _`storage_files_manager;chown/4`:

	.. erl:function:: chown(Storage_helper_info :: record(), Dir :: string(), User :: string(), Group :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Change file's owner (if user or group shouldn't be changed use "" as an argument)

	.. _`storage_files_manager;create/2`:

	.. erl:function:: create(Storage_helper_info :: record(), File :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Creates file (operates only on storage). First it checks if file exists. If not, it creates file.

	.. _`storage_files_manager;delete/2`:

	.. erl:function:: delete(Storage_helper_info :: record(), File :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Deletes file (operates only on storage). First it checks if file exists and is regular file. If everything is ok, it deletes file.

	.. _`storage_files_manager;delete_dir/2`:

	.. erl:function:: delete_dir(Storage_helper_info :: record(), Dir :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Deletes dir on storage

	.. _`storage_files_manager;get_cached_value/3`:

	.. erl:function:: get_cached_value(File :: string(), ValueName :: atom(), Storage_helper_info :: record()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** {ok, Value} | {ErrorGeneral, ErrorDetail}
	* **Value:** term()

	Checks value using storage helper or gets its from cache

	.. _`storage_files_manager;ls/0`:

	.. erl:function:: ls() -> {error, not_implemented_yet}

	Lists files in directory on storage

	.. _`storage_files_manager;mkdir/2`:

	.. erl:function:: mkdir(Storage_helper_info :: record(), Dir :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Creates dir on storage

	.. _`storage_files_manager;mv/3`:

	.. erl:function:: mv(Storage_helper_info :: record(), From :: string(), To :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Moves file on storage

	.. _`storage_files_manager;read/4`:

	.. erl:function:: read(Storage_helper_info :: record(), File :: string(), Offset :: integer(), Size :: integer()) -> Result

	* **Bytes:** binary()
	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** {ok, Bytes} | {ErrorGeneral, ErrorDetail}

	Reads file (operates only on storage). First it checks file attributes (file type and file size). If everything is ok, it reads data from file.

	.. _`storage_files_manager;truncate/3`:

	.. _`storage_files_manager;write/3`:

	.. erl:function:: write(Storage_helper_info :: record(), File :: string(), Buf :: binary()) -> Result

	* **BytesWritten:** integer()
	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** BytesWritten | {ErrorGeneral, ErrorDetail}

	Appends data to the end of file (operates only on storage). First it checks file attributes (file type and file size). If everything is ok, it reads data from file.

	.. _`storage_files_manager;write/4`:

	.. erl:function:: write(Storage_helper_info :: record(), File :: string(), Offset :: integer(), Buf :: binary()) -> Result

	* **BytesWritten:** integer()
	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** BytesWritten | {ErrorGeneral, ErrorDetail}

	Writes data to file (operates only on storage). First it checks file attributes (file type and file size). If everything is ok, it reads data from file.

