.. _logical_files_manager:

logical_files_manager
=====================

	:Authors: Michal Wrzeszcz
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module provides high level file system operations that use logical names of files.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`cache_size/2 <logical_files_manager;cache_size/2>`
	* :ref:`change_file_perm/2 <logical_files_manager;change_file_perm/2>`
	* :ref:`chown/0 <logical_files_manager;chown/0>`
	* :ref:`create/1 <logical_files_manager;create/1>`
	* :ref:`create_share/2 <logical_files_manager;create_share/2>`
	* :ref:`create_standard_share/1 <logical_files_manager;create_standard_share/1>`
	* :ref:`delete/1 <logical_files_manager;delete/1>`
	* :ref:`doUploadTest/4 <logical_files_manager;doUploadTest/4>`
	* :ref:`error_to_string/1 <logical_files_manager;error_to_string/1>`
	* :ref:`exists/1 <logical_files_manager;exists/1>`
	* :ref:`get_ets_name/0 <logical_files_manager;get_ets_name/0>`
	* :ref:`get_file_by_uuid/1 <logical_files_manager;get_file_by_uuid/1>`
	* :ref:`get_file_full_name_by_uuid/1 <logical_files_manager;get_file_full_name_by_uuid/1>`
	* :ref:`get_file_name_by_uuid/1 <logical_files_manager;get_file_name_by_uuid/1>`
	* :ref:`get_file_user_dependent_name_by_uuid/1 <logical_files_manager;get_file_user_dependent_name_by_uuid/1>`
	* :ref:`get_share/1 <logical_files_manager;get_share/1>`
	* :ref:`getfileattr/1 <logical_files_manager;getfileattr/1>`
	* :ref:`getfilelocation/1 <logical_files_manager;getfilelocation/1>`
	* :ref:`ls/3 <logical_files_manager;ls/3>`
	* :ref:`mkdir/1 <logical_files_manager;mkdir/1>`
	* :ref:`mv/2 <logical_files_manager;mv/2>`
	* :ref:`read/3 <logical_files_manager;read/3>`
	* :ref:`remove_share/1 <logical_files_manager;remove_share/1>`
	* :ref:`rmdir/1 <logical_files_manager;rmdir/1>`
	* :ref:`truncate/2 <logical_files_manager;truncate/2>`
	* :ref:`write/2 <logical_files_manager;write/2>`
	* :ref:`write/3 <logical_files_manager;write/3>`
	* :ref:`write_from_stream/2 <logical_files_manager;write_from_stream/2>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: logical_files_manager

	.. _`logical_files_manager;cache_size/2`:

	.. erl:function:: cache_size(File :: string(), BuffSize :: integer()) -> Result

	* **Result:** integer()

	Gets and updates size of file.

	.. _`logical_files_manager;change_file_perm/2`:

	.. erl:function:: change_file_perm(FileName :: string(), NewPerms :: integer()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Changes file's permissions in db

	.. _`logical_files_manager;chown/0`:

	.. erl:function:: chown() -> {error, not_implemented_yet}

	Changes owner of file (in db)

	.. _`logical_files_manager;create/1`:

	.. erl:function:: create(File :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Creates file (uses logical name of file). First it creates file in db and gets information about storage helper and file id at helper. Next it uses storage helper to create file on storage.

	.. _`logical_files_manager;create_share/2`:

	.. erl:function:: create_share(File :: string(), Share_With :: term()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** {ok, Share_info} | {ErrorGeneral, ErrorDetail}
	* **Share_info:** term()

	Creates share info for file (file path is an argument).

	.. _`logical_files_manager;create_standard_share/1`:

	.. erl:function:: create_standard_share(File :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** {ok, Share_info} | {ErrorGeneral, ErrorDetail}
	* **Share_info:** term()

	Creates standard share info (share with all) for file (file path is an argument).

	.. _`logical_files_manager;delete/1`:

	.. erl:function:: delete(File :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Deletes file (uses logical name of file). First it gets information about storage helper and file id at helper. Next it uses storage helper to delete file from storage. Afterwards it deletes information about file from db.

	.. _`logical_files_manager;doUploadTest/4`:

	.. erl:function:: doUploadTest(File :: string(), WriteFunNum :: integer(), Size :: integer(), Times :: integer()) -> Result

	* **BytesWritten:** integer()
	* **Result:** {BytesWritten, WriteTime}
	* **WriteTime:** integer()

	Tests upload speed

	.. _`logical_files_manager;error_to_string/1`:

	.. erl:function:: error_to_string(Error :: term()) -> Result

	* **Result:** string()

	Translates error to text message.

	.. _`logical_files_manager;exists/1`:

	.. erl:function:: exists(File :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** boolean() | {ErrorGeneral, ErrorDetail}

	Checks if file exists.

	.. _`logical_files_manager;get_ets_name/0`:

	.. _`logical_files_manager;get_file_by_uuid/1`:

	.. erl:function:: get_file_by_uuid(UUID :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **File:** term()
	* **Result:** {ok, File} | {ErrorGeneral, ErrorDetail}

	Gets file record on the basis of uuid.

	.. _`logical_files_manager;get_file_full_name_by_uuid/1`:

	.. erl:function:: get_file_full_name_by_uuid(UUID :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **FullPath:** string()
	* **Result:** {ok, FullPath} | {ErrorGeneral, ErrorDetail}

	Gets file full name (with root of the user's system) on the basis of uuid.

	.. _`logical_files_manager;get_file_name_by_uuid/1`:

	.. erl:function:: get_file_name_by_uuid(UUID :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Name:** term()
	* **Result:** {ok, Name} | {ErrorGeneral, ErrorDetail}

	Gets file name on the basis of uuid.

	.. _`logical_files_manager;get_file_user_dependent_name_by_uuid/1`:

	.. erl:function:: get_file_user_dependent_name_by_uuid(UUID :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **FullPath:** string()
	* **Result:** {ok, FullPath} | {ErrorGeneral, ErrorDetail}

	Gets file full name relative to user's dir on the basis of uuid.

	.. _`logical_files_manager;get_share/1`:

	.. erl:function:: get_share(Key:: {file, File :: uuid()} | {user, User :: uuid()} | {uuid, UUID :: uuid()}) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** {ok, Share_doc} | {ErrorGeneral, ErrorDetail}
	* **Share_doc:** term()

	Gets info about share from db.

	.. _`logical_files_manager;getfileattr/1`:

	.. erl:function:: getfileattr(FileName :: string()) -> Result

	* **Attributes:** term()
	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** {ok, Attributes} | {ErrorGeneral, ErrorDetail}

	Returns file attributes

	.. _`logical_files_manager;getfilelocation/1`:

	.. erl:function:: getfilelocation(File :: term()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Helper:** term()
	* **Id:** term()
	* **Result:** {ok, {Helper, Id}} | {ErrorGeneral, ErrorDetail}

	Gets file location from fslogic or from cache. File can be string (path) or {uuid, UUID}.

	.. _`logical_files_manager;ls/3`:

	.. erl:function:: ls(DirName :: string(), ChildrenNum :: integer(), Offset :: integer()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **FilesList:** list()
	* **Result:** {ok, FilesList} | {ErrorGeneral, ErrorDetail}

	Lists directory (uses data from db)

	.. _`logical_files_manager;mkdir/1`:

	.. erl:function:: mkdir(DirName :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Creates directory (in db)

	.. _`logical_files_manager;mv/2`:

	.. erl:function:: mv(From :: string(), To :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Moves directory (in db)

	.. _`logical_files_manager;read/3`:

	.. erl:function:: read(File :: term(), Offset :: integer(), Size :: integer()) -> Result

	* **Bytes:** binary()
	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** {ok, Bytes} | {ErrorGeneral, ErrorDetail}

	Reads file (uses logical name of file). First it gets information about storage helper and file id at helper. Next it uses storage helper to read data from file. File can be string (path) or {uuid, UUID}.

	.. _`logical_files_manager;remove_share/1`:

	.. erl:function:: remove_share(Key:: {file, File :: uuid()} | {user, User :: uuid()} | {uuid, UUID :: uuid()}) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Removes info about share from db.

	.. _`logical_files_manager;rmdir/1`:

	.. erl:function:: rmdir(DirName :: string()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Deletes directory (in db)

	.. _`logical_files_manager;truncate/2`:

	.. erl:function:: truncate(File :: string(), Size :: integer()) -> Result

	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** ok | {ErrorGeneral, ErrorDetail}

	Truncates file (uses logical name of file). First it gets information about storage helper and file id at helper. Next it uses storage helper to truncate file on storage.

	.. _`logical_files_manager;write/2`:

	.. erl:function:: write(File :: string(), Buf :: binary()) -> Result

	* **BytesWritten:** integer()
	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** BytesWritten | {ErrorGeneral, ErrorDetail}

	Appends data to the end of file (uses logical name of file). First it gets information about storage helper and file id at helper. Next it uses storage helper to write data to file.

	.. _`logical_files_manager;write/3`:

	.. erl:function:: write(File :: string(), Offset :: integer(), Buf :: binary()) -> Result

	* **BytesWritten:** integer()
	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** BytesWritten | {ErrorGeneral, ErrorDetail}

	Writes data to file (uses logical name of file). First it gets information about storage helper and file id at helper. Next it uses storage helper to write data to file.

	.. _`logical_files_manager;write_from_stream/2`:

	.. erl:function:: write_from_stream(File :: string(), Buf :: binary()) -> Result

	* **BytesWritten:** integer()
	* **ErrorDetail:** term()
	* **ErrorGeneral:** atom()
	* **Result:** BytesWritten | {ErrorGeneral, ErrorDetail}

	Appends data to the end of file (uses logical name of file). First it gets information about storage helper and file id at helper. Next it uses storage helper to write data to file.

