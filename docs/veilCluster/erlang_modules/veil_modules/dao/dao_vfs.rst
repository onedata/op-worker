.. _dao_vfs:

dao_vfs
=======

	:Authors: Rafal Slota
	:Copyright: This software is released under the :ref:`license`.
	:Descritpion: This module gives high level DB API which contain veil file system specific methods. All DAO API functions should not be called directly. Call dao:handle(_, {vfs, MethodName, ListOfArgs) instead. See dao:handle/2 for more details.

Function Index
~~~~~~~~~~~~~~~

	* :ref:`count_subdirs/1 <dao_vfs;count_subdirs/1>`
	* :ref:`find_files/1 <dao_vfs;find_files/1>`
	* :ref:`get_descriptor/1 <dao_vfs;get_descriptor/1>`
	* :ref:`get_file/1 <dao_vfs;get_file/1>`
	* :ref:`get_file_meta/1 <dao_vfs;get_file_meta/1>`
	* :ref:`get_path_info/1 <dao_vfs;get_path_info/1>`
	* :ref:`get_storage/1 <dao_vfs;get_storage/1>`
	* :ref:`list_descriptors/3 <dao_vfs;list_descriptors/3>`
	* :ref:`list_dir/3 <dao_vfs;list_dir/3>`
	* :ref:`list_storage/0 <dao_vfs;list_storage/0>`
	* :ref:`lock_file/3 <dao_vfs;lock_file/3>`
	* :ref:`remove_descriptor/1 <dao_vfs;remove_descriptor/1>`
	* :ref:`remove_file/1 <dao_vfs;remove_file/1>`
	* :ref:`remove_file_meta/1 <dao_vfs;remove_file_meta/1>`
	* :ref:`remove_storage/1 <dao_vfs;remove_storage/1>`
	* :ref:`rename_file/2 <dao_vfs;rename_file/2>`
	* :ref:`save_descriptor/1 <dao_vfs;save_descriptor/1>`
	* :ref:`save_file/1 <dao_vfs;save_file/1>`
	* :ref:`save_file_meta/1 <dao_vfs;save_file_meta/1>`
	* :ref:`save_new_file/2 <dao_vfs;save_new_file/2>`
	* :ref:`save_storage/1 <dao_vfs;save_storage/1>`
	* :ref:`unlock_file/3 <dao_vfs;unlock_file/3>`

Function Details
~~~~~~~~~~~~~~~~~

	.. erl:module:: dao_vfs

	.. _`dao_vfs;count_subdirs/1`:

	.. _`dao_vfs;find_files/1`:

	.. erl:function:: find_files(FileCriteria :: file_criteria()) -> {ok, [file_doc()]} | no_return()

	Returns list of uuids of files that matches to criteria passed as FileCriteria Current implementation does not support specifying ctime and mtime at the same time, other combinations of criterias are supported.

	.. _`dao_vfs;get_descriptor/1`:

	.. erl:function:: get_descriptor(Fd :: fd()) -> {ok, fd_doc()} | {error, any()} | no_return()

	Gets file descriptor from DB. Argument should be uuid() of #file_descriptor record Non-error return value is always {ok, #veil_document{record = #file_descriptor}. See :ref:`dao:save_record/1 <dao;save_record/1>` and :ref:`dao:get_record/1 <dao;get_record/1>` for more details about #veil_document{} wrapper. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;get_file/1`:

	.. erl:function:: get_file(File :: file()) -> {ok, file_doc()} | {error, any()} | no_return()

	Gets file from DB. Argument should be file() - see dao_types.hrl for more details Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;get_file_meta/1`:

	.. erl:function:: get_file_meta(Fd :: fd()) -> {ok, fd_doc()} | {error, any()} | no_return()

	Gets file meta from DB. Argument should be uuid() of #file_meta record Non-error return value is always {ok, #veil_document{record = #file_meta}. See :ref:`dao:save_record/1 <dao;save_record/1>` and :ref:`dao:get_record/1 <dao;get_record/1>` for more details about #veil_document{} wrapper. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;get_path_info/1`:

	.. erl:function:: get_path_info(File :: file_path()) -> {ok, [file_doc()]} | {error, any()} | no_return()

	Gets all files existing in given path from DB. Argument should be file_path() - see dao_types.hrl for more details Similar to get_file/1 but returns list containing file_doc() for every file within given path(), not only the last one Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;get_storage/1`:

	.. erl:function:: get_storage({uuid, DocUUID :: uuid()} | {id, StorageID :: integer()}) -> {ok, storage_doc()} | {error, any()} | no_return()

	Gets storage info from DB. Argument should be uuid() of storage document or ID of storage. Non-error return value is always {ok, #veil_document{record = #storage_info{}}. See :ref:`dao:save_record/1 <dao;save_record/1>` and :ref:`dao:get_record/1 <dao;get_record/1>` for more details about #veil_document{} wrapper. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;list_descriptors/3`:

	.. erl:function:: list_descriptors(MatchCriteria :: fd_select(), N :: pos_integer(), Offset :: non_neg_integer()) -> {ok, fd_doc()} | {error, any()} | no_return()

	Lists file descriptor from DB. First argument is a two-element tuple containing type of resource used to filter descriptors and resource itself Currently only {by_file, File :: file()} is supported. Second argument limits number of rows returned. 3rd argument sets offset of query (skips first Offset rows) Non-error return value is always {ok, [#veil_document{record = #file_descriptor]}. See :ref:`dao:save_record/1 <dao;save_record/1>` and :ref:`dao:get_record/1 <dao;get_record/1>` for more details about #veil_document{} wrapper. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;list_dir/3`:

	.. erl:function:: list_dir(Dir :: file(), N :: pos_integer(), Offset :: non_neg_integer()) -> {ok, [file_doc()]}

	Lists N files from specified directory starting from Offset. Non-error return value is always list of #veil_document{record = #file{}} records. Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).

	.. _`dao_vfs;list_storage/0`:

	.. erl:function:: list_storage() -> {ok, [storage_doc()]} | no_return()

	Lists all storage docs. Non-error return value is always list of #veil_document{record = #storage_info{}} records. Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).

	.. _`dao_vfs;lock_file/3`:

	.. erl:function:: lock_file(UserID :: string(), FileID :: string(), Mode :: write | read) -> not_yet_implemented

	Puts a read/write lock on specified file owned by specified user. Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details). Not yet implemented. This is placeholder/template method only!

	.. _`dao_vfs;remove_descriptor/1`:

	.. erl:function:: remove_descriptor(Fd :: fd() | fd_select()) -> ok | {error, any()} | no_return()

	Removes file descriptor from DB. Argument should be uuid() of #file_descriptor or same as in :ref:`dao_vfs:list_descriptors/3 <dao_vfs;list_descriptors/3>` . Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;remove_file/1`:

	.. erl:function:: remove_file(File :: file()) -> ok | {error, any()} | no_return()

	Removes file from DB. Argument should be file() - see dao_types.hrl for more details Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;remove_file_meta/1`:

	.. erl:function:: remove_file_meta(FMeta :: uuid()) -> ok | {error, any()} | no_return()

	Removes file_meta from DB. Argument should be uuid() of veil_document - see dao_types.hrl for more details Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;remove_storage/1`:

	.. erl:function:: remove_storage({uuid, DocUUID :: uuid()} | {id, StorageID :: integer()}) -> ok | {error, any()} | no_return()

	Removes storage info from DB. Argument should be uuid() of storage document or ID of storage Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;rename_file/2`:

	.. erl:function:: rename_file(File :: file(), NewName :: string()) -> {ok, NewUUID :: uuid()} | no_return()

	Renames specified file to NewName. Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).

	.. _`dao_vfs;save_descriptor/1`:

	.. erl:function:: save_descriptor(Fd :: fd_info() | fd_doc()) -> {ok, uuid()} | {error, any()} | no_return()

	Saves file descriptor to DB. Argument should be either #file_descriptor{} record (if you want to save it as new document) or #veil_document{} that wraps #file_descriptor{} if you want to update descriptor in DB. See :ref:`dao:save_record/1 <dao;save_record/1>` and :ref:`dao:get_record/1 <dao;get_record/1>` for more details about #veil_document{} wrapper. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;save_file/1`:

	.. erl:function:: save_file(File :: file_info() | file_doc()) -> {ok, uuid()} | {error, any()} | no_return()

	Saves file to DB. Argument should be either #file{} record (if you want to save it as new document) or #veil_document{} that wraps #file{} if you want to update file in DB. See :ref:`dao:save_record/1 <dao;save_record/1>` and :ref:`dao:get_record/1 <dao;get_record/1>` for more details about #veil_document{} wrapper. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;save_file_meta/1`:

	.. erl:function:: save_file_meta(FMeta :: #file_meta{} | #veil_document{}) -> {ok, uuid()} | {error, any()} | no_return()

	Saves file_meta to DB. Argument should be either #file_meta{} record (if you want to save it as new document) or #veil_document{} that wraps #file_meta{} if you want to update file meta in DB. See :ref:`dao:save_record/1 <dao;save_record/1>` and :ref:`dao:get_record/1 <dao;get_record/1>` for more details about #veil_document{} wrapper. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;save_new_file/2`:

	.. erl:function:: save_new_file(FilePath :: string(), File :: file_info()) -> {ok, uuid()} | {error, any()} | no_return()

	Saves new file to DB See :ref:`dao:save_record/1 <dao;save_record/1>` and :ref:`dao:get_record/1 <dao;get_record/1>` for more details about #veil_document{} wrapper. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;save_storage/1`:

	.. erl:function:: save_storage(Storage :: #storage_info{} | #veil_document{}) -> {ok, uuid()} | {error, any()} | no_return()

	Saves storage info to DB. Argument should be either #storage_info{} record (if you want to save it as new document) or #veil_document{} that wraps #storage_info{} if you want to update storage info in DB. See :ref:`dao:save_record/1 <dao;save_record/1>` and :ref:`dao:get_record/1 <dao;get_record/1>` for more details about #veil_document{} wrapper. Should not be used directly, use :ref:`dao:handle/2 <dao;handle/2>` instead (See :ref:`dao:handle/2 <dao;handle/2>` for more details).

	.. _`dao_vfs;unlock_file/3`:

	.. erl:function:: unlock_file(UserID :: string(), FileID :: string(), Mode :: write | read) -> not_yet_implemented

	Takes off a read/write lock on specified file owned by specified user. Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details). Not yet implemented. This is placeholder/template method only!

