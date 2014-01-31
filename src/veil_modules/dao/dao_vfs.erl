%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module gives high level DB API which contain veil file system specific methods.
%% All DAO API functions should not be called directly. Call dao:handle(_, {vfs, MethodName, ListOfArgs) instead.
%% See dao:handle/2 for more details.
%% @end
%% ===================================================================
-module(dao_vfs).

-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/dao_helper.hrl").
-include_lib("veil_modules/dao/dao_types.hrl").
-include_lib("files_common.hrl").
-include("logging.hrl").

%% API - File system management
-export([list_dir/3, count_subdirs/1, rename_file/2, lock_file/3, unlock_file/3, find_files/1]). %% High level API functions
-export([save_descriptor/1, remove_descriptor/1, get_descriptor/1, list_descriptors/3]). %% Base descriptor management API functions
-export([save_new_file/2, save_file/1, remove_file/1, get_file/1, get_path_info/1]). %% Base file management API function
-export([save_storage/1, remove_storage/1, get_storage/1, list_storage/0]). %% Base storage info management API function
-export([save_file_meta/1, remove_file_meta/1, get_file_meta/1]).


-ifdef(TEST).
-compile([export_all]).
-endif.

%% ===================================================================
%% API functions
%% ===================================================================

%% ===================================================================
%% File Descriptors Management
%% ===================================================================

%% save_descriptor/1
%% ====================================================================
%% @doc Saves file descriptor to DB. Argument should be either #file_descriptor{} record
%% (if you want to save it as new document) <br/>
%% or #veil_document{} that wraps #file_descriptor{} if you want to update descriptor in DB. <br/>
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec save_descriptor(Fd :: fd_info() | fd_doc()) -> {ok, uuid()} | {error, any()} | no_return().
%% ====================================================================
save_descriptor(#file_descriptor{} = Fd) ->
    save_descriptor(#veil_document{record = Fd});
save_descriptor(#veil_document{record = #file_descriptor{}} = FdDoc) ->
    dao:set_db(?DESCRIPTORS_DB_NAME),
    dao:save_record(FdDoc).


%% remove_descriptor/1
%% ====================================================================
%% @doc Removes file descriptor from DB. Argument should be uuid() of #file_descriptor or same as in {@link dao_vfs:list_descriptors/3} .
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec remove_descriptor(Fd :: fd() | fd_select()) -> ok | {error, any()} | no_return().
%% ====================================================================
remove_descriptor(ListSpec) when is_tuple(ListSpec) ->
    remove_descriptor3(ListSpec, 1000, 0);
remove_descriptor(Fd) when is_list(Fd) ->
    dao:set_db(?DESCRIPTORS_DB_NAME),
    dao:remove_record(Fd).

remove_descriptor3(ListSpec, BatchSize, Offset) ->
    case list_descriptors(ListSpec, BatchSize, Offset) of
        {ok, []} -> ok;
        {ok, Docs} ->
            [remove_descriptor(Fd) || #veil_document{uuid = Fd} <- Docs, is_list(Fd)],
            remove_descriptor3(ListSpec, BatchSize, Offset + BatchSize);
        Other -> Other
    end.


%% get_descriptor/1
%% ====================================================================
%% @doc Gets file descriptor from DB. Argument should be uuid() of #file_descriptor record
%% Non-error return value is always {ok, #veil_document{record = #file_descriptor}.
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec get_descriptor(Fd :: fd()) -> {ok, fd_doc()} | {error, any()} | no_return().
%% ====================================================================
get_descriptor(Fd) ->
    dao:set_db(?DESCRIPTORS_DB_NAME),
    case dao:get_record(Fd) of
        {ok, #veil_document{record = #file_descriptor{}} = Doc} ->
            {ok, Doc};
        {ok, #veil_document{}} ->
            {error, invalid_fd_record};
        Other ->
            Other
    end.


%% list_descriptors/3
%% ====================================================================
%% @doc Lists file descriptor from DB. <br/>
%% First argument is a two-element tuple containing type of resource used to filter descriptors and resource itself<br/>
%% Currently only {by_file, File :: file()} is supported. <br/>
%% Second argument limits number of rows returned. 3rd argument sets offset of query (skips first Offset rows) <br/>
%% Non-error return value is always  {ok, [#veil_document{record = #file_descriptor]}.
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec list_descriptors(MatchCriteria :: fd_select(), N :: pos_integer(), Offset :: non_neg_integer()) ->
    {ok, fd_doc()} | {error, any()} | no_return().
%% ====================================================================
list_descriptors({by_file, File}, N, Offset) when N > 0, Offset >= 0 ->
    list_descriptors({by_file_n_owner, {File, ""}}, N, Offset);
list_descriptors({by_file_n_owner, {File, Owner}}, N, Offset) when N > 0, Offset >= 0 ->
    {ok, #veil_document{uuid = FileId}} = get_file(File),
    list_descriptors({by_uuid_n_owner, {FileId, Owner}}, N, Offset);
list_descriptors({by_uuid_n_owner, {FileId, Owner}}, N, Offset) when N > 0, Offset >= 0 ->
    StartKey = [dao_helper:name(FileId), dao_helper:name(Owner)],
    EndKey = case Owner of "" -> [dao_helper:name(uca_increment(FileId)), dao_helper:name("")]; _ -> [dao_helper:name((FileId)), dao_helper:name(uca_increment(Owner))] end,
    QueryArgs = #view_query_args{start_key = StartKey, end_key = EndKey, include_docs = true, limit = N, skip = Offset},
    case dao:list_records(?FD_BY_FILE_VIEW, QueryArgs) of
        {ok, #view_result{rows = Rows}} ->
            {ok, [FdDoc || #view_row{doc = #veil_document{record = #file_descriptor{file = FileId1, fuse_id = OwnerId}} = FdDoc} <- Rows,
                FileId1 == FileId, OwnerId == Owner orelse Owner == ""]};
        Data ->
            lager:error("Invalid file descriptor view response: ~p", [Data]),
            throw({inavlid_data, Data})
    end;
list_descriptors({by_expired_before, Time}, N, Offset) when N > 0, Offset >= 0 ->
    StartKey = 0,
    EndKey = Time,
    QueryArgs = #view_query_args{start_key = StartKey, end_key = EndKey, include_docs = true, limit = N, skip = Offset},
    case dao:list_records(?FD_BY_EXPIRED_BEFORE_VIEW, QueryArgs) of
        {ok, #view_result{rows = Rows}} ->
            {ok, [FdDoc || #view_row{doc = #veil_document{record = #file_descriptor{}} = FdDoc} <- Rows]};
        Data ->
            lager:error("Invalid file descriptor view response: ~p", [Data]),
            throw({inavlid_data, Data})
    end;
list_descriptors({_Type, _Resource}, _N, _Offset) when _N > 0, _Offset >= 0 ->
    not_yet_implemented.


%% ===================================================================
%% Files Meta Management
%% ===================================================================

%% save_file_meta/1
%% ====================================================================
%% @doc Saves file_meta to DB. Argument should be either #file_meta{} record
%% (if you want to save it as new document) <br/>
%% or #veil_document{} that wraps #file_meta{} if you want to update file meta in DB. <br/>
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec save_file_meta(FMeta :: #file_meta{} | #veil_document{}) -> {ok, uuid()} | {error, any()} | no_return().
%% ====================================================================
save_file_meta(#file_meta{} = FMeta) ->
    save_file_meta(#veil_document{record = FMeta});
save_file_meta(#veil_document{record = #file_meta{}} = FMetaDoc) ->
    dao:set_db(?FILES_DB_NAME),
    dao:save_record(FMetaDoc).

%% remove_file_meta/1
%% ====================================================================
%% @doc Removes file_meta from DB. Argument should be uuid() of veil_document - see dao_types.hrl for more details <br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec remove_file_meta(FMeta :: uuid()) -> ok | {error, any()} | no_return().
%% ====================================================================
remove_file_meta(FMeta) ->
    dao:set_db(?FILES_DB_NAME),
    dao:remove_record(FMeta).


%% get_file_meta/1
%% ====================================================================
%% @doc Gets file meta from DB. Argument should be uuid() of #file_meta record
%% Non-error return value is always {ok, #veil_document{record = #file_meta}.
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec get_file_meta(Fd :: fd()) -> {ok, fd_doc()} | {error, any()} | no_return().
%% ====================================================================
get_file_meta(FMetaUUID) ->
    dao:set_db(?FILES_DB_NAME),
    {ok, #veil_document{record = #file_meta{}}} = dao:get_record(FMetaUUID).  


%% ===================================================================
%% Files Management
%% ===================================================================

%% save_new_file/2
%% ====================================================================
%% @doc Saves new file to DB
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec save_new_file(FilePath :: string(), File :: file_info()) -> {ok, uuid()} | {error, any()} | no_return().
%% ====================================================================
save_new_file(FilePath, #file{} = File) ->
  try
    AnalyzedPath = file_path_analyze(FilePath),

    {FindStatus, FindTmpAns} = try
      get_file(AnalyzedPath)
    catch
      _:Error ->
        {error, Error}
    end,

    case {FindStatus, FindTmpAns} of
      {error, file_not_found} ->
        SaveAns = save_file(File),
        case SaveAns of
          {ok, UUID} ->
            try
              get_file(AnalyzedPath, true),
              {ok, UUID}
            catch
              _:file_duplicated ->
                dao:remove_record(UUID),
                {error, file_exists};
              _:Error2 ->
                {error, Error2}
            end;
          _ -> SaveAns
        end;
      {ok, _} ->
        {error, file_exists};
      _ ->
        {FindStatus, FindTmpAns}
    end
  catch
    _:Error3 ->
      {error, Error3}
  end.


%% save_file/1
%% ====================================================================
%% @doc Saves file to DB. Argument should be either #file{} record
%% (if you want to save it as new document) <br/>
%% or #veil_document{} that wraps #file{} if you want to update file in DB. <br/>
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec save_file(File :: file_info() | file_doc()) -> {ok, uuid()} | {error, any()} | no_return().
%% ====================================================================
save_file(#file{} = File) ->
    save_file(#veil_document{record = File});
save_file(#veil_document{record = #file{}} = FileDoc) ->
    dao:set_db(?FILES_DB_NAME),
    dao:save_record(FileDoc).

%% remove_file/1
%% ====================================================================
%% @doc Removes file from DB. Argument should be file() - see dao_types.hrl for more details <br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec remove_file(File :: file()) -> ok | {error, any()} | no_return().
%% ====================================================================
remove_file(File) ->
    dao:set_db(?FILES_DB_NAME),
    {ok, FData} = get_file(File),

    %% Remove file meta
    case FData of 
        #veil_document{record = #file{meta_doc = FMeta}} when is_list(FMeta) ->
            case remove_file_meta(FMeta) of 
                ok -> ok;
                {error, Reason} ->
                    ?warning("Cannot remove file_meta ~p due to error: ~p", [FMeta, Reason])
            end;
        _ -> ok
    end,

    %% Remove file shares
    try dao_share:remove_file_share({file, FData#veil_document.uuid}) of
        ok -> ok;
        {error, Reason2} ->
            ?warning("Cannot clear shares for file ~p due to error: ~p", [File, Reason2])
    catch
        throw:share_not_found -> ok;
        ErrType:Reason3 ->
            ?warning("Cannot clear shares for file ~p due to ~p: ~p", [File, ErrType, Reason3])
    end,

    %% Remove descriptors
    case remove_descriptor({by_file, {uuid, FData#veil_document.uuid}}) of
        ok -> ok;
        {error, Reason1} ->
            ?warning("Cannot remove file_descriptors ~p due to error: ~p", [{by_file, {uuid, FData#veil_document.uuid}}, Reason1])
    end,

    dao:remove_record(FData#veil_document.uuid).

%% get_file/1
%% ====================================================================
%% @doc Gets file from DB. Argument should be file() - see dao_types.hrl for more details <br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec get_file(File :: file()) -> {ok, file_doc()} | {error, any()} | no_return(). %% Throws file_not_found and invalid_data
%% ====================================================================
get_file({internal_path, [], []}) -> %% Root dir query
    {ok, #veil_document{uuid = "", record = #file{type = ?DIR_TYPE, perms = ?RD_ALL_PERM bor ?WR_ALL_PERM bor ?EX_ALL_PERM}}};
get_file({internal_path, [Dir | Path], Root}) ->
  get_file({internal_path, [Dir | Path], Root}, false);
get_file({uuid, UUID}) ->
    dao:set_db(?FILES_DB_NAME),
    case dao:get_record(UUID) of
        {ok, #veil_document{record = #file{}} = Doc} ->
            {ok, Doc};
        {ok, #veil_document{}} ->
            {error, invalid_file_record};
        Other ->
            Other
    end;
get_file(Path) ->
    get_file(file_path_analyze(Path)).

%% get_file/2
%% ====================================================================
%% @doc Gets file from DB. Argument should be file() - see dao_types.hrl for more details <br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec get_file(File :: file(), MultiError :: boolean()) -> {ok, file_doc()} | {error, any()} | no_return(). %% Throws file_not_found and invalid_data
%% ====================================================================
get_file({internal_path, [Dir | Path], Root}, MultiError) ->
  QueryArgs =
    #view_query_args{keys = [[dao_helper:name(Root), dao_helper:name(Dir)]],
    include_docs = case Path of [] -> true; _ -> false end}, %% Include doc representing leaf of our file path
  {NewRoot, FileDoc} =
    case dao:list_records(?FILE_TREE_VIEW, QueryArgs) of
      {ok, #view_result{rows = [#view_row{id = Id, doc = FDoc}]}} ->
        {Id, FDoc};
      {ok, #view_result{rows = []}} ->
        lager:error("File ~p not found (root = ~p)", [Dir, Root]),
        throw(file_not_found);
      {ok, #view_result{rows = [#view_row{id = Id, doc = FDoc} | _Tail]}} ->
        case MultiError of
          true -> throw(file_duplicated);
          false ->
            lager:warning("File ~p (root = ~p) is duplicated. Returning first copy. Others: ~p", [Dir, Root, _Tail]),
            {Id, FDoc}
        end;
      _Other ->
        lager:error("Invalid view response: ~p", [_Other]),
        throw(invalid_data)
    end,
  case Path of
    [] -> {ok, FileDoc};
    _ -> get_file({internal_path, Path, NewRoot})
  end.

%% get_path_info/1
%% ====================================================================
%% @doc Gets all files existing in given path from DB. Argument should be file_path() - see dao_types.hrl for more details <br/>
%% Similar to get_file/1 but returns list containing file_doc() for every file within given path(), not only the last one<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec get_path_info(File :: file_path()) -> {ok, [file_doc()]} | {error, any()} | no_return(). %% Throws file_not_found and invalid_data
%% ====================================================================
get_path_info({internal_path, Path, Root}) ->
    {FullPath, _} =
        lists:foldl(fun(Elem, {AccIn, AccRoot}) ->
            {ok, FileInfo = #veil_document{uuid = NewRoot}} = get_file({relative_path, [Elem], AccRoot}),
            {[FileInfo | AccIn], NewRoot}
        end, {[], Root}, Path),
    {ok, lists:reverse(FullPath)};
get_path_info(File) ->
    get_path_info(file_path_analyze(File)).


%% rename_file/2
%% ====================================================================
%% @doc Renames specified file to NewName.
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% @end
-spec rename_file(File :: file(), NewName :: string()) -> {ok, NewUUID :: uuid()} | no_return().
%% ====================================================================
rename_file(File, NewName) ->
    {ok, #veil_document{record = FileInfo} = FileDoc} = get_file(File),
    {ok, _} = save_file(FileDoc#veil_document{record = FileInfo#file{name = NewName}}).


%% list_dir/3
%% ====================================================================
%% @doc Lists N files from specified directory starting from Offset. <br/>
%% Non-error return value is always list of #veil_document{record = #file{}} records.<br/>
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% @end
-spec list_dir(Dir :: file(), N :: pos_integer(), Offset :: non_neg_integer()) -> {ok, [file_doc()]}.
%% ====================================================================
list_dir(Dir, N, Offset) ->
    Id =
        case get_file(Dir) of
            {ok, #veil_document{record = #file{type = ?DIR_TYPE}, uuid = UUID}} ->
                UUID;
            R ->
                lager:error("Directory ~p not found. Error: ~p", [Dir, R]),
                throw({dir_not_found, R})
        end,
    NextId =  uca_increment(Id), %% Dirty hack needed because `inclusive_end` option does not work in BigCouch for some reason
    QueryArgs =
        #view_query_args{start_key = [dao_helper:name(Id), dao_helper:name("")], end_key = [dao_helper:name(NextId), dao_helper:name("")],
            limit = N, include_docs = true, skip = Offset, inclusive_end = false}, %% Inclusive end does not work, disable to be sure
    case dao:list_records(?FILE_TREE_VIEW, QueryArgs) of
        {ok, #view_result{rows = Rows}} -> %% We need to strip results that don't match search criteria (tail of last query possibly), because
                                           %% `end_key` seems to behave strange combined with `limit` option. TODO: get rid of it after DBMS switch
            {ok, [FileDoc || #view_row{doc = #veil_document{record = #file{parent = Parent} } = FileDoc } <- Rows, Parent == Id]};
        _Other ->
            lager:error("Invalid view response: ~p", [_Other]),
            throw(inavlid_data)
    end.

%% count_subdirs/1
%% ====================================================================
%% @doc Returns number of first level subdirectories for specified directory.
%% @end
-spec count_subdirs({uuid, UUID :: uuid()}) -> {ok, non_neg_integer()}.
%% ====================================================================
count_subdirs({uuid, Id}) ->
    NextId = uca_increment(Id),
    QueryArgs = #view_query_args{
        start_key = [dao_helper:name(Id), dao_helper:name("")],
        end_key = [dao_helper:name(NextId), dao_helper:name("")],
        view_type = reduce,
        inclusive_end = false
    },
    case dao:list_records(?FILE_SUBDIRS_VIEW, QueryArgs) of
        {ok, #view_result{rows = [#view_row{value = Sum}]}} -> {ok, Sum};
        {ok, #view_result{rows = []}} -> {ok, 0};
        _Other ->
            lager:error("Invalid view response: ~p", [_Other]),
            throw(invalid_data)
    end.

%% lock_file/3
%% ====================================================================
%% @doc Puts a read/write lock on specified file owned by specified user.
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% Not yet implemented. This is placeholder/template method only!
%% @end
-spec lock_file(UserID :: string(), FileID :: string(), Mode :: write | read) -> not_yet_implemented.
%% ====================================================================
lock_file(_UserID, _FileID, _Mode) ->
    not_yet_implemented.


%% unlock_file/3
%% ====================================================================
%% @doc Takes off a read/write lock on specified file owned by specified user.
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% Not yet implemented. This is placeholder/template method only!
%% @end
-spec unlock_file(UserID :: string(), FileID :: string(), Mode :: write | read) -> not_yet_implemented.
%% ====================================================================
unlock_file(_UserID, _FileID, _Mode) ->
    not_yet_implemented.


%% ===================================================================
%% Storage management functions
%% ===================================================================


%% save_storage/1
%% ====================================================================
%% @doc Saves storage info to DB. Argument should be either #storage_info{} record
%% (if you want to save it as new document) <br/>
%% or #veil_document{} that wraps #storage_info{} if you want to update storage info in DB. <br/>
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec save_storage(Storage :: #storage_info{} | #veil_document{}) -> {ok, uuid()} | {error, any()} | no_return().
%% ====================================================================
save_storage(#storage_info{} = Storage) ->
    save_storage(#veil_document{record = Storage}, false);
save_storage(StorageDoc) ->
  save_storage(StorageDoc, true).

%% save_storage/2
%% ====================================================================
%% @doc Saves storage info to DB. Argument should be either #storage_info{} record
%% (if you want to save it as new document) <br/>
%% or #veil_document{} that wraps #storage_info{} if you want to update storage info in DB. <br/>
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec save_storage(Storage :: #storage_info{} | #veil_document{}, ClearCache :: boolean()) -> {ok, uuid()} | {error, any()} | no_return().
%% ====================================================================
save_storage(#veil_document{record = #storage_info{}} = StorageDoc, ClearCache) ->
    case ClearCache of
      true ->
        Doc = StorageDoc#veil_document.record,
        clear_cache([{uuid, StorageDoc#veil_document.uuid}, {id, Doc#storage_info.id}]);
      false ->
        ok
    end,
    dao:set_db(?SYSTEM_DB_NAME),
    dao:save_record(StorageDoc).


%% remove_storage/1
%% ====================================================================
%% @doc Removes storage info from DB. Argument should be uuid() of storage document or ID of storage <br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec remove_storage({uuid, DocUUID :: uuid()} | {id, StorageID :: integer()}) -> ok | {error, any()} | no_return().
%% ====================================================================
remove_storage({uuid, DocUUID}) when is_list(DocUUID) ->
    {Ans, SData} = get_storage({uuid, DocUUID}),
    case Ans of
      ok ->
        Doc = SData#veil_document.record,
        clear_cache([{uuid, DocUUID}, {id, Doc#storage_info.id}]),

        dao:set_db(?SYSTEM_DB_NAME),
        dao:remove_record(DocUUID);
      _ -> {Ans, SData}
    end;
remove_storage({id, StorageID}) when is_integer(StorageID) ->
    {Ans, SData} = get_storage({id, StorageID}),
    case Ans of
      ok ->
        clear_cache([{uuid, SData#veil_document.uuid}, {id, StorageID}]),
        dao:set_db(?SYSTEM_DB_NAME),
        dao:remove_record(SData#veil_document.uuid);
      _ -> {Ans, SData}
    end.

%% get_storage/1
%% ====================================================================
%% @doc Gets storage info from DB. Argument should be uuid() of storage document or ID of storage. <br/>
%% Non-error return value is always {ok, #veil_document{record = #storage_info{}}.
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec get_storage({uuid, DocUUID :: uuid()} | {id, StorageID :: integer()}) -> {ok, storage_doc()} | {error, any()} | no_return().
%% ====================================================================
get_storage(Key) ->
  case ets:lookup(storage_cache, Key) of
    [] -> %% Cached document not found. Fetch it from DB and save in cache
      DBAns = get_storage_from_db(Key),
      case DBAns of
        {ok, Doc} ->
          ets:insert(storage_cache, {Key, Doc}),
          {ok, Doc};
        Other -> Other
      end;
    [{_, Ans}] -> %% Return document from cache
      {ok, Ans}
  end.


%% get_storage_from_db/1
%% ====================================================================
%% @doc Gets storage info from DB. Argument should be uuid() of storage document or ID of storage. <br/>
%% Non-error return value is always {ok, #veil_document{record = #storage_info{}}.
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec get_storage_from_db({uuid, DocUUID :: uuid()} | {id, StorageID :: integer()}) -> {ok, storage_doc()} | {error, any()} | no_return().
%% ====================================================================
get_storage_from_db({uuid, DocUUID}) when is_list(DocUUID) ->
    dao:set_db(?SYSTEM_DB_NAME),
    case dao:get_record(DocUUID) of
        {ok, #veil_document{record = #storage_info{}} = Doc} ->
            {ok, Doc};
        {ok, #veil_document{}} ->
            {error, invalid_storage_record};
        Other ->
            Other
    end;
get_storage_from_db({id, StorageID}) when is_integer(StorageID) ->
    QueryArgs =
        #view_query_args{keys = [StorageID], include_docs = true}, 
    case dao:list_records(?STORAGE_BY_ID_VIEW, QueryArgs) of
        {ok, #view_result{rows = [Row]}} ->
            #view_row{doc = #veil_document{record = #storage_info{} } = Doc } = Row,
            {ok, Doc};
        {ok, #view_result{rows = Rows}} ->
            [#view_row{doc = #veil_document{record = #storage_info{} } = Doc } | _Tail] = Rows,
            lager:warning("Storage with ID ~p is duplicated. Returning first copy. All: ~p", [StorageID, Rows]),
            {ok, Doc};
        _Other ->
            lager:error("Invalid view response: ~p", [_Other]),
            throw(inavlid_data)
    end. 


%% list_storage/0
%% ====================================================================
%% @doc Lists all storage docs. <br/>
%% Non-error return value is always list of #veil_document{record = #storage_info{}} records.<br/>
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% @end
-spec list_storage() -> {ok, [storage_doc()]} | no_return().
%% ====================================================================
list_storage() ->
    QueryArgs =
        #view_query_args{start_key = 0, end_key = 1, include_docs = true}, %% All keys are (int)0 so will get all documents
    case dao:list_records(?ALL_STORAGE_VIEW, QueryArgs) of
        {ok, #view_result{rows = Rows}} ->
            {ok, [Doc || #view_row{doc = #veil_document{record = #storage_info{} } = Doc } <- Rows]};
        _Other ->
            lager:error("Invalid view response: ~p", [_Other]),
            throw(inavlid_data)
    end.

%% find_files/1
%% @doc Returns list of uuids of files that matches to criteria passed as FileCriteria <br/>
%% Current implementation does not support specifying ctime and mtime at the same time, other combinations of criterias
%% are supported.
%% @end
%% ====================================================================
-spec find_files(FileCriteria :: file_criteria()) -> {ok, [file_doc()]} | no_return().
%% ====================================================================
find_files(FileCriteria) when is_record(FileCriteria, file_criteria) ->
  case are_name_or_uid_set(FileCriteria) of
    true -> find_file_by_file_name_or_uuid(FileCriteria);
    _ ->
      case are_time_criteria_set(FileCriteria) of
        true -> find_by_times(FileCriteria);

        % no restrictions set - find_file_when_file_name_or_uuid_set handles this case
        _ -> find_file_by_file_name_or_uuid(FileCriteria)
      end
  end.


%% ===================================================================
%% Internal functions
%% ===================================================================

%% clear_cache/1
%% ====================================================================
%% @doc Deletes key from storage caches at all nodes
-spec clear_cache(Key :: term()) -> ok.
%% ====================================================================
clear_cache(Key) ->
  ets:delete(storage_cache, Key),
  case worker_host:clear_cache({storage_cache, Key}) of
    ok -> ok;
    Error -> throw({error_during_global_cache_clearing, Error})
  end.

%% are_time_criteria_set/1
%% @doc Returns true if any of time criteria (ctime or mtime) are set
%% @end
%% ====================================================================
-spec are_time_criteria_set(FileCriteria :: file_criteria()) -> boolean().
%% ====================================================================
are_time_criteria_set(FileCriteria) when is_record(FileCriteria, file_criteria) ->
  (FileCriteria#file_criteria.ctime /= #time_criteria{}) or (FileCriteria#file_criteria.mtime /= #time_criteria{}).

%% are_name_or_uid_set/1
%% @doc Returns true if name or uid is set.
%% @end
%% ====================================================================
-spec are_name_or_uid_set(FileCriteria :: file_criteria()) -> boolean().
%% ====================================================================
are_name_or_uid_set(FileCriteria) when is_record(FileCriteria, file_criteria) ->
  (FileCriteria#file_criteria.file_pattern /= "") or (FileCriteria#file_criteria.uid /= null).

%% end_key_for_prefix/1
%% @doc Returns given prefix concatenated with high value unicode character.
%% Useful when finding by prefix.
%% See more at http://wiki.apache.org/couchdb/View_collation (paragraph string ranges)
%% @end
%% ====================================================================
-spec end_key_for_prefix(Prefix :: string()) -> binary().
%% ====================================================================
end_key_for_prefix(Prefix) ->
  unicode:characters_to_binary(Prefix ++ unicode:characters_to_list(string:chars(255, 2))).

%% get_desired_filetypes/1
%% @doc Returns list of file types that should be included according to given file_criteria record.
%% Elements of list are integers defined in files_common.hrl.
%% @end
%% ====================================================================
-spec get_desired_filetypes(file_criteria()) -> binary().
%% ====================================================================
get_desired_filetypes(#file_criteria{include_dirs = IncludeDirs, include_files = IncludeFiles, include_links = IncludeLinks}) ->
  Types = [{IncludeDirs, ?DIR_TYPE}, {IncludeFiles, ?REG_TYPE}, {IncludeLinks, ?LNK_TYPE}],
  lists:filtermap(
    fun({Included, Type}) ->
      case Included of
        true -> {Included, Type};
        _ -> false
      end
    end, Types).

%% fetch_rows/2
%% @doc Query given view with given #view_query_args and results list of rows
%% @end
%% ====================================================================
-spec fetch_rows(ViewName :: string(), QueryArgs :: #view_query_args{}) -> list(#view_row{}) | no_return().
%% ====================================================================
fetch_rows(ViewName, QueryArgs) ->
  case dao:list_records(ViewName, QueryArgs) of
    {ok, #view_result{rows = Rows}} ->
      Rows;
    Error ->
      ?error("Invalid view response: ~p", [Error]),
      throw(invalid_data)
  end.

%% get_filter_predicate_for_time/1
%% @doc Returns predicate function for filtering by time according to given #file_criteria
%% @end
%% ====================================================================
-spec get_filter_predicate_for_time(#file_criteria{}) -> fun((#view_row{}) -> boolean()).
%% ====================================================================
% TODO: introduce some macro to make it more concise
get_filter_predicate_for_time(#file_criteria{ctime = #time_criteria{time = Time, time_relation = TimeRelation}, mtime = #time_criteria{time = 0}}) ->
  case TimeRelation of
    older_than ->
      fun(#view_row{doc = FileMetaDoc}) ->
        FileMetaDoc#veil_document.record#file_meta.ctime < Time
      end;
    newer_than ->
      fun(#view_row{doc = FileMetaDoc}) ->
        FileMetaDoc#veil_document.record#file_meta.ctime > Time
      end
  end;
get_filter_predicate_for_time(#file_criteria{mtime = #time_criteria{time = Time, time_relation = TimeRelation}, ctime = #time_criteria{time = 0}}) ->
  case TimeRelation of
    older_than ->
      fun(#view_row{doc = FileMetaDoc}) ->
        FileMetaDoc#veil_document.record#file_meta.mtime < Time
      end;
    newer_than ->
      fun(#view_row{doc = FileMetaDoc}) ->
        FileMetaDoc#veil_document.record#file_meta.mtime > Time
      end
  end.

%% find_file_by_file_name_or_uuid/1
%% @doc Returns uuids of file documents found with given #file_criteria.
%% Ignores ctime and mtime so should be called only if they are not set.
%% @end
%% ====================================================================
-spec find_file_by_file_name_or_uuid(FileCriteria :: #file_criteria{}) -> {ok, [string()]} | no_return().
%% ====================================================================
find_file_by_file_name_or_uuid(FileCriteria) ->
  FilePattern = FileCriteria#file_criteria.file_pattern,
  UidBin = case FileCriteria#file_criteria.uid of
             null -> null;
             Uid when is_binary(Uid) -> Uid;
             Uid when is_list(Uid) -> list_to_binary(Uid);
             Uid when is_integer(Uid) -> list_to_binary(integer_to_list(Uid))
           end,

  QueryArgs = case FilePattern of
                "" ->
                  case UidBin of
                    null -> #view_query_args{start_key = [<<"">>, null], end_key = [{}, {}]};
                    _ -> #view_query_args{start_key = [UidBin, null], end_key = [UidBin, {}]}
                  end;
                _ ->
                  case lists:nth(length(FilePattern), FilePattern) of
                    $* ->
                      Prefix = lists:sublist(FilePattern, length(FilePattern) - 1),
                      #view_query_args{start_key = [UidBin, list_to_binary(Prefix)], end_key = [UidBin, end_key_for_prefix(Prefix)]};
                    _ -> #view_query_args{keys = [[UidBin, list_to_binary(FilePattern)]]}
                  end
              end,

  Rows = fetch_rows(?FILES_BY_UID_AND_FILENAME, QueryArgs#view_query_args{include_docs = true}),
  Results = case are_time_criteria_set(FileCriteria) of
  % if are_time_criteria_set then we need to additionally filter Rows
              true ->
                FilterPredicate = get_filter_predicate_for_time(FileCriteria),
                lists:filter(FilterPredicate, Rows);
              _ -> Rows
            end,

  DesiredTypes = get_desired_filetypes(FileCriteria),
  FilteredByTypes = lists:filter(
    fun(#view_row{value = V}) ->
      {Value} = V,
      Type = proplists:get_value(<<"type">>, Value),
      lists:member(Type, DesiredTypes)
    end,
    Results),
  {ok, lists:map(fun(#view_row{id = Id}) -> Id end, FilteredByTypes)}.

%% find_by_times/1
%% @doc Returns uuids of file documents found with given #file_criteria.
%% @end
%% ====================================================================
-spec find_by_times(FileCriteria :: #file_criteria{}) -> {ok, [string()]} | no_return().
%% ====================================================================
find_by_times(FileCriteria) ->
  MetaIds = get_file_meta_ids(FileCriteria),
  DesiredTypes = get_desired_filetypes(FileCriteria),
  MetaIdsBin = lists:merge(lists:map(
    fun(MetaId) ->
      lists:map(
        fun(DesiredType) ->
          [list_to_binary(MetaId), DesiredType]
        end,
        DesiredTypes)
    end,
    MetaIds)),

  Rows = fetch_rows(?FILES_BY_META_DOC, #view_query_args{keys = MetaIdsBin, include_docs = true}),
  Result = lists:filter(fun(#view_row{doc = FileDoc}) ->
    lists:member(FileDoc#veil_document.record#file.type, DesiredTypes) end, Rows),
  {ok, lists:map(fun(#view_row{id = Id}) -> Id end, Result)}.

%% get_file_meta_ids/1
%% ====================================================================
%% @doc Get file_meta ids using file_meta_by_times view
%% Function assumes that ctime and mtime are never used together at the same time
%% @end
-spec get_file_meta_ids(file_criteria()) -> list(string()) | no_return().
%% ====================================================================
get_file_meta_ids(FileCriteria) ->
  QueryArgs1 = case FileCriteria#file_criteria.ctime of
                 #time_criteria{time = Time, time_relation = older_than} ->
                   #view_query_args{start_key = [0, null], end_key = [Time, {}]};
                 #time_criteria{time = Time, time_relation = newer_than} ->
                   #view_query_args{start_key = [Time + 1, null]};
                 #time_criteria{} ->
                   case FileCriteria#file_criteria.mtime of
                     #time_criteria{time = Time, time_relation = older_than} ->
                       #view_query_args{end_key = [null, Time]};
                     #time_criteria{time = Time, time_relation = newer_than} ->
                       #view_query_args{start_key = [null, Time + 1], end_key = [0, null]};
                     #time_criteria{} -> null
                   end
               end,

  case QueryArgs1 of
    (QueryArgs) when is_record(QueryArgs, view_query_args) ->
      case dao:list_records(?FILE_META_BY_TIMES, QueryArgs) of
        {ok, #view_result{rows = Rows}} ->
          lists:map(fun(#view_row{id = Id}) -> Id end, Rows);
        Error ->
          ?error("Invalid view response: ~p", [Error]),
          throw(invalid_data)
      end;
    _ -> []
  end.

%% file_path_analyze/1
%% ====================================================================
%% @doc Converts Path :: file_path() to internal dao format
-spec file_path_analyze(Path :: file_path()) -> {internal_path, TokenList :: list(), RootUUID :: uuid()}.
%% ====================================================================
file_path_analyze({Path, Root}) when is_list(Path), is_list(Root) ->
    file_path_analyze({relative_path, Path, Root});
file_path_analyze(Path) when is_list(Path) ->
    file_path_analyze({relative_path, Path, ""});
file_path_analyze({absolute_path, Path}) when is_list(Path) ->
    file_path_analyze({relative_path, Path, ""});
file_path_analyze({relative_path, [?PATH_SEPARATOR | Path], Root}) when is_list(Path), is_list(Root) ->
    file_path_analyze({relative_path, Path, Root});
file_path_analyze({relative_path, Path, Root}) when is_list(Path), is_list(Root) ->
    TokenPath = string:tokens(Path, [?PATH_SEPARATOR]),
    {internal_path, TokenPath, Root};
file_path_analyze(Path) ->
    throw({invalid_file_path, Path}).


%% uca_increment/1
%% ====================================================================
%% @doc Returns "incremented string" based on Unicode Collation Algorithm. 
%%      This method works only for alpha-numeric strings.
%% @end
-spec uca_increment(Id :: string()) -> string().
%% ====================================================================
uca_increment(Str) when is_list(Str) ->
    case try_toupper(Str) of 
        {Str1, true} -> Str1;
        {_, false} ->   
            case lists:reverse(uca_increment({lists:reverse(Str), true})) of
                [10 | T] -> [$Z || _ <- T] ++ [10];
                Other -> Other
            end
    end;
uca_increment({Tail, false}) ->
    Tail;
uca_increment({[], true}) ->
    [10];
uca_increment({[C | Tail], true}) ->
    {NC, Continue} = next_char(C),
    [NC | uca_increment({Tail ,Continue})].

%% Internal helper function used by uca_increment/1
try_toupper(Str) when is_list(Str) ->
    {Return, Status} = try_toupper({lists:reverse(Str), true}),
    {lists:reverse(Return), Status};
try_toupper({"", true}) ->
    {"", false};
try_toupper({[C | Tail], true}) when C >= $a, C =< $z ->
    { [(C - 32) | Tail], true };
try_toupper({[C | Tail], true}) ->
    { NewTail, Status} = try_toupper({Tail, true}),
    { [ C | NewTail], Status }.

%% Internal helper function used by uca_increment/1
next_char(C) when C >= $a, C < $z; C >= $A, C < $Z; C >= $0, C < $9 ->
    {C + 1, false};
next_char($9) ->
    {$a, false};
next_char($z) ->
    {$0, true};
next_char($Z) ->
    {$0, true};
next_char($$) ->
    {$0, false};
next_char(Other) ->
    Collation = " `^_-,;:!?.'\"()[]{}@*/\\&#%+<=>|~$",
    Res = 
        case lists:dropwhile(fun(Char) -> Char =/= Other end, Collation) of 
            [Other | [ Next | _ ]] -> Next;
            [] -> throw({unsupported_char, Other})
        end,
    {Res, false}.
