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

%% API - File system management
-export([list_dir/3, rename_file/2, lock_file/3, unlock_file/3, test_init/0]). %% High level API functions
-export([save_descriptor/1, remove_descriptor/1, get_descriptor/1, list_descriptors/3]). %% Base descriptor management API functions
-export([save_file/1, remove_file/1, get_file/1, get_path_info/1]). %% Base file management API function


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
%% @doc Removes file descriptor from DB. Argument should be uuid() of #file_descriptor.
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec remove_descriptor(Fd :: fd()) -> ok | {error, any()} | no_return().
%% ====================================================================
remove_descriptor(Fd) ->
    dao:set_db(?DESCRIPTORS_DB_NAME),
    dao:remove_record(Fd).


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
-spec list_descriptors(MatchCriteria, N :: pos_integer(), Offset :: non_neg_integer()) ->
    {ok, fd_doc()} | {error, any()} | no_return() when
    MatchCriteria :: {by_file, File :: file()}.
%% ====================================================================
list_descriptors({by_file, File}, N, Offset) ->
    {ok, #veil_document{uuid = FileId}} = get_file(File),
    Res = dao_helper:query_view(?FD_BY_FILE_VIEW#view_info.db_name, ?FD_BY_FILE_VIEW#view_info.design, ?FD_BY_FILE_VIEW#view_info.name,
        #view_query_args{keys = [dao_helper:name(FileId)], include_docs = true,
            limit = N, skip = Offset}),
    case dao_helper:parse_view_result(Res) of
        {ok, #view_result{rows = Rows}} ->
            {ok, [FdDoc || #view_row{doc = #veil_document{record = #file_descriptor{file = FileId1}} = FdDoc} <- Rows, FileId1 == FileId]};
        Data ->
            %% TODO: error handling
            throw({inavlid_data, Data})
    end;
list_descriptors({_Type, _Resouce}, _N, _Offset) ->
    not_yet_implemented.


%% ===================================================================
%% Files Management
%% ===================================================================

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
    dao:set_db(?FILES_DB_NAME),
    Res = dao_helper:query_view(?FILE_TREE_VIEW#view_info.db_name, ?FILE_TREE_VIEW#view_info.design, ?FILE_TREE_VIEW#view_info.name,
                                #view_query_args{keys = [[dao_helper:name(Root), dao_helper:name(Dir)]],
                                    include_docs = case Path of [] -> true; _ -> false end}), %% Include doc representing leaf of our file path
    {NewRoot, FileDoc} =
        case dao_helper:parse_view_result(Res) of
            {ok, #view_result{rows = [#view_row{id = Id, doc = FDoc}]}} ->
                {Id, FDoc};
            {ok, #view_result{rows = []}} ->
                lager:error("File ~p not found (root = ~p)", [Dir, Root]),
                throw(file_not_found);
            {ok, #view_result{rows = [#view_row{id = Id, doc = FDoc} | _Tail]}} ->
                lager:warning("File ~p (root = ~p) is duplicated. Returning first copy. Others: ", [Dir, Root, _Tail]),
                {Id, FDoc};
            _Other ->
                lager:error("Invalid view response: ~p", [_Other]),
                throw(invalid_data)
        end,
    case Path of
        [] -> {ok, FileDoc};
        _ -> get_file({internal_path, Path, NewRoot})
    end;
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


%% list_dir/2
%% ====================================================================
%% @doc Lists N files from specified directory starting from Offset. <br/>
%% Non-error return value is always list of #veil_document{record = #file{}} records.<br/>
%% Should not be used directly, use dao:handle/2 instead (See dao:handle/2 for more details).
%% @end
-spec list_dir(Dir :: file(), N :: pos_integer(), Offset :: non_neg_integer()) -> [file_info()].
%% ====================================================================
list_dir(Dir, N, Offset) ->
    Id =
        case get_file(Dir) of
            {ok, #veil_document{record = #file{type = ?DIR_TYPE}, uuid = UUID}} ->
                UUID;
            R ->
                %% TODO: error handling
                throw({dir_not_found, R})
        end,
    NextId =  integer_to_list(list_to_integer(case Id of [] -> "0"; _ -> Id end, 16)+1, 16), %% Dirty hack needed because `inclusive_end` option does not work in BigCouch for some reason
    Res = dao_helper:query_view(?FILE_TREE_VIEW#view_info.db_name, ?FILE_TREE_VIEW#view_info.design, ?FILE_TREE_VIEW#view_info.name,
        #view_query_args{start_key = [dao_helper:name(Id), dao_helper:name("")], end_key = [dao_helper:name(NextId), dao_helper:name("")],
                           limit = N, include_docs = true, skip = Offset, inclusive_end = false}), %% Inclusive end does not work, disable to be sure
    case dao_helper:parse_view_result(Res) of
        {ok, #view_result{rows = Rows}} -> %% We need to strip results that don't match search criteria (tail of last query possibly), because
                                           %% `end_key` seems to behave strange combined with `limit` option. TODO: get rid of it after DBMS switch
            {ok, [FileDoc || #view_row{doc = #veil_document{record = #file{parent = Parent} } = FileDoc } <- Rows, Parent == Id]};
        _ ->
            %% TODO: error handling
            throw(inavlid_data)
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
%% Test Method - TODO: delete before creating pull request
%% ===================================================================

test_init() ->
    {ok, UUID1} = save_file(#file{name = "users", type = ?DIR_TYPE}),
    {ok, UUID2} = save_file(#file{name = "plgroxeon", type = ?DIR_TYPE, parent = UUID1}),
    save_file(#file{name = "plguser1", type = ?DIR_TYPE, parent = UUID1}),
    save_file(#file{name = "plguser2", type = ?DIR_TYPE, parent = UUID1}),
    save_file(#file{name = "file1", parent = UUID2}),
    save_file(#file{name = "file2", parent = UUID2}),
    save_file(#file{name = "file3", parent = UUID2}),
    {ok, UUID3} = save_file(#file{name = "dir1", type = ?DIR_TYPE, parent = UUID2}),
    {ok, UUID4} = save_file(#file{name = "dir2", type = ?DIR_TYPE, parent = UUID2}),
    save_file(#file{name = "file4", parent = UUID3}),
    save_file(#file{name = "file5", parent = UUID3}),
    save_file(#file{name = "file6", parent = UUID4}),
    save_file(#file{name = "file7", parent = UUID4}),
    save_file(#file{name = "file8", parent = UUID4}),
    save_file(#file{name = "file1", parent = UUID4}),
    save_descriptor(#file_descriptor{file = UUID3}),
    save_descriptor(#file_descriptor{file = UUID3}),
    save_descriptor(#file_descriptor{file = UUID4}),
    save_descriptor(#file_descriptor{file = UUID2}).


%% ===================================================================
%% Internal functions
%% ===================================================================


%% file_path_analyze/1
%% ====================================================================
%% @doc Converts Path :: file_path() to internal dao format
%% @end
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
    {internal_path, string:tokens(Path, [?PATH_SEPARATOR]), Root};
file_path_analyze(Path) ->
    throw({invalid_file_path, Path}).