%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Jakub Liput
%%% @author Tomasz Lichon
%%% @copyright (C) 2015-2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the file model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(file_data_backend).
-author("Lukasz Opiola").
-author("Jakub Liput").
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("http/gui_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([init/0, terminate/0]).
-export([find/2, find_all/1, find_query/2]).
-export([create_record/2, update_record/3, delete_record/2]).
-export([file_record/4, create_file/4]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback init/0.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ?dump({init, self()}),
    NewETS = ets:new(?LS_SUB_CACHE_ETS, [
        set, public,
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    ets:insert(?LS_CACHE_ETS, {self(), NewETS}),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback terminate/0.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    ?dump({terminate, self()}),
    ets:delete(?LS_CACHE_ETS, self()),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find/2.
%% @end
%%--------------------------------------------------------------------
-spec find(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find(<<"file">>, FileId) ->
    SessionId = g_session:get_session_id(),
    try
        file_record(SessionId, FileId, false, 0)
    catch T:M ->
        ?warning_stacktrace("Cannot get file-meta for file (~p). ~p:~p", [
            FileId, T, M
        ]),
        {ok, [{<<"id">>, FileId}, {<<"type">>, <<"broken">>}]}
    end;
find(<<"file-acl">>, FileId) ->
    SessionId = g_session:get_session_id(),
    file_acl_record(SessionId, FileId).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | gui_error:error_result().
find_all(<<"file">>) ->
    gui_error:report_error(<<"Not iplemented">>);
find_all(<<"file-acl">>) ->
    gui_error:report_error(<<"Not iplemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_query/2.
%% @end
%%--------------------------------------------------------------------
-spec find_query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
find_query(<<"file">>, _Data) ->
    gui_error:report_error(<<"Not implemented">>);
find_query(<<"file-acl">>, _Data) ->
    gui_error:report_error(<<"Not implemented">>);
find_query(<<"file-distribution">>, [{<<"fileId">>, FileId}]) ->
    SessionId = g_session:get_session_id(),
    {ok, Distributions} = logical_file_manager:get_file_distribution(SessionId, {guid, FileId}),
    Res = lists:map(
        fun(#{<<"providerId">> := ProviderId, <<"blocks">> := Blocks}) ->
            BlocksList =
                case Blocks of
                    [] ->
                        [0, 0];
                    _ ->
                        lists:foldl(
                            fun([Offset, Size], Acc) ->
                                Acc ++ [Offset, Offset + Size]
                            end, [], Blocks)
                end,
            [
                {<<"id">>, op_gui_utils:ids_to_association(FileId, ProviderId)},
                {<<"fileId">>, FileId},
                {<<"provider">>, ProviderId},
                {<<"blocks">>, BlocksList}
            ]
        end, Distributions),
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | gui_error:error_result().
create_record(<<"file">>, Data) ->
    try
        SessionId = g_session:get_session_id(),
        Name = proplists:get_value(<<"name">>, Data),
        Type = proplists:get_value(<<"type">>, Data),
        ParentGUID = proplists:get_value(<<"parent">>, Data, null),
        {ok, ParentPath} = logical_file_manager:get_file_path(
            SessionId, ParentGUID),
        Path = filename:join([ParentPath, Name]),
        FileId = case Type of
            <<"file">> ->
                {ok, FId} = logical_file_manager:create(SessionId, Path),
                FId;
            <<"dir">> ->
                {ok, DirId} = logical_file_manager:mkdir(SessionId, Path),
                DirId
        end,
        {ok, #file_attr{
            name = Name,
            size = SizeAttr,
            mtime = ModificationTime,
            mode = PermissionsAttr}} =
            logical_file_manager:stat(SessionId, {guid, FileId}),
        Size = case Type of
            <<"dir">> -> null;
            _ -> SizeAttr
        end,
        Permissions = integer_to_binary(PermissionsAttr, 8),
        Res = [
            {<<"id">>, FileId},
            {<<"name">>, Name},
            {<<"type">>, Type},
            {<<"permissions">>, Permissions},
            {<<"modificationTime">>, ModificationTime},
            {<<"size">>, Size},
            {<<"parent">>, ParentGUID},
            {<<"children">>, []},
            {<<"fileAcl">>, FileId}
        ],
        {ok, Res}
    catch _:_ ->
        case proplists:get_value(<<"type">>, Data, <<"dir">>) of
            <<"dir">> ->
                gui_error:report_warning(<<"Failed to create new directory.">>);
            <<"file">> ->
                gui_error:report_warning(<<"Failed to create new file.">>)
        end
    end;
create_record(<<"file-acl">>, Data) ->
    Id = proplists:get_value(<<"file">>, Data),
    case update_record(<<"file-acl">>, Id, Data) of
        ok ->
            file_acl_record(?ROOT_SESS_ID, Id);
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | gui_error:error_result().
update_record(<<"file">>, FileId, Data) ->
    try
        SessionId = g_session:get_session_id(),
        case proplists:get_value(<<"permissions">>, Data, undefined) of
            undefined ->
                ok;
            NewPerms ->
                Perms = case is_integer(NewPerms) of
                    true ->
                        binary_to_integer(integer_to_binary(NewPerms), 8);
                    false ->
                        binary_to_integer(NewPerms, 8)
                end,
                case Perms >= 0 andalso Perms =< 8#777 of
                    true ->
                        ok = logical_file_manager:set_perms(
                            SessionId, {guid, FileId}, Perms);
                    false ->
                        gui_error:report_warning(<<"Cannot change permissions, "
                        "invalid octal value.">>)
                end
        end
    catch _:_ ->
        gui_error:report_warning(<<"Cannot change permissions.">>)
    end;
update_record(<<"file-acl">>, FileId, Data) ->
    try
        SessionId = g_session:get_session_id(),
        Acl = acl_utils:json_to_acl(Data),
        case logical_file_manager:set_acl(SessionId, {guid, FileId}, Acl) of
            ok ->
                ok;
            {error, ?EACCES} ->
                gui_error:report_warning(<<"Cannot change ACL - access denied.">>)
        end
    catch _:_ ->
        gui_error:report_warning(<<"Cannot change ACL.">>)
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | gui_error:error_result().
delete_record(<<"file">>, FileId) ->
    SessionId = g_session:get_session_id(),
    case logical_file_manager:rm_recursive(SessionId, {guid, FileId}) of
        ok ->
            ok;
        {error, ?EACCES} ->
            gui_error:report_warning(
                <<"Cannot remove file or directory - access denied.">>)
    end;
delete_record(<<"file-acl">>, FileId) ->
    SessionId = g_session:get_session_id(),
    case logical_file_manager:remove_acl(SessionId, {guid, FileId}) of
        ok ->
            ok;
        {error, ?EACCES} ->
            gui_error:report_warning(
                <<"Cannot remove ACL - access denied.">>)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns the UUID of parent of given file. This is needed because
%% spaces dir has two different UUIDs, should be removed when this is fixed.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(SessionId :: binary(), FileGUID :: binary()) -> binary().
get_parent(SessionId, FileGUID) ->
    {ok, ParentGUID} = logical_file_manager:get_parent(SessionId, {guid, FileGUID}),
    ParentGUID.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns the UUID of user's root dir.
%% @end
%%--------------------------------------------------------------------
-spec get_user_root_dir_uuid(SessionId :: binary()) -> binary().
get_user_root_dir_uuid(SessionId) ->
    {ok, #file_attr{uuid = UserRootDirUUID}} = logical_file_manager:stat(
        SessionId, {path, <<"/">>}),
    UserRootDirUUID.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs a file record from given FileId. The options ChildrenFromCache
%% and ChildrenOffset (valid for dirs only) allow to specify that dir's
%% children should be server from cache and what is the current offset in
%% directory (essentially how many files from the dir are currently displayed
%% in gui).
%% @end
%%--------------------------------------------------------------------
-spec file_record(SessionId :: binary(), FileId :: binary(),
    ChildrenFromCache :: boolean(), ChildrenOffset :: non_neg_integer()) ->
    {ok, proplists:proplist()}.
file_record(SessionId, FileId, ChildrenFromCache, ChildrenOffset) ->
    case logical_file_manager:stat(SessionId, {guid, FileId}) of
        {error, ?ENOENT} ->
            gui_error:report_error(<<"No such file or directory.">>);
        {ok, FileAttr} ->
            #file_attr{
                name = Name,
                type = TypeAttr,
                size = SizeAttr,
                mtime = ModificationTime,
                mode = PermissionsAttr,
                shares = Shares,
                provider_id = ProviderId
            } = FileAttr,

            UserRootDirUUID = get_user_root_dir_uuid(SessionId),
            ParentUUID = case get_parent(SessionId, FileId) of
                UserRootDirUUID -> null;
                Other -> Other
            end,

            Permissions = integer_to_binary((PermissionsAttr rem 1000), 8),

            {Type, Size, Children, TotalChildrenCount} = case TypeAttr of
                ?DIRECTORY_TYPE ->
                    {ChildrenList, TotalCount} = case ChildrenFromCache of
                        false ->
                            ?dump({ls_dir, Name}),
                            ls_dir(SessionId, FileId);
                        true ->
                            ?dump({fetch_dir_children, Name}),
                            fetch_dir_children(FileId, ChildrenOffset)
                    end,
                    ?dump(ChildrenList),
                    {<<"dir">>, 0, ChildrenList, TotalCount};
                _ ->
                    {<<"file">>, SizeAttr, [], 0}
            end,

            % Currently only one share per file is allowed
            Share = case Shares of
                [] -> null;
                [ShareId] -> ShareId
            end,
            {ok, HasCustomMetadata} = logical_file_manager:has_custom_metadata(
                SessionId, {guid, FileId}
            ),
            Metadata = case HasCustomMetadata of
                false -> null;
                true -> FileId
            end,
            Res = [
                {<<"id">>, FileId},
                {<<"name">>, Name},
                {<<"type">>, Type},
                {<<"permissions">>, Permissions},
                {<<"modificationTime">>, ModificationTime},
                {<<"size">>, Size},
                {<<"totalChildrenCount">>, TotalChildrenCount},
                {<<"parent">>, ParentUUID},
                {<<"children">>, Children},
                {<<"fileAcl">>, FileId},
                {<<"share">>, Share},
                {<<"provider">>, ProviderId},
                {<<"fileProperty">>, Metadata}
            ],
            {ok, Res}
    end.


create_file(SessionId, Name, ParentId, Type) ->
    try
        SessionId = g_session:get_session_id(),
        {ok, ParentPath} = logical_file_manager:get_file_path(
            SessionId, ParentId),
        Path = filename:join([ParentPath, Name]),
        FileId = case Type of
            <<"file">> ->
                {ok, FId} = logical_file_manager:create(SessionId, Path),
                FId;
            <<"dir">> ->
                {ok, DirId} = logical_file_manager:mkdir(SessionId, Path),
                DirId
        end,
        {ok, #file_attr{
            name = Name,
            size = SizeAttr,
            mtime = ModificationTime,
            mode = PermissionsAttr}} =
            logical_file_manager:stat(SessionId, {guid, FileId}),
        Size = case Type of
            <<"dir">> -> null;
            _ -> SizeAttr
        end,
        Permissions = integer_to_binary(PermissionsAttr, 8),
        Res = [
            {<<"id">>, FileId},
            {<<"name">>, Name},
            {<<"type">>, Type},
            {<<"permissions">>, Permissions},
            {<<"modificationTime">>, ModificationTime},
            {<<"size">>, Size},
            {<<"parent">>, ParentId},
            {<<"children">>, []},
            {<<"fileAcl">>, FileId}
        ],
        {ok, Res}
    catch _:_ ->
        case Type of
            <<"dir">> ->
                gui_error:report_warning(<<"Failed to create new directory.">>);
            <<"file">> ->
                gui_error:report_warning(<<"Failed to create new file.">>)
        end
    end.


fetch_dir_children(DirId, CurrentChildrenCount) ->
    % Fetch total children count and number of chunks
    [{{DirId, size}, TotalChildrenCount}] = ets:lookup(
        ls_sub_cache_name(), {DirId, size}
    ),
    [{{DirId, chunk_count}, ChunkCount}] = ets:lookup(
        ls_sub_cache_name(), {DirId, chunk_count}
    ),
    % First, get all new files that have been created during this session
    % (they are prepended at the beginning of the files list).
    [{{DirId, 0}, NewFiles}] = ets:lookup(ls_sub_cache_name(), {DirId, 0}),
    % Check LS chunk size
    {ok, LsChunkSize} = application:get_env(
        ?APP_NAME, gui_file_children_chunk_size
    ),
    % Calculate how many children should be served from cache
    ?dump({CurrentChildrenCount, LsChunkSize, NewFiles}),
    FilesToFetch = CurrentChildrenCount + LsChunkSize - length(NewFiles),
    ChunksToFetch = min(FilesToFetch div LsChunkSize, ChunkCount),
    Result = lists:foldl(
        fun(Counter, Acc) ->
            [{{DirId, Counter}, Chunk}] = ets:lookup(
                ls_sub_cache_name(), {DirId, Counter}
            ),
            Acc ++ Chunk
        end, NewFiles, lists:seq(1, ChunksToFetch)),
    {Result, TotalChildrenCount}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Constructs a file acl record for given FileId.
%% @end
%%--------------------------------------------------------------------
-spec file_acl_record(SessionId :: binary(), FileId :: binary()) ->
    {ok, proplists:proplist()}.
file_acl_record(SessionId, FileId) ->
    case logical_file_manager:get_acl(SessionId, {guid, FileId}) of
        {error, ?ENOENT} ->
            gui_error:report_error(<<"No such file or directory.">>);
        {error, ?ENOATTR} ->
            gui_error:report_error(<<"No ACL defined.">>);
        {error, ?EACCES} ->
            gui_error:report_error(<<"Cannot read ACL - access denied.">>);
        {ok, Acl} ->
            Res = acl_utils:acl_to_json(FileId, Acl),
            {ok, Res}
    end.


ls_dir(SessionId, DirId) ->
    % Check LS chunk size and configurable limit of single LS
    {ok, LsChunkSize} = application:get_env(
        ?APP_NAME, gui_file_children_chunk_size
    ),
    {ok, LsLimit} = application:get_env(?APP_NAME, gui_ls_limit),
    % LS the dir
    Children = ls_dir_chunked(SessionId, DirId, 0, LsLimit, []),
    % File names must be converted to strings and to lowercase for
    % proper comparison.
    ChildrenStrings = lists:map(
        fun({Id, Name}) ->
            {Id, string:to_lower(unicode:characters_to_list(Name))}
        end, Children),
    ChildrenSortedTuples = lists:keysort(2, ChildrenStrings),
    % We do not care for file names, only ids, as fine names are resolved
    % from fileattr later.
    {ChildrenSorted, _} = lists:unzip(ChildrenSortedTuples),
    % Cache the results
    cache_ls_result(DirId, ChildrenSorted, LsChunkSize),
    ChildrenSortedLength = length(ChildrenSorted),
    case ChildrenSortedLength > LsChunkSize of
        false ->
            % Return the whole list
            {ChildrenSorted, ChildrenSortedLength};
        true ->
            % return the first chunk of the list
            {lists:sublist(ChildrenSorted, LsChunkSize), ChildrenSortedLength}
    end.


ls_dir_chunked(SessionId, DirId, Offset, Limit, Result) ->
    {ok, LS} = logical_file_manager:ls(SessionId, {guid, DirId}, Offset, Limit),
    NewResult = Result ++ LS,
    case length(LS) =:= Limit of
        true ->
            ls_dir_chunked(SessionId, DirId, Offset + Limit, Limit, NewResult);
        false ->
            NewResult
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Caches a LS result in ETS to optimize DB load and allow for pagination
%% in GUI data view. Input list of directory children should be sorted.
%% Must be called by an async process. The whole cache is held
%% is an ETS dedicated for parent websocket process.
%% @end
%%--------------------------------------------------------------------
-spec cache_ls_result(DirId :: fslogic_worker:file_guid(),
    DirChildren :: [{fslogic_worker:file_guid(), file_meta:name()}],
    LsChunkSize :: non_neg_integer()) -> ok.
cache_ls_result(DirId, ChildrenSorted, LsChunkSize) ->
    % Split the children list into chunks of size equal to LS chunk size
    Chunks = split_into_chunks(ChildrenSorted, LsChunkSize),
    TotalChildrenCount = length(ChildrenSorted),
    ChunkCount = TotalChildrenCount div LsChunkSize,
    % Cache the total children count
    ets:insert(ls_sub_cache_name(), {{DirId, size}, TotalChildrenCount}),
    % Cache the total chunk count
    ets:insert(ls_sub_cache_name(), {{DirId, chunk_count}, ChunkCount}),
    % Cache a placeholder for new files created during this session
    ets:insert(ls_sub_cache_name(), {{DirId, 0}, []}),
    % Insert the chunks into the ETS
    lists:foldl(
        fun(Chunk, Counter) ->
            ets:insert(ls_sub_cache_name(), {{DirId, Counter}, Chunk}),
            ?dump({{DirId, Counter}, length(Chunk)}),
            Counter + 1
        end, 1, Chunks),
    ok.


add_new_file_to_cache(FileId, DirId) ->
    [{{DirId, size}, CurrentSize}] = ets:lookup(
        ls_sub_cache_name(), {DirId, size}
    ),
    ets:insert(ls_sub_cache_name(), {{DirId, size}, CurrentSize + 1}),
    [{{DirId, 0}, CurrentNewFiles}] = ets:lookup(
        ls_sub_cache_name(), {DirId, 0}
    ),
    ets:insert(ls_sub_cache_name(), {{DirId, 0}, [FileId | CurrentNewFiles]}),
    ok.


% Resolve ETS identifier
ls_sub_cache_name() ->
    WSPid = gui_async:get_ws_process(),
    [{WSPid, LsSubCache}] = ets:lookup(?LS_CACHE_ETS, WSPid),
    LsSubCache.



split_into_chunks(List, ChunkSize) ->
    split_into_chunks(List, ChunkSize, []).

split_into_chunks(List, ChunkSize, ResultList) when ChunkSize >= length(List) ->
    lists:reverse([List | ResultList]);

split_into_chunks(List, ChunkSize, ResultList) ->
    {Chunk, Tail} = lists:split(ChunkSize, List),
    split_into_chunks(Tail, ChunkSize, [Chunk | ResultList]).