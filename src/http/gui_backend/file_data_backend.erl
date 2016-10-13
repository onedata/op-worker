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
-behavior(data_backend_behaviour).
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
-export([file_record/2, file_record/4, create_file/4]).

%%%===================================================================
%%% data_backend_behaviour callbacks
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
    {ok, Distributions} = logical_file_manager:get_file_distribution(
        SessionId, {guid, FileId}
    ),
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
create_record(<<"file">>, _) ->
    % File are created via an RPC call
    gui_error:report_error(<<"Not implemented">>);

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
                gui_error:report_warning(
                    <<"Cannot change ACL - access denied.">>
                )
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
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Constructs a file record from given FileId.
%% @end
%%--------------------------------------------------------------------
-spec file_record(SessionId :: session:id(), fslogic_worker:file_guid()) ->
    {ok, proplists:proplist()}.
file_record(SessionId, FileId) ->
    file_record(SessionId, FileId, false, 0).


%%--------------------------------------------------------------------
%% @doc
%% Constructs a file record from given FileId. The options ChildrenFromCache
%% and ChildrenOffset (valid for dirs only) allow to specify that dir's
%% children should be server from cache and what is the current offset in
%% directory (essentially how many files from the dir are currently displayed
%% in gui).
%% @end
%%--------------------------------------------------------------------
-spec file_record(SessionId :: session:id(), fslogic_worker:file_guid(),
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

            ParentGuid = get_parent(SessionId, FileId),
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
                {<<"parent">>, ParentGuid},
                {<<"children">>, Children},
                {<<"fileAcl">>, FileId},
                {<<"share">>, Share},
                {<<"provider">>, ProviderId},
                {<<"fileProperty">>, Metadata}
            ],
            {ok, Res}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a new file. Automatically adds the file to LS cache of parent dir
%% and pushes the update of parent dir children list to the client.
%% @end
%%--------------------------------------------------------------------
-spec create_file(SessionId :: session:id(), Name :: binary(),
    fslogic_worker:file_guid(), Type :: binary()) ->
    {ok, fslogic_worker:file_guid()} | {error, term()}.
create_file(SessionId, Name, ParentId, Type) ->
    try
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
        {ok, FileData} = add_new_file_to_cache(SessionId, FileId, ParentId),
        gui_async:push_updated(<<"file">>, FileData),
        {ok, FileId}
    catch Error:Message ->
        ?error_stacktrace(
            "Cannot create file via GUI - ~p:~p", [Error, Message]
        ),
        case Type of
            <<"dir">> ->
                gui_error:report_warning(<<"Failed to create new directory.">>);
            <<"file">> ->
                gui_error:report_warning(<<"Failed to create new file.">>)
        end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns the GUID of parent of given file, or null if the file resides in
%% space's root dir.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(SessionId :: session:id(), fslogic_worker:file_guid()) ->
    binary() | null.
get_parent(SessionId, FileGuid) ->
    {ok, #file_attr{uuid = UserRootDirGuid}} = logical_file_manager:stat(
        SessionId, {path, <<"/">>}),
    {ok, ParentGuid} = logical_file_manager:get_parent(
        SessionId, {guid, FileGuid}
    ),
    case ParentGuid of
        UserRootDirGuid -> null;
        _ -> ParentGuid
    end.


fetch_dir_children(DirId, CurrentChildrenCount) ->
    LsSubCacheName = ls_sub_cache_name(),
    % Fetch total children count and number of chunks
    [{{DirId, size}, TotalChildrenCount}] = ets:lookup(
        LsSubCacheName, {DirId, size}
    ),
    [{{DirId, chunk_count}, ChunkCount}] = ets:lookup(
        LsSubCacheName, {DirId, chunk_count}
    ),
    % First, get all new files that have been created during this session
    % (they are prepended at the beginning of the files list).
    [{{DirId, 0}, NewFiles}] = ets:lookup(LsSubCacheName, {DirId, 0}),
    % Check LS chunk size
    {ok, LsChunkSize} = application:get_env(
        ?APP_NAME, gui_file_children_chunk_size
    ),
    % Calculate how many children should be served from cache
    FilesToFetch = CurrentChildrenCount + LsChunkSize - length(NewFiles),
    ChunksToFetch = min(FilesToFetch div LsChunkSize, ChunkCount),
    ?dump({FilesToFetch div LsChunkSize, ChunkCount, ChunksToFetch}),
    % Cache the last served children count
    ets:insert(
        LsSubCacheName, {{DirId, last_children_count}, CurrentChildrenCount}
    ),
    Result = lists:foldl(
        fun(Counter, Acc) ->
            [{{DirId, Counter}, Chunk}] = ets:lookup(
                LsSubCacheName, {DirId, Counter}
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
-spec file_acl_record(SessionId :: session:id(), fslogic_worker:file_guid()) ->
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


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Lists all children of given directory, sorts them by name, caches the result
%% and returns the first chunk of file children.
%% @end
%%--------------------------------------------------------------------
-spec ls_dir(SessionId :: session:id(), DirId :: fslogic_worker:file_guid()) ->
    [fslogic_worker:file_guid()].
ls_dir(SessionId, DirId) ->
    % Check LS chunk size and configurable limit of single LS operation
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
    % We do not care for file names, only ids, as file names are resolved
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
            % Return the first chunk of the list
            {lists:sublist(ChildrenSorted, LsChunkSize), ChildrenSortedLength}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Recursively lists children of a directory.
%% @end
%%--------------------------------------------------------------------
-spec ls_dir_chunked(SessionId :: session:id(),
    DirId :: fslogic_worker:file_guid(), Offset :: non_neg_integer(),
    Limit :: non_neg_integer(), Result :: [fslogic_worker:file_guid()]) ->
    [fslogic_worker:file_guid()].
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
    ChunkCount = length(Chunks),
    LsSubCacheName = ls_sub_cache_name(),
    % Cache the total children count
    ets:insert(LsSubCacheName, {{DirId, size}, TotalChildrenCount}),
    % Cache the total chunk count
    ets:insert(LsSubCacheName, {{DirId, chunk_count}, ChunkCount}),
    % Cache the last number of chunks served to gui
    ets:insert(LsSubCacheName, {{DirId, last_children_count}, 0}),
    % Cache a placeholder for new files created during this session
    ets:insert(LsSubCacheName, {{DirId, 0}, []}),
    % Insert the chunks into the ETS
    lists:foldl(
        fun(Chunk, Counter) ->
            ets:insert(LsSubCacheName, {{DirId, Counter}, Chunk}),
            Counter + 1
        end, 1, Chunks),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds a newly created file to LS cache. New files are always prepended to the
%% beginning of the file list. The file stay at this place till new websocket
%% session is initialized (user refreshes the page).
%% @end
%%--------------------------------------------------------------------
-spec add_new_file_to_cache(SessionId :: session:id(),
    FileId :: fslogic_worker:file_guid(),
    DirId :: fslogic_worker:file_guid()) -> integer().
add_new_file_to_cache(SessionId, FileId, DirId) ->
    LsSubCacheName = ls_sub_cache_name(),
    [{{DirId, size}, CurrentSize}] = ets:lookup(
        LsSubCacheName, {DirId, size}
    ),
    ets:insert(LsSubCacheName, {{DirId, size}, CurrentSize + 1}),
    [{{DirId, 0}, CurrentNewFiles}] = ets:lookup(
        LsSubCacheName, {DirId, 0}
    ),
    ets:insert(LsSubCacheName, {{DirId, 0}, [FileId | CurrentNewFiles]}),
    [{{DirId, last_children_count}, LastChildrenCount}] = ets:lookup(
        LsSubCacheName, {DirId, last_children_count}
    ),
    ets:insert(
        LsSubCacheName, {{DirId, last_children_count}, LastChildrenCount + 1}
    ),
    file_record(SessionId, DirId, true, LastChildrenCount + 1).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Splits a list into a list of chunks, each of ChunkSize (except the last one).
%% @end
%%--------------------------------------------------------------------
-spec split_into_chunks(List :: [term()], ChunkSize :: non_neg_integer()) -> ok.
split_into_chunks(List, ChunkSize) ->
    split_into_chunks(List, ChunkSize, []).

split_into_chunks(List, ChunkSize, ResultList) when ChunkSize >= length(List) ->
    lists:reverse([List | ResultList]);

split_into_chunks(List, ChunkSize, ResultList) ->
    {Chunk, Tail} = lists:split(ChunkSize, List),
    split_into_chunks(Tail, ChunkSize, [Chunk | ResultList]).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolves LS cache ETS identifier for current process.
%% @end
%%--------------------------------------------------------------------
-spec ls_sub_cache_name() -> integer().
ls_sub_cache_name() ->
    WSPid = gui_async:get_ws_process(),
    [{WSPid, LsSubCache}] = ets:lookup(?LS_CACHE_ETS, WSPid),
    LsSubCache.