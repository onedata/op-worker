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
-include("http/gui_paths.hrl").
-include("http/http_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% data_backend_behaviour callbacks
-export([init/0, terminate/0]).
-export([find_record/2, find_all/1, query/2, query_record/2]).
-export([create_record/2, update_record/3, delete_record/2]).
%% API
-export([file_record/2, file_record/3, file_record/5]).
-export([create_file/4, report_file_upload/2, report_file_batch_complete/1]).
-export([fetch_more_dir_children/2]).

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
    NewETS = ets:new(?LS_SUB_CACHE_ETS, [
        set, public
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
    ets:delete(?LS_CACHE_ETS, self()),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_record/2.
%% @end
%%--------------------------------------------------------------------
-spec find_record(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
find_record(<<"file">>, FileId) ->
    SessionId = op_gui_session:get_session_id(),
    try
        file_record(SessionId, FileId)
    catch T:M ->
        ?warning_stacktrace("Cannot get file-meta for file (~p). ~p:~p", [
            FileId, T, M
        ]),
        {ok, [{<<"id">>, FileId}, {<<"type">>, <<"broken">>}]}
    end;
find_record(<<"file-shared">>, AssocId) ->
    SessionId = op_gui_session:get_session_id(),
    file_record(<<"file-shared">>, SessionId, AssocId);
find_record(<<"file-public">>, AssocId) ->
    SessionId = ?GUEST_SESS_ID,
    file_record(<<"file-public">>, SessionId, AssocId).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().
find_all(<<"file">>) ->
    op_gui_error:report_error(<<"Not implemented">>);
find_all(<<"file-shared">>) ->
    op_gui_error:report_error(<<"Not implemented">>);
find_all(<<"file-public">>) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query/2.
%% @end
%%--------------------------------------------------------------------
-spec query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().
query(<<"file">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>);
query(<<"file-shared">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>);
query(<<"file-public">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query_record/2.
%% @end
%%--------------------------------------------------------------------
-spec query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
query_record(<<"file">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>);
query_record(<<"file-shared">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>);
query_record(<<"file-public">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
create_record(<<"file">>, _Data) ->
    % Files are created via an RPC call
    op_gui_error:report_error(<<"Not implemented">>);
create_record(<<"file-shared">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>);
create_record(<<"file-public">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | op_gui_error:error_result().
update_record(<<"file-shared">>, _Id, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>);
update_record(<<"file-public">>, _Id, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>);
update_record(<<"file">>, FileId, [{<<"name">>, NewName}]) ->
    try
        SessionId = op_gui_session:get_session_id(),
        {ok, OldPath} = lfm:get_file_path(
            SessionId, FileId
        ),
        DirPathTokens = fslogic_path:split(filename:dirname(OldPath)),
        NewPath = fslogic_path:join(DirPathTokens ++ [NewName]),
        case lfm:mv(SessionId, {guid, FileId}, NewPath) of
            {ok, _} ->
                ok;
            {error, ?EPERM} ->
                op_gui_error:report_warning(<<"Permission denied.">>);
            {error, ?EACCES} ->
                op_gui_error:report_warning(<<"Access denied.">>);
            {error, ?EINVAL} ->
                op_gui_error:report_warning(<<"Invalid argument.">>)
        end
    catch Error:Message ->
        ?error_stacktrace("Cannot rename file via GUI - ~p:~p", [
            Error, Message
        ]),
        op_gui_error:report_warning(
            <<"Cannot rename file due to unknown error.">>)
    end;
update_record(<<"file">>, _Id, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | op_gui_error:error_result().
delete_record(<<"file-shared">>, _Id) ->
    op_gui_error:report_error(<<"Not implemented">>);
delete_record(<<"file-public">>, _Id) ->
    op_gui_error:report_error(<<"Not implemented">>);
delete_record(<<"file">>, FileId) ->
    SessionId = op_gui_session:get_session_id(),
    {ok, ParentId} = lfm:get_parent(SessionId, {guid, FileId}),
    case lfm:rm_recursive(SessionId, {guid, FileId}) of
        ok ->
            modify_ls_cache(remove, FileId, ParentId),
            ok;
        {error, ?EACCES} ->
            op_gui_error:report_warning(
                <<"Cannot remove file or directory - access denied.">>)
    end.


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Constructs a file record for given FileId.
%% @end
%%--------------------------------------------------------------------
-spec file_record(SessionId :: session:id(), fslogic_worker:file_guid()) ->
    {ok, proplists:proplist()}.
file_record(SessionId, FileId) ->
    file_record(<<"file">>, SessionId, FileId, false, 0).

%%--------------------------------------------------------------------
%% @doc
%% Constructs a file record for given FileId and ModelName. ModelName can be
%% one of: file, file-shared, file-public.
%% @end
%%--------------------------------------------------------------------
-spec file_record(ModelName :: binary(), SessionId :: session:id(),
    fslogic_worker:file_guid()) -> {ok, proplists:proplist()}.
file_record(ModelName, SessionId, FileId) ->
    file_record(ModelName, SessionId, FileId, false, 0).


%%--------------------------------------------------------------------
%% @doc
%% Constructs a file record for given ResourceId. The options ChildrenFromCache
%% and ChildrenOffset (valid for dirs only) allow to specify that dir's
%% children should be server from cache and what is the current offset in
%% directory (essentially how many files from the dir are currently displayed
%% in gui). ModelType can be one of: file, file-shared, file-public.
%% @end
%%--------------------------------------------------------------------
-spec file_record(ModelType :: binary(), SessionId :: session:id(),
    ResourceId :: binary(), ChildrenFromCache :: boolean(),
    ChildrenOffset :: non_neg_integer()) -> {ok, proplists:proplist()}.
file_record(<<"file-shared">>, _, <<"containerDir.", ShareId/binary>>, _, _) ->
    {ok, #document{
        value = #od_share{
            name = Name,
            root_file = RootFileId
        }}} = share_logic:get(op_gui_session:get_session_id(), ShareId),
    FileId = file_id:share_guid_to_guid(RootFileId),
    Res = [
        {<<"id">>, <<"containerDir.", ShareId/binary>>},
        {<<"name">>, Name},
        {<<"type">>, <<"dir">>},
        {<<"modificationTime">>, 0},
        {<<"size">>, null},
        {<<"canViewDir">>, true},
        {<<"totalChildrenCount">>, 1},
        {<<"parent">>, null},
        {<<"children">>, [op_gui_utils:ids_to_association(ShareId, FileId)]},
        {<<"filePermission">>, null},
        {<<"share">>, null},
        {<<"provider">>, null},
        {<<"fileProperty">>, null}
    ],
    {ok, Res};

file_record(<<"file-public">>, _, <<"containerDir.", ShareId/binary>>, _, _) ->
    {ok, #document{
        value = #od_share{
            name = Name,
            root_file = RootFile
        }}} = share_logic:get_public_data(?GUEST_SESS_ID, ShareId),
    Res = [
        {<<"id">>, <<"containerDir.", ShareId/binary>>},
        {<<"name">>, Name},
        {<<"type">>, <<"dir">>},
        {<<"modificationTime">>, 0},
        {<<"size">>, null},
        {<<"canViewDir">>, true},
        {<<"totalChildrenCount">>, 1},
        {<<"parent">>, null},
        {<<"children">>, [op_gui_utils:ids_to_association(ShareId, RootFile)]},
        {<<"filePermission">>, null},
        {<<"share">>, null},
        {<<"provider">>, null},
        {<<"fileProperty">>, null}
    ],
    {ok, Res};

file_record(ModelType, SessionId, ResId, ChildrenFromCache, ChildrenLimit) ->
    % Record Ids are different for different file models
    {ShareId, FileId} = case ModelType of
        <<"file">> ->
            {undefined, ResId};
        <<"file-", _/binary>> -> % Covers file-shared and file-public
            op_gui_utils:association_to_ids(ResId)
    end,
    case lfm:stat(SessionId, {guid, FileId}) of
        {error, ?ENOENT} ->
            op_gui_error:report_error(<<"No such file or directory.">>);
        {ok, FileAttr} ->
            #file_attr{
                name = Name,
                type = TypeAttr,
                size = SizeAttr,
                mtime = ModificationTime,
                shares = Shares,
                provider_id = ProviderId
            } = FileAttr,

            {ok, ParentGuid} = lfm:get_parent(
                SessionId, {guid, FileId}
            ),
            Parent = case ModelType of
                <<"file">> ->
                    % Space root dir has no parent (null)
                    case get_user_root_dir_uuid() of
                        ParentGuid -> null;
                        _ -> ParentGuid
                    end;

                <<"file-", _/binary>> -> % Covers file-shared and file-public
                    case Shares of
                        [ShareId] ->
                            % Check if this is the root dir of given share
                            <<"containerDir.", ShareId/binary>>;
                        _ ->
                            op_gui_utils:ids_to_association(ShareId, ParentGuid)
                    end
            end,


            {Type, Size, ChildrenIds, TotalChildrenCount, CanViewDir} = case TypeAttr of
                ?DIRECTORY_TYPE ->
                    try
                        {ChildrenList, TotalCount} = case ChildrenFromCache of
                            false ->
                                ls_dir(SessionId, FileId);
                            true ->
                                fetch_dir_children(FileId, ChildrenLimit)
                        end,
                        {<<"dir">>, null, ChildrenList, TotalCount, true}
                    catch
                        _:_ ->
                            {<<"dir">>, null, [], 0, false}
                    end;
                _ ->
                    {<<"file">>, SizeAttr, [], 0, false}
            end,
            % Depending on model name, convert IDs to associations
            Children = case ModelType of
                <<"file">> ->
                    ChildrenIds;
                <<"file-", _/binary>> ->
                    lists:map(
                        fun(FId) ->
                            op_gui_utils:ids_to_association(ShareId, FId)
                        end, ChildrenIds)
            end,
            % Depending on model name, properly set the ACL field
            FileAcl = case ModelType of
                <<"file">> ->
                    FileId;
                _ ->
                    null
            end,

            % Currently only one share per file is allowed.
            % Share is always null for file-shared and file-public so as not to
            % show nested shares.
            Share = case {ModelType, Shares} of
                {<<"file">>, [ShId]} ->
                    ShId;
                _ ->
                    null
            end,
            {ok, HasCustomMetadata} = lfm:has_custom_metadata(
                SessionId, {guid, FileId}
            ),
            Metadata = case HasCustomMetadata of
                false -> null;
                true -> ResId
            end,
            {ok, CdmiId} = file_id:guid_to_objectid(FileId),
            Res = [
                {<<"id">>, ResId},
                {<<"name">>, Name},
                {<<"type">>, Type},
                {<<"modificationTime">>, ModificationTime},
                {<<"size">>, Size},
                {<<"canViewDir">>, CanViewDir},
                {<<"totalChildrenCount">>, TotalChildrenCount},
                {<<"parent">>, Parent},
                {<<"children">>, Children},
                {<<"filePermission">>, FileAcl},
                {<<"share">>, Share},
                {<<"provider">>, ProviderId},
                {<<"fileProperty">>, Metadata},
                {<<"cdmiObjectId">>, CdmiId}
            ],
            {ok, Res}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file. Automatically adds the file to LS cache of parent dir
%% and pushes the update of parent dir children list to the client.
%% @end
%%--------------------------------------------------------------------
-spec create_file(SessionId :: session:id(), Name :: binary(),
    ParentId :: fslogic_worker:file_guid(), Type :: binary()) ->
    {ok, fslogic_worker:file_guid()} | {error, term()}.
create_file(SessionId, Name, ParentId, Type) ->
    try
        {ok, ParentPath} = lfm:get_file_path(
            SessionId, ParentId),
        Path = filename:join([ParentPath, Name]),
        Result = case Type of
            <<"file">> ->
                lfm:create(SessionId, Path);
            <<"dir">> ->
                lfm:mkdir(SessionId, Path)
        end,
        case Result of
            {ok, FileId} ->
                NewChildrenCount = modify_ls_cache(add, FileId, ParentId),
                {ok, FileData} = file_record(
                    <<"file">>, SessionId, ParentId, true, NewChildrenCount
                ),
                op_gui_async:push_updated(<<"file">>, FileData),
                {ok, FileId};
            {error, ?EPERM} ->
                op_gui_error:report_warning(<<"Permission denied.">>);
            {error, ?EACCES} ->
                op_gui_error:report_warning(<<"Access denied.">>)
        end
    catch Error:Message ->
        ?error_stacktrace(
            "Cannot create file via GUI - ~p:~p", [Error, Message]
        ),
        case Type of
            <<"dir">> ->
                op_gui_error:report_warning(<<"Failed to create new directory.">>);
            <<"file">> ->
                op_gui_error:report_warning(<<"Failed to create new file.">>)
        end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Updates LS cache, should be called after a file has been
%% successfully uploaded.
%% @end
%%--------------------------------------------------------------------
-spec report_file_upload(FileId :: fslogic_worker:file_guid(),
    ParentId :: fslogic_worker:file_guid()) -> ok.
report_file_upload(FileId, ParentId) ->
    modify_ls_cache(add, FileId, ParentId),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Should be called after a whole upload has finished, pushes new parent
%% dir children to the client.
%% @end
%%--------------------------------------------------------------------
-spec report_file_batch_complete(DirId :: fslogic_worker:file_guid()) -> ok.
report_file_batch_complete(DirId) ->
    SessionId = op_gui_session:get_session_id(),
    LsSubCacheName = ls_sub_cache_name(),
    [{{DirId, last_children_count}, LastChCount}] = ets:lookup(
        LsSubCacheName, {DirId, last_children_count}
    ),
    {ok, FileData} = file_record(
        <<"file">>, SessionId, DirId, true, LastChCount
    ),
    op_gui_async:push_updated(<<"file">>, FileData),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Returns directory record with more children than was returned recently.
%% This mechanism allows to dynamically load files when scrolling rather than
%% return the whole list at once.
%% @end
%%--------------------------------------------------------------------
-spec fetch_more_dir_children(SessionId :: session:id(),
    Props :: proplists:proplist()) ->
    {ok, proplists:proplist()} | {error, term()}.
fetch_more_dir_children(SessionId, Props) ->
    DirId = proplists:get_value(<<"dirId">>, Props),
    CurrentChCount = proplists:get_value(<<"currentChildrenCount">>, Props),
    % FileModelType is one of file, file-shared or file-public
    FileModelType = proplists:get_value(<<"fileModelType">>, Props),
    % Check LS chunk size
    {ok, ChChunkSize} = application:get_env(
        ?APP_NAME, gui_file_children_chunk_size
    ),
    {ok, FileData} = file_data_backend:file_record(
        FileModelType, SessionId, DirId, true, CurrentChCount + ChChunkSize
    ),
    NewChCount = proplists:get_value(<<"children">>, FileData),
    op_gui_async:push_updated(FileModelType, FileData),
    {ok, [{<<"newChildrenCount">>, length(NewChCount)}]}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns the GUID of user's root space (the one that contains all spaces).
%% @end
%%--------------------------------------------------------------------
-spec get_user_root_dir_uuid() -> fslogic_worker:file_guid().
get_user_root_dir_uuid() ->
    fslogic_uuid:user_root_dir_guid(op_gui_session:get_user_id()).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Lists all children of given directory, sorts them by name, caches the result
%% and returns the first chunk of file children.
%% @end
%%--------------------------------------------------------------------
-spec ls_dir(SessionId :: session:id(), DirId :: fslogic_worker:file_guid()) ->
    {[fslogic_worker:file_guid()], ChildrenCount :: non_neg_integer()}.
ls_dir(SessionId, DirId) ->
    % Check LS chunk size
    {ok, ChildrenChunkSize} = application:get_env(
        ?APP_NAME, gui_file_children_chunk_size
    ),
    {ok, LsChunkSize} = application:get_env(?APP_NAME, ls_chunk_size),
    % LS the dir
    Children = ls_dir_chunked(SessionId, DirId, 0, LsChunkSize, []),
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
    ChildrenSortedLength = length(ChildrenSorted),
    FirstChunk = case ChildrenSortedLength > ChildrenChunkSize of
        true ->
            lists:sublist(ChildrenSorted, ChildrenChunkSize);
        false ->
            ChildrenSorted
    end,
    cache_ls_result(DirId, ChildrenSorted, length(FirstChunk)),
    {FirstChunk, ChildrenSortedLength}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Recursively lists children of a directory.
%% @end
%%--------------------------------------------------------------------
-spec ls_dir_chunked(SessionId :: session:id(),
    DirId :: fslogic_worker:file_guid(), Offset :: non_neg_integer(),
    Limit :: non_neg_integer(), Result :: [fslogic_worker:file_guid()]) ->
    [{fslogic_worker:file_guid(), file_meta:name()}].
ls_dir_chunked(SessionId, DirId, Offset, Limit, Result) ->
    {ok, LS} = lfm:ls(SessionId, {guid, DirId}, Offset, Limit),
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
    LastChildrenCount :: non_neg_integer()) -> ok.
cache_ls_result(DirId, ChildrenSorted, LastChildrenCount) ->
    LsSubCacheName = ls_sub_cache_name(),
    % Cache dir's children
    ets:insert(LsSubCacheName, {{DirId, children}, ChildrenSorted}),
    % Cache the total children count
    ets:insert(LsSubCacheName, {{DirId, size}, length(ChildrenSorted)}),
    % Cache the last children count known to the client.
    ets:insert(LsSubCacheName, {{DirId, last_children_count}, LastChildrenCount}),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fetches a list of dir children from LS cache.
%% @end
%%--------------------------------------------------------------------
-spec fetch_dir_children(DirId :: fslogic_worker:file_guid(),
    ChildrenLimit :: non_neg_integer()) ->
    {[fslogic_worker:file_guid()], ChildrenCount :: non_neg_integer()}.
fetch_dir_children(DirId, ChildrenLimit) ->
    LsSubCacheName = ls_sub_cache_name(),
    % Fetch total children count and number of chunks
    [{{DirId, size}, TotalChildrenCount}] = ets:lookup(
        LsSubCacheName, {DirId, size}
    ),
    % Cache the last known client's children count
    ets:insert(
        LsSubCacheName, {{DirId, last_children_count}, ChildrenLimit}
    ),
    [{{DirId, children}, Children}] = ets:lookup(
        LsSubCacheName, {DirId, children}
    ),
    {
        lists:sublist(Children, ChildrenLimit),
        TotalChildrenCount
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Modifies LS cache by adding a newly created file or deleting a recently
%% removed file.
%% New files are always prepended to the beginning of the file list.
%% The file stay at this place till new websocket session is initialized
%% (user refreshes the page).
%% @end
%%--------------------------------------------------------------------
-spec modify_ls_cache(Operation :: add | remove,
    FileId :: fslogic_worker:file_guid(),
    DirId :: fslogic_worker:file_guid()) -> non_neg_integer().
modify_ls_cache(Operation, FileId, DirId) ->
    LsSubCacheName = ls_sub_cache_name(),
    % ETS does not offer atomic updates, so a lock is required here
    acquire_lock(),
    % Try-catch will prevent the process from unexpectedly crashing and taking
    % the lock to the grave.
    NewLastChildrenCount = try
        % Lookup current values in cache
        [{{DirId, children}, CurrentChildren}] = ets:lookup(
            LsSubCacheName, {DirId, children}
        ),
        [{{DirId, last_children_count}, LastChCount}] = ets:lookup(
            LsSubCacheName, {DirId, last_children_count}
        ),
        [{{DirId, size}, CurrentSize}] = ets:lookup(
            LsSubCacheName, {DirId, size}
        ),
        % Modify the values according to operation
        {NewChildren, NewSize, NewLastChCount} = case Operation of
            add ->
                {[FileId | CurrentChildren], CurrentSize + 1, LastChCount + 1};
            remove ->
                {CurrentChildren -- [FileId], CurrentSize - 1, LastChCount - 1}
        end,
        ets:insert(LsSubCacheName, {{DirId, children}, NewChildren}),
        % Update directory size
        ets:insert(LsSubCacheName, {{DirId, size}, NewSize}),
        % Update last known children count
        ets:insert(
            LsSubCacheName, {{DirId, last_children_count}, NewLastChCount}
        ),
        NewLastChCount
    catch Type:Message ->
        ?error_stacktrace("Unexpected error in ~p:modify_ls_cache - ~p:~p", [
            ?MODULE, Type, Message
        ]),
        0
    after
        release_lock()
    end,
    NewLastChildrenCount.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Acquires a lock on ets by synchronizing on a counter {lock, Value}.
%% Tries to increment the value by 2, but if it exceeds 2, the value is reset
%% to 1. Hence, only when current value is 0 the process can increment it to two
%% and 'acquires' the lock. Other processes will wait until the acquiring process
%% releases the lock (zeroes the value).
%% @end
%%--------------------------------------------------------------------
-spec acquire_lock() -> ok.
acquire_lock() ->
    % args are: (Tab, Key, {Pos, Increment, Threshold, SetValue}, Default)
    % If Threshold is exceeded, the value is set to SetValue
    case ets:update_counter(ls_sub_cache_name(), lock, {2, 2, 2, 1}, {lock, 0}) of
        2 ->
            ok;
        1 ->
            timer:sleep(50),
            acquire_lock()
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Releases the lock on ets by zeroing the counter in ets. From now, other
%% processes will be able to acquire the lock.
%% @end
%%--------------------------------------------------------------------
-spec release_lock() -> ok.
release_lock() ->
    ets:update_counter(ls_sub_cache_name(), lock, {2, -2, 0, 0}, {lock, 0}),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolves LS cache ETS identifier for current process.
%% @end
%%--------------------------------------------------------------------
-spec ls_sub_cache_name() -> ets:tid().
ls_sub_cache_name() ->
    WSPid = op_gui_async:get_ws_process(),
    [{WSPid, LsSubCache}] = ets:lookup(?LS_CACHE_ETS, WSPid),
    LsSubCache.