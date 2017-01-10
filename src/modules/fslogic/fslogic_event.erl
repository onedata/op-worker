%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module responsible for pushing new file's information to sessions.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_event).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("timeouts.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([maybe_emit_file_written/4, maybe_emit_file_read/4, emit_file_truncated/3,
    emit_file_attr_changed/2, emit_file_sizeless_attrs_update/1,
    emit_file_location_changed/2, emit_file_location_changed/3,
    emit_file_perm_changed/1, emit_file_removed/2, emit_file_renamed/3,
    emit_file_renamed_to_client/3, emit_quota_exeeded/0, flush_event_queue/3]).
-export([handle_file_read_events/2, handle_file_written_events/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends a file written event if DoEmit flag is set to true
%% @end
%%--------------------------------------------------------------------
-spec maybe_emit_file_written(DoEmit :: boolean(),fslogic_worker:file_guid(),
    fslogic_blocks:blocks(), session:id()) ->
    ok | {error, Reason :: term()}.
maybe_emit_file_written(false, _FileGuid, _WrittenBlocks, _SessionId) ->
    ok;
maybe_emit_file_written(true, FileGuid, WrittenBlocks, SessionId) ->
    event:emit(#file_written_event{
        file_uuid = FileGuid, blocks = WrittenBlocks
    }, SessionId).

%%--------------------------------------------------------------------
%% @doc
%% Sends a file read event if DoEmit flag is set to true
%% @end
%%--------------------------------------------------------------------
-spec maybe_emit_file_read(DoEmit :: boolean(),fslogic_worker:file_guid(),
    fslogic_blocks:blocks(), session:id()) ->
    ok | {error, Reason :: term()}.
maybe_emit_file_read(false, _FileGuid, _ReadBlocks, _SessionId) ->
    ok;
maybe_emit_file_read(true, FileGuid, ReadBlocks, SessionId) ->
    event:emit(#file_read_event{
        file_uuid = FileGuid, blocks = ReadBlocks
    }, SessionId).

%%--------------------------------------------------------------------
%% @doc
%% Sends a file truncated event if DoEmit flag is set to true
%% @end
%%--------------------------------------------------------------------
-spec emit_file_truncated(fslogic_worker:file_guid(), non_neg_integer(), session:id()) ->
    ok | {error, Reason :: term()}.
emit_file_truncated(FileGuid, Size, SessionId) ->
    event:emit(#file_written_event{
        file_uuid = FileGuid, blocks = [], file_size = Size
    }, SessionId).

%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes to all subscribers except for the ones present
%% in 'ExcludedSessions' list.
%% @end
%%--------------------------------------------------------------------
% TODO - SpaceID may be forwarded from dbsync instead getting from DB
-spec emit_file_attr_changed(fslogic_worker:file(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_attr_changed(FileEntry, ExcludedSessions) ->
    {ok, FileUUID} = file_meta:to_uuid(FileEntry),
    FileGUID = fslogic_uuid:uuid_to_guid(FileUUID),
    case logical_file_manager:stat(?ROOT_SESS_ID, {guid, FileGUID}) of
        {ok, #file_attr{size = Size} = FileAttr} ->
            ?debug("Sending new attributes for file ~p to all sessions except ~p"
            ", size ~p", [FileEntry, ExcludedSessions, Size]),
            event:emit(#file_attr_changed_event{file_attr = FileAttr},
                {exclude, ExcludedSessions});
        {error, Reason} ->
            ?error("Unable to get new attributes for file ~p due to: ~p", [FileEntry, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes excluding size to all subscribers.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_sizeless_attrs_update(fslogic_worker:file()) ->
    ok | {error, Reason :: term()}.
emit_file_sizeless_attrs_update(FileEntry) ->
    {ok, FileUUID} = file_meta:to_uuid(FileEntry),
    FileGUID = fslogic_uuid:uuid_to_guid(FileUUID),
    case logical_file_manager:stat(?ROOT_SESS_ID, {guid, FileGUID}) of
        {ok, #file_attr{} = FileAttr} ->
            ?debug("Sending new times for file ~p to all subscribers", [FileEntry]),
            event:emit(#file_attr_changed_event{
                file_attr = FileAttr#file_attr{size = undefined}
            });
        {error, Reason} ->
            ?error("Unable to get new times for file ~p due to: ~p", [FileEntry, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv emit_file_location_changed(FileEntry, ExcludedSessions, undefined)
%% @end
%%--------------------------------------------------------------------
-spec emit_file_location_changed(fslogic_worker:file(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_location_changed(FileEntry, ExcludedSessions) ->
    emit_file_location_changed(FileEntry, ExcludedSessions, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Sends current file location to all subscribers except for the ones present
%% in 'ExcludedSessions' list. The given range tells what range is requested,
%% so we may fill the gaps within, id defaults to whole file.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_location_changed(fslogic_worker:file(), [session:id()], fslogic_blocks:block() | undefined) ->
    ok | {error, Reason :: term()}.
emit_file_location_changed(FileEntry, ExcludedSessions, Range) ->
    try
        event:emit(#file_location_changed_event{
            file_location = fslogic_file_location:prepare_location_for_client(FileEntry, Range)
        }, {exclude, ExcludedSessions})
    catch
        _:Reason ->
            ?error_stacktrace("Unable to push new location for file ~p due to: ~p", [FileEntry, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sends event informing subscribed client that permissions of file has changed.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_perm_changed(FileUuid :: file_meta:uuid()) ->
    ok | {error, Reason :: term()}.
emit_file_perm_changed(FileUuid) ->
    event:emit(#file_perm_changed_event{
        file_uuid = fslogic_uuid:uuid_to_guid(FileUuid)
    }).

%%--------------------------------------------------------------------
%% @doc
%% Sends event informing subscribed client about file removal.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_removed(FileGUID :: fslogic_worker:file_guid(),
    ExcludedSessions :: [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_removed(FileGUID, ExcludedSessions) ->
    event:emit(#file_removed_event{file_uuid = FileGUID}, {exclude, ExcludedSessions}).

%%--------------------------------------------------------------------
%% @doc
%% Sends an event informing subscribed client about file rename and guid change.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_renamed(TopEntry :: #file_renamed_entry{},
    ChildEntries :: [#file_renamed_entry{}], ExcludedSessions :: [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_renamed(TopEntry, ChildEntries, ExcludedSessions) ->
    event:emit(#file_renamed_event{
        top_entry = TopEntry, child_entries = ChildEntries
    }, {exclude, ExcludedSessions}).

%%--------------------------------------------------------------------
%% @doc
%% Sends an event informing given client about file rename.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_renamed_to_client(file_context:ctx(), file_meta:name(),
    session:id()) -> ok | {error, Reason :: term()}.
emit_file_renamed_to_client(File, NewName, SessionId) ->
    {ok, UserId} = session:get_user_id(SessionId),
    Guid = file_context:get_guid(File),
    {ParentGuid, _File3} = file_context:get_parent_guid(File, UserId),
    event:emit(#file_renamed_event{top_entry = #file_renamed_entry{
        old_uuid = Guid,
        new_uuid = Guid,
        new_parent_uuid = ParentGuid,
        new_name = NewName
    }}, SessionId).

%%--------------------------------------------------------------------
%% @doc
%% Sends a list of currently disabled spaces due to exeeded quota.
%% @end
%%--------------------------------------------------------------------
-spec emit_quota_exeeded() ->
    ok | {error, Reason :: term()}.
emit_quota_exeeded() ->
    BlockedSpaces = space_quota:get_disabled_spaces(),
    ?debug("Sending disabled spaces ~p", [BlockedSpaces]),
    event:emit(#quota_exceeded_event{spaces = BlockedSpaces}).

%%--------------------------------------------------------------------
%% @doc
%% Processes file written events and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_file_written_events(Evts :: [event:event()], Ctx :: maps:map()) ->
    [ok | {error, Reason :: term()}].
handle_file_written_events(Evts, #{session_id := SessId} = Ctx) ->
    Results = lists:map(fun(Ev) ->
        try_handle_event(fun() -> handle_file_written_event(Ev, SessId) end)
    end, Evts),

    case Ctx of
        #{notify := NotifyFun} ->
            NotifyFun(#server_message{message_body = #status{code = ?OK}});
        _ -> ok
    end,

    Results.

%%--------------------------------------------------------------------
%% @doc
%% Processes file read events and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_file_read_events(Evts :: [event:event()], Ctx :: maps:map()) ->
    [ok | {error, Reason :: term()}].
handle_file_read_events(Evts, #{session_id := SessId} = _Ctx) ->
    lists:map(fun(Ev) ->
        try_handle_event(fun() -> handle_file_read_event(Ev, SessId) end)
    end, Evts).

%%--------------------------------------------------------------------
%% @doc
%% Flushes event streams associated with the file written subscription
%% for a given session, uuid and provider_id.
%% @end
%%--------------------------------------------------------------------
-spec flush_event_queue(session:id(), od_provider:id(), file_meta:uuid()) ->
    ok | {error, term()}.
flush_event_queue(SessionId, ProviderId, FileUuid) ->
    case session:is_special(SessionId) of
        true ->
            ok;
        false ->
            RecvRef = event:flush(ProviderId, FileUuid, ?FILE_WRITTEN_SUB_ID,
                self(), SessionId),
            receive
                {RecvRef, Response} ->
                    Response
            after ?DEFAULT_REQUEST_TIMEOUT ->
                {error, timeout}
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private @doc
%% Processes a file written event and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_file_written_event(event:event(), session:id()) ->
    ok | {error, Reason :: term()}.
handle_file_written_event(Evt, SessId) ->
    #file_written_event{counter = Counter, size = Size, blocks = Blocks,
        file_uuid = FileGUID, file_size = FileSize} = Evt,

    {FileUUID, SpaceId} = fslogic_uuid:unpack_guid(FileGUID),
    {ok, #document{value = #session{identity = #user_identity{
        user_id = UserId}}}} = session:get(SessId),
    monitoring_event:emit_file_written_statistics(SpaceId, UserId, Size, Counter),

    UpdatedBlocks = lists:map(fun(#file_block{file_id = FileId, storage_id = StorageId} = Block) ->
        {ValidFileId, ValidStorageId} = file_location:validate_block_data(FileUUID, FileId, StorageId),
        Block#file_block{file_id = ValidFileId, storage_id = ValidStorageId}
    end, Blocks),

    case replica_updater:update(FileUUID, UpdatedBlocks, FileSize, true, undefined) of
        {ok, size_changed} ->
            {ok, #document{value = #session{identity = #user_identity{
                user_id = UserId}}}} = session:get(SessId),
            fslogic_times:update_mtime_ctime({uuid, FileUUID}, UserId),
            fslogic_event:emit_file_attr_changed({uuid, FileUUID}, [SessId]),
            fslogic_event:emit_file_location_changed({uuid, FileUUID}, [SessId]);
        {ok, size_not_changed} ->
            {ok, #document{value = #session{identity = #user_identity{
                user_id = UserId}}}} = session:get(SessId),
            fslogic_times:update_mtime_ctime({uuid, FileUUID}, UserId),
            fslogic_event:emit_file_location_changed({uuid, FileUUID}, [SessId]);
        {error, Reason} ->
            ?error("Unable to update blocks for file ~p due to: ~p.", [FileUUID, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private @doc
%% Processes a file read event and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_file_read_event(event:event(), session:id()) ->
    ok | {error, Reason :: term()}.
handle_file_read_event(Evt, SessId) ->
    #file_read_event{counter = Counter, file_uuid = FileGUID,
        size = Size} = Evt,

    {FileUUID, SpaceId} = fslogic_uuid:unpack_guid(FileGUID),
    {ok, #document{value = #session{identity = #user_identity{
        user_id = UserId}}}} = session:get(SessId),
    monitoring_event:emit_file_read_statistics(SpaceId, UserId, Size, Counter),

    {ok, #document{value = #session{identity = #user_identity{
        user_id = UserId}}}} = session:get(SessId),
    fslogic_times:update_atime({uuid, FileUUID}, UserId).

%%--------------------------------------------------------------------
%% @private @doc
%% Provides an exception-safe environment for event handling.
%% @end
%%--------------------------------------------------------------------
-spec try_handle_event(fun(() -> HandleResult)) -> HandleResult
    when HandleResult :: ok | {error, Reason :: any()}.
try_handle_event(HandleFun) ->
    try HandleFun()
    catch
        Type:Reason -> {error, {Type, Reason}}
    end.