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
-include_lib("ctool/include/logging.hrl").

%% API
-export([emit_file_attr_changed/2, emit_file_sizeless_attrs_update/1,
    emit_file_location_changed/2, emit_file_location_changed/3,
    emit_file_perm_changed/1, emit_file_removed/2, emit_file_renamed/3,
    emit_file_renamed/4, emit_quota_exeeded/0]).
-export([handle_file_read_events/2, handle_file_written_events/2]).

%%%===================================================================
%%% API
%%%===================================================================

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
%% Send event informing subscribed client that permissions of file has changed.
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
%% Send event informing subscribed client about file removal.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_removed(FileGUID :: fslogic_worker:file_guid(),
    ExcludedSessions :: [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_removed(FileGUID, ExcludedSessions) ->
    event:emit(#file_removed_event{file_uuid = FileGUID}, {exclude, ExcludedSessions}).

%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about file rename and guid change.
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
%% Send event informing given client about file rename.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_renamed(file_meta:uuid(), od_space:id(), file_meta:name(),
    session:id()) -> ok | {error, Reason :: term()}.
emit_file_renamed(FileUUID, SpaceId, NewName, SessionId) ->
    {ok, UserId} = session:get_user_id(SessionId),
    ParentUUID = fslogic_uuid:parent_uuid({uuid, FileUUID}, UserId),
    ParentGUID = fslogic_uuid:uuid_to_guid(ParentUUID, SpaceId),
    FileGUID = fslogic_uuid:uuid_to_guid(FileUUID, SpaceId),
    event:emit(#file_renamed_event{top_entry = #file_renamed_entry{
        old_uuid = FileGUID,
        new_uuid = FileGUID,
        new_parent_uuid = ParentGUID,
        new_name = NewName
    }}, SessionId).

%%--------------------------------------------------------------------
%% @doc
%% Sends list of currently disabled spaces due to exeeded quota.
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
%% Processes write events and returns a response.
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
%% Processes read events and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_file_read_events(Evts :: [event:event()], Ctx :: maps:map()) ->
    [ok | {error, Reason :: term()}].
handle_file_read_events(Evts, #{session_id := SessId} = _Ctx) ->
    lists:map(fun(Ev) ->
        try_handle_event(fun() -> handle_file_read_event(Ev, SessId) end)
    end, Evts).

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
%% Processes a file ead event and returns a response.
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


-spec try_handle_event(fun(() -> HandleResult)) -> HandleResult
    when HandleResult :: ok | {error, Reason :: any()}.
try_handle_event(HandleFun) ->
    try HandleFun()
    catch
        Type:Reason -> {error, {Type, Reason}}
    end.