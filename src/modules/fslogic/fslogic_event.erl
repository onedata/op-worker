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
    emit_file_attr_changed/2, emit_sizeless_file_attrs_changed/1,
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
-spec maybe_emit_file_written(fslogic_worker:file_guid(),
    fslogic_blocks:blocks(), session:id(), DoEmit :: boolean()) ->
    ok | {error, Reason :: term()}.
maybe_emit_file_written(_FileGuid, _WrittenBlocks, _SessionId, false) ->
    ok;
maybe_emit_file_written(FileGuid, WrittenBlocks, SessionId, true) ->
    event:emit(#file_written_event{
        file_uuid = FileGuid,
        blocks = WrittenBlocks
    }, SessionId).

%%--------------------------------------------------------------------
%% @doc
%% Sends a file read event if DoEmit flag is set to true
%% @end
%%--------------------------------------------------------------------
-spec maybe_emit_file_read(fslogic_worker:file_guid(),
    fslogic_blocks:blocks(), session:id(), DoEmit :: boolean()) ->
    ok | {error, Reason :: term()}.
maybe_emit_file_read(_FileGuid, _ReadBlocks, _SessionId, false) ->
    ok;
maybe_emit_file_read(FileGuid, ReadBlocks, SessionId, true) ->
    event:emit(#file_read_event{
        file_uuid = FileGuid,
        blocks = ReadBlocks
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
        file_uuid = FileGuid,
        blocks = [],
        file_size = Size
    }, SessionId).

%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes to all subscribers except for the ones present
%% in 'ExcludedSessions' list.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_attr_changed(file_ctx:ctx(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_attr_changed(FileCtx, ExcludedSessions) ->
    #fuse_response{fuse_response = #file_attr{} = FileAttr} =
        attr_req:get_file_attr_insecure(user_ctx:new(?ROOT_SESS_ID), FileCtx),
    event:emit(#file_attr_changed_event{file_attr = FileAttr},
        {exclude, ExcludedSessions}).

%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes excluding size to all subscribers.
%% @end
%%--------------------------------------------------------------------
-spec emit_sizeless_file_attrs_changed(file_ctx:ctx()) ->
    ok | {error, Reason :: term()}.
emit_sizeless_file_attrs_changed(FileCtx) ->
    #fuse_response{fuse_response = #file_attr{} = FileAttr} =
        attr_req:get_file_attr_insecure(user_ctx:new(?ROOT_SESS_ID), FileCtx),
    event:emit(#file_attr_changed_event{
        file_attr = FileAttr#file_attr{size = undefined}
    }).

%%--------------------------------------------------------------------
%% @doc
%% @equiv emit_file_location_changed(FileEntry, ExcludedSessions, undefined)
%% @end
%%--------------------------------------------------------------------
-spec emit_file_location_changed(file_ctx:ctx(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_location_changed(FileCtx, ExcludedSessions) ->
    emit_file_location_changed(FileCtx, ExcludedSessions, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Sends current file location to all subscribers except for the ones present
%% in 'ExcludedSessions' list. The given range tells what range is requested,
%% so we may fill the gaps within, id defaults to whole file.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_location_changed(file_ctx:ctx(), [session:id()],
    fslogic_blocks:block() | undefined) -> ok | {error, Reason :: term()}.
emit_file_location_changed(FileCtx, ExcludedSessions, Range) ->
    event:emit(#file_location_changed_event{
        file_location = fslogic_file_location:prepare_location_for_client(FileCtx, Range)
    }, {exclude, ExcludedSessions}).

%%--------------------------------------------------------------------
%% @doc
%% Sends event informing subscribed client that permissions of file has changed.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_perm_changed(file_ctx:ctx()) -> ok | {error, Reason :: term()}.
emit_file_perm_changed(FileCtx) ->
    event:emit(#file_perm_changed_event{
        file_uuid = file_ctx:get_guid_const(FileCtx)
    }).

%%--------------------------------------------------------------------
%% @doc
%% Sends event informing subscribed client about file removal.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_removed(file_ctx:ctx(), ExcludedSessions :: [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_removed(FileCtx, ExcludedSessions) ->
    event:emit(#file_removed_event{file_uuid = file_ctx:get_guid_const(FileCtx)},
        {exclude, ExcludedSessions}).

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
-spec emit_file_renamed_to_client(file_ctx:ctx(), file_meta:name(),
    session:id()) -> ok | {error, Reason :: term()}.
emit_file_renamed_to_client(FileCtx, NewName, SessionId) ->
    {ok, UserId} = session:get_user_id(SessionId),
    Guid = file_ctx:get_guid_const(FileCtx),
    {ParentGuid, _FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserId),
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
-spec handle_file_written_events(Evts :: [event:event()], UserCtxMap :: maps:map()) ->
    [ok | {error, Reason :: term()}].
handle_file_written_events(Evts, #{session_id := SessId} = UserCtxMap) ->
    Results = lists:map(fun(Ev) ->
        handle_file_written_event(Ev, SessId)
    end, Evts),

    case UserCtxMap of
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
-spec handle_file_read_events(Evts :: [event:event()], UserCtxMap :: maps:map()) ->
    [ok | {error, Reason :: term()}].
handle_file_read_events(Evts, #{session_id := SessId} = _UserCtxMap) ->
    lists:map(fun(Ev) ->
        handle_file_read_event(Ev, SessId)
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
-spec handle_file_written_event(event:event(), session:id()) -> ok.
handle_file_written_event(#file_written_event{
    counter = Counter,
    size = Size,
    blocks = Blocks,
    file_uuid = FileGuid,
    file_size = FileSize
}, SessId) ->
    FileCtx = file_ctx:new_by_guid(FileGuid),
    {ok, UserId} = session:get_user_id(SessId),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    monitoring_event:emit_file_written_statistics(SpaceId, UserId, Size, Counter),

    case replica_updater:update(FileCtx, Blocks, FileSize, true) of
        {ok, size_changed} ->
            fslogic_times:update_mtime_ctime(FileCtx, UserId),
            fslogic_event:emit_file_attr_changed(FileCtx, [SessId]),
            fslogic_event:emit_file_location_changed(FileCtx, [SessId]);
        {ok, size_not_changed} ->
            fslogic_times:update_mtime_ctime(FileCtx, UserId),
            fslogic_event:emit_file_location_changed(FileCtx, [SessId])
    end.

%%--------------------------------------------------------------------
%% @private @doc
%% Processes a file read event and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_file_read_event(event:event(), session:id()) ->
    ok | {error, Reason :: term()}.
handle_file_read_event(#file_read_event{
    counter = Counter,
    file_uuid = FileGuid,
    size = Size
}, SessId) ->
    FileCtx = file_ctx:new_by_guid(FileGuid),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, UserId} = session:get_user_id(SessId),
    monitoring_event:emit_file_read_statistics(SpaceId, UserId, Size, Counter),
    fslogic_times:update_atime(FileCtx, UserId).