%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handlers for fslogic events.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_event_handler).
-author("Tomasz Lichon").

-include("modules/events/definitions.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API Handling
-export([handle_file_written_events/2, handle_file_read_events/2]).
%% API Aggregation
-export([aggregate_file_read_events/2, aggregate_file_written_events/2,
    aggregate_attr_changed_events/2]).

%%%===================================================================
%%% API Handling
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Processes file written events and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_file_written_events(Evts :: [event:type()], UserCtxMap :: map()) ->
    [ok | {error, Reason :: term()}].
handle_file_written_events(Evts, #{session_id := SessId} = UserCtxMap) ->
    Results = lists:map(fun(Evt) ->
        handle_file_written_event(Evt, SessId)
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
-spec handle_file_read_events(Evts :: [event:type()], UserCtxMap :: map()) ->
    [ok | {error, Reason :: term()}].
handle_file_read_events(Evts, #{session_id := SessId} = _UserCtxMap) ->
    lists:map(fun(Ev) ->
        handle_file_read_event(Ev, SessId)
    end, Evts).

%%%===================================================================
%%% API Aggregation
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Provides default implementation for a file read events aggregation rule.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_file_read_events(OldEvt :: event:type(), NewEvt :: event:type()) ->
    NewEvt :: event:type().
aggregate_file_read_events(OldEvt, NewEvt) ->
    NewEvt#file_read_event{
        counter = OldEvt#file_read_event.counter + NewEvt#file_read_event.counter,
        size = OldEvt#file_read_event.size + NewEvt#file_read_event.size,
        blocks = fslogic_blocks:aggregate(
            OldEvt#file_read_event.blocks,
            NewEvt#file_read_event.blocks
        )
    }.

%%--------------------------------------------------------------------
%% @doc
%% Provides default implementation for a file written events aggregation rule.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_file_written_events(OldEvt :: event:type(), NewEvt :: event:type()) ->
    NewEvt :: event:type().
aggregate_file_written_events(OldEvt, NewEvt) ->
    {FileSize, OldBlocks} = case NewEvt#file_written_event.file_size of
        undefined ->
            {OldEvt#file_written_event.file_size, OldEvt#file_written_event.blocks};
        NewSize ->
            % file_size is set only during file truncate - filter blocks written before truncate
            {NewSize, fslogic_blocks:filter_or_trim_truncated(OldEvt#file_written_event.blocks, NewSize)}
    end,

    NewEvt#file_written_event{
        counter = OldEvt#file_written_event.counter + NewEvt#file_written_event.counter,
        file_size = FileSize,
        size = OldEvt#file_written_event.size + NewEvt#file_written_event.size,
        blocks = fslogic_blocks:aggregate(OldBlocks, NewEvt#file_written_event.blocks)
    }.

%%--------------------------------------------------------------------
%% @doc
%% Provides default implementation for a attr changed events aggregation rule.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_attr_changed_events(OldEvt :: event:type(), NewEvt :: event:type()) ->
    NewEvt :: event:type().
aggregate_attr_changed_events(OldEvent, NewEvent) ->
    OldAttr = OldEvent#file_attr_changed_event.file_attr,
    NewAttr = NewEvent#file_attr_changed_event.file_attr,
    OldEvent#file_attr_changed_event{
        file_attr = NewAttr#file_attr{
            size = case NewAttr#file_attr.size of
                undefined -> OldAttr#file_attr.size;
                X -> X
            end,
            is_fully_replicated = case NewAttr#file_attr.is_fully_replicated of
                undefined -> OldAttr#file_attr.is_fully_replicated;
                FR -> FR
            end
        }
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a file written event and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_file_written_event(event:type(), session:id()) -> ok.
handle_file_written_event(#file_written_event{
    counter = Counter,
    size = Size,
    blocks = Blocks,
    file_guid = FileGuid,
    file_size = FileSize
}, SessId) ->
    FileCtx = file_ctx:new_by_guid(FileGuid),
    {ok, UserId} = session:get_user_id(SessId),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    monitoring_event_emitter:emit_file_written_statistics(SpaceId, UserId, Size, Counter),

    replica_synchronizer:force_flush_events(file_ctx:get_logical_uuid_const(FileCtx)),
    case replica_synchronizer:update_replica(FileCtx, Blocks, FileSize, true) of
        {ok, ReplicaUpdateResult} ->
            LocationChanges = replica_updater:get_location_changes(ReplicaUpdateResult),
            SizeChanged = replica_updater:has_size_changed(ReplicaUpdateResult),
            ReplicaStatusChanged = replica_updater:has_replica_status_changed(ReplicaUpdateResult),
            fslogic_times:report_change(FileCtx, [mtime, ctime]),
            case {ReplicaStatusChanged, SizeChanged} of
                {true, _} ->
                    fslogic_event_emitter:emit_file_attr_changed_with_replication_status(FileCtx, SizeChanged, [SessId]);
                {false, true} ->
                    fslogic_event_emitter:emit_file_attr_changed(FileCtx, [SessId]);
                _ ->
                    ok
            end,
            fslogic_event_emitter:emit_file_locations_changed(LocationChanges, [SessId]);
        {error, not_found} ->
            ?debug("Handling file_written_event for file ~p failed because file_location was not found.",
                [FileGuid]),
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a file read event and returns a response.
%% @end
%%--------------------------------------------------------------------
-spec handle_file_read_event(event:type(), session:id()) ->
    ok | {error, Reason :: term()}.
handle_file_read_event(#file_read_event{
    counter = Counter,
    file_guid = FileGuid,
    size = Size
}, SessId) ->
    FileCtx = file_ctx:new_by_guid(FileGuid),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    % TODO VFS-7851 handle case when get_user_id returns not_found
    case session:get_user_id(SessId) of
        {ok, UserId} ->
            monitoring_event_emitter:emit_file_read_statistics(SpaceId, UserId, Size, Counter);
        ?ERROR_NOT_FOUND ->
            ok
    end,
    fslogic_times:report_change(FileCtx, [atime]).
