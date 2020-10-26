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
    NewEvt#file_written_event{
        counter = OldEvt#file_written_event.counter + NewEvt#file_written_event.counter,
        size = OldEvt#file_written_event.size + NewEvt#file_written_event.size,
        blocks = fslogic_blocks:aggregate(
            OldEvt#file_written_event.blocks,
            NewEvt#file_written_event.blocks
        )
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
            fully_replicated = case NewAttr#file_attr.fully_replicated of
                undefined -> OldAttr#file_attr.fully_replicated;
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

    replica_synchronizer:force_flush_events(file_ctx:get_uuid_const(FileCtx)),
    case replica_synchronizer:update_replica(FileCtx, Blocks, FileSize, true) of
        {ok, #{location_changes := LocationChangedEvents} = UpdateDescription} ->
            fslogic_times:update_mtime_ctime(FileCtx),
            case UpdateDescription of
                #{replica_status_changed := true, size_changed := SizeChanged} ->
                    fslogic_event_emitter:emit_file_attr_changed_with_replication_status(FileCtx, SizeChanged, [SessId]);
                #{size_changed := true} ->
                    fslogic_event_emitter:emit_file_attr_changed(FileCtx, [SessId]);
                _ ->
                    ok
            end,
            fslogic_event_emitter:emit_file_locations_changed(LocationChangedEvents, [SessId]);
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
    {ok, UserId} = session:get_user_id(SessId),
    monitoring_event_emitter:emit_file_read_statistics(SpaceId, UserId, Size, Counter),
    fslogic_times:update_atime(FileCtx).
