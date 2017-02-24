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

%% API
-export([handle_file_written_events/2, handle_file_read_events/2]).

%%%===================================================================
%%% API
%%%===================================================================

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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
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
            fslogic_times:update_mtime_ctime(FileCtx),
            fslogic_event_emitter:emit_file_attr_changed(FileCtx, [SessId]),
            fslogic_event_emitter:emit_file_location_changed(FileCtx, [SessId]);
        {ok, size_not_changed} ->
            fslogic_times:update_mtime_ctime(FileCtx),
            fslogic_event_emitter:emit_file_location_changed(FileCtx, [SessId])
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
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
    fslogic_times:update_atime(FileCtx).
