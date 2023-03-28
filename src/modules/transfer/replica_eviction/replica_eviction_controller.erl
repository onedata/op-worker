%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Manages replica eviction, which include starting the replica eviction and
%%% tracking replica eviction's status.
%%% It will change status on receiving certain messages according to
%%% state machine presented in replica_eviction_status module.
%%% Such gen_server is created for each replica eviction.
%%% @end
%%%--------------------------------------------------------------------
-module(replica_eviction_controller).
-author("Tomasz Lichon").

-behaviour(gen_server).

-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    is_alive/1,
    mark_aborting/2, mark_completed/1, mark_failed/1, mark_cancelled/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-record(state, {
    transfer_id :: transfer:id(),
    session_id :: session:id(),
    file_guid :: fslogic_worker:file_guid(),
    callback :: transfer:callback(),
    space_id :: od_space:id(),
    status :: transfer:subtask_status(),
    supporting_provider_id :: od_provider:id(),
    view_name :: transfer:view_name(),
    query_view_params :: transfer:query_view_params()
}).

-define(whereis(__TRANSFER_ID), global:whereis_name(TransferId)).

%%%===================================================================
%%% API
%%%===================================================================

-spec is_alive(transfer:id()) -> boolean().
is_alive(TransferId) ->
    ?whereis(TransferId) /= undefined.

%%-------------------------------------------------------------------
%% @doc
%% Informs replica_eviction_controller process about aborting replica eviction.
%% @end
%%-------------------------------------------------------------------
-spec mark_aborting(transfer:id(), term()) -> ok.
mark_aborting(TransferId, Reason) ->
    gen_server2:cast(?whereis(TransferId), {replica_eviction_aborting, TransferId, Reason}).

%%-------------------------------------------------------------------
%% @doc
%% Stops replica_eviction_controller process and marks replica eviction as completed.
%% @end
%%-------------------------------------------------------------------
-spec mark_completed(transfer:id()) -> ok.
mark_completed(TransferId) ->
    gen_server2:cast(?whereis(TransferId), {replica_eviction_completed, TransferId}).

%%-------------------------------------------------------------------
%% @doc
%% Stops replica_eviction_controller process and marks transfer as failed.
%% @end
%%-------------------------------------------------------------------
-spec mark_failed(transfer:id()) -> ok.
mark_failed(TransferId) ->
    gen_server2:cast(?whereis(TransferId), {replica_eviction_failed, TransferId}).

%%-------------------------------------------------------------------
%% @doc
%% Stops replica_eviction_controller process and marks transfer as cancelled.
%% @end
%%-------------------------------------------------------------------
-spec mark_cancelled(transfer:id()) -> ok.
mark_cancelled(TransferId) ->
    gen_server2:cast(?whereis(TransferId), {replica_eviction_cancelled, TransferId}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SessionId, TransferId, FileGuid, Callback, SupportingProviderId,
    ViewName, QueryViewParams
]) ->
    ok = gen_server2:cast(self(), start_replica_eviction),
    {ok, #state{
        transfer_id = TransferId,
        session_id = SessionId,
        file_guid = FileGuid,
        callback = Callback,
        space_id = file_id:guid_to_space_id(FileGuid),
        status = ?ENQUEUED_STATUS,
        supporting_provider_id = SupportingProviderId,
        view_name = ViewName,
        query_view_params = QueryViewParams
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_cast(start_replica_eviction, State = #state{
    transfer_id = TransferId,
    supporting_provider_id = SupportingProviderId
}) ->
    flush(),
    case replica_eviction_status:handle_active(TransferId) of
        {ok, TransferDoc} ->
            register_controller_process(TransferId),
            replica_eviction_traverse:start(SupportingProviderId, TransferDoc),
            {noreply, State#state{status = ?ACTIVE_STATUS}};
        {error, ?ACTIVE_STATUS} ->
            register_controller_process(TransferId),
            {noreply, State#state{status = ?ACTIVE_STATUS}};
        {error, ?ABORTING_STATUS} ->
            register_controller_process(TransferId),
            {noreply, State#state{status = ?ABORTING_STATUS}};
        {error, S} when S == ?COMPLETED_STATUS orelse S == ?CANCELLED_STATUS orelse S == ?FAILED_STATUS ->
            {stop, normal, S}
    end;

handle_cast({replica_eviction_completed, TransferId}, State = #state{
    transfer_id = TransferId,
    callback = Callback,
    status = ?ACTIVE_STATUS
}) ->
    {ok, _} = replica_eviction_status:handle_completed(TransferId),
    ?catch_exceptions(notify_callback(Callback, TransferId)),
    {stop, normal, State};

handle_cast({replica_eviction_aborting, TransferId, Reason}, State = #state{
    transfer_id = TransferId,
    file_guid = FileGuid,
    status = ?ACTIVE_STATUS
}) ->
    {ok, _} = replica_eviction_status:handle_aborting(TransferId),
    ?error("Could not evict file replica ~p due to ~p", [FileGuid, Reason]),
    {noreply, State#state{status = ?ABORTING_STATUS}};

% Due to asynchronous nature of transfer_changes, aborting msg can be
% sent several times. In case the controller is already in aborting
% state, it can be safely ignored.
handle_cast({replica_eviction_aborting, TransferId, _Reason}, State = #state{
    transfer_id = TransferId,
    status = ?ABORTING_STATUS
}) ->
    {noreply, State};

handle_cast({replica_eviction_cancelled, TransferId}, State = #state{
    transfer_id = TransferId,
    status = ?ABORTING_STATUS
}) ->
    {ok, _} = replica_eviction_status:handle_cancelled(TransferId),
    {stop, normal, State};

handle_cast({replica_eviction_failed, TransferId}, State = #state{
    transfer_id = TransferId,
    status = ?ABORTING_STATUS
}) ->
    {ok, _} = replica_eviction_status:handle_failed(TransferId, false),
    {stop, normal, State};

handle_cast(Request, State = #state{status = Status}) ->
    case Request of
        {replica_eviction_completed, TransferId} ->
            ?debug("Eviction completed message ignored for transfer ~p", TransferId);
        {replica_eviction_aborting, TransferId, _Reason} ->
            ?debug("Eviction aborting message ignored for transfer ~p", TransferId);
        {replica_eviction_failed, TransferId} ->
            ?debug("Eviction failed message ignored for transfer ~p", TransferId);
        {replica_eviction_cancelled, TransferId} ->
            ?debug("Eviction cancelled message ignored for transfer ~p", TransferId);
        _ ->
            ?warning("~p:~p - bad request ~p while in status ~p", [
                ?MODULE, ?LINE, Request, Status
            ])
    end,
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term().
terminate(_Reason, #state{transfer_id = TransferId}) ->
    unregister_controller_process(TransferId),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) -> {ok, NewState :: #state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec register_controller_process(transfer:id()) -> ok.
register_controller_process(TransferId) ->
    yes = global:register_name(TransferId, self()),
    ok.


%% @private
-spec unregister_controller_process(transfer:id()) -> ok.
unregister_controller_process(TransferId) ->
    global:unregister_name(TransferId).


%%--------------------------------------------------------------------
%% @doc
%% Notifies callback about successful replica eviction
%% @end
%%--------------------------------------------------------------------
-spec notify_callback(transfer:callback(), transfer:id()) -> ok.
notify_callback(undefined, _TransferId) -> ok;
notify_callback(<<>>, _TransferId) -> ok;
notify_callback(Callback, TransferId) ->
    {ok, _, _, _} = http_client:post(Callback, #{}, json_utils:encode(#{
        <<"transferId">> => TransferId
    })).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Flushes message queue. It is necessary because this module is executed
%% by some pool worker, which could have taken care of other replica eviction
%% previously. As such some messages for previous replica eviction may be still
%% in queue.
%% @end
%%--------------------------------------------------------------------
-spec flush() -> ok.
flush() ->
    receive
        replica_eviction_completed ->
            flush();
        {replica_eviction_aborting, _Reason} ->
            flush();
        replica_eviction_failed ->
            flush();
        replica_eviction_cancelled ->
            flush()
    after 0 ->
        ok
    end.
