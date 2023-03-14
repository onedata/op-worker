%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Manages data replication, which include starting the replication and
%%% tracking replication's status.
%%% It will change status on receiving certain messages according to
%%% state machine presented in replication_status module.
%%% @end
%%%--------------------------------------------------------------------
-module(replication_controller).
-author("Tomasz Lichon").

-behaviour(gen_server).

-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    mark_active/2, mark_aborting/3,
    mark_completed/2, mark_failed/2, mark_cancelled/2
]).
%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-record(state, {}).

-type state() :: #state{}.

-define(log_bad_replication_msg(__Req, __Status, __TransferId),
    ?debug("~p:~p - bad request ~p while in status ~p, transfer: ~p", [
        ?MODULE, ?LINE, __Req, __Status, __TransferId
    ])
).


%%%===================================================================
%%% API
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Informs replication_controller about transition to active state.
%% @end
%%-------------------------------------------------------------------
-spec mark_active(pid(), transfer:id()) -> ok.
mark_active(Pid, TransferId) ->
    Pid ! {replication_active, TransferId},
    ok.


%%-------------------------------------------------------------------
%% @doc
%% Informs replication_controller about aborting transfer.
%% @end
%%-------------------------------------------------------------------
-spec mark_aborting(pid(), transfer:id(), term()) -> ok.
mark_aborting(Pid, TransferId, Reason) ->
    Pid ! {replication_aborting, TransferId, Reason},
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Stops replication_controller process and marks transfer as failed.
%% @end
%%-------------------------------------------------------------------
-spec mark_failed(pid(), transfer:id()) -> ok.
mark_failed(Pid, TransferId) ->
    Pid ! {replication_failed, TransferId},
    ok.


%%-------------------------------------------------------------------
%% @doc
%% Stops replication_controller process and marks transfer as cancelled.
%% @end
%%-------------------------------------------------------------------
-spec mark_cancelled(pid(), transfer:id()) -> ok.
mark_cancelled(Pid, TransferId) ->
    Pid ! {replication_cancelled, TransferId},
    ok.


%%-------------------------------------------------------------------
%% @doc
%% Stops replication_controller process and marks transfer as completed.
%% @end
%%-------------------------------------------------------------------
-spec mark_completed(pid(), transfer:id()) -> ok.
mark_completed(Pid, TransferId) ->
    Pid ! {replication_completed, TransferId},
    ok.


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
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init(_Args) ->
    {ok, #state{}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, wrong_request, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast({start_replication, TransferId, Callback, EvictSourceReplica}, State) ->
    case replication_status:handle_enqueued(TransferId) of
        {ok, TransferDoc} ->
            replication_traverse:start(TransferDoc),
            handle_enqueued(TransferId, Callback, EvictSourceReplica);
        {error, ?ENQUEUED_STATUS} ->
            {ok, _} = transfer:set_controller_process(TransferId),
            handle_enqueued(TransferId, Callback, EvictSourceReplica);
        {error, ?ACTIVE_STATUS} ->
            {ok, _} = transfer:set_controller_process(TransferId),
            handle_active(TransferId, Callback, EvictSourceReplica);
        {error, ?ABORTING_STATUS} ->
            {ok, _} = transfer:set_controller_process(TransferId),
            handle_aborting(TransferId);
        {error, S} when S == ?COMPLETED_STATUS orelse S == ?CANCELLED_STATUS orelse S == ?FAILED_STATUS ->
            ok
    end,
    {noreply, State, hibernate};
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info(Info, State) ->
    case Info of
        {replication_completed, TransferId} ->
            ?debug("Replication completed message ignored for transfer ~p", TransferId);
        {replication_active, TransferId} ->
            ?debug("Replication active message ignored for transfer ~p", TransferId);
        {replication_aborting, TransferId, _Reason} ->
            ?debug("Replication aborting message ignored for transfer ~p", TransferId);
        {replication_failed, TransferId} ->
            ?debug("Replication failed message ignored for transfer ~p", TransferId);
        {replication_cancelled, TransferId} ->
            ?debug("Replication cancelled message ignored for transfer ~p", TransferId);
        _ ->
            ?log_bad_request(Info)
    end,
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
    State :: state()) -> term().
terminate(_Reason, #state{}) ->
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) -> {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec handle_enqueued(transfer:id(), transfer:callback(), boolean()) -> ok.
handle_enqueued(TransferId, Callback, EvictSourceReplica) ->
    receive
        {replication_active, TransferId} ->
            {ok, _} = replication_status:handle_active(TransferId),
            handle_active(TransferId, Callback, EvictSourceReplica);
        {replication_aborting, TransferId, Reason} ->
            {ok, _} = replication_status:handle_aborting(TransferId),
            ?error("Replication ~p aborting due to ~p", [TransferId, Reason]),
            handle_aborting(TransferId);
        Msg ->
            ?log_bad_replication_msg(Msg, ?ENQUEUED_STATUS, TransferId),
            handle_enqueued(TransferId, Callback, EvictSourceReplica)
    end,
    ok.


-spec handle_active(transfer:id(), transfer:callback(), boolean()) -> ok.
handle_active(TransferId, Callback, EvictSourceReplica) ->
    receive
        % Due to asynchronous nature of transfer_changes, active msg can be
        % sent several times. In case the controller is already in active state,
        % it can be safely ignored.
        {replication_active, TransferId} ->
            handle_active(TransferId, Callback, EvictSourceReplica);
        {replication_completed, TransferId} ->
            {ok, _} = replication_status:handle_completed(TransferId),
            notify_callback(Callback, EvictSourceReplica, TransferId);
        {replication_aborting, TransferId, Reason} ->
            {ok, _} = replication_status:handle_aborting(TransferId),
            ?error("Replication ~p aborting due to ~p", [TransferId, Reason]),
            handle_aborting(TransferId);
        Msg ->
            ?log_bad_replication_msg(Msg, ?ACTIVE_STATUS, TransferId),
            handle_active(TransferId, Callback, EvictSourceReplica)
    end,
    ok.


-spec handle_aborting(transfer:id()) -> ok.
handle_aborting(TransferId) ->
    receive
        % Due to asynchronous nature of transfer_changes, aborting msg can be
        % sent several times. In case the controller is already in aborting
        % state, it can be safely ignored.
        {replication_aborting, TransferId, _Reason} ->
            handle_aborting(TransferId);
        {replication_cancelled, TransferId} ->
            {ok, _} = replication_status:handle_cancelled(TransferId);
        {replication_failed, TransferId} ->
            {ok, _} = replication_status:handle_failed(TransferId, false);
        Msg ->
            ?log_bad_replication_msg(Msg, ?ABORTING_STATUS, TransferId),
            handle_aborting(TransferId)
    end,
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Notifies callback about successful replication
%% @end
%%--------------------------------------------------------------------
-spec notify_callback(transfer:callback(), EvictSourceReplica :: boolean(),
    transfer:id()) -> ok.
notify_callback(_Callback, true, _TransferId) -> ok;
notify_callback(undefined, false, _TransferId) -> ok;
notify_callback(<<>>, false, _TransferId) -> ok;
notify_callback(Callback, false, TransferId) ->
    {ok, _, _, _} = http_client:post(Callback, #{}, json_utils:encode(#{
        <<"transferId">> => TransferId
    })).