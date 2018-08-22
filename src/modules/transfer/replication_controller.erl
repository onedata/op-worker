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

-include_lib("ctool/include/logging.hrl").

%% API
-export([
    mark_active/1, mark_aborting/2,
    mark_completed/1, mark_failed/1, mark_cancelled/1
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
    ?warning("~p:~p - bad request ~p while in status ~p proceeding transfer: ~p", [
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
-spec mark_active(pid()) -> ok.
mark_active(Pid) ->
    Pid ! replication_active,
    ok.


%%-------------------------------------------------------------------
%% @doc
%% Informs replication_controller about aborting transfer.
%% @end
%%-------------------------------------------------------------------
-spec mark_aborting(pid(), term()) -> ok.
mark_aborting(Pid, Reason) ->
    Pid ! {replication_aborting, Reason},
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Stops replication_controller process and marks transfer as failed.
%% @end
%%-------------------------------------------------------------------
-spec mark_failed(pid()) -> ok.
mark_failed(Pid) ->
    Pid ! replication_failed,
    ok.


%%-------------------------------------------------------------------
%% @doc
%% Stops replication_controller process and marks transfer as cancelled.
%% @end
%%-------------------------------------------------------------------
-spec mark_cancelled(pid()) -> ok.
mark_cancelled(Pid) ->
    Pid ! replication_cancelled,
    ok.


%%-------------------------------------------------------------------
%% @doc
%% Stops replication_controller process and marks transfer as completed.
%% @end
%%-------------------------------------------------------------------
-spec mark_completed(pid()) -> ok.
mark_completed(Pid) ->
    Pid ! replication_completed,
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
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
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
handle_cast({start_replication, SessionId, TransferId, FileGuid, Callback,
    EvictSourceReplica}, State
) ->
    flush(),
    case replication_status:handle_enqueued(TransferId) of
        {ok, _} ->
            sync_req:enqueue_file_replication(user_ctx:new(SessionId),
                file_ctx:new_by_guid(FileGuid), undefined, TransferId
            ),
            handle_enqueued(TransferId, Callback, EvictSourceReplica);
        {error, enqueued} ->
            {ok, _} = transfer:set_controller_process(TransferId),
            handle_enqueued(TransferId, Callback, EvictSourceReplica);
        {error, active} ->
            {ok, _} = transfer:set_controller_process(TransferId),
            handle_active(TransferId, Callback, EvictSourceReplica);
        {error, aborting} ->
            {ok, _} = transfer:set_controller_process(TransferId),
            handle_aborting(TransferId);
        {error, S} when S == completed orelse S == cancelled orelse S == failed ->
            ok
    end,
    {noreply, State, hibernate};
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
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
handle_info(_Info, State) ->
    ?log_bad_request(_Info),
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
        replication_active ->
            {ok, _} = replication_status:handle_active(TransferId),
            handle_active(TransferId, Callback, EvictSourceReplica);
        {replication_aborting, Reason} ->
            {ok, _} = replication_status:handle_aborting(TransferId),
            ?error("Replication ~p aborting due to ~p", [TransferId, Reason]),
            handle_aborting(TransferId);
        Msg ->
            ?log_bad_replication_msg(Msg, enqueued, TransferId),
            handle_enqueued(TransferId, Callback, EvictSourceReplica)
    end,
    ok.


-spec handle_active(transfer:id(), transfer:callback(), boolean()) -> ok.
handle_active(TransferId, Callback, EvictSourceReplica) ->
    receive
        replication_completed ->
            {ok, _} = replication_status:handle_completed(TransferId),
            notify_callback(Callback, EvictSourceReplica);
        {replication_aborting, Reason} ->
            {ok, _} = replication_status:handle_aborting(TransferId),
            ?error("Replication ~p aborting due to ~p", [TransferId, Reason]),
            handle_aborting(TransferId);
        Msg ->
            ?log_bad_replication_msg(Msg, active, TransferId),
            handle_active(TransferId, Callback, EvictSourceReplica)
    end,
    ok.


-spec handle_aborting(transfer:id()) -> ok.
handle_aborting(TransferId) ->
    receive
        replication_cancelled ->
            {ok, _} = replication_status:handle_cancelled(TransferId);
        replication_failed ->
            {ok, _} = replication_status:handle_failed(TransferId, false);
        Msg ->
            ?log_bad_replication_msg(Msg, aborting, TransferId),
            handle_aborting(TransferId)
    end,
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Notifies callback about successful replication
%% @end
%%--------------------------------------------------------------------
-spec notify_callback(transfer:callback(),
    EvictSourceReplica :: boolean()) -> ok.
notify_callback(_Callback, true) -> ok;
notify_callback(undefined, false) -> ok;
notify_callback(<<>>, false) -> ok;
notify_callback(Callback, false) ->
    {ok, _, _, _} = http_client:get(Callback).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Flushes message queue. It is necessary because this module is executed
%% by some pool worker, which could have taken care of other replication
%% previously. As such some messages for previous replication may be still
%% in queue.
%% @end
%%--------------------------------------------------------------------
-spec flush() -> ok.
flush() ->
    receive
        replication_completed ->
            flush();
        replication_active ->
            flush();
        {replication_aborting, _Reason} ->
            flush();
        replication_failed ->
            flush();
        replication_cancelled ->
            flush()
    after 0 ->
        ok
    end.
