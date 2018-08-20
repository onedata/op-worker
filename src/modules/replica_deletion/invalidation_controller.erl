%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Manages replica invalidation, which include starting the invalidation and
%%% tracking invalidation's status.
%%% It will change status on receiving certain messages according to
%%% state machine presented in invalidation_status module.
%%% Such gen_server is created for each replica invalidation.
%%% @end
%%%--------------------------------------------------------------------
-module(invalidation_controller).
-author("Tomasz Lichon").

-behaviour(gen_server).

-include_lib("ctool/include/logging.hrl").

%% API
-export([mark_aborting/2, mark_completed/1, mark_failed/1, mark_cancelled/1]).

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
    status :: transfer:status(),
    supporting_provider_id :: od_provider:id()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Informs invalidation_controller process about aborting invalidation.
%% @end
%%-------------------------------------------------------------------
-spec mark_aborting(pid(), term()) -> ok.
mark_aborting(Pid, Reason) ->
    gen_server2:cast(Pid, {invalidation_aborting, Reason}).

%%-------------------------------------------------------------------
%% @doc
%% Stops invalidation_controller process and marks invalidation as completed.
%% @end
%%-------------------------------------------------------------------
-spec mark_completed(pid()) -> ok.
mark_completed(Pid) ->
    gen_server2:cast(Pid, invalidation_completed).

%%-------------------------------------------------------------------
%% @doc
%% Stops invalidation_controller process and marks transfer as failed.
%% @end
%%-------------------------------------------------------------------
-spec mark_failed(pid()) -> ok.
mark_failed(Pid) ->
    gen_server2:cast(Pid, invalidation_failed).

%%-------------------------------------------------------------------
%% @doc
%% Stops invalidation_controller process and marks transfer as cancelled.
%% @end
%%-------------------------------------------------------------------
-spec mark_cancelled(pid()) -> ok.
mark_cancelled(Pid) ->
    gen_server2:cast(Pid, invalidation_cancelled).


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
init([SessionId, TransferId, FileGuid, Callback, SupportingProviderId]) ->
    ok = gen_server2:cast(self(), start_invalidation),
    {ok, #state{
        transfer_id = TransferId,
        session_id = SessionId,
        file_guid = FileGuid,
        callback = Callback,
        space_id = fslogic_uuid:guid_to_space_id(FileGuid),
        status = enqueued,
        supporting_provider_id = SupportingProviderId
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
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
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
handle_cast(start_invalidation, State = #state{
    transfer_id = TransferId,
    session_id = SessionId,
    file_guid = FileGuid,
    supporting_provider_id = SupportingProviderId
}) ->
    flush(),
    case invalidation_status:handle_active(TransferId) of
        {ok, _} ->
            UserCtx = user_ctx:new(SessionId),
            FileCtx = file_ctx:new_by_guid(FileGuid),
            invalidation_req:enqueue_file_invalidation(UserCtx, FileCtx, SupportingProviderId, TransferId),
            {noreply, State#state{status = active}};
        {error, active} ->
            {ok, _} = transfer:set_controller_process(TransferId),
            {noreply, State#state{status = active}};
        {error, aborting} ->
            {ok, _} = transfer:set_controller_process(TransferId),
            {noreply, State#state{status = aborting}};
        {error, S} when S == completed orelse S == cancelled orelse S == failed ->
            {stop, normal, S}
    end;

handle_cast(invalidation_completed, State = #state{
    transfer_id = TransferId,
    callback = Callback,
    status = active
}) ->
    {ok, _} = invalidation_status:handle_completed(TransferId),
    notify_callback(Callback),
    {stop, normal, State};

handle_cast({invalidation_aborting, Reason}, State = #state{
    transfer_id = TransferId,
    file_guid = FileGuid,
    status = active
}) ->
    {ok, _} = invalidation_status:handle_aborting(TransferId),
    ?error_stacktrace("Could not invalidate file ~p due to ~p", [
        FileGuid, Reason
    ]),
    {noreply, State#state{status = aborting}};

handle_cast(invalidation_cancelled, State = #state{
    transfer_id = TransferId,
    status = aborting
}) ->
    {ok, _} = invalidation_status:handle_cancelled(TransferId),
    {stop, normal, State};

handle_cast(invalidation_failed, State = #state{
    transfer_id = TransferId,
    status = aborting
}) ->
    {ok, _} = invalidation_status:handle_failed(TransferId, false),
    {stop, normal, State};

handle_cast(Request, State = #state{status = Status}) ->
    ?warning("~p:~p - bad request ~p while in status ~p", [
        ?MODULE, ?LINE, Request, Status
    ]),
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
    State :: #state{}) -> term().
terminate(_Reason, _State) ->
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


%%--------------------------------------------------------------------
%% @doc
%% Notifies callback about successful invalidation
%% @end
%%--------------------------------------------------------------------
-spec notify_callback(transfer:callback()) -> ok.
notify_callback(undefined) -> ok;
notify_callback(<<>>) -> ok;
notify_callback(Callback) ->
    {ok, _, _, _} = http_client:get(Callback).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Flushes message queue. It is necessary because this module is executed
%% by some pool worker, which could have taken care of other invalidation
%% previously. As such some messages for previous invalidation may be still
%% in queue.
%% @end
%%--------------------------------------------------------------------
flush() ->
    receive
        invalidation_completed ->
            flush();
        {invalidation_aborting, _Reason} ->
            flush();
        invalidation_failed ->
            flush();
        invalidation_cancelled ->
            flush()
    after 0 ->
        ok
    end.
