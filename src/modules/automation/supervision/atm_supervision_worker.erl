%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles atm workflow executions restart and graceful pause
%%% when stopping Oneprovider.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_supervision_worker).
-author("Bartosz Walkowicz").

-behaviour(worker_plugin_behaviour).

-include("modules/automation/atm_execution.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([supervisor_flags/0, supervisor_children_spec/0]).
-export([try_to_gracefully_stop_atm_workflow_executions/0]).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).


-define(ATM_WORKFLOW_EXECUTIONS_RESTART_MSG, restart_atm_workflow_executions).
-define(ATM_WORKFLOW_EXECUTIONS_RESTART_RETRY_DELAY, op_worker:get_env(
    atm_workflow_executions_restart_retry_delay, 10000
)).

-define(ATM_WORKFLOW_EXECUTIONS_GRACEFUL_STOP_TIMEOUT_SEC, op_worker:get_env(
    atm_workflow_executions_graceful_stop_timeout_sec, 3600
)).

-define(GRACEFUL_STOP_BACKOFF_INITIAL_SEC, 15).
-define(GRACEFUL_STOP_BACKOFF_INCREASE_RATE, 1.1).
-define(GRACEFUL_STOP_BACKOFF_MAX_SEC, 60).


%%%===================================================================
%%% API
%%%===================================================================


-spec supervisor_flags() -> supervisor:sup_flags().
supervisor_flags() ->
    #{strategy => one_for_one, intensity => 10, period => 3600}.


-spec supervisor_children_spec() -> [supervisor:child_spec()].
supervisor_children_spec() ->
    [].


%%--------------------------------------------------------------------
%% @doc
%% This function should be called only when provider is stopping to gracefully
%% stop all running atm workflow executions. Executions that will fail to stop
%% within configured time interval will be abruptly interrupted by provider
%% shutdown.
%% @end
%%--------------------------------------------------------------------
-spec try_to_gracefully_stop_atm_workflow_executions() -> ok.
try_to_gracefully_stop_atm_workflow_executions() ->
    case provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            ?info("Starting automation workflow executions graceful stop procedure..."),

            RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
            CountdownTimer = countdown_timer:start_seconds(
                ?ATM_WORKFLOW_EXECUTIONS_GRACEFUL_STOP_TIMEOUT_SEC
            ),
            case try_to_gracefully_stop_atm_workflow_executions(
                RootUserCtx, SpaceIds, CountdownTimer, ?GRACEFUL_STOP_BACKOFF_INITIAL_SEC
            ) of
                ok ->
                    ?info("automation workflow executions graceful stop procedure finished succesfully.");
                timeout ->
                    ?warning(
                        "Automation workflow executions graceful stop procedure finished "
                        "due to timeout while not all executions were cleanly stopped. "
                        "Leftover ones will be abruptly interrupted."
                    ),
                    stop_atm_workflow_executions(RootUserCtx, SpaceIds, interrupt)
            end;
        {error, _} = Error ->
            ?warning(?autoformat_with_msg(
                "Skipping automation workflow executions graceful stop procedure:", Error
            ))
    end.


%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    schedule_atm_workflow_executions_restart(),

    {ok, #{}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(ping | healthcheck | monitor_streams) ->
    pong | ok | {ok, term()} | errors:error().
handle(ping) ->
    pong;

handle(healthcheck) ->
    ok;

handle(?ATM_WORKFLOW_EXECUTIONS_RESTART_MSG) ->
    restart_atm_workflow_executions(),
    ok;

handle(Request) ->
    ?log_bad_request(Request).


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> ok.
cleanup() ->
    % graceful stopping of executions is triggered in node_manager_plugin:after_listeners_stop/0 callback
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec schedule_atm_workflow_executions_restart() -> ok.
schedule_atm_workflow_executions_restart() ->
    schedule(
        ?ATM_WORKFLOW_EXECUTIONS_RESTART_MSG,
        ?ATM_WORKFLOW_EXECUTIONS_RESTART_RETRY_DELAY
    ).


%% @private
-spec restart_atm_workflow_executions() -> ok.
restart_atm_workflow_executions() ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            ?info("Starting automation workflow executions restart procedure..."),

            lists:foreach(fun restart_atm_workflow_executions/1, SpaceIds),

            ?info("Automation workflow executions restart procedure finished.");
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            schedule_atm_workflow_executions_restart();
        ?ERROR_NO_CONNECTION_TO_ONEZONE ->
            schedule_atm_workflow_executions_restart();
        Error = {error, _} ->
            ?error("Unable to restart automation workflow executions due to: ~tp", [Error])
    catch Type:Reason:Stacktrace ->
        ?error_stacktrace(
            "Unable to restart automation workflow executions due to:~n~tp:~tp",
            [Type, Reason],
            Stacktrace
        )
    end.


%% @private
-spec restart_atm_workflow_executions(od_space:id()) -> ok.
restart_atm_workflow_executions(SpaceId) ->
    CallbackFun = fun(AtmWorkflowExecutionId) ->
        try
            atm_workflow_execution_handler:restart(AtmWorkflowExecutionId)
        catch Type:Reason:Stacktrace ->
            ?error_stacktrace(
                "Unexpected error while trying to restart automation workflow execution ~ts:~n~tp:~tp",
                [AtmWorkflowExecutionId, Type, Reason],
                Stacktrace
            )
        end
    end,

    atm_workflow_execution_api:foreach(SpaceId, ?WAITING_PHASE, CallbackFun),
    atm_workflow_execution_api:foreach(SpaceId, ?ONGOING_PHASE, CallbackFun),
    atm_workflow_execution_api:foreach(SpaceId, ?SUSPENDED_PHASE, CallbackFun).


%% @private
-spec try_to_gracefully_stop_atm_workflow_executions(
    user_ctx:ctx(),
    [od_space:id()],
    countdown_timer:instance(),
    pos_integer()
) ->
    ok | timeout.
try_to_gracefully_stop_atm_workflow_executions(UserCtx, SpaceIds, CountdownTimer, BackoffSec) ->
    case countdown_timer:is_expired(CountdownTimer) of
        true ->
            timeout;
        false ->
            case stop_atm_workflow_executions(UserCtx, SpaceIds, op_worker_stopping) of
                true ->
                    ok;
                false ->
                    timer:sleep(timer:seconds(BackoffSec)),

                    try_to_gracefully_stop_atm_workflow_executions(
                        UserCtx, SpaceIds, CountdownTimer, backoff(BackoffSec)
                    )
            end
    end.


%% @private
-spec backoff(pos_integer()) -> pos_integer().
backoff(BackoffSec) ->
    round(min(?GRACEFUL_STOP_BACKOFF_MAX_SEC, BackoffSec * ?GRACEFUL_STOP_BACKOFF_INCREASE_RATE)).


%% @private
-spec stop_atm_workflow_executions(
    user_ctx:ctx(),
    od_space:id() | [od_space:id()],
    op_worker_stopping | interrupt
) ->
    AllStopped :: boolean().
stop_atm_workflow_executions(UserCtx, SpaceId, StoppingReason) when is_binary(SpaceId) ->
    CallbackFun = fun(AtmWorkflowExecutionId, _) ->
        try
            % There is no problem with calling init_stop on already stopping automation workflow
            % execution as it is idempotent and it may help stop execution that failed to stop
            % previously due to some errors
            atm_workflow_execution_handler:init_stop(UserCtx, AtmWorkflowExecutionId, StoppingReason)
        catch Type:Reason:Stacktrace ->
            ?error_stacktrace(
                "Unexpected error while trying to stop automation workflow execution ~ts:~n~tp:~tp",
                [AtmWorkflowExecutionId, Type, Reason],
                Stacktrace
            )
        end,
        false
    end,

    NoneWaiting = atm_workflow_execution_api:foldl(SpaceId, ?WAITING_PHASE, CallbackFun, true),
    NoneOngoing = atm_workflow_execution_api:foldl(SpaceId, ?ONGOING_PHASE, CallbackFun, true),

    NoneWaiting andalso NoneOngoing;

stop_atm_workflow_executions(UserCtx, SpaceIds, StoppingReason) when is_list(SpaceIds) ->
    lists:all(
        fun(AllStopped) -> AllStopped end,
        [stop_atm_workflow_executions(UserCtx, SpaceId, StoppingReason) || SpaceId <- SpaceIds]
    ).


%% @private
-spec schedule(term(), non_neg_integer()) -> ok.
schedule(Request, Timeout) ->
    erlang:send_after(Timeout, ?MODULE, {sync_timer, Request}),
    ok.
