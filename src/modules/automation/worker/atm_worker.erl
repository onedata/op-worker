%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handle graceful atm workflow executions shutdown when stopping
%%% Oneprovider.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_worker).
-author("Bartosz Walkowicz").

-behaviour(worker_plugin_behaviour).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([supervisor_flags/0, supervisor_children_spec/0]).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).


-define(RESTART_ATM_WORKFLOW_EXECUTIONS_MSG, restart_atm_workflow_executions).
-define(RESTART_ATM_WORKFLOW_EXECUTIONS_RETRY_DELAY, op_worker:get_env(
    restart_atm_workflow_executions_retry_delay, 10000
)).


%%%===================================================================
%%% API
%%%===================================================================


-spec supervisor_flags() -> supervisor:sup_flags().
supervisor_flags() ->
    #{strategy => one_for_one, intensity => 1000, period => 3600}.


-spec supervisor_children_spec() -> [supervisor:child_spec()].
supervisor_children_spec() ->
    [].


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

handle(?RESTART_ATM_WORKFLOW_EXECUTIONS_MSG) ->
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
    % @TODO VFS-9846 Implement graceful atm workflow execution shutdown when stopping op
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec schedule_atm_workflow_executions_restart() -> ok.
schedule_atm_workflow_executions_restart() ->
    schedule(
        ?RESTART_ATM_WORKFLOW_EXECUTIONS_MSG,
        ?RESTART_ATM_WORKFLOW_EXECUTIONS_RETRY_DELAY
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function should be called only after provider restart to handle
%% stale (processes handling execution no longer exists) workflows.
%% All waiting and ongoing workflow executions for given space are:
%% a) terminated as ?CRASHED/?CANCELLED/?FAILED if execution was already stopping
%% b) terminated as ?INTERRUPTED otherwise (running execution was interrupted by
%%    provider shutdown). Such executions will be resumed.
%% @end
%%--------------------------------------------------------------------
-spec restart_atm_workflow_executions() -> ok.
restart_atm_workflow_executions() ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            ?info("Starting atm_workflow_executions restart procedure..."),

            lists:foreach(fun restart_atm_workflow_executions/1, SpaceIds),

            ?info("atm_workflow_executions restart procedure finished.");
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            schedule_atm_workflow_executions_restart();
        ?ERROR_NO_CONNECTION_TO_ONEZONE ->
            schedule_atm_workflow_executions_restart();
        Error = {error, _} ->
            ?error("Unable to restart atm workflow executions due to: ~p", [Error])
    catch Class:Reason:Stacktrace ->
        ?error_stacktrace(
            "Unable to restart atm workflow executions due to: ~p",
            [{Class, Reason}],
            Stacktrace
        )
    end.


%% @private
-spec restart_atm_workflow_executions(od_space:id()) -> ok.
restart_atm_workflow_executions(SpaceId) ->
    CallbackFun = fun(AtmWorkflowExecutionId) ->
        try
            atm_workflow_execution_handler:on_provider_restart(AtmWorkflowExecutionId)
        catch Type:Reason:Stacktrace ->
            ?atm_examine_error(Type, Reason, Stacktrace)
        end
    end,

    atm_workflow_execution_api:foreach(SpaceId, ?WAITING_PHASE, CallbackFun),
    atm_workflow_execution_api:foreach(SpaceId, ?ONGOING_PHASE, CallbackFun).


%% @private
-spec schedule(term(), non_neg_integer()) -> ok.
schedule(Request, Timeout) ->
    erlang:send_after(Timeout, ?MODULE, {sync_timer, Request}),
    ok.
