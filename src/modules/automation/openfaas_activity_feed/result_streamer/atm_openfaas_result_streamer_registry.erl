%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% In-memory registry of result streamers in the context of a specific task
%%% execution within a specific workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_result_streamer_registry).
-author("Lukasz Opiola").

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").


%% API
-export([register/4, has/4, deregister/4]).
-export([get_all/2]).
-export([claim_conclusion_orchestration/3]).
-export([await_deregistration_of_all_streamers/0]).
-export([clear/2]).

%% datastore_model callbacks
-export([get_ctx/0]).


-type result_streamer_id() :: binary().
-export_type([result_streamer_id/0]).

% process that orchestrates conclusion of the result stream by prompting all
% active result streamers to finish their work and deregister
-type conclusion_orchestrator() :: pid().

% memory only datastore model holding all registered result streamers
% for given pair of atm workflow/task execution id
-record(atm_openfaas_result_streamer_registry, {
    registry :: #{result_streamer_id() => atm_openfaas_activity_feed_ws_handler:connection_ref()},
    % undefined unless a process claims conclusion orchestration
    conclusion_orchestrator = undefined :: undefined | conclusion_orchestrator()
}).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

-define(id(WorkflowExecutionId, TaskExecutionId), datastore_key:new_from_digest([WorkflowExecutionId, TaskExecutionId])).

-define(AWAIT_DEREGISTRATION_TIMEOUT, timer:seconds(60)).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec register(
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    result_streamer_id(),
    atm_openfaas_activity_feed_ws_handler:connection_ref()
) -> ok.
register(WorkflowExecutionId, TaskExecutionId, ResultStreamerId, ConnectionRef) ->
    Diff = fun(Record = #atm_openfaas_result_streamer_registry{registry = Registry}) ->
        % @TODO VFS-9388 Support reconnects and batch accounting in result streaming
        {ok, Record#atm_openfaas_result_streamer_registry{
            registry = maps:put(ResultStreamerId, ConnectionRef, Registry)
        }}
    end,
    Default = #atm_openfaas_result_streamer_registry{
        registry = #{ResultStreamerId => ConnectionRef}
    },
    ok = ?extract_ok(datastore_model:update(?CTX, ?id(WorkflowExecutionId, TaskExecutionId), Diff, Default)).


-spec has(
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    result_streamer_id(),
    atm_openfaas_activity_feed_ws_handler:connection_ref()
) -> boolean().
has(WorkflowExecutionId, TaskExecutionId, ResultStreamerId, ConnectionRef) ->
    case datastore_model:get(?CTX, ?id(WorkflowExecutionId, TaskExecutionId)) of
        {ok, #document{value = #atm_openfaas_result_streamer_registry{
            registry = #{ResultStreamerId := ConnectionRef}
        }}} ->
            true;
        _ ->
            false
    end.


-spec deregister(
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    result_streamer_id(),
    atm_openfaas_activity_feed_ws_handler:connection_ref()
) -> ok.
deregister(WorkflowExecutionId, TaskExecutionId, ResultStreamerId, ConnectionRef) ->
    Diff = fun(Record = #atm_openfaas_result_streamer_registry{registry = Registry}) ->
        {ok, Record#atm_openfaas_result_streamer_registry{
            registry = case maps:find(ResultStreamerId, Registry) of
                {ok, ConnectionRef} ->
                    % deregister only if the connection ref matches the one currently
                    % used by the result streamer (it may have reconnected and re-registered)
                    maps:remove(ResultStreamerId, Registry);
                _ ->
                    Registry
            end
        }}
    end,
    case datastore_model:update(?CTX, ?id(WorkflowExecutionId, TaskExecutionId), Diff) of
        {error, not_found} ->
            % possible when streaming has already been concluded, but a streamer that
            % missed a finalization signal still tries to deregister
            ok;
        {ok, #document{value = #atm_openfaas_result_streamer_registry{
            registry = UpdatedRegistry,
            conclusion_orchestrator = ConclusionOrchestrator
        }}} ->
            case {maps_utils:is_empty(UpdatedRegistry), ConclusionOrchestrator} of
                {_, undefined} ->
                    ok;
                {false, _} ->
                    ok;
                {true, Pid} ->
                    Pid ! last_streamer_has_deregistered,
                    ok
            end
    end.


-spec get_all(atm_workflow_execution:id(), atm_task_execution:id()) -> [result_streamer_id()].
get_all(WorkflowExecutionId, TaskExecutionId) ->
    {ok, #document{value = #atm_openfaas_result_streamer_registry{
        registry = Registry
    }}} = datastore_model:get(?CTX, ?id(WorkflowExecutionId, TaskExecutionId)),
    maps:keys(Registry).


%%--------------------------------------------------------------------
%% @doc
%% Assesses the state of the registry - whether the stream is ready to be closed.
%% Marks the provided process as the stream conclusion orchestrator, which may then
%% await deregistration of all active streamers (if needed).
%% Does not handle race conditions - expected to be called once, by one process.
%% @end
%%--------------------------------------------------------------------
-spec claim_conclusion_orchestration(
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    conclusion_orchestrator()
) -> no_streamers_ever_registered | all_streamers_deregistered | {active_result_streamers, [result_streamer_id()]}.
claim_conclusion_orchestration(WorkflowExecutionId, TaskExecutionId, ConclusionOrchestrator) ->
    Diff = fun(Record = #atm_openfaas_result_streamer_registry{}) ->
        {ok, Record#atm_openfaas_result_streamer_registry{
            conclusion_orchestrator = ConclusionOrchestrator
        }}
    end,
    case datastore_model:update(?CTX, ?id(WorkflowExecutionId, TaskExecutionId), Diff) of
        {error, not_found} ->
            no_streamers_ever_registered;
        {ok, #document{value = #atm_openfaas_result_streamer_registry{registry = UpdatedRegistry}}} ->
            case maps_utils:is_empty(UpdatedRegistry) of
                true ->
                    all_streamers_deregistered;
                false ->
                    {active_result_streamers, maps:values(UpdatedRegistry)}
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% May only be called by the process provided in claim_conclusion_orchestration/3 as
%% conclusion orchestrator.
%% @end
%%--------------------------------------------------------------------
-spec await_deregistration_of_all_streamers() -> workflow_engine:stream_closing_result().
await_deregistration_of_all_streamers() ->
    receive
        last_streamer_has_deregistered ->
            success
    after ?AWAIT_DEREGISTRATION_TIMEOUT ->
        {failure, ?ERROR_TIMEOUT}
    end.


-spec clear(atm_workflow_execution:id(), atm_task_execution:id()) -> ok.
clear(WorkflowExecutionId, TaskExecutionId) ->
    ok = datastore_model:delete(?CTX, ?id(WorkflowExecutionId, TaskExecutionId)).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.
