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
-export([register/5, qualify_chunk_report/5, mark_chunk_report_processed/4, deregister/4]).
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

-record(streamer_state, {
    connection_ref :: atm_openfaas_activity_feed_ws_handler:connection_ref(),
    processed_reports = gb_sets:new() :: gb_sets:set(atm_openfaas_result_streamer_report:id())
}).
-type streamer_state() :: #streamer_state{}.

% memory only datastore model holding all registered result streamers
% for given pair of atm workflow/task execution id
-record(atm_openfaas_result_streamer_registry, {
    streamer_states :: #{result_streamer_id() => streamer_state()},
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

%% NOTE: handles checks for duplicate reports internally
-spec register(
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    result_streamer_id(),
    atm_openfaas_activity_feed_ws_handler:connection_ref(),
    atm_openfaas_result_streamer_report:id()
) -> stream_open | conclusion_ongoing.
register(WorkflowExecutionId, TaskExecutionId, ResultStreamerId, ConnectionRef, ReportId) ->
    Diff = fun(Registry = #atm_openfaas_result_streamer_registry{streamer_states = StreamerStates}) ->
        PreviousState = maps:get(ResultStreamerId, StreamerStates, #streamer_state{connection_ref = ConnectionRef}),
        case gb_sets:is_element(ReportId, PreviousState#streamer_state.processed_reports) of
            true ->
                {ok, Registry};
            false ->
                {ok, Registry#atm_openfaas_result_streamer_registry{
                    streamer_states = StreamerStates#{
                        % in case of re-registration, the connection ref is updated and the previous one
                        % is considered obsolete (stale reports sent via such connections will be ignored),
                        % the history of processed reports is preserved
                        ResultStreamerId => PreviousState#streamer_state{
                            connection_ref = ConnectionRef,
                            processed_reports = gb_sets:add_element(ReportId, PreviousState#streamer_state.processed_reports)
                        }
                    }
                }}
        end
    end,
    Default = #atm_openfaas_result_streamer_registry{
        streamer_states = #{ResultStreamerId => #streamer_state{connection_ref = ConnectionRef}}
    },
    {ok, UpdatedDoc} = datastore_model:update(?CTX, ?id(WorkflowExecutionId, TaskExecutionId), Diff, Default),
    case UpdatedDoc#document.value#atm_openfaas_result_streamer_registry.conclusion_orchestrator of
        undefined ->
            stream_open;
        _ ->
            % it is possible that a registration report will appear after
            % stream conclusion has been initiated
            conclusion_ongoing
    end.


%% NOTE: handling of duplicate chunk reports must be split between below two functions.
%% A preflight check must be done before streaming the data to workflow engine,
%% and then the report must be marked processed. There is no risk of race conditions
%% as reporting in the context of one result steamer is done by one
%% websocket controller process.
-spec qualify_chunk_report(
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    result_streamer_id(),
    atm_openfaas_activity_feed_ws_handler:connection_ref(),
    atm_openfaas_result_streamer_report:id()
) ->
    to_process | duplicate | stale.
qualify_chunk_report(WorkflowExecutionId, TaskExecutionId, ResultStreamerId, ConnectionRef, ReportId) ->
    case datastore_model:get(?CTX, ?id(WorkflowExecutionId, TaskExecutionId)) of
        {ok, #document{value = #atm_openfaas_result_streamer_registry{
            streamer_states = #{ResultStreamerId := #streamer_state{connection_ref = ConnectionRef} = StreamerState}
        }}} ->
            case gb_sets:is_element(ReportId, StreamerState#streamer_state.processed_reports) of
                true ->
                    duplicate;
                false ->
                    to_process
            end;
        _ ->
            stale
    end.


-spec mark_chunk_report_processed(
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    result_streamer_id(),
    atm_openfaas_result_streamer_report:id()
) ->
    ok.
mark_chunk_report_processed(WorkflowExecutionId, TaskExecutionId, ResultStreamerId, ReportId) ->
    Diff = fun(Registry = #atm_openfaas_result_streamer_registry{streamer_states = StreamerStates}) ->
        PreviousState = maps:get(ResultStreamerId, StreamerStates),
        {ok, Registry#atm_openfaas_result_streamer_registry{
            streamer_states = StreamerStates#{
                ResultStreamerId => PreviousState#streamer_state{
                    processed_reports = gb_sets:add_element(ReportId, PreviousState#streamer_state.processed_reports)
                }
            }
        }}
    end,
    ?extract_ok(datastore_model:update(?CTX, ?id(WorkflowExecutionId, TaskExecutionId), Diff)).


%% NOTE: does not need to check for duplicate reports as it is idempotent anyway
%% and the processed report history is cleared upon deregistration
-spec deregister(
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    result_streamer_id(),
    atm_openfaas_activity_feed_ws_handler:connection_ref()
) -> ok.
deregister(WorkflowExecutionId, TaskExecutionId, ResultStreamerId, ConnectionRef) ->
    Diff = fun(Registry = #atm_openfaas_result_streamer_registry{streamer_states = StreamerStates}) ->
        {ok, Registry#atm_openfaas_result_streamer_registry{
            streamer_states = case maps:find(ResultStreamerId, StreamerStates) of
                {ok, #streamer_state{connection_ref = ConnectionRef}} ->
                    % deregister only if the connection ref matches the one currently
                    % used by the result streamer (it may have reconnected and re-registered)
                    maps:remove(ResultStreamerId, StreamerStates);
                _ ->
                    StreamerStates
            end
        }}
    end,
    case datastore_model:update(?CTX, ?id(WorkflowExecutionId, TaskExecutionId), Diff) of
        {error, not_found} ->
            % possible when streaming has already been concluded, but a streamer that
            % missed a finalization signal still tries to deregister
            ok;
        {ok, #document{value = #atm_openfaas_result_streamer_registry{
            streamer_states = UpdatedStreamerStates,
            conclusion_orchestrator = ConclusionOrchestrator
        }}} ->
            case {maps_utils:is_empty(UpdatedStreamerStates), ConclusionOrchestrator} of
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
        streamer_states = Registry
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
) ->
    no_streamers_ever_registered |
    all_streamers_deregistered |
    {active_result_streamers, [atm_openfaas_activity_feed_ws_handler:connection_ref()]}.
claim_conclusion_orchestration(WorkflowExecutionId, TaskExecutionId, ConclusionOrchestrator) ->
    Diff = fun(Registry = #atm_openfaas_result_streamer_registry{conclusion_orchestrator = undefined}) ->
        {ok, Registry#atm_openfaas_result_streamer_registry{
            conclusion_orchestrator = ConclusionOrchestrator
        }}
    end,
    case datastore_model:update(?CTX, ?id(WorkflowExecutionId, TaskExecutionId), Diff) of
        {error, not_found} ->
            no_streamers_ever_registered;
        {ok, #document{value = #atm_openfaas_result_streamer_registry{streamer_states = UpdatedStreamerStates}}} ->
            case maps_utils:is_empty(UpdatedStreamerStates) of
                true ->
                    all_streamers_deregistered;
                false ->
                    ActiveConnRefs = [S#streamer_state.connection_ref || S <- maps:values(UpdatedStreamerStates)],
                    {active_result_streamers, ActiveConnRefs}
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
