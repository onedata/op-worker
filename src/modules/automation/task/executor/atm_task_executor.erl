%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `atm_task_executor` interface - an object used for
%%% executing a job (an automation task with specific arguments).
%%%
%%%                             !!! Caution !!!
%%% 1) This behaviour must be implemented by proper models, that is modules with
%%%    records of the same name.
%%% 2) Models implementing this behaviour must also implement `persistent_record`
%%%    behaviour.
%%% 3) Modules implementing this behaviour must be registered in
%%%    `engine_to_executor_model/executor_model_to_engine` functions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_executor).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/automation/automation.hrl").

%% API
-export([create/4, initiate/2, teardown/2, delete/1, get_type/1, is_in_readonly_mode/1, run/3]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type model() :: atm_openfaas_task_executor.
-type record() :: atm_openfaas_task_executor:record().

-type initiation_ctx() :: #atm_task_executor_initiation_ctx{}.

-type job_args() :: json_utils:json_map().
-type job_results() :: json_utils:json_map() | errors:error().

%% Below types format can't be expressed directly in type spec due to dialyzer
%% limitations in specifying individual maps keys in case of binaries.
%% Instead it is shown below their declaration.

-type lambda_input() :: json_utils:json_map().
%% #{
%%      <<"argsBatch">> := [job_args()],
%%      <<"ctx">> := #{<<"heartbeatUrl">> := binary()}
%% }
-type lambda_output() :: json_utils:json_map() | errors:error().
%% #{
%%      <<"resultsBatch">> := [job_results()]
%% }

-type streamed_data() :: {chunk, json_utils:json_map()} | errors:error().

-export_type([initiation_ctx/0]).
-export_type([job_args/0, job_results/0, lambda_input/0, lambda_output/0, streamed_data/0]).

-export_type([model/0, record/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback create(
    atm_workflow_execution_ctx:record(),
    atm_lane_execution:index(),
    atm_task_schema:record(),
    atm_lambda_revision:record()
) ->
    record() | no_return().

-callback initiate(initiation_ctx(), record()) -> workflow_engine:task_spec() | no_return().

-callback teardown(atm_workflow_execution_ctx:record(), record()) -> ok | no_return().

-callback delete(record()) -> ok | no_return().

-callback is_in_readonly_mode(record()) -> boolean().

-callback run(atm_run_job_batch_ctx:record(), lambda_input(), record()) ->
    ok | no_return().


%%%===================================================================
%%% API
%%%===================================================================


-spec create(
    atm_workflow_execution_ctx:record(),
    atm_lane_execution:index(),
    atm_task_schema:record(),
    atm_lambda_revision:record()
) ->
    record() | no_return().
create(AtmWorkflowExecutionCtx, AtmLaneIndex, AtmTaskSchema, AtmLambdaRevision = #atm_lambda_revision{
    operation_spec = AtmLambadaOperationSpec
}) ->
    Engine = atm_lambda_operation_spec:get_engine(AtmLambadaOperationSpec),
    Model = engine_to_executor_model(Engine),
    Model:create(AtmWorkflowExecutionCtx, AtmLaneIndex, AtmTaskSchema, AtmLambdaRevision).


-spec initiate(initiation_ctx(), record()) -> workflow_engine:task_spec() | no_return().
initiate(AtmTaskExecutorInitiationCtx, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:initiate(AtmTaskExecutorInitiationCtx, AtmTaskExecutor).


-spec teardown(atm_workflow_execution_ctx:record(), record()) -> ok | no_return().
teardown(AtmWorkflowExecutionCtx, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:teardown(AtmWorkflowExecutionCtx, AtmTaskExecutor).


-spec delete(record()) -> ok | no_return().
delete(AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:delete(AtmTaskExecutor).


-spec get_type(record()) -> model().
get_type(AtmTaskExecutor) ->
    utils:record_type(AtmTaskExecutor).


-spec is_in_readonly_mode(record()) -> boolean().
is_in_readonly_mode(AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:is_in_readonly_mode(AtmTaskExecutor).


-spec run(atm_run_job_batch_ctx:record(), lambda_input(), record()) ->
    ok | no_return().
run(AtmRunJobBatchCtx, LambdaInput, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:run(AtmRunJobBatchCtx, LambdaInput, AtmTaskExecutor).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(AtmTaskExecutor, NestedRecordEncoder) ->
    Model = utils:record_type(AtmTaskExecutor),
    Engine = executor_model_to_engine(Model),

    maps:merge(
        #{<<"engine">> => atm_lambda_operation_spec:engine_to_json(Engine)},
        NestedRecordEncoder(AtmTaskExecutor, Model)
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"engine">> := EngineJson} = AtmTaskExecutorJson, NestedRecordDecoder) ->
    Engine = atm_lambda_operation_spec:engine_from_json(EngineJson),
    Model = engine_to_executor_model(Engine),

    NestedRecordDecoder(AtmTaskExecutorJson, Model).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec engine_to_executor_model(atm_lambda_operation_spec:engine()) -> model().
engine_to_executor_model(openfaas) -> atm_openfaas_task_executor.


%% @private
-spec executor_model_to_engine(model()) -> atm_lambda_operation_spec:engine().
executor_model_to_engine(atm_openfaas_task_executor) -> openfaas.
