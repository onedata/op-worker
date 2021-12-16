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

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/automation/automation.hrl").

%% API
-export([create/4, initiate/4, teardown/2, delete/1, get_type/1, in_readonly_mode/1, run/3]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type model() :: atm_openfaas_task_executor.
-type record() :: atm_openfaas_task_executor:record().

-type args() :: json_utils:json_map().
-type results() :: errors:error() | json_utils:json_map().

%% Below types format can't be expressed directly in type spec due to dialyzer
%% limitations in specifying individual maps keys in case of binaries.
%% Instead it is shown below their declaration.

-type input() :: json_utils:json_map().
%% #{
%%      <<"argsBatch">> := [args()],
%%      <<"ctx">> := #{heartbeatUrl := binary()}
%% }
-type outcome() :: errors:error() | json_utils:json_map().
%% #{
%%      <<"resultsBatch">> := [results()]
%% }

-export_type([args/0, results/0, input/0, outcome/0]).

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

-callback initiate(
    atm_workflow_execution_ctx:record(),
    atm_task_schema:record(),
    atm_lambda_revision:record(),
    record()
) ->
    workflow_engine:task_spec() | no_return().

-callback teardown(atm_lane_execution_handler:teardown_ctx(), record()) -> ok | no_return().

-callback delete(record()) -> ok | no_return().

-callback in_readonly_mode(record()) -> boolean().

-callback run(atm_job_ctx:record(), input(), record()) ->
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


-spec initiate(
    atm_workflow_execution_ctx:record(),
    atm_task_schema:record(),
    atm_lambda_revision:record(),
    record()
) ->
    workflow_engine:task_spec() | no_return().
initiate(AtmWorkflowExecutionCtx, AtmTaskSchema, AtmLambdaRevision, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:initiate(AtmWorkflowExecutionCtx, AtmTaskSchema, AtmLambdaRevision, AtmTaskExecutor).


-spec teardown(atm_lane_execution_handler:teardown_ctx(), record()) -> ok | no_return().
teardown(AtmLaneExecutionRunTeardownCtx, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:teardown(AtmLaneExecutionRunTeardownCtx, AtmTaskExecutor).


-spec delete(record()) -> ok | no_return().
delete(AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:delete(AtmTaskExecutor).


-spec get_type(record()) -> model().
get_type(AtmTaskExecutor) ->
    utils:record_type(AtmTaskExecutor).


-spec in_readonly_mode(record()) -> boolean().
in_readonly_mode(AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:in_readonly_mode(AtmTaskExecutor).


-spec run(atm_job_ctx:record(), input(), record()) ->
    ok | no_return().
run(AtmJobCtx, Arguments, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:run(AtmJobCtx, Arguments, AtmTaskExecutor).


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
