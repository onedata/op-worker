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

%% API
-export([build/3, initiate/2, teardown/2, in_readonly_mode/1, run/3]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type model() :: atm_openfaas_task_executor.
-type record() :: atm_openfaas_task_executor:record().

-export_type([model/0, record/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback build(
    atm_workflow_execution_ctx:record(),
    atm_lane_execution:index(),
    atm_lambda_snapshot:record()
) ->
    record() | no_return().

-callback initiate(atm_workflow_execution_ctx:record(), record()) ->
    workflow_engine:task_spec() | no_return().

-callback teardown(atm_lane_execution_handler:teardown_ctx(), record()) -> ok | no_return().

-callback in_readonly_mode(record()) -> boolean().

-callback run(atm_job_ctx:record(), json_utils:json_map(), record()) ->
    ok | no_return().


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    atm_workflow_execution_ctx:record(),
    atm_lane_execution:index(),
    atm_lambda_snapshot:record()
) ->
    record() | no_return().
build(AtmWorkflowExecutionCtx, AtmLaneIndex, AtmLambdaSnapshot = #atm_lambda_snapshot{
    operation_spec = AtmLambadaOperationSpec
}) ->
    Engine = atm_lambda_operation_spec:get_engine(AtmLambadaOperationSpec),
    Model = engine_to_executor_model(Engine),
    Model:build(AtmWorkflowExecutionCtx, AtmLaneIndex, AtmLambdaSnapshot).


-spec initiate(atm_workflow_execution_ctx:record(), record()) ->
    workflow_engine:task_spec() | no_return().
initiate(AtmWorkflowExecutionCtx, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:initiate(AtmWorkflowExecutionCtx, AtmTaskExecutor).


-spec teardown(atm_lane_execution_handler:teardown_ctx(), record()) -> ok | no_return().
teardown(AtmLaneExecutionRunTeardownCtx, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:teardown(AtmLaneExecutionRunTeardownCtx, AtmTaskExecutor).


-spec in_readonly_mode(record()) -> boolean().
in_readonly_mode(AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:in_readonly_mode(AtmTaskExecutor).


-spec run(atm_job_ctx:record(), json_utils:json_map(), record()) ->
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
