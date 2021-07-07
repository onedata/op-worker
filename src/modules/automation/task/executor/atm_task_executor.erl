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

-include("modules/automation/atm_tmp.hrl").

%% API
-export([create/2, prepare/2, clean/1, get_spec/1, in_readonly_mode/1, run/3]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type model() :: atm_openfaas_task_executor.
-type record() :: atm_openfaas_task_executor:record().

-export_type([model/0, record/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback create(atm_workflow_execution:id(), atm_lambda_operation_spec:record()) ->
    record() | no_return().

-callback prepare(atm_workflow_execution_ctx:record(), record()) -> ok | no_return().

-callback clean(record()) -> ok | no_return().

-callback get_spec(record()) -> workflow_engine:task_spec().

-callback in_readonly_mode(record()) -> boolean().

-callback run(atm_job_execution_ctx:record(), json_utils:json_map(), record()) ->
    ok | no_return().


%%%===================================================================
%%% API
%%%===================================================================


-spec create(atm_workflow_execution:id(), atm_lambda_operation_spec:record()) ->
    record() | no_return().
create(AtmWorkflowExecutionId, AtmLambadaOperationSpec) ->
    Engine = atm_lambda_operation_spec:get_engine(AtmLambadaOperationSpec),
    Model = engine_to_executor_model(Engine),
    Model:create(AtmWorkflowExecutionId, AtmLambadaOperationSpec).


-spec prepare(atm_workflow_execution_ctx:record(), record()) -> ok | no_return().
prepare(AtmWorkflowExecutionCtx, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:prepare(AtmWorkflowExecutionCtx, AtmTaskExecutor).


-spec clean(record()) -> ok | no_return().
clean(AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:clean(AtmTaskExecutor).


-spec get_spec(record()) -> workflow_engine:task_spec().
get_spec(AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:get_spec(AtmTaskExecutor).


-spec in_readonly_mode(record()) -> boolean().
in_readonly_mode(AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:in_readonly_mode(AtmTaskExecutor).


-spec run(atm_job_execution_ctx:record(), json_utils:json_map(), record()) ->
    ok | no_return().
run(AtmTaskExecutionCtx, Arguments, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:run(AtmTaskExecutionCtx, Arguments, AtmTaskExecutor).


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
