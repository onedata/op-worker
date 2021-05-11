%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `atm_task_executor` interface - an object which can be
%%% used to execute an automation task with given arguments.
%%%
%%%                             !!! Caution !!!
%%% 1) This behaviour must be implemented by proper models, that is modules with
%%%    records of the same name.
%%% 2) Models implementing this behaviour must also implement `persistent_record`
%%%    behaviour.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_executor).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_tmp.hrl").

%% API
-export([create/2, init/1, run/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type model() :: atm_openfaas_task_executor.
-type executor() :: atm_openfaas_task_executor:executor().

-export_type([model/0, executor/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates executor model. It is expected to return fast and as such
%% mustn't perform any long lasting initialization tasks.
%% @end
%%--------------------------------------------------------------------
-callback create(atm_workflow_execution:id(), atm_lambda_operation_spec:record()) ->
    executor() | no_return().


%%--------------------------------------------------------------------
%% @doc
%% Initializes executor (e.g. by registering functions in external services
%% like Openfaas).
%% @end
%%--------------------------------------------------------------------
-callback init(executor()) -> ok | no_return().


%%--------------------------------------------------------------------
%% @doc
%% Schedules task execution for specified arguments.
%% @end
%%--------------------------------------------------------------------
-callback run(json_utils:json_map(), executor()) ->
    {ok, atm_task_execution_api:task_id()} | no_return().


%%%===================================================================
%%% API
%%%===================================================================


-spec create(atm_workflow_execution:id(), atm_lambda_operation_spec:record()) ->
    executor() | no_return().
create(AtmWorkflowExecutionId, AtmLambadaOperationSpec) ->
    Engine = atm_lambda_operation_spec:get_engine(AtmLambadaOperationSpec),
    Model = engine_to_executor_model(Engine),
    Model:create(AtmWorkflowExecutionId, AtmLambadaOperationSpec).


-spec init(executor()) -> ok | no_return().
init(AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:init(AtmTaskExecutor).


-spec run(json_utils:json_map(), executor()) ->
    {ok, atm_task_execution_api:task_id()} | no_return().
run(Arguments, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:run(Arguments, AtmTaskExecutor).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(executor(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(AtmTaskExecutor, NestedRecordEncoder) ->
    Model = utils:record_type(AtmTaskExecutor),
    Engine = executor_model_to_engine(Model),

    maps:merge(
        #{<<"engine">> => atm_lambda_operation_spec:engine_to_json(Engine)},
        NestedRecordEncoder(AtmTaskExecutor, Model)
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    executor().
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
