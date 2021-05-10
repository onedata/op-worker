%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `atm_task_executor` interface - an object which can be
%%% used to run an automation task.
%%%
%%%                             !!! Caution !!!
%%% This behaviour must be implemented by proper models, that is modules with
%%% records of the same name.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_executor).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_tmp.hrl").

%% API
-export([create/3, init/1, run/2]).
-export([encode/1, decode/1]).


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
-callback create(atm_workflow_execution:id(), atm_lambda_operation_spec()) ->
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
%% Schedules task execution.
%% @end
%%--------------------------------------------------------------------
-callback run(json_utils:json_map(), executor()) ->
    {ok, atm_task_execution_api:task_id()} | no_return().


%%--------------------------------------------------------------------
%% @doc
%% Encodes a record into a database-compliant JSON object.
%% @end
%%--------------------------------------------------------------------
-callback db_encode(executor()) -> json_utils:json_map().


%%--------------------------------------------------------------------
%% @doc
%% Decodes a record from a database-compliant JSON object.
%% @end
%%--------------------------------------------------------------------
-callback db_decode(json_utils:json_map()) -> executor().


%%%===================================================================
%%% API
%%%===================================================================


-spec create(
    operation_spec_engine(),
    atm_workflow_execution:id(),
    atm_lambda_operation_spec()
) ->
    executor() | no_return().
create(Engine, AtmWorkflowExecutionId, AtmLambadaOperationSpec) ->
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


-spec encode(executor()) -> binary().
encode(AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    AtmContainerJson = Model:db_encode(AtmTaskExecutor),
    json_utils:encode(AtmContainerJson#{<<"_type">> => atom_to_binary(Model, utf8)}).


-spec decode(binary()) -> executor().
decode(AtmTaskExecutorBin) ->
    AtmTaskExecutorJson = json_utils:decode(AtmTaskExecutorBin),
    {ModelBin, AtmTaskExecutorJson2} = maps:take(<<"_type">>, AtmTaskExecutorJson),
    Model = binary_to_atom(ModelBin, utf8),
    Model:db_decode(AtmTaskExecutorJson2).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec engine_to_executor_model(operation_spec_engine()) -> model().
engine_to_executor_model(openfaas) -> atm_openfaas_task_executor.
