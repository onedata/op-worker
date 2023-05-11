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
-export([
    create/1,
    initiate/2,
    abort/2,
    teardown/2,
    delete/1,
    get_type/1,
    is_in_readonly_mode/1,
    run/3,
    trigger_stream_conclusion/2
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type model() ::
    atm_openfaas_task_executor |
    atm_replicate_function_task_executor.
-type record() ::
    atm_openfaas_task_executor:record() |
    atm_replicate_function_task_executor:record().

-type creation_args() :: #atm_task_executor_creation_args{}.
-type initiation_ctx() :: #atm_task_executor_initiation_ctx{}.

-type job_args() :: json_utils:json_map().
-type job_results() :: json_utils:json_map() | errors:error().

-type job_batch_id() :: binary().

-type lambda_input() :: #atm_lambda_input{}.
-type lambda_output() :: #atm_lambda_output{}.

-type job_batch_result() ::
    {ok, lambda_output()} |
    % special error signaling that job execution has been rejected.
    % It will be specially handled when stopping execution - items_in_processing
    % counter will be decremented but overall job is not consider as failed.
    % In any other case it will be treated as normal error.
    ?ERROR_ATM_JOB_BATCH_WITHDRAWN(binary()) |
    errors:error().

-type streamed_data() :: {chunk, json_utils:json_map()} | errors:error().

-export_type([creation_args/0, initiation_ctx/0]).
-export_type([job_args/0, job_results/0]).
-export_type([job_batch_id/0, lambda_input/0, lambda_output/0, job_batch_result/0]).
-export_type([streamed_data/0]).

-export_type([model/0, record/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback create(creation_args()) -> record() | no_return().

-callback initiate(initiation_ctx(), record()) -> workflow_engine:task_spec() | no_return().

-callback abort(atm_workflow_execution_ctx:record(), record()) -> ok | no_return().

-callback teardown(atm_workflow_execution_ctx:record(), record()) -> ok | no_return().

-callback delete(record()) -> ok | no_return().

-callback is_in_readonly_mode(record()) -> boolean().

-callback run(atm_run_job_batch_ctx:record(), lambda_input(), record()) ->
    ok | lambda_output() | no_return().

-callback trigger_stream_conclusion(atm_workflow_execution_ctx:record(), record()) ->
    ok | no_return().


%%%===================================================================
%%% API
%%%===================================================================


-spec create(creation_args()) -> record() | no_return().
create(AtmTaskExecutorCreationArgs = #atm_task_executor_creation_args{
    lambda_revision = AtmLambdaRevision
}) ->
    Model = get_executor_model(AtmLambdaRevision#atm_lambda_revision.operation_spec),
    Model:create(AtmTaskExecutorCreationArgs).


-spec initiate(initiation_ctx(), record()) -> workflow_engine:task_spec() | no_return().
initiate(AtmTaskExecutorInitiationCtx, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:initiate(AtmTaskExecutorInitiationCtx, AtmTaskExecutor).


-spec abort(atm_workflow_execution_ctx:record(), record()) -> ok | no_return().
abort(AtmWorkflowExecutionCtx, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:abort(AtmWorkflowExecutionCtx, AtmTaskExecutor).


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
    ok | lambda_output() | no_return().
run(AtmRunJobBatchCtx, LambdaInput, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:run(AtmRunJobBatchCtx, LambdaInput, AtmTaskExecutor).


-spec trigger_stream_conclusion(atm_workflow_execution_ctx:record(), record()) ->
    ok | no_return().
trigger_stream_conclusion(AtmWorkflowExecutionCtx, AtmTaskExecutor) ->
    Model = utils:record_type(AtmTaskExecutor),
    Model:trigger_stream_conclusion(AtmWorkflowExecutionCtx, AtmTaskExecutor).


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

    maps:merge(
        #{<<"_model">> => atom_to_binary(Model, utf8)},
        NestedRecordEncoder(AtmTaskExecutor, Model)
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"_model">> := ModelJson} = AtmTaskExecutorJson, NestedRecordDecoder) ->
    NestedRecordDecoder(AtmTaskExecutorJson, binary_to_existing_atom(ModelJson, utf8)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_executor_model(atm_lambda_operation_spec:record()) ->
    module().
get_executor_model(#atm_openfaas_operation_spec{}) ->
    atm_openfaas_task_executor;
get_executor_model(#atm_onedata_function_operation_spec{function_id = <<"replicate">>}) ->
    atm_replicate_function_task_executor.
