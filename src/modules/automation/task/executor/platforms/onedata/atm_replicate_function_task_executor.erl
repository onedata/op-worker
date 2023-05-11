%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_task_executor` functionality for `replicate`
%%% onedata function.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_replicate_function_task_executor).
-author("Bartosz Walkowicz").

-behaviour(atm_task_executor).
-behaviour(persistent_record).
-behaviour(transfer_stats_callback_behaviour).

-include("modules/automation/atm_execution.hrl").
-include("proto/oneclient/common_messages.hrl").


%% atm_task_executor callbacks
-export([
    create/1,
    initiate/2,
    abort/2,
    teardown/2,
    delete/1,
    is_in_readonly_mode/1,
    run/3,
    trigger_stream_conclusion/2
]).

%% transfer_stats_callback_behaviour
-export([flush_stats/3]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_replicate_function_task_executor, {
    id :: binary()
}).
-type record() :: #atm_replicate_function_task_executor{}.

-export_type([record/0]).


-define(ID_SEPARATOR, "#").
-define(SYNCHRONIZATION_PRIORITY, 224).


%%%===================================================================
%%% atm_task_executor callbacks
%%%===================================================================


-spec create(atm_task_executor:creation_args()) -> record() | no_return().
create(#atm_task_executor_creation_args{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
    task_id = AtmTaskExecutionId
}) ->
    AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
        AtmWorkflowExecutionCtx
    ),
    #atm_replicate_function_task_executor{id = build_id(AtmWorkflowExecutionId, AtmTaskExecutionId)}.


-spec initiate(atm_task_executor:initiation_ctx(), record()) ->
    workflow_engine:task_spec() | no_return().
initiate(_AtmTaskExecutorInitiationCtx, _AtmTaskExecutor) ->
    #{
        type => sync,  %% TODO VFS-10858 async
        data_stream_enabled => true
    }.


-spec abort(atm_workflow_execution_ctx:record(), record()) -> ok | no_return().
abort(_AtmWorkflowExecutionCtx, #atm_replicate_function_task_executor{id = ExecutorId}) ->
    %% TODO VFS-10858 handle races when aborting and scheduling
    replica_synchronizer:cancel(ExecutorId).


-spec teardown(atm_workflow_execution_ctx:record(), record()) -> ok.
teardown(_AtmWorkflowExecutionCtx, _AtmTaskExecutor) -> ok.


-spec delete(record()) -> ok.
delete(_AtmTaskExecutor) -> ok.


-spec is_in_readonly_mode(record()) -> false.
is_in_readonly_mode(_AtmTaskExecutor) -> false.


-spec run(atm_run_job_batch_ctx:record(), atm_task_executor:lambda_input(), record()) ->
    atm_task_executor:lambda_output() | no_return().
run(AtmRunJobBatchCtx, LambdaInput, AtmTaskExecutor) ->
    AtmWorkflowExecutionAuth = atm_run_job_batch_ctx:get_workflow_execution_auth(
        AtmRunJobBatchCtx
    ),
    UserCtx = atm_workflow_execution_auth:get_user_ctx(AtmWorkflowExecutionAuth),

    lists:foreach(fun
        (#{<<"file">> := #{<<"type">> := <<"REG">>, <<"file_id">> := ObjectId}}) ->
            {ok, Guid} = file_id:objectid_to_guid(ObjectId),
            replicate_file(UserCtx, file_ctx:new_by_guid(Guid), AtmTaskExecutor);
        (_) ->
            ok
    end, LambdaInput#atm_lambda_input.args_batch),

    #atm_lambda_output{results_batch = undefined}.


-spec trigger_stream_conclusion(atm_workflow_execution_ctx:record(), record()) ->
    ok | no_return().
trigger_stream_conclusion(AtmWorkflowExecutionCtx, _AtmTaskExecutor) ->
    % All remaining stats must have been flushed before replication ended
    workflow_engine:report_task_data_streaming_concluded(
        atm_workflow_execution_ctx:get_workflow_execution_id(AtmWorkflowExecutionCtx),
        atm_workflow_execution_ctx:get_task_execution_id(AtmWorkflowExecutionCtx),
        success
    ).


%%%===================================================================
%%% transfer_stats_callback_behaviour
%%%===================================================================


-spec flush_stats(od_space:id(), binary(), #{od_provider:id() => non_neg_integer()}) ->
    ok | {error, term()}.
flush_stats(_SpaceId, ExecutorId, BytesPerProvider) ->
    {AtmWorkflowExecutionId, AtmTaskExecutionId} = unpack_id(ExecutorId),

    Timestamp = global_clock:timestamp_seconds(),
    Measurements = lists:map(fun({ProviderId, ReplicatedBytes}) ->
        #{
            <<"tsName">> => <<"bytesReplicated_", ProviderId/binary>>,
            <<"timestamp">> => Timestamp,
            <<"value">> => ReplicatedBytes
        }
    end, maps:to_list(BytesPerProvider)),

    workflow_engine:stream_task_data(AtmWorkflowExecutionId, AtmTaskExecutionId, {chunk, #{
        <<"stats">> => Measurements
    }}),

    % TODO VFS-10858 report as space stats?

    ok.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_replicate_function_task_executor{id = Id}, _NestedRecordEncoder) ->
    #{<<"id">> => Id}.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"id">> := Id} = _RecordJson, _NestedRecordDecoder) ->
    #atm_replicate_function_task_executor{id = Id}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_id(atm_workflow_execution:id(), atm_task_execution:id()) -> binary().
build_id(AtmWorkflowExecutionId, AtmTaskExecutionId) ->
    <<AtmWorkflowExecutionId/binary, ?ID_SEPARATOR, AtmTaskExecutionId/binary>>.


%% @private
-spec unpack_id(binary()) -> {atm_workflow_execution:id(), atm_task_execution:id()}.
unpack_id(ExecutorId) ->
    [AtmWorkflowExecutionId, AtmTaskExecutionId] = binary:split(ExecutorId, <<?ID_SEPARATOR>>),
    {AtmWorkflowExecutionId, AtmTaskExecutionId}.


%% @private
-spec replicate_file(user_ctx:ctx(), file_ctx:ctx(), record()) -> ok | no_return().
replicate_file(UserCtx, FileCtx, #atm_replicate_function_task_executor{id = ExecutorId}) ->
    {Size, FileCtx2} = file_ctx:get_file_size(FileCtx),
    FileBlock = #file_block{offset = 0, size = Size},

    %% TODO VFS-10858 async
    {ok, _} = replica_synchronizer:synchronize(
        UserCtx, FileCtx2, FileBlock, false, ExecutorId, ?SYNCHRONIZATION_PRIORITY, ?MODULE
    ),
    ok.
