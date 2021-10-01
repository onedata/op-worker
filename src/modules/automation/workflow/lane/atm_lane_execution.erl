%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing information about automation lane execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    get_schema_id/2,
    get_schema/2,
    get_curr_run/2
]).
-export([update_curr_run/3, update_curr_run/4]).
-export([to_json/1]).

%% persistent_record callbacks
-export([version/0, upgrade_encoded_record/2, db_encode/2, db_decode/2]).


-type status() ::
    ?SCHEDULED_STATUS | ?PREPARING_STATUS | ?ENQUEUED_STATUS |
    ?ACTIVE_STATUS | ?ABORTING_STATUS |
    ?FINISHED_STATUS | ?CANCELLED_STATUS | ?FAILED_STATUS | ?INTERRUPTED_STATUS.

-type run_elements() :: #atm_lane_execution_run_elements{}.
-type run() :: #atm_lane_execution_run{}.

-type record() :: #atm_lane_execution{}.

-export_type([status/0, run_elements/0, run/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


%% @private
-spec get_schema_id(undefined | pos_integer(), atm_workflow_execution:record()) ->
    automation:id().
get_schema_id(GivenLaneIndex, #atm_workflow_execution{
    lanes = AtmLaneExecutions,
    curr_lane_index = CurrLaneIndex
}) ->
    LaneIndex = utils:ensure_defined(GivenLaneIndex, CurrLaneIndex),
    #atm_lane_execution{schema_id = AtmLaneSchema} = lists:nth(LaneIndex, AtmLaneExecutions),
    AtmLaneSchema.


%% @private
-spec get_schema(undefined | pos_integer(), atm_workflow_execution:record()) ->
    atm_lane_schema:record().
get_schema(GivenLaneIndex, #atm_workflow_execution{
    schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
    curr_lane_index = CurrLaneIndex
}) ->
    LaneIndex = utils:ensure_defined(GivenLaneIndex, CurrLaneIndex),

    {ok, #document{value = #atm_workflow_schema_snapshot{
        lanes = AtmLaneSchemas
    }}} = atm_workflow_schema_snapshot:get(AtmWorkflowSchemaSnapshotId),

    lists:nth(LaneIndex, AtmLaneSchemas).


-spec get_curr_run(undefined | pos_integer(), atm_workflow_execution:record()) ->
    {ok, run()} | errors:error().
get_curr_run(GivenLaneIndex, #atm_workflow_execution{
    lanes = AtmLaneExecutions,
    curr_lane_index = CurrLaneIndex,
    curr_run_no = CurrRunNo
}) ->
    LaneIndex = utils:ensure_defined(GivenLaneIndex, CurrLaneIndex),

    case lists:nth(LaneIndex, AtmLaneExecutions) of
        #atm_lane_execution{runs = [#atm_lane_execution_run{run_no = undefined} = Run | _]} ->
            {ok, Run};
        #atm_lane_execution{runs = [#atm_lane_execution_run{run_no = CurrRunNo} = Run | _]} ->
            {ok, Run};
        _ ->
            ?ERROR_NOT_FOUND
    end.


%% @private
-spec update_curr_run(
    undefined | pos_integer(),
    fun((atm_lane_execution:run()) -> {ok, atm_lane_execution:run()} | errors:error()),
    atm_workflow_execution:record()
) ->
    {ok, atm_workflow_execution:record()} | errors:error().
update_curr_run(GivenLaneIndex, UpdateFun, AtmWorkflowExecution) ->
    update_curr_run(GivenLaneIndex, UpdateFun, undefined, AtmWorkflowExecution).


-spec update_curr_run(
    undefined | pos_integer(),
    fun((atm_lane_execution:run()) -> {ok, atm_lane_execution:run()} | errors:error()),
    undefined | atm_lane_execution:run(),
    atm_workflow_execution:record()
) ->
    {ok, atm_workflow_execution:record()} | errors:error().
update_curr_run(GivenLaneIndex, Diff, Default, AtmWorkflowExecution = #atm_workflow_execution{
    lanes = AtmLaneExecutions,
    curr_lane_index = CurrLaneIndex,
    curr_run_no = CurrRunNo
}) ->
    LaneIndex = utils:ensure_defined(GivenLaneIndex, CurrLaneIndex),
    AtmLaneExecution = #atm_lane_execution{runs = Runs} = lists:nth(LaneIndex, AtmLaneExecutions),

    UpdateRunsResult = case Runs of
        [#atm_lane_execution_run{run_no = RunNo} = CurrRun | RestRuns] when
            RunNo == undefined;
            RunNo == CurrRunNo
        ->
            case Diff(CurrRun) of
                {ok, UpdatedCurrRun} -> {ok, [UpdatedCurrRun | RestRuns]};
                {error, _} = Error1 -> Error1
            end;
        _ when Default /= undefined ->
            {ok, [Default | Runs]};
        _ ->
            ?ERROR_NOT_FOUND
    end,

    case UpdateRunsResult of
        {ok, NewRuns} ->
            NewAtmLaneExecution = AtmLaneExecution#atm_lane_execution{runs = NewRuns},
            {ok, AtmWorkflowExecution#atm_workflow_execution{lanes = lists_utils:replace_at(
                NewAtmLaneExecution, LaneIndex, AtmLaneExecutions
            )}};
        {error, _} = Error2 ->
            Error2
    end.


-spec to_json(record()) -> json_utils:json_map().
to_json(#atm_lane_execution{schema_id = AtmLaneSchemaId, runs = Runs}) ->
    #{
        <<"schemaId">> => AtmLaneSchemaId,
        <<"runs">> => lists:map(fun run_to_json/1, Runs)
    }.


%% @private
-spec run_to_json(run()) -> json_utils:json_map().
run_to_json(#atm_lane_execution_run{
    run_no = RunNo,
    status = Status,
    iterated_store_id = IteratedStoreId,
    exception_store_id = ExceptionStoreId,
    parallel_boxes = AtmParallelBoxExecutions
}) ->
    #{
%%        <<"runNo">> => utils:undefined_to_null(RunNo),  %% TODO uncomment when gui allows null
        <<"runNo">> => utils:ensure_defined(RunNo, undefined, 100),
        <<"iteratedStoreId">> => utils:undefined_to_null(IteratedStoreId),
        <<"exceptionStoreId">> => utils:undefined_to_null(ExceptionStoreId),
        <<"status">> => atom_to_binary(Status, utf8),
        <<"parallelBoxes">> => lists:map(
            fun atm_parallel_box_execution:to_json/1, AtmParallelBoxExecutions
        ),

        % TODO VFS-8226 add more types after implementing retries/reruns
        <<"runType">> => <<"regular">>,
        <<"srcRunNo">> => null
    }.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    2.


-spec upgrade_encoded_record(persistent_record:record_version(), json_utils:json_map()) ->
    {persistent_record:record_version(), json_utils:json_map()}.
upgrade_encoded_record(1, #{
    <<"schemaId">> := AtmLaneSchemaId,
    <<"status">> := StatusBin,
    <<"parallelBoxes">> := AtmParallelBoxExecutionsJson
}) ->
    UpgradedStatusBin = case binary_to_atom(StatusBin, utf8) of
        ?PENDING_STATUS -> atom_to_binary(?SCHEDULED_STATUS, utf8);
        ?SKIPPED_STATUS -> atom_to_binary(?FINISHED_STATUS, utf8);
        _ -> StatusBin
    end,

    {2, #{<<"schemaId">> => AtmLaneSchemaId, <<"runs">> => [#{
        <<"runNo">> => 1,
        <<"iteratedStoreId">> => null,
        <<"exceptionStoreId">> => null,
        <<"status">> => UpgradedStatusBin,
        <<"parallelBoxes">> => AtmParallelBoxExecutionsJson
    }]}}.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_map().
db_encode(#atm_lane_execution{schema_id = AtmLaneSchemaId, runs = Runs}, NestedRecordEncoder) ->
    #{
        <<"schemaId">> => AtmLaneSchemaId,
        <<"runs">> => lists:map(fun(Run) -> encode_run(NestedRecordEncoder, Run) end, Runs)
    }.


%% @private
-spec encode_run(persistent_record:nested_record_encoder(), run()) ->
    json_utils:json_map().
encode_run(NestedRecordEncoder, #atm_lane_execution_run{
    run_no = RunNo,
    status = Status,
    iterated_store_id = IteratedStoreId,
    exception_store_id = ExceptionStoreId,
    parallel_boxes = AtmParallelBoxExecutions
}) ->
    #{
        <<"runNo">> => RunNo,
        <<"iteratedStoreId">> => utils:undefined_to_null(IteratedStoreId),
        <<"exceptionStoreId">> => utils:undefined_to_null(ExceptionStoreId),
        <<"status">> => atom_to_binary(Status, utf8),
        <<"parallelBoxes">> => lists:map(fun(AtmParallelBoxExecution) ->
            NestedRecordEncoder(AtmParallelBoxExecution, atm_parallel_box_execution)
        end, AtmParallelBoxExecutions)
    }.


-spec db_decode(json_utils:json_map(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"schemaId">> := AtmLaneSchemaId, <<"runs">> := RunsJson}, NestedRecordDecoder) ->
    #atm_lane_execution{
        schema_id = AtmLaneSchemaId,
        runs = lists:map(fun(RunJson) -> decode_run(NestedRecordDecoder, RunJson) end, RunsJson)
    }.


%% @private
-spec decode_run(persistent_record:nested_record_decoder(), json_utils:json_map()) ->
    run().
decode_run(NestedRecordDecoder, #{
    <<"runNo">> := RunNo,
    <<"iteratedStoreId">> := IteratedStoreId,
    <<"exceptionStoreId">> := ExceptionStoreId,
    <<"status">> := StatusBin,
    <<"parallelBoxes">> := AtmParallelBoxExecutionsJson
}) ->
    #atm_lane_execution_run{
        run_no = RunNo,
        status = binary_to_atom(StatusBin, utf8),
        iterated_store_id = utils:null_to_undefined(IteratedStoreId),
        exception_store_id = utils:null_to_undefined(ExceptionStoreId),
        parallel_boxes = lists:map(fun(AtmParallelBoxExecutionJson) ->
            NestedRecordDecoder(AtmParallelBoxExecutionJson, atm_parallel_box_execution)
        end, AtmParallelBoxExecutionsJson)
    }.
