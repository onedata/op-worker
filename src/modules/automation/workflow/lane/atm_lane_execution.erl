%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing information about automation lane execution.
%%% TODO CAUTION - FUNCTIONS MAY BE USED DURING DOC UPDATE AND AS SUCH MUSTNT TOUCH DATASTORE
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    resolve_selector/2,

    get_schema_id/2,
    get_schema/2,

    get_current_run/2,
    update_current_run/3, update_current_run/4
]).
-export([get/2, update/3]).
-export([to_json/1]).

%% persistent_record callbacks
-export([version/0, upgrade_encoded_record/2, db_encode/2, db_decode/2]).


-type index() :: pos_integer().
-type selector() :: current | index().

-type status() ::
    ?SCHEDULED_STATUS | ?PREPARING_STATUS | ?ENQUEUED_STATUS |
    ?ACTIVE_STATUS | ?ABORTING_STATUS |
    ?FINISHED_STATUS | ?CANCELLED_STATUS | ?FAILED_STATUS | ?INTERRUPTED_STATUS.

-type run_diff() :: fun((run()) -> {ok, run()} | {error, term()}).
-type run() :: #atm_lane_execution_run{}.

-type diff() :: fun((record()) -> {ok, record()} | {error, term()}).
-type record() :: #atm_lane_execution{}.

-export_type([status/0, run_diff/0, run/0]).
-export_type([index/0, selector/0, diff/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec resolve_selector(selector(), atm_workflow_execution:record()) -> index().
resolve_selector(current, #atm_workflow_execution{current_lane_index = CurrentAtmLaneIndex}) ->
    CurrentAtmLaneIndex;
resolve_selector(AtmLaneIndex, _) ->
    AtmLaneIndex.


%% @private
-spec get_schema_id(selector(), atm_workflow_execution:record()) ->
    automation:id().
get_schema_id(AtmLaneSelector, AtmWorkflowExecution) ->
    AtmLaneExecution = get(AtmLaneSelector, AtmWorkflowExecution),
    AtmLaneExecution#atm_lane_execution.schema_id.


%% @private
-spec get_schema(selector(), atm_workflow_execution:record()) ->
    atm_lane_schema:record().
get_schema(AtmLaneSelector, AtmWorkflowExecution = #atm_workflow_execution{
    schema_snapshot_id = AtmWorkflowSchemaSnapshotId
}) ->
    AtmLaneIndex = resolve_selector(AtmLaneSelector, AtmWorkflowExecution),

    {ok, #document{value = AtmWorkflowSchemaSnapshot}} = atm_workflow_schema_snapshot:get(
        AtmWorkflowSchemaSnapshotId
    ),
    lists:nth(AtmLaneIndex, AtmWorkflowSchemaSnapshot#atm_workflow_schema_snapshot.lanes).


-spec get_current_run(selector(), atm_workflow_execution:record()) ->
    {ok, run()} | errors:error().
get_current_run(AtmLaneSelector, AtmWorkflowExecution = #atm_workflow_execution{
    current_run_num = CurrentRunNum
}) ->
    case get_latest_run(get(AtmLaneSelector, AtmWorkflowExecution)) of
        #atm_lane_execution_run{run_num = undefined} = Run ->
            {ok, Run};
        #atm_lane_execution_run{run_num = CurrentRunNum} = Run ->
            {ok, Run};
        _ ->
            ?ERROR_NOT_FOUND
    end.


%% @private
-spec update_current_run(selector(), run_diff(), atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()} | errors:error().
update_current_run(AtmLaneSelector, UpdateFun, AtmWorkflowExecution) ->
    update_current_run(AtmLaneSelector, UpdateFun, undefined, AtmWorkflowExecution).


-spec update_current_run(
    selector(),
    run_diff(),
    undefined | atm_lane_execution:run(),
    atm_workflow_execution:record()
) ->
    {ok, atm_workflow_execution:record()} | errors:error().
update_current_run(AtmLaneSelector, Diff, Default, AtmWorkflowExecution = #atm_workflow_execution{
    lanes = AtmLaneExecutions,
    current_run_num = CurrentRunNum
}) ->
    AtmLaneIndex = resolve_selector(AtmLaneSelector, AtmWorkflowExecution),
    AtmLaneExecution = maps:get(AtmLaneIndex, AtmLaneExecutions),

    UpdateRunsResult = case get_latest_run(AtmLaneExecution) of
        #atm_lane_execution_run{run_num = undefined} ->
            update_latest_run(Diff, AtmLaneExecution);
        #atm_lane_execution_run{run_num = CurrentRunNum} ->
            update_latest_run(Diff, AtmLaneExecution);
        _ when Default /= undefined ->
            {ok, add_new_run(Default, AtmLaneExecution)};
        _ ->
            ?ERROR_NOT_FOUND
    end,

    case UpdateRunsResult of
        {ok, NewAtmLaneExecution} ->
            {ok, AtmWorkflowExecution#atm_workflow_execution{lanes = AtmLaneExecutions#{
                AtmLaneIndex => NewAtmLaneExecution
            }}};
        {error, _} = Error ->
            Error
    end.


-spec get(selector(), atm_workflow_execution:record()) -> record().
get(AtmLaneSelector, AtmWorkflowExecution = #atm_workflow_execution{lanes = AtmLaneExecutions}) ->
    maps:get(resolve_selector(AtmLaneSelector, AtmWorkflowExecution), AtmLaneExecutions).


-spec update(selector(), diff(), atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()} | errors:error().
update(AtmLaneSelector, Diff, AtmWorkflowExecution = #atm_workflow_execution{
    lanes = AtmLaneExecutions
}) ->
    AtmLaneIndex = resolve_selector(AtmLaneSelector, AtmWorkflowExecution),

    case Diff(maps:get(AtmLaneIndex, AtmLaneExecutions)) of
        {ok, NewAtmLaneExecution} ->
            {ok, AtmWorkflowExecution#atm_workflow_execution{lanes = AtmLaneExecutions#{
                AtmLaneIndex => NewAtmLaneExecution
            }}};
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
    run_num = RunNum,
    origin_run_num = OriginRunNum,
    status = Status,
    iterated_store_id = IteratedStoreId,
    exception_store_id = ExceptionStoreId,
    parallel_boxes = AtmParallelBoxExecutions
}) ->
    #{
%%        <<"runNumber">> => utils:undefined_to_null(RunNum),  %% TODO uncomment when gui allows null
        <<"runNumber">> => utils:ensure_defined(RunNum, 100),
        <<"originRunNumber">> => utils:undefined_to_null(OriginRunNum),
        <<"status">> => atom_to_binary(Status, utf8),
        <<"iteratedStoreId">> => utils:undefined_to_null(IteratedStoreId),
        <<"exceptionStoreId">> => utils:undefined_to_null(ExceptionStoreId),
        <<"parallelBoxes">> => lists:map(
            fun atm_parallel_box_execution:to_json/1, AtmParallelBoxExecutions
        ),

        % TODO VFS-8226 add more types after implementing retries/reruns
        <<"runType">> => case OriginRunNum of
            undefined -> <<"regular">>;
            _ -> <<"retry">>
        end
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

    {2, #{<<"schemaId">> => AtmLaneSchemaId, <<"retriesLeft">> => 0, <<"runs">> => [#{
        <<"runNum">> => 1,
        <<"status">> => UpgradedStatusBin,
        <<"parallelBoxes">> => AtmParallelBoxExecutionsJson
    }]}}.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_map().
db_encode(#atm_lane_execution{
    schema_id = AtmLaneSchemaId,
    retries_left = RetriesLeft,
    runs = Runs
}, NestedRecordEncoder) ->
    #{
        <<"schemaId">> => AtmLaneSchemaId,
        <<"retriesLeft">> => RetriesLeft,
        <<"runs">> => lists:map(fun(Run) -> encode_run(NestedRecordEncoder, Run) end, Runs)
    }.


%% @private
-spec encode_run(persistent_record:nested_record_encoder(), run()) ->
    json_utils:json_map().
encode_run(NestedRecordEncoder, #atm_lane_execution_run{
    run_num = RunNum,
    origin_run_num = OriginRunNum,
    status = Status,
    aborting_reason = AbortingReason,
    iterated_store_id = IteratedStoreId,
    exception_store_id = ExceptionStoreId,
    parallel_boxes = AtmParallelBoxExecutions
}) ->
    EncodedRun1 = #{
        <<"runNum">> => RunNum,
        <<"status">> => atom_to_binary(Status, utf8),
        <<"parallelBoxes">> => lists:map(fun(AtmParallelBoxExecution) ->
            NestedRecordEncoder(AtmParallelBoxExecution, atm_parallel_box_execution)
        end, AtmParallelBoxExecutions)
    },
    EncodedRun2 = maps_utils:put_if_defined(EncodedRun1, <<"originRunNum">>, OriginRunNum),
    EncodedRun3 = case AbortingReason of
        undefined -> EncodedRun2;
        _ -> EncodedRun2#{<<"abortingReason">> => atom_to_binary(AbortingReason, utf8)}
    end,
    EncodedRun4 = maps_utils:put_if_defined(EncodedRun3, <<"iteratedStoreId">>, IteratedStoreId),
    maps_utils:put_if_defined(EncodedRun4, <<"exceptionStoreId">>, ExceptionStoreId).


-spec db_decode(json_utils:json_map(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"schemaId">> := AtmLaneSchemaId,
    <<"retriesLeft">> := RetriesLeft,
    <<"runs">> := RunsJson
}, NestedRecordDecoder) ->
    #atm_lane_execution{
        schema_id = AtmLaneSchemaId,
        retries_left = RetriesLeft,
        runs = lists:map(fun(RunJson) -> decode_run(NestedRecordDecoder, RunJson) end, RunsJson)
    }.


%% @private
-spec decode_run(persistent_record:nested_record_decoder(), json_utils:json_map()) ->
    run().
decode_run(NestedRecordDecoder, EncodedRun = #{
    <<"runNum">> := RunNum,
    <<"status">> := StatusBin,
    <<"parallelBoxes">> := AtmParallelBoxExecutionsJson
}) ->
    #atm_lane_execution_run{
        run_num = RunNum,
        origin_run_num = maps:get(<<"originRunNum">>, EncodedRun, undefined),
        status = binary_to_atom(StatusBin, utf8),
        aborting_reason = case maps:get(<<"abortingReason">>, EncodedRun, undefined) of
            undefined -> undefined;
            EncodedAbortingReason -> binary_to_atom(EncodedAbortingReason, utf8)
        end,
        iterated_store_id = maps:get(<<"iteratedStoreId">>, EncodedRun, undefined),
        exception_store_id = maps:get(<<"exceptionStoreId">>, EncodedRun, undefined),
        parallel_boxes = lists:map(fun(AtmParallelBoxExecutionJson) ->
            NestedRecordDecoder(AtmParallelBoxExecutionJson, atm_parallel_box_execution)
        end, AtmParallelBoxExecutionsJson)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec add_new_run(run(), record()) -> record().
add_new_run(Run, #atm_lane_execution{runs = Runs} = AtmLaneExecution) ->
    AtmLaneExecution#atm_lane_execution{runs = [Run | Runs]}.


%% @private
-spec get_latest_run(record()) -> run() | ?ERROR_NOT_FOUND.
get_latest_run(#atm_lane_execution{runs = [Run | _]}) -> Run;
get_latest_run(_) -> ?ERROR_NOT_FOUND.


%% @private
-spec update_latest_run(run_diff(), record()) -> {ok, record()} | ?ERROR_NOT_FOUND.
update_latest_run(Diff, #atm_lane_execution{runs = [Run | RestRuns]} = AtmLaneExecution) ->
    case Diff(Run) of
        {ok, UpdatedRun} ->
            {ok, AtmLaneExecution#atm_lane_execution{runs = [UpdatedRun | RestRuns]}};
        {error, _} = Error1 ->
            Error1
    end;
update_latest_run(_, _) ->
    ?ERROR_NOT_FOUND.
