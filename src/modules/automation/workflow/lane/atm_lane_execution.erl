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

    get_curr_run/2,
    update_curr_run/3, update_curr_run/4
]).
-export([get/2]).
-export([to_json/1]).

%% persistent_record callbacks
-export([version/0, upgrade_encoded_record/2, db_encode/2, db_decode/2]).


-type index() :: pos_integer().
% Indicates specific atm lane execution either directly using index() or indirectly
% using 'undefined' which means currently (or last in case of ended atm workflow
% execution) executed atm lane execution.
-type marker() :: undefined | index().

-type status() ::
    ?SCHEDULED_STATUS | ?PREPARING_STATUS | ?ENQUEUED_STATUS |
    ?ACTIVE_STATUS | ?ABORTING_STATUS |
    ?FINISHED_STATUS | ?CANCELLED_STATUS | ?FAILED_STATUS | ?INTERRUPTED_STATUS.

-type run_elements() :: #atm_lane_execution_run_elements{}.
-type run_diff() :: fun((atm_lane_execution:run()) ->
    {ok, atm_lane_execution:run()} | errors:error()
).
-type run() :: #atm_lane_execution_run{}.

-type record() :: #atm_lane_execution{}.

-export_type([run_elements/0, run_diff/0, run/0]).
-export_type([status/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


%% @private
-spec get_schema_id(marker(), atm_workflow_execution:record()) ->
    automation:id().
get_schema_id(AtmLaneMarker, AtmWorkflowExecution) ->
    AtmLaneExecution = get(AtmLaneMarker, AtmWorkflowExecution),
    AtmLaneExecution#atm_lane_execution.schema_id.


%% @private
-spec get_schema(marker(), atm_workflow_execution:record()) ->
    atm_lane_schema:record().
get_schema(AtmLaneMarker, AtmWorkflowExecution = #atm_workflow_execution{
    schema_snapshot_id = AtmWorkflowSchemaSnapshotId
}) ->
    AtmLaneIndex = resolve_index(AtmLaneMarker, AtmWorkflowExecution),

    {ok, #document{value = AtmWorkflowSchemaSnapshot}} = atm_workflow_schema_snapshot:get(
        AtmWorkflowSchemaSnapshotId
    ),
    lists:nth(AtmLaneIndex, AtmWorkflowSchemaSnapshot#atm_workflow_schema_snapshot.lanes).


-spec get_curr_run(marker(), atm_workflow_execution:record()) ->
    {ok, run()} | errors:error().
get_curr_run(AtmLaneMarker, AtmWorkflowExecution = #atm_workflow_execution{
    curr_run_no = CurrRunNo
}) ->
    case get(AtmLaneMarker, AtmWorkflowExecution) of
        #atm_lane_execution{runs = [#atm_lane_execution_run{run_no = undefined} = Run | _]} ->
            {ok, Run};
        #atm_lane_execution{runs = [#atm_lane_execution_run{run_no = CurrRunNo} = Run | _]} ->
            {ok, Run};
        _ ->
            ?ERROR_NOT_FOUND
    end.


%% @private
-spec update_curr_run(marker(), run_diff(), atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()} | errors:error().
update_curr_run(AtmLaneMarker, UpdateFun, AtmWorkflowExecution) ->
    update_curr_run(AtmLaneMarker, UpdateFun, undefined, AtmWorkflowExecution).


-spec update_curr_run(
    marker(),
    run_diff(),
    undefined | atm_lane_execution:run(),
    atm_workflow_execution:record()
) ->
    {ok, atm_workflow_execution:record()} | errors:error().
update_curr_run(AtmLaneMarker, Diff, Default, AtmWorkflowExecution = #atm_workflow_execution{
    lanes = AtmLaneExecutions,
    curr_run_no = CurrRunNo
}) ->
    AtmLaneIndex = resolve_index(AtmLaneMarker, AtmWorkflowExecution),
    AtmLaneExecution = #atm_lane_execution{runs = Runs} = lists:nth(AtmLaneIndex, AtmLaneExecutions),

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
                NewAtmLaneExecution, AtmLaneIndex, AtmLaneExecutions
            )}};
        {error, _} = Error2 ->
            Error2
    end.


-spec get(marker(), atm_workflow_execution:record()) -> record().
get(AtmLaneMarker, AtmWorkflowExecution = #atm_workflow_execution{lanes = AtmLaneExecutions}) ->
    lists:nth(resolve_index(AtmLaneMarker, AtmWorkflowExecution), AtmLaneExecutions).


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
    src_run_no = SrcRunNo,
    status = Status,
    iterated_store_id = IteratedStoreId,
    exception_store_id = ExceptionStoreId,
    parallel_boxes = AtmParallelBoxExecutions
}) ->
    #{
%%        <<"runNo">> => utils:undefined_to_null(RunNo),  %% TODO uncomment when gui allows null
        <<"runNo">> => utils:ensure_defined(RunNo, 100),
        <<"srcRunNo">> => utils:undefined_to_null(SrcRunNo),
        <<"status">> => atom_to_binary(Status, utf8),
        <<"iteratedStoreId">> => utils:undefined_to_null(IteratedStoreId),
        <<"exceptionStoreId">> => utils:undefined_to_null(ExceptionStoreId),
        <<"parallelBoxes">> => lists:map(
            fun atm_parallel_box_execution:to_json/1, AtmParallelBoxExecutions
        ),

        % TODO VFS-8226 add more types after implementing retries/reruns
        <<"runType">> => case SrcRunNo of
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

    {2, #{<<"schemaId">> => AtmLaneSchemaId, <<"runs">> => [#{
        <<"runNo">> => 1,
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
    src_run_no = SrcRunNo,
    status = Status,
    aborting_reason = AbortingReason,
    iterated_store_id = IteratedStoreId,
    exception_store_id = ExceptionStoreId,
    parallel_boxes = AtmParallelBoxExecutions
}) ->
    EncodedRun1 = #{
        <<"runNo">> => RunNo,
        <<"status">> => atom_to_binary(Status, utf8),
        <<"parallelBoxes">> => lists:map(fun(AtmParallelBoxExecution) ->
            NestedRecordEncoder(AtmParallelBoxExecution, atm_parallel_box_execution)
        end, AtmParallelBoxExecutions)
    },
    EncodedRun2 = maps_utils:put_if_defined(EncodedRun1, <<"srcRunNo">>, SrcRunNo),
    EncodedRun3 = case AbortingReason of
        undefined -> EncodedRun2;
        _ -> EncodedRun2#{<<"abortingReason">> => atom_to_binary(AbortingReason, utf8)}
    end,
    EncodedRun4 = maps_utils:put_if_defined(EncodedRun3, <<"iteratedStoreId">>, IteratedStoreId),
    maps_utils:put_if_defined(EncodedRun4, <<"exceptionStoreId">>, ExceptionStoreId).


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
decode_run(NestedRecordDecoder, EncodedRun = #{
    <<"runNo">> := RunNo,
    <<"status">> := StatusBin,
    <<"parallelBoxes">> := AtmParallelBoxExecutionsJson
}) ->
    #atm_lane_execution_run{
        run_no = RunNo,
        src_run_no = maps:get(<<"srcRunNo">>, EncodedRun, undefined),
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
-spec resolve_index(marker(), atm_workflow_execution:record()) -> index().
resolve_index(AtmLaneMarker, #atm_workflow_execution{curr_lane_index = CurrAtmLaneIndex}) ->
    utils:ensure_defined(AtmLaneMarker, CurrAtmLaneIndex).
