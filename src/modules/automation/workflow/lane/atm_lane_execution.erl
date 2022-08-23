%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing information about automation lane execution.
%%%
%%%                             !!! Caution !!!
%%% Functions in this module can be called inside tp process during
%%% atm_workflow_execution doc update and as such shouldn't do any
%%% operations on datastore themselves.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    is_current_lane_run/2,
    resolve_selector/2,

    get_schema_id/2,
    get_schema/2,

    get_run/2,
    update_run/3, update_run/4
]).
-export([get/2, update/3]).
-export([to_json/2]).

%% persistent_record callbacks
-export([version/0, upgrade_encoded_record/2, db_encode/2, db_decode/2]).


-type index() :: pos_integer().
-type selector() :: current | index().

-type record() :: #atm_lane_execution{}.
-type diff() :: fun((record()) -> {ok, record()} | {error, term()}).

-type run_num() :: pos_integer().
-type run_selector() :: current | run_num().

-type run_status() ::
    % waiting
    ?RESUMING_STATUS | ?SCHEDULED_STATUS | ?PREPARING_STATUS | ?ENQUEUED_STATUS |
    % ongoing
    ?ACTIVE_STATUS | ?STOPPING_STATUS |
    % ended
    ?FINISHED_STATUS |
    ?CRUSHED_STATUS | ?CANCELLED_STATUS | ?FAILED_STATUS | ?INTERRUPTED_STATUS | ?PAUSED_STATUS.

-type run_stopping_reason() :: crush | cancel | failure | interrupt | pause.

-type run_diff() :: fun((run()) -> {ok, run()} | {error, term()}).
-type run() :: #atm_lane_execution_run{}.

%% TODO VFS-8660 replace tuple with record
-type lane_run_selector() :: {selector(), run_selector()}.

-export_type([index/0, selector/0, run_stopping_reason/0, diff/0, record/0]).
-export_type([run_num/0, run_selector/0, run_status/0, run_diff/0, run/0]).
-export_type([lane_run_selector/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec is_current_lane_run(lane_run_selector(), atm_workflow_execution:record()) ->
    boolean().
is_current_lane_run({AtmLaneSelector, AtmRunSelector}, AtmWorkflowExecution = #atm_workflow_execution{
    current_lane_index = CurrentAtmLaneIndex,
    current_run_num = CurrentRunNum
}) ->
    CurrentAtmLaneIndex == resolve_selector(AtmLaneSelector, AtmWorkflowExecution) andalso
        CurrentRunNum == resolve_run_selector(AtmRunSelector, AtmWorkflowExecution).


-spec resolve_selector(selector(), atm_workflow_execution:record()) -> index().
resolve_selector(current, #atm_workflow_execution{current_lane_index = CurrentAtmLaneIndex}) ->
    CurrentAtmLaneIndex;
resolve_selector(AtmLaneIndex, _) ->
    AtmLaneIndex.


%% @private
-spec get_schema_id(selector() | lane_run_selector(), atm_workflow_execution:record()) ->
    automation:id().
get_schema_id({AtmLaneSelector, _}, AtmWorkflowExecution) ->
    get_schema_id(AtmLaneSelector, AtmWorkflowExecution);
get_schema_id(AtmLaneSelector, AtmWorkflowExecution) ->
    AtmLaneExecution = get(AtmLaneSelector, AtmWorkflowExecution),
    AtmLaneExecution#atm_lane_execution.schema_id.


%% @private
-spec get_schema(selector() | lane_run_selector(), atm_workflow_execution:record()) ->
    atm_lane_schema:record().
get_schema({AtmLaneSelector, _}, AtmWorkflowExecution) ->
    get_schema(AtmLaneSelector, AtmWorkflowExecution);
get_schema(AtmLaneSelector, AtmWorkflowExecution = #atm_workflow_execution{
    schema_snapshot_id = AtmWorkflowSchemaSnapshotId
}) ->
    AtmLaneIndex = resolve_selector(AtmLaneSelector, AtmWorkflowExecution),

    {ok, #document{value = #atm_workflow_schema_snapshot{
        revision = #atm_workflow_schema_revision{
            lanes = AtmLaneSchemas
        }
    }}} = atm_workflow_schema_snapshot:get(AtmWorkflowSchemaSnapshotId),

    lists:nth(AtmLaneIndex, AtmLaneSchemas).


-spec get_run(lane_run_selector(), atm_workflow_execution:record()) ->
    {ok, run()} | errors:error().
get_run({AtmLaneSelector, RunSelector}, AtmWorkflowExecution) ->
    AtmLaneExecution = get(AtmLaneSelector, AtmWorkflowExecution),

    case locate_run(RunSelector, AtmLaneExecution, AtmWorkflowExecution) of
        {ok, _, Run} -> {ok, Run};
        {error, _} = Error -> Error
    end.


%% @private
-spec update_run(lane_run_selector(), run_diff(), atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()} | errors:error().
update_run(AtmLaneSelector, UpdateFun, AtmWorkflowExecution) ->
    update_run(AtmLaneSelector, UpdateFun, undefined, AtmWorkflowExecution).


-spec update_run(
    lane_run_selector(),
    run_diff(),
    undefined | atm_lane_execution:run(),
    atm_workflow_execution:record()
) ->
    {ok, atm_workflow_execution:record()} | errors:error().
update_run({AtmLaneSelector, RunSelector}, Diff, Default, AtmWorkflowExecution = #atm_workflow_execution{
    lanes = AtmLaneExecutions,
    current_run_num = CurrentRunNum
}) ->
    AtmLaneIndex = resolve_selector(AtmLaneSelector, AtmWorkflowExecution),
    RunNum = resolve_run_selector(RunSelector, AtmWorkflowExecution),

    AtmLaneExecution = #atm_lane_execution{runs = Runs} = maps:get(AtmLaneIndex, AtmLaneExecutions),

    case locate_run(RunNum, AtmLaneExecution, AtmWorkflowExecution) of
        {ok, RunIndex, Run} ->
            case Diff(Run) of
                {ok, UpdatedRun} ->
                    NewAtmLaneExecution = AtmLaneExecution#atm_lane_execution{
                        runs = lists_utils:replace_at(UpdatedRun, RunIndex, Runs)
                    },
                    {ok, replace(AtmLaneIndex, NewAtmLaneExecution, AtmWorkflowExecution)};
                {error, _} = Error1 ->
                    Error1
            end;

        ?ERROR_NOT_FOUND when Default /= undefined andalso CurrentRunNum =< RunNum ->
            NewAtmLaneExecution = add_new_run(Default, AtmLaneExecution),
            {ok, replace(AtmLaneIndex, NewAtmLaneExecution, AtmWorkflowExecution)};

        {error, _} = Error2 ->
            Error2
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


-spec to_json(selector(), atm_workflow_execution:record()) -> json_utils:json_map().
to_json(AtmLaneSelector, AtmWorkflowExecution) ->
    AtmLaneExecution = get(AtmLaneSelector, AtmWorkflowExecution),

    {RunsJson, _} = lists:mapfoldr(fun(#atm_lane_execution_run{run_num = RunNum} = Run, RunsPerNum) ->
        {run_to_json(Run, RunsPerNum, AtmWorkflowExecution), RunsPerNum#{RunNum => Run}}
    end, #{}, AtmLaneExecution#atm_lane_execution.runs),

    #{
        <<"schemaId">> => AtmLaneExecution#atm_lane_execution.schema_id,
        <<"runs">> => RunsJson
    }.


%% @private
-spec run_to_json(run(), #{run_num() => run()}, atm_workflow_execution:record()) ->
    json_utils:json_map().
run_to_json(Run = #atm_lane_execution_run{
    run_num = RunNum,
    origin_run_num = OriginRunNum,
    status = Status,
    iterated_store_id = IteratedStoreId,
    exception_store_id = ExceptionStoreId,
    parallel_boxes = AtmParallelBoxExecutions
}, RunsPerNum, AtmWorkflowExecution) ->
    #{
        <<"runNumber">> => utils:undefined_to_null(RunNum),
        <<"originRunNumber">> => utils:undefined_to_null(OriginRunNum),
        <<"status">> => atom_to_binary(Status, utf8),
        <<"iteratedStoreId">> => utils:undefined_to_null(IteratedStoreId),
        <<"exceptionStoreId">> => utils:undefined_to_null(ExceptionStoreId),
        <<"parallelBoxes">> => lists:map(
            fun atm_parallel_box_execution:to_json/1, AtmParallelBoxExecutions
        ),

        <<"runType">> => case OriginRunNum of
            undefined ->
                <<"regular">>;
            _ ->
                OriginRun = maps:get(OriginRunNum, RunsPerNum),

                case IteratedStoreId == OriginRun#atm_lane_execution_run.exception_store_id of
                    true -> <<"retry">>;
                    false -> <<"rerun">>
                end
        end,
        <<"isRetriable">> => atm_lane_execution_status:can_manual_lane_run_repeat_be_scheduled(
            retry, Run, AtmWorkflowExecution
        ),
        <<"isRerunable">> => atm_lane_execution_status:can_manual_lane_run_repeat_be_scheduled(
            rerun, Run, AtmWorkflowExecution
        )
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
    stopping_reason = StoppingReason,
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
    EncodedRun3 = case StoppingReason of
        undefined -> EncodedRun2;
        _ -> EncodedRun2#{<<"stoppingReason">> => atom_to_binary(StoppingReason, utf8)}
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
        stopping_reason = case maps:get(<<"stoppingReason">>, EncodedRun, undefined) of
            undefined -> undefined;
            EncodedStoppingReason -> binary_to_atom(EncodedStoppingReason, utf8)
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
-spec resolve_run_selector(run_selector(), atm_workflow_execution:record()) -> run_num().
resolve_run_selector(current, #atm_workflow_execution{current_run_num = CurrentRunNum}) ->
    CurrentRunNum;
resolve_run_selector(RunNum, _) ->
    RunNum.


%% @private
-spec replace(index(), record(), atm_workflow_execution:record()) ->
    atm_workflow_execution:record().
replace(AtmLaneIndex, AtmLaneExecution, AtmWorkflowExecution = #atm_workflow_execution{
    lanes = AtmLaneExecutions
}) ->
    AtmWorkflowExecution#atm_workflow_execution{lanes = AtmLaneExecutions#{
        AtmLaneIndex => AtmLaneExecution
    }}.


%% @private
-spec locate_run(run_selector(), record(), atm_workflow_execution:record()) ->
    {ok, RunIndex :: pos_integer(), run()} | ?ERROR_NOT_FOUND.
locate_run(RunSelector, AtmLaneExecution, AtmWorkflowExecution = #atm_workflow_execution{
    current_run_num = CurrentRunNum
}) ->
    TargetRunNum = resolve_run_selector(RunSelector, AtmWorkflowExecution),

    Result = lists_utils:foldl_while(fun
        (#atm_lane_execution_run{run_num = undefined} = Run, Acc) when CurrentRunNum =< TargetRunNum ->
            {halt, {ok, Acc + 1, Run}};
        (#atm_lane_execution_run{run_num = RunNum} = Run, Acc) when RunNum =:= TargetRunNum ->
            {halt, {ok, Acc + 1, Run}};
        (#atm_lane_execution_run{run_num = RunNum}, _) when RunNum < TargetRunNum ->
            {halt, ?ERROR_NOT_FOUND};
        (_, Acc) ->
            {cont, Acc + 1}
    end, 0, AtmLaneExecution#atm_lane_execution.runs),

    case is_integer(Result) of
        true -> ?ERROR_NOT_FOUND;
        false -> Result
    end.


%% @private
-spec add_new_run(run(), record()) -> record().
add_new_run(Run, #atm_lane_execution{runs = Runs} = AtmLaneExecution) ->
    AtmLaneExecution#atm_lane_execution{runs = [Run | Runs]}.
