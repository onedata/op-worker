%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    ensure_all_ended/1,
    clean_all/1, clean/1
]).
-export([gather_statuses/1]).
-export([to_json/1]).

%% persistent_record callbacks
-export([version/0, upgrade_encoded_record/2, db_encode/2, db_decode/2]).


-type status() ::
    ?SCHEDULED_STATUS | ?PREPARING_STATUS | ?ENQUEUED_STATUS |
    ?ACTIVE_STATUS | ?ABORTING_STATUS |
    ?FINISHED_STATUS | ?CANCELLED_STATUS | ?FAILED_STATUS.

-type run() :: #atm_lane_execution_run{}.
-type record2() :: #atm_lane_execution_rec{}.

-export_type([status/0, run/0, record2/0]).

-record(atm_lane_execution, {
    schema_id :: automation:id(),
    status :: atm_parallel_box_execution_status:status(),
    parallel_boxes :: [atm_parallel_box_execution:record()]
}).
-type record() :: #atm_lane_execution{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec ensure_all_ended([record()]) -> ok | no_return().
ensure_all_ended(AtmLaneExecutions) ->
    pforeach_not_ended(fun(#atm_lane_execution{parallel_boxes = AtmParallelBoxExecutions}) ->
        atm_parallel_box_execution:ensure_all_ended(AtmParallelBoxExecutions)
    end, AtmLaneExecutions).


-spec clean_all([record()]) -> ok.
clean_all(AtmLaneExecutions) ->
    lists:foreach(fun clean/1, AtmLaneExecutions).


-spec clean(record()) -> ok.
clean(#atm_lane_execution{parallel_boxes = AtmParallelBoxExecutions}) ->
    atm_parallel_box_execution:teardown_all(AtmParallelBoxExecutions).


-spec gather_statuses([record()]) -> [AtmLaneExecutionStatus :: atm_task_execution:status()].
gather_statuses(AtmLaneExecutions) ->
    lists:map(fun(#atm_lane_execution{status = Status}) -> Status end, AtmLaneExecutions).


-spec to_json(record2()) -> json_utils:json_map().
to_json(#atm_lane_execution_rec{
    schema_id = AtmLaneSchemaId,
    runs = AllRuns
}) ->
    VisibleRuns = lists:dropwhile(fun(#atm_lane_execution_run{run_no = RunNo}) ->
        RunNo == undefined
    end, AllRuns),

    #{
        <<"schemaId">> => AtmLaneSchemaId,
        <<"runs">> => lists:map(fun run_to_json/1, VisibleRuns)
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
        <<"runNo">> => RunNo,
        <<"iteratedStoreId">> => IteratedStoreId,
        <<"exceptionStoreId">> => ExceptionStoreId,
        <<"status">> => atom_to_binary(Status, utf8),
        <<"parallelBoxes">> => lists:map(
            fun atm_parallel_box_execution:to_json/1, AtmParallelBoxExecutions
        ),

        % TODO VFS-8226 add more types after implementing restarts/reruns
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
    {2, #{<<"schemaId">> => AtmLaneSchemaId, <<"runs">> => [#{
        <<"runNo">> => 1,
        <<"iteratedStoreId">> => null,
        <<"exceptionStoreId">> => null,
        <<"status">> => StatusBin,
        <<"parallelBoxes">> => AtmParallelBoxExecutionsJson
    }]}}.


-spec db_encode(record2(), persistent_record:nested_record_encoder()) ->
    json_utils:json_map().
db_encode(#atm_lane_execution_rec{
    schema_id = AtmLaneSchemaId,
    runs = Runs
}, NestedRecordEncoder) ->
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
    record2().
db_decode(#{<<"schemaId">> := AtmLaneSchemaId, <<"runs">> := RunsJson}, NestedRecordDecoder) ->
    #atm_lane_execution_rec{
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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes given Callback for each not ended lane execution. Specified function
%% must take into account possible race conditions when lane transition to
%% ended phase just right before Callback is called.
%% @end
%%--------------------------------------------------------------------
-spec pforeach_not_ended(fun((record()) -> ok | {error, term()}), [record()]) ->
    ok | no_return().
pforeach_not_ended(Callback, AtmLaneExecutions) ->
    atm_parallel_runner:foreach(fun(#atm_lane_execution{status = Status} = AtmLaneExecution) ->
        case atm_parallel_box_execution_status:is_ended(Status) of
            true -> ok;
            false -> Callback(AtmLaneExecution)
        end
    end, AtmLaneExecutions).
