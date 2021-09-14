%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module operating on automation lane executions.
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
-export([get_parallel_box_execution_specs/1]).
-export([gather_statuses/1, update_task_status/4]).
-export([to_json/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type status() ::
    ?SCHEDULED_STATUS | ?PREPARING_STATUS | ?ENQUEUED_STATUS |
    ?ACTIVE_STATUS | ?ABORTING_STATUS |
    ?FINISHED_STATUS | ?CANCELLED_STATUS | ?FAILED_STATUS.

-type run() :: #atm_lane_execution_run{}.
-type record2() :: #atm_lane_execution_rec{}.

-export_type([status/0, run/0, record2/0]).

-record(atm_lane_execution, {
    schema_id :: automation:id(),
    status :: atm_workflow_block_execution_status:status(),
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


-spec get_parallel_box_execution_specs(record()) -> [workflow_engine:parallel_box_spec()].
get_parallel_box_execution_specs(#atm_lane_execution{}) ->
    []. %% TODO rm


-spec gather_statuses([record()]) -> [AtmLaneExecutionStatus :: atm_task_execution:status()].
gather_statuses(AtmLaneExecutions) ->
    lists:map(fun(#atm_lane_execution{status = Status}) -> Status end, AtmLaneExecutions).


%%--------------------------------------------------------------------
%% @doc
%% Updates task status for specific parallel box execution within lane execution.
%%
%%                              !! CAUTION !!
%% This function is called when updating atm_workflow_execution_doc and as such
%% shouldn't touch any other persistent models.
%% @end
%%--------------------------------------------------------------------
-spec update_task_status(
    non_neg_integer(),
    atm_task_execution:id(),
    atm_task_execution:status(),
    record()
) ->
    {ok, record()} | {error, term()}.
update_task_status(AtmParallelBoxIndex, AtmTaskExecutionId, NewStatus, #atm_lane_execution{
    parallel_boxes = AtmParallelBoxExecutions
} = AtmLaneExecution) ->
    AtmParallelBoxExecution = lists:nth(AtmParallelBoxIndex, AtmParallelBoxExecutions),

    case atm_parallel_box_execution:update_task_status(
        AtmTaskExecutionId, NewStatus, AtmParallelBoxExecution
    ) of
        {ok, NewParallelBoxExecution} ->
            NewAtmParallelBoxExecutions = lists_utils:replace_at(
                NewParallelBoxExecution, AtmParallelBoxIndex, AtmParallelBoxExecutions
            ),
            {ok, AtmLaneExecution#atm_lane_execution{
                status = atm_workflow_block_execution_status:infer(
                    atm_parallel_box_execution:gather_statuses(NewAtmParallelBoxExecutions)
                ),
                parallel_boxes = NewAtmParallelBoxExecutions
            }};
        {error, _} = Error ->
            Error
    end.


-spec to_json(record()) -> json_utils:json_term().
to_json(#atm_lane_execution{
    schema_id = AtmLaneSchemaId,
    status = AtmLaneExecutionStatus,
    parallel_boxes = AtmParallelBoxExecutions
}) ->
    #{
        <<"schemaId">> => AtmLaneSchemaId,
        <<"status">> => atom_to_binary(AtmLaneExecutionStatus, utf8),
        <<"parallelBoxes">> => lists:map(
            fun atm_parallel_box_execution:to_json/1, AtmParallelBoxExecutions
        )
    }.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_lane_execution{
    schema_id = AtmLaneSchemaId,
    status = AtmLaneExecutionStatus,
    parallel_boxes = AtmParallelBoxExecutions
}, NestedRecordEncoder) ->
    #{
        <<"schemaId">> => AtmLaneSchemaId,
        <<"status">> => atom_to_binary(AtmLaneExecutionStatus, utf8),
        <<"parallelBoxes">> => lists:map(fun(AtmParallelBoxExecution) ->
            NestedRecordEncoder(AtmParallelBoxExecution, atm_parallel_box_execution)
        end, AtmParallelBoxExecutions)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"schemaId">> := AtmLaneSchemaId,
    <<"status">> := AtmLaneExecutionStatusBin,
    <<"parallelBoxes">> := AtmParallelBoxExecutionsJson
}, NestedRecordDecoder) ->
    #atm_lane_execution{
        schema_id = AtmLaneSchemaId,
        status = binary_to_atom(AtmLaneExecutionStatusBin, utf8),
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
        case atm_workflow_block_execution_status:is_ended(Status) of
            true -> ok;
            false -> Callback(AtmLaneExecution)
        end
    end, AtmLaneExecutions).
