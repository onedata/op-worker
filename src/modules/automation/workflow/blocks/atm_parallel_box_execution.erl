%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module operating on automation parallel box executions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_parallel_box_execution).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

-export([
    create_all/3, create/4,
    prepare_all/1, prepare/1,
    delete_all/1, delete/1,

    gather_statuses/1,
    update_task_status/3
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type status() :: atm_task_execution:status().
-type record() :: #atm_parallel_box_execution{}.

-export_type([status/0, record/0]).


%%%===================================================================
%%% atm_container callbacks
%%%===================================================================


-spec create_all(
    atm_api:creation_ctx(),
    non_neg_integer(),
    [atm_parallel_box_schema:record()]
) ->
    [record()] | no_return().
create_all(AtmExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxSchemas) ->
    lists:reverse(lists:foldl(fun({AtmParallelBoxIndex, #atm_parallel_box_schema{
        id = AtmParallelBoxSchemaId
    } = AtmParallelBoxSchema}, Acc) ->
        try
            AtmParallelBoxExecution = create(
                AtmExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmParallelBoxSchema
            ),
            [AtmParallelBoxExecution | Acc]
        catch _:Reason ->
            catch delete_all(Acc),
            throw(?ERROR_ATM_PARALLEL_BOX_EXECUTION_CREATION_FAILED(AtmParallelBoxSchemaId, Reason))
        end
    end, [], lists_utils:enumerate(AtmParallelBoxSchemas))).


-spec create(
    atm_api:creation_ctx(),
    non_neg_integer(),
    non_neg_integer(),
    atm_parallel_box_schema:record()
) ->
    record() | no_return().
create(AtmExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, #atm_parallel_box_schema{
    id = AtmParallelBoxSchemaId,
    tasks = AtmTaskSchemas
}) ->
    AtmTaskExecutionDocs = atm_task_execution_api:create_all(
        AtmExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmTaskSchemas
    ),
    AtmTaskExecutionStatuses = lists:foldl(fun(#document{
        key = AtmTaskExecutionId,
        value = #atm_task_execution{status = AtmTaskExecutionStatus}
    }, Acc) ->
        Acc#{AtmTaskExecutionId => AtmTaskExecutionStatus}
    end, #{}, AtmTaskExecutionDocs),

    #atm_parallel_box_execution{
        schema_id = AtmParallelBoxSchemaId,
        status = atm_status_utils:converge(maps:values(AtmTaskExecutionStatuses)),
        tasks = AtmTaskExecutionStatuses
    }.


-spec prepare_all([record()]) -> ok | no_return().
prepare_all(AtmParallelBoxExecutions) ->
    lists:foreach(fun(#atm_parallel_box_execution{
        schema_id = AtmParallelBoxSchemaId
    } = AtmParallelBoxExecution) ->
        try
            prepare(AtmParallelBoxExecution)
        catch _:Reason ->
            throw(?ERROR_ATM_PARALLEL_BOX_EXECUTION_PREPARATION_FAILED(AtmParallelBoxSchemaId, Reason))
        end
    end, AtmParallelBoxExecutions).


-spec prepare(record()) -> ok | no_return().
prepare(#atm_parallel_box_execution{tasks = AtmTaskExecutionRegistry}) ->
    atm_task_execution_api:prepare_all(maps:keys(AtmTaskExecutionRegistry)).


-spec delete_all([record()]) -> ok.
delete_all(AtmParallelBoxExecutions) ->
    lists:foreach(fun delete/1, AtmParallelBoxExecutions).


-spec delete(record()) -> ok.
delete(#atm_parallel_box_execution{tasks = AtmTaskExecutions}) ->
    atm_task_execution_api:delete_all(maps:keys(AtmTaskExecutions)).


-spec gather_statuses([record()]) -> [status()].
gather_statuses(AtmParallelBoxExecutions) ->
    lists:map(fun(#atm_parallel_box_execution{status = Status}) ->
        Status
    end, AtmParallelBoxExecutions).


-spec update_task_status(atm_task_execution:id(), atm_task_execution:status(), record()) ->
    {ok, record()} | {error, term()}.
update_task_status(AtmTaskExecutionId, NewStatus, #atm_parallel_box_execution{
    tasks = AtmTaskExecutionRegistry
} = AtmParallelBoxExecution) ->
    AtmTaskExecutionStatus = maps:get(AtmTaskExecutionId, AtmTaskExecutionRegistry),

    case atm_status_utils:is_transition_allowed(AtmTaskExecutionStatus, NewStatus) of
        true ->
            NewAtmTaskExecutionRegistry = AtmTaskExecutionRegistry#{
                AtmTaskExecutionId => NewStatus
            },
            {ok, AtmParallelBoxExecution#atm_parallel_box_execution{
                status = atm_status_utils:converge(maps:values(NewAtmTaskExecutionRegistry)),
                tasks = NewAtmTaskExecutionRegistry
            }};
        false ->
            {error, AtmTaskExecutionStatus}
    end.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_parallel_box_execution{
    schema_id = AtmParallelBoxSchemaId,
    status = AtmParallelBoxExecutionStatus,
    tasks = Tasks
}, _NestedRecordEncoder) ->
    #{
        <<"schemaId">> => AtmParallelBoxSchemaId,
        <<"status">> => atom_to_binary(AtmParallelBoxExecutionStatus, utf8),
        <<"tasks">> => maps:map(fun(_AtmTaskExecutionId, AtmTaskExecutionStatus) ->
            atom_to_binary(AtmTaskExecutionStatus, utf8)
        end, Tasks)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"schemaId">> := AtmParallelBoxSchemaId,
    <<"status">> := AtmParallelBoxExecutionStatusBin,
    <<"tasks">> := AtmTasksJson
}, _NestedRecordDecoder) ->
    #atm_parallel_box_execution{
        schema_id = AtmParallelBoxSchemaId,
        status = binary_to_atom(AtmParallelBoxExecutionStatusBin, utf8),
        tasks = maps:map(fun(_AtmTaskExecutionId, AtmTaskExecutionStatusBin) ->
            binary_to_atom(AtmTaskExecutionStatusBin, utf8)
        end, AtmTasksJson)
    }.