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

%% API
-export([
    create_all/3, create/4,
    prepare_all/2, prepare/2,
    ensure_all_ended/1,
    clean_all/1, clean/1,
    delete_all/1, delete/1
]).
-export([get_spec/1]).
-export([get_statuses/1, update_task_status/3]).
-export([to_json/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_parallel_box_execution, {
    schema_id :: automation:id(),
    status :: atm_workflow_block_execution_status:status(),
    task_registry :: #{AtmTaskSchemaId :: automation:id() => atm_task_execution:id()},
    task_statuses :: #{atm_task_execution:id() => atm_task_execution:status()}
}).
-type record() :: #atm_parallel_box_execution{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all(
    atm_workflow_execution:creation_ctx(),
    non_neg_integer(),
    [atm_parallel_box_schema:record()]
) ->
    [record()] | no_return().
create_all(AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxSchemas) ->
    lists:reverse(lists:foldl(fun({AtmParallelBoxIndex, #atm_parallel_box_schema{
        id = AtmParallelBoxSchemaId
    } = AtmParallelBoxSchema}, Acc) ->
        try
            AtmParallelBoxExecution = create(
                AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmParallelBoxSchema
            ),
            [AtmParallelBoxExecution | Acc]
        catch _:Reason ->
            catch delete_all(Acc),
            throw(?ERROR_ATM_PARALLEL_BOX_EXECUTION_CREATION_FAILED(AtmParallelBoxSchemaId, Reason))
        end
    end, [], lists_utils:enumerate(AtmParallelBoxSchemas))).


-spec create(
    atm_workflow_execution:creation_ctx(),
    non_neg_integer(),
    non_neg_integer(),
    atm_parallel_box_schema:record()
) ->
    record() | no_return().
create(_AtmWorkflowExecutionCreationCtx, _AtmLaneIndex, _AtmParallelBoxIndex, #atm_parallel_box_schema{
    id = AtmParallelBoxSchemaId,
    tasks = []
}) ->
    throw(?ERROR_ATM_EMPTY_PARALLEL_BOX(AtmParallelBoxSchemaId));

create(AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, #atm_parallel_box_schema{
    id = AtmParallelBoxSchemaId,
    tasks = AtmTaskSchemas
}) ->
    AtmTaskExecutionDocs = atm_task_execution_api:create_all(
        AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmTaskSchemas
    ),
    {AtmTaskRegistry, AtmTaskExecutionStatuses} = lists:foldl(fun(#document{
        key = AtmTaskExecutionId,
        value = #atm_task_execution{
            schema_id = AtmTaskSchemaId,
            status = AtmTaskExecutionStatus
        }
    }, {AtmTaskRegistryAcc, AtmTaskExecutionStatusesAcc}) ->
        {
            AtmTaskRegistryAcc#{AtmTaskSchemaId => AtmTaskExecutionId},
            AtmTaskExecutionStatusesAcc#{AtmTaskExecutionId => AtmTaskExecutionStatus}
        }
    end, {#{}, #{}}, AtmTaskExecutionDocs),

    #atm_parallel_box_execution{
        schema_id = AtmParallelBoxSchemaId,
        status = atm_workflow_block_execution_status:infer(maps:values(AtmTaskExecutionStatuses)),
        task_registry = AtmTaskRegistry,
        task_statuses = AtmTaskExecutionStatuses
    }.


-spec prepare_all(atm_workflow_execution_auth:record(), [record()]) -> ok | no_return().
prepare_all(AtmWorkflowExecutionAuth, AtmParallelBoxExecutions) ->
    atm_parallel_runner:foreach(fun(#atm_parallel_box_execution{
        schema_id = AtmParallelBoxSchemaId
    } = AtmParallelBoxExecution) ->
        try
            prepare(AtmWorkflowExecutionAuth, AtmParallelBoxExecution)
        catch _:Reason ->
            throw(?ERROR_ATM_PARALLEL_BOX_EXECUTION_PREPARATION_FAILED(AtmParallelBoxSchemaId, Reason))
        end
    end, AtmParallelBoxExecutions).


-spec prepare(atm_workflow_execution_auth:record(), record()) -> ok | no_return().
prepare(AtmWorkflowExecutionAuth, #atm_parallel_box_execution{
    task_registry = AtmTaskExecutionRegistry
}) ->
    atm_task_execution_api:prepare_all(
        AtmWorkflowExecutionAuth, maps:values(AtmTaskExecutionRegistry)
    ).


-spec ensure_all_ended([record()]) -> ok | no_return().
ensure_all_ended(AtmParallelBoxExecutions) ->
    pforeach_not_ended_task(fun atm_task_execution_handler:handle_ended/1, AtmParallelBoxExecutions).


-spec clean_all([record()]) -> ok.
clean_all(AtmParallelBoxExecutions) ->
    lists:foreach(fun clean/1, AtmParallelBoxExecutions).


-spec clean(record()) -> ok.
clean(#atm_parallel_box_execution{task_registry = AtmTaskExecutionRegistry}) ->
    atm_task_execution_api:clean_all(maps:values(AtmTaskExecutionRegistry)).


-spec delete_all([record()]) -> ok.
delete_all(AtmParallelBoxExecutions) ->
    lists:foreach(fun delete/1, AtmParallelBoxExecutions).


-spec delete(record()) -> ok.
delete(#atm_parallel_box_execution{task_registry = AtmTaskExecutions}) ->
    atm_task_execution_api:delete_all(maps:values(AtmTaskExecutions)).


-spec get_spec(record()) -> workflow_engine:parallel_box_spec().
get_spec(#atm_parallel_box_execution{task_registry = AtmTaskExecutions}) ->
    lists:foldl(fun(AtmTaskExecutionId, Acc) ->
        Acc#{AtmTaskExecutionId => atm_task_execution_api:get_spec(AtmTaskExecutionId)}
    end, #{}, maps:values(AtmTaskExecutions)).


-spec get_statuses([record()]) -> [AtmParallelBoxExecutionStatus :: atm_task_execution:status()].
get_statuses(AtmParallelBoxExecutions) ->
    lists:map(fun(#atm_parallel_box_execution{status = Status}) ->
        Status
    end, AtmParallelBoxExecutions).


%%--------------------------------------------------------------------
%% @doc
%% Updates specified task status.
%%
%%                              !! CAUTION !!
%% This function is called when updating atm_workflow_execution_doc and as such
%% shouldn't touch any other persistent models.
%% @end
%%--------------------------------------------------------------------
-spec update_task_status(atm_task_execution:id(), atm_task_execution:status(), record()) ->
    {ok, record()} | {error, term()}.
update_task_status(AtmTaskExecutionId, NewStatus, #atm_parallel_box_execution{
    task_statuses = AtmTaskExecutionStatuses
} = AtmParallelBoxExecution) ->
    AtmTaskExecutionStatus = maps:get(AtmTaskExecutionId, AtmTaskExecutionStatuses),

    case atm_task_execution_status:is_transition_allowed(AtmTaskExecutionStatus, NewStatus) of
        true ->
            NewAtmTaskExecutionStatuses = AtmTaskExecutionStatuses#{
                AtmTaskExecutionId => NewStatus
            },
            {ok, AtmParallelBoxExecution#atm_parallel_box_execution{
                status = atm_workflow_block_execution_status:infer(
                    maps:values(NewAtmTaskExecutionStatuses)
                ),
                task_statuses = NewAtmTaskExecutionStatuses
            }};
        false ->
            {error, AtmTaskExecutionStatus}
    end.


-spec to_json(record()) -> json_utils:json_term().
to_json(#atm_parallel_box_execution{
    schema_id = AtmParallelBoxSchemaId,
    status = AtmParallelBoxExecutionStatus,
    task_registry = AtmTaskExecutionRegistry
}) ->
    #{
        <<"schemaId">> => AtmParallelBoxSchemaId,
        <<"status">> => atom_to_binary(AtmParallelBoxExecutionStatus, utf8),
        <<"taskRegistry">> => AtmTaskExecutionRegistry
    }.


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
    task_registry = AtmTaskExecutionRegistry,
    task_statuses = AtmTaskExecutionStatuses
}, _NestedRecordEncoder) ->
    #{
        <<"schemaId">> => AtmParallelBoxSchemaId,
        <<"status">> => atom_to_binary(AtmParallelBoxExecutionStatus, utf8),
        <<"taskRegistry">> => AtmTaskExecutionRegistry,
        <<"taskStatuses">> => maps:map(fun(_AtmTaskExecutionId, AtmTaskExecutionStatus) ->
            atom_to_binary(AtmTaskExecutionStatus, utf8)
        end, AtmTaskExecutionStatuses)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"schemaId">> := AtmParallelBoxSchemaId,
    <<"status">> := AtmParallelBoxExecutionStatusBin,
    <<"taskRegistry">> := AtmTaskExecutionRegistry,
    <<"taskStatuses">> := AtmTaskExecutionStatusesJson
}, _NestedRecordDecoder) ->
    #atm_parallel_box_execution{
        schema_id = AtmParallelBoxSchemaId,
        status = binary_to_atom(AtmParallelBoxExecutionStatusBin, utf8),
        task_registry = AtmTaskExecutionRegistry,
        task_statuses = maps:map(fun(_AtmTaskExecutionId, AtmTaskExecutionStatusBin) ->
            binary_to_atom(AtmTaskExecutionStatusBin, utf8)
        end, AtmTaskExecutionStatusesJson)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes given Callback for each not ended task. Specified function
%% must take into account possible race conditions when task transition to
%% ended phase just right before Callback is called.
%% @end
%%--------------------------------------------------------------------
-spec pforeach_not_ended_task(
    fun((atm_task_execution:id()) -> ok | {error, term()}),
    [atm_task_execution:id()]
) ->
    ok | no_return().
pforeach_not_ended_task(Callback, AtmParallelBoxExecutions) ->
    atm_parallel_runner:foreach(fun(#atm_parallel_box_execution{
        status = AtmParallelBoxExecutionStatus,
        task_statuses = AtmTaskExecutionStatuses
    }) ->
        case atm_workflow_block_execution_status:is_ended(AtmParallelBoxExecutionStatus) of
            true ->
                ok;
            false ->
                atm_parallel_runner:foreach(fun({AtmTaskExecutionId, AtmTaskExecutionStatus}) ->
                    case atm_task_execution_status:is_ended(AtmTaskExecutionStatus) of
                        true -> ok;
                        false -> Callback(AtmTaskExecutionId)
                    end
                end, maps:to_list(AtmTaskExecutionStatuses))
        end
    end, AtmParallelBoxExecutions).
