%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for storing information about automation task execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([create/1, get/1, update/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0, upgrade_record/2]).


-type id() :: binary().
-type record() :: #atm_task_execution{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-type status() ::
    ?PENDING_STATUS |
    ?ACTIVE_STATUS |
    ?FINISHED_STATUS | ?FAILED_STATUS | ?SKIPPED_STATUS.

-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([status/0]).


-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(record()) -> {ok, doc()} | {error, term()}.
create(AtmTaskExecutionRecord) ->
    datastore_model:create(?CTX, #document{value = AtmTaskExecutionRecord}).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(AtmTaskExecutionId) ->
    datastore_model:get(?CTX, AtmTaskExecutionId).


-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(AtmTaskExecutionId, Diff1) ->
    Diff2 = fun(#atm_task_execution{status = PrevStatus} = AtmTaskExecution) ->
        case Diff1(AtmTaskExecution#atm_task_execution{status_changed = false}) of
            {ok, #atm_task_execution{status = NewStatus} = NewAtmTaskExecution} ->
                {ok, NewAtmTaskExecution#atm_task_execution{
                    status_changed = NewStatus /= PrevStatus
                }};
            {error, _} = Error ->
                Error
        end
    end,
    datastore_model:update(?CTX, AtmTaskExecutionId, Diff2).


-spec delete(id()) -> ok | {error, term()}.
delete(AtmStoreId) ->
    datastore_model:delete(?CTX, AtmStoreId).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {workflow_execution_id, string},
        {lane_index, integer},
        {parallel_box_index, integer},

        {schema_id, string},

        {executor, {custom, string, {persistent_record, encode, decode, atm_task_executor}}},
        {argument_specs, [{custom, string, {
            persistent_record, encode, decode, atm_task_execution_argument_spec
        }}]},
        {result_specs, [{custom, string, {
            persistent_record, encode, decode, atm_task_execution_result_spec
        }}]},

        {status, atom},
        {status_changed, boolean},

        {items_in_processing, integer},
        {items_processed, integer},
        {items_failed, integer}
    ]};
get_record_struct(2) ->
    {record, [
        {workflow_execution_id, string},
        {lane_index, integer},
        {parallel_box_index, integer},

        {schema_id, string},

        {executor, {custom, string, {persistent_record, encode, decode, atm_task_executor}}},
        {argument_specs, [{custom, string, {
            persistent_record, encode, decode, atm_task_execution_argument_spec
        }}]},
        {result_specs, [{custom, string, {
            persistent_record, encode, decode, atm_task_execution_result_spec
        }}]},

        {system_audit_log_id, string},  %% new field

        {status, atom},
        {status_changed, boolean},
        {aborting_reason, atom},  %% new field

        {items_in_processing, integer},
        {items_processed, integer},
        {items_failed, integer}
    ]}.


%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {
    ?MODULE,
    AtmWorkflowExecutionId,
    LaneIndex,
    ParallelBoxIndex,
    SchemaId,
    Executor,
    ArgumentSpecs,
    ResultSpecs,
    Status,
    StatusChanged,
    ItemsInProcessing,
    ItemsProcessed,
    ItemsFailed
}) ->
    {2, #atm_task_execution{
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_index = LaneIndex,
        parallel_box_index = ParallelBoxIndex,

        schema_id = SchemaId,

        executor = Executor,
        argument_specs = ArgumentSpecs,
        result_specs = ResultSpecs,

        system_audit_log_id = undefined,

        status = Status,
        status_changed = StatusChanged,
        aborting_reason = undefined,

        items_in_processing = ItemsInProcessing,
        items_processed = ItemsProcessed,
        items_failed = ItemsFailed
    }}.