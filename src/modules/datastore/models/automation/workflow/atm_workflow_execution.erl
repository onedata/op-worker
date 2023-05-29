%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing information about automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    create/1,
    get/1, get/2,
    update/2, update/3,
    delete/1
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, upgrade_record/2]).


-type id() :: binary().
-type diff() :: datastore_doc:diff(record()).
-type record() :: #atm_workflow_execution{}.
-type doc() :: datastore_doc:doc(record()).

-type store_registry() :: #{AtmStoreSchemaId :: automation:id() => atm_store:id()}.
-type lambda_snapshot_registry() :: #{od_atm_lambda:id() => atm_lambda_snapshot:id()}.

-type repeat_type() :: rerun | retry.
%% Incarnation tells how many times given atm workflow execution was run
%% (origin run + manual repeats)
-type incarnation() :: non_neg_integer().

-type phase() :: ?WAITING_PHASE | ?ONGOING_PHASE | ?SUSPENDED_PHASE | ?ENDED_PHASE.

-type status() ::
    % waiting
    ?RESUMING_STATUS | ?SCHEDULED_STATUS |
    % ongoing
    ?ACTIVE_STATUS | ?STOPPING_STATUS |
    % suspended
    ?INTERRUPTED_STATUS | ?PAUSED_STATUS |
    % ended
    ?FINISHED_STATUS | ?CRASHED_STATUS | ?CANCELLED_STATUS | ?FAILED_STATUS.

-type timestamp() :: time:seconds().

-type summary() :: #atm_workflow_execution_summary{}.

-export_type([id/0, diff/0, record/0, doc/0]).
-export_type([store_registry/0, lambda_snapshot_registry/0]).
-export_type([repeat_type/0, incarnation/0]).
-export_type([phase/0, status/0, timestamp/0]).
-export_type([summary/0]).


-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec create(doc()) -> {ok, doc()} | {error, term()}.
create(AtmWorkflowExecutionDoc) ->
    datastore_model:create(?CTX, AtmWorkflowExecutionDoc).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(AtmWorkflowExecutionId) ->
    get(AtmWorkflowExecutionId, ignore_discarded).


-spec get(id(), ignore_discarded | include_discarded) ->
    {ok, doc()} | {error, term()}.
get(AtmWorkflowExecutionId, ignore_discarded) ->
    case get(AtmWorkflowExecutionId, include_discarded) of
        {ok, #document{value = #atm_workflow_execution{discarded = true}}} ->
            ?ERROR_NOT_FOUND;
        Result ->
            Result
    end;

get(AtmWorkflowExecutionId, include_discarded) ->
    datastore_model:get(?CTX, AtmWorkflowExecutionId).


-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(AtmWorkflowExecutionId, Diff1) ->
    update(AtmWorkflowExecutionId, Diff1, ignore_discarded).


-spec update(id(), diff(), ignore_discarded | include_discarded) ->
    {ok, doc()} | {error, term()}.
update(AtmWorkflowExecutionId, Diff1, Policy) ->
    Diff2 = fun
        (#atm_workflow_execution{discarded = true}) when Policy =:= ignore_discarded ->
            ?ERROR_NOT_FOUND;
        (#atm_workflow_execution{status = PrevStatus} = AtmWorkflowExecution) ->
            Diff1(AtmWorkflowExecution#atm_workflow_execution{prev_status = PrevStatus})
    end,
    datastore_model:update(?CTX, AtmWorkflowExecutionId, Diff2).


-spec delete(id()) -> ok | {error, term()}.
delete(AtmWorkflowExecutionId) ->
    datastore_model:delete(?CTX, AtmWorkflowExecutionId).


%%%===================================================================
%%% Datastore callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {discarded, boolean},

        {user_id, string},
        {space_id, string},
        {atm_inventory_id, string},

        {name, string},
        {schema_snapshot_id, string},
        {lambda_snapshot_registry, #{string => string}},

        {store_registry, #{string => string}},
        {system_audit_log_store_id, string},

        {lanes, #{integer => {custom, string, {persistent_record, encode, decode, atm_lane_execution}}}},
        {lanes_count, integer},

        {incarnation, integer},
        {current_lane_index, integer},
        {current_run_num, integer},

        {status, atom},
        {prev_status, atom},

        {callback, string},

        {schedule_time, integer},
        {start_time, integer},
        {suspend_time, integer},
        {finish_time, integer}
    ]};

get_record_struct(2) ->
    % New fields:
    % - logging_level
    {record, [
        {discarded, boolean},

        {user_id, string},
        {space_id, string},
        {atm_inventory_id, string},

        {name, string},
        {schema_snapshot_id, string},
        {lambda_snapshot_registry, #{string => string}},

        {store_registry, #{string => string}},
        {system_audit_log_store_id, string},

        {lanes, #{integer => {custom, string, {persistent_record, encode, decode, atm_lane_execution}}}},
        {lanes_count, integer},

        {incarnation, integer},
        {current_lane_index, integer},
        {current_run_num, integer},

        {status, atom},
        {prev_status, atom},

        {logging_level, string},  % new field

        {callback, string},

        {schedule_time, integer},
        {start_time, integer},
        {suspend_time, integer},
        {finish_time, integer}
    ]}.


-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE,
    Discarded,
    UserId, SpaceId, AtmInventoryId,
    Name, SchemaSnapshotId, LambdaRevisionRegistry,
    StoreRegistry, SystemAuditLogStoreId,
    Lanes, LanesCount,
    Incarnation, CurrentLaneIndex, CurrentRunNum,
    Status, PrevStatus,
    Callback,
    ScheduleTime, StartTime, SuspendTime, FinishTime
}) ->
    {2, {?MODULE,
        Discarded,
        UserId, SpaceId, AtmInventoryId,
        Name, SchemaSnapshotId, LambdaRevisionRegistry,
        StoreRegistry, SystemAuditLogStoreId,
        Lanes, LanesCount,
        Incarnation, CurrentLaneIndex, CurrentRunNum,
        Status, PrevStatus,
        ?LOGGER_INFO,
        Callback,
        ScheduleTime, StartTime, SuspendTime, FinishTime
    }}.
