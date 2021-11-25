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
-export([create/1, get/1, update/2, delete/1]).

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

-type phase() :: ?WAITING_PHASE | ?ONGOING_PHASE | ?ENDED_PHASE.

-type status() ::
    ?SCHEDULED_STATUS |
    ?ACTIVE_STATUS | ?ABORTING_STATUS |
    ?FINISHED_STATUS | ?CANCELLED_STATUS | ?FAILED_STATUS.

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
    datastore_model:get(?CTX, AtmWorkflowExecutionId).


-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(AtmWorkflowExecutionId, Diff1) ->
    Diff2 = fun(#atm_workflow_execution{status = PrevStatus} = AtmWorkflowExecution) ->
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
    4.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {space_id, string},
        {atm_inventory_id, string},

        {name, string},
        {schema_snapshot_id, string},
        {lambda_snapshot_registry, #{string => string}},

        {store_registry, #{string => string}},
        {lanes, [{custom, string, {persistent_record, encode, decode, atm_lane_execution}}]},

        {status, atom},
        {status_changed, boolean},

        {callback, string},

        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer}
    ]};
get_record_struct(2) ->
    {record, [
        {user_id, string},  %% new field
        {space_id, string},
        {atm_inventory_id, string},

        {name, string},
        {schema_snapshot_id, string},
        {lambda_snapshot_registry, #{string => string}},

        {store_registry, #{string => string}},
        {lanes, [{custom, string, {persistent_record, encode, decode, atm_lane_execution}}]},

        {status, atom},
        {prev_status, atom},  %% This field was previously `status_changed`

        {callback, string},

        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer}
    ]};
get_record_struct(3) ->
    {record, [
        {user_id, string},
        {space_id, string},
        {atm_inventory_id, string},

        {name, string},
        {schema_snapshot_id, string},
        {lambda_snapshot_registry, #{string => string}},

        {store_registry, #{string => string}},
        {system_audit_log_id, string},  %% new field
        {lanes, [{custom, string, {persistent_record, encode, decode, atm_lane_execution}}]},

        {status, atom},
        {prev_status, atom},
        {aborting_reason, atom},  %% new field

        {callback, string},

        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer}
    ]};
get_record_struct(4) ->
    % 'aborting_reason' field was removed
    {record, [
        {user_id, string},
        {space_id, string},
        {atm_inventory_id, string},

        {name, string},
        {schema_snapshot_id, string},
        {lambda_snapshot_registry, #{string => string}},

        {store_registry, #{string => string}},
        {system_audit_log_id, string},

        % this field structure was changed from list to map
        {lanes, #{integer => {custom, string, {persistent_record, encode, decode, atm_lane_execution}}}},
        {lanes_count, integer},      %% new field

        {incarnation, integer},          %% new field
        {current_lane_index, integer},   %% new field
        {current_run_num, integer},      %% new field

        % ?PREPARING_STATUS and ?ENQUEUED_STATUS were removed from possible workflow statuses
        {status, atom},
        {prev_status, atom},

        {callback, string},

        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer}
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
    SpaceId,
    AtmInventoryId,
    Name,
    SchemaSnapshotId,
    LambdaSnapshotRegistry,
    StoreRegistry,
    Lanes,
    Status,
    _StatusChanged,
    Callback,
    ScheduleTime,
    StartTime,
    FinishTime
}) ->
    {2, {?MODULE,
        <<"unknown">>,
        SpaceId,
        AtmInventoryId,

        Name,
        SchemaSnapshotId,
        LambdaSnapshotRegistry,

        StoreRegistry,
        Lanes,

        Status,
        Status,

        Callback,

        ScheduleTime,
        StartTime,
        FinishTime
    }};
upgrade_record(2, {
    ?MODULE,
    UserId,
    SpaceId,
    AtmInventoryId,
    Name,
    SchemaSnapshotId,
    LambdaSnapshotRegistry,
    StoreRegistry,
    Lanes,
    Status,
    PrevStatus,
    Callback,
    ScheduleTime,
    StartTime,
    FinishTime
}) ->
    {3, {?MODULE,
        UserId,
        SpaceId,
        AtmInventoryId,

        Name,
        SchemaSnapshotId,
        LambdaSnapshotRegistry,

        StoreRegistry,
        undefined,
        Lanes,

        Status,
        PrevStatus,
        undefined,

        Callback,

        ScheduleTime,
        StartTime,
        FinishTime
    }};
upgrade_record(3, {?MODULE,
    UserId,
    SpaceId,
    AtmInventoryId,

    Name,
    SchemaSnapshotId,
    LambdaSnapshotRegistry,

    StoreRegistry,
    AtmSystemAuditLogId,
    Lanes,

    Status,
    PrevStatus,
    _AbortingReason,

    Callback,

    ScheduleTime,
    StartTime,
    FinishTime
}) ->
    {4, #atm_workflow_execution{
        user_id = UserId,
        space_id = SpaceId,
        atm_inventory_id = AtmInventoryId,

        name = Name,
        schema_snapshot_id = SchemaSnapshotId,
        lambda_snapshot_registry = LambdaSnapshotRegistry,

        store_registry = StoreRegistry,
        system_audit_log_id = AtmSystemAuditLogId,

        lanes = maps:from_list(lists_utils:enumerate(Lanes)),
        lanes_count = length(Lanes),

        incarnation = 1,
        current_lane_index = 1,
        current_run_num = 1,

        status = case lists:member(Status, [?PREPARING_STATUS, ?ENQUEUED_STATUS]) of
            true -> ?SCHEDULED_STATUS;
            false -> Status
        end,
        prev_status = case lists:member(PrevStatus, [?PREPARING_STATUS, ?ENQUEUED_STATUS]) of
            true -> ?SCHEDULED_STATUS;
            false -> Status
        end,

        callback = Callback,

        schedule_time = ScheduleTime,
        start_time = StartTime,
        finish_time = FinishTime
    }}.
