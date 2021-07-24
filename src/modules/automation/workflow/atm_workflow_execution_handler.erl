%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements callbacks for handling automation workflow
%%% execution process.
%%% @end
%%%--------------------------------------------------------------------
-module(atm_workflow_execution_handler).
-author("Bartosz Walkowicz").

-behaviour(workflow_handler).

-include("modules/automation/atm_execution.hrl").
-include("workflow_engine.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").

% API
-export([
    init_engine/0,
    start/2,
    cancel/1
]).

% workflow_handler callbacks
-export([
    prepare/2,
    get_lane_spec/3,

    process_item/6,
    process_result/4,

    handle_task_execution_ended/3,
    handle_lane_execution_ended/3,
    handle_workflow_execution_ended/2
]).


-define(ATM_WORKFLOW_EXECUTION_ENGINE, <<"atm_workflow_execution_engine">>).

-define(ENGINE_ASYNC_CALLS_LIMIT, op_worker:get_env(atm_workflow_engine_async_calls_limit, 1000)).
-define(ENGINE_SLOTS_COUNT, op_worker:get_env(atm_workflow_engine_slots_count, 20)).
-define(JOB_TIMEOUT_SEC, op_worker:get_env(atm_workflow_job_timeout_sec, 300)).
-define(JOB_TIMEOUT_CHECK_PERIOD_SEC, op_worker:get_env(atm_workflow_job_timeout_check_period_sec, 300)).

-define(INITIAL_NOTIFICATION_INTERVAL(), rand:uniform(timer:seconds(2))).
-define(MAX_NOTIFICATION_INTERVAL, timer:hours(2)).
-define(MAX_NOTIFICATION_RETRIES, 30).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_engine() -> ok.
init_engine() ->
    Options = #{
        workflow_async_call_pools_to_use => [{?DEFAULT_ASYNC_CALL_POOL_ID, ?ENGINE_ASYNC_CALLS_LIMIT}],
        slots_limit => ?ENGINE_SLOTS_COUNT,
        default_keepalive_timeout => ?JOB_TIMEOUT_SEC,
        init_workflow_timeout_server => {true, ?JOB_TIMEOUT_CHECK_PERIOD_SEC}
    },
    workflow_engine:init(?ATM_WORKFLOW_EXECUTION_ENGINE, Options).


-spec start(user_ctx:ctx(), atm_workflow_execution:doc()) -> ok.
start(UserCtx, #document{
    key = AtmWorkflowExecutionId,
    value = #atm_workflow_execution{
        space_id = SpaceId,
        store_registry = AtmStoreRegistry
    }
}) ->
    ok = atm_workflow_execution_session:init(AtmWorkflowExecutionId, UserCtx),

    workflow_engine:execute_workflow(?ATM_WORKFLOW_EXECUTION_ENGINE, #{
        id => AtmWorkflowExecutionId,
        workflow_handler => ?MODULE,
        execution_context => atm_workflow_execution_env:build(
            SpaceId, AtmWorkflowExecutionId, AtmStoreRegistry
        )
    }).


-spec cancel(atm_workflow_execution:id()) -> ok | {error, already_ended}.
cancel(AtmWorkflowExecutionId) ->
    case atm_workflow_execution_status:handle_cancel(AtmWorkflowExecutionId) of
        ok ->
            workflow_engine:cancel_execution(AtmWorkflowExecutionId);
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% workflow_handler callbacks
%%%===================================================================


-spec prepare(atm_workflow_execution:id(), atm_workflow_execution_env:record()) ->
    ok | error.
prepare(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv) ->
    try
        prepare_internal(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO PREPARE WORKFLOW DUE TO: ~p", [
            AtmWorkflowExecutionId, Reason
        ]),
        error
    end.


-spec get_lane_spec(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    non_neg_integer()
) ->
    {ok, workflow_engine:lane_spec()} | error.
get_lane_spec(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmLaneIndex) ->
    try
        {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
        AtmLaneSchema = get_lane_schema(AtmLaneIndex, AtmWorkflowExecutionDoc),
        AtmLaneExecution = get_lane_execution(AtmLaneIndex, AtmWorkflowExecutionDoc),

        freeze_lane_iteration_store(AtmWorkflowExecutionEnv, AtmLaneSchema),

        {ok, #{
            parallel_boxes => atm_lane_execution:get_parallel_box_execution_specs(
                AtmLaneExecution
            ),
            iterator => acquire_iterator_for_lane(AtmWorkflowExecutionEnv, AtmLaneSchema),
            is_last => is_last_lane(AtmLaneIndex, AtmWorkflowExecutionDoc)
        }}
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO GET LANE ~p SPEC DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmLaneIndex, Reason
        ]),
        error
    end.


-spec process_item(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    automation:item(),
    binary(),
    binary()
) ->
    ok | error.
process_item(
    AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId,
    Item, ReportResultUrl, HeartbeatUrl
) ->
    try
        ok = atm_task_execution_handler:process_item(
            AtmWorkflowExecutionEnv, AtmTaskExecutionId, Item,
            ReportResultUrl, HeartbeatUrl
        )
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO RUN TASK ~p DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmTaskExecutionId, Reason
        ]),
        report_task_execution_failed(AtmWorkflowExecutionEnv, AtmTaskExecutionId),
        error
    end.


-spec process_result(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    {error, term()} | json_utils:json_map()
) ->
    ok | error.
process_result(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId, {error, _} = Error) ->
    % TODO VFS-7637 use audit log
    ?error("[~p] ASYNC TASK EXECUTION ~p FAILED DUE TO: ~p", [
        AtmWorkflowExecutionId, AtmTaskExecutionId, Error
    ]),
    report_task_execution_failed(AtmWorkflowExecutionEnv, AtmTaskExecutionId),
    error;

process_result(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId, Results) ->
    try
        atm_task_execution_handler:process_results(AtmWorkflowExecutionEnv, AtmTaskExecutionId, Results)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO PROCESS RESULTS FOR TASK EXECUTION ~p DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmTaskExecutionId, Reason
        ]),
        report_task_execution_failed(AtmWorkflowExecutionEnv, AtmTaskExecutionId),
        error
    end.


-spec handle_task_execution_ended(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    atm_task_execution:id()
) ->
    ok.
handle_task_execution_ended(AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv, AtmTaskExecutionId) ->
    try
        ok = atm_task_execution_handler:handle_ended(AtmTaskExecutionId)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO MARK TASK EXECUTION ~p AS ENDED DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmTaskExecutionId, Reason
        ])
    end.


-spec handle_lane_execution_ended(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record(),
    non_neg_integer()
) ->
    ok.
handle_lane_execution_ended(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmLaneIndex) ->
    try
        {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
        AtmLaneSchema = get_lane_schema(AtmLaneIndex, AtmWorkflowExecutionDoc),

        unfreeze_lane_iteration_store(AtmWorkflowExecutionEnv, AtmLaneSchema)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO MARK LANE EXECUTION ~p AS ENDED DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmLaneIndex, Reason
        ])
    end.


-spec handle_workflow_execution_ended(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record()
) ->
    ok.
handle_workflow_execution_ended(AtmWorkflowExecutionId, _AtmWorkflowExecutionEnv) ->
    try
        ensure_all_tasks_ended(AtmWorkflowExecutionId),

        {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution_status:handle_ended(
            AtmWorkflowExecutionId
        ),
        teardown(AtmWorkflowExecutionDoc),
        notify_ended(AtmWorkflowExecutionDoc)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO MARK WORKFLOW EXECUTION AS ENDED DUE TO: ~p", [
            AtmWorkflowExecutionId, Reason
        ])
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec prepare_internal(
    atm_workflow_execution:id(),
    atm_workflow_execution_env:record()
) ->
    ok | no_return().
prepare_internal(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv) ->
    {ok, #document{value = #atm_workflow_execution{
        lanes = AtmLaneExecutions
    }}} = atm_workflow_execution_status:handle_preparing(AtmWorkflowExecutionId),

    AtmWorkflowExecutionAuth = atm_workflow_execution_env:acquire_workflow_execution_auth(
        AtmWorkflowExecutionEnv
    ),
    atm_lane_execution:prepare_all(AtmWorkflowExecutionAuth, AtmLaneExecutions),

    atm_workflow_execution_status:handle_enqueued(AtmWorkflowExecutionId),
    ok.


%% @private
-spec is_last_lane(non_neg_integer(), atm_workflow_execution:doc()) ->
    boolean().
is_last_lane(AtmLaneIndex, #document{value = #atm_workflow_execution{
    lanes = AtmLaneExecutions
}}) ->
    AtmLaneIndex == length(AtmLaneExecutions).


%% @private
-spec get_lane_execution(non_neg_integer(), atm_workflow_execution:doc()) ->
    atm_lane_execution:record().
get_lane_execution(AtmLaneIndex, #document{value = #atm_workflow_execution{
    lanes = AtmLaneExecutions
}}) ->
    lists:nth(AtmLaneIndex, AtmLaneExecutions).


%% @private
-spec get_lane_schema(non_neg_integer(), atm_workflow_execution:doc()) ->
    atm_lane_schema:record().
get_lane_schema(AtmLaneIndex, #document{value = #atm_workflow_execution{
    schema_snapshot_id = AtmWorkflowSchemaSnapshotId
}}) ->
    {ok, #document{value = #atm_workflow_schema_snapshot{
        lanes = AtmLaneSchemas
    }}} = atm_workflow_schema_snapshot:get(AtmWorkflowSchemaSnapshotId),

    lists:nth(AtmLaneIndex, AtmLaneSchemas).


%% @private
-spec freeze_lane_iteration_store(atm_workflow_execution_env:record(), atm_lane_schema:record()) ->
    ok | no_return().
freeze_lane_iteration_store(AtmWorkflowExecutionEnv, AtmLaneSchema) ->
    AtmStoreId = get_lane_iteration_store_id(AtmWorkflowExecutionEnv, AtmLaneSchema),
    ok = atm_store_api:freeze(AtmStoreId).


%% @private
-spec unfreeze_lane_iteration_store(atm_workflow_execution_env:record(), atm_lane_schema:record()) ->
    ok | no_return().
unfreeze_lane_iteration_store(AtmWorkflowExecutionEnv, AtmLaneSchema) ->
    AtmStoreId = get_lane_iteration_store_id(AtmWorkflowExecutionEnv, AtmLaneSchema),
    ok = atm_store_api:unfreeze(AtmStoreId).


%% @private
-spec get_lane_iteration_store_id(atm_workflow_execution_env:record(), atm_lane_schema:record()) ->
    atm_store:id().
get_lane_iteration_store_id(AtmWorkflowExecutionEnv, #atm_lane_schema{
    store_iterator_spec = #atm_store_iterator_spec{store_schema_id = AtmStoreSchemaId}
}) ->
    atm_workflow_execution_env:get_store_id(AtmStoreSchemaId, AtmWorkflowExecutionEnv).


%% @private
-spec acquire_iterator_for_lane(atm_workflow_execution_env:record(), atm_lane_schema:record()) ->
    atm_store_iterator:record() | no_return().
acquire_iterator_for_lane(AtmWorkflowExecutionEnv, #atm_lane_schema{
    store_iterator_spec = AtmStoreIteratorSpec
}) ->
    atm_store_api:acquire_iterator(AtmWorkflowExecutionEnv, AtmStoreIteratorSpec).


%% @private
-spec report_task_execution_failed(atm_workflow_execution_env:record(), atm_task_execution:id()) ->
    ok.
report_task_execution_failed(AtmWorkflowExecutionEnv, AtmTaskExecutionId) ->
    catch atm_task_execution_handler:process_results(AtmWorkflowExecutionEnv, AtmTaskExecutionId, error),
    ok.


%% @private
-spec ensure_all_tasks_ended(atm_workflow_execution:id()) -> ok | no_return().
ensure_all_tasks_ended(AtmWorkflowExecutionId) ->
    {ok, #document{value = #atm_workflow_execution{
        lanes = AtmLaneExecutions
    }}} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    atm_lane_execution:ensure_all_ended(AtmLaneExecutions).


%% @private
-spec teardown(atm_workflow_execution:doc()) -> ok.
teardown(#document{
    key = AtmWorkflowExecutionId,
    value = #atm_workflow_execution{lanes = AtmLaneExecutions}
}) ->
    atm_lane_execution:clean_all(AtmLaneExecutions),
    atm_workflow_execution_session:terminate(AtmWorkflowExecutionId).


%% @private
-spec notify_ended(atm_workflow_execution:doc()) -> ok.
notify_ended(#document{value = #atm_workflow_execution{callback = undefined}}) ->
    ok;
notify_ended(#document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
    status = AtmWorkflowExecutionStatus,
    callback = CallbackUrl
}}) ->
    spawn(fun() ->
        Headers = #{
            ?HDR_CONTENT_TYPE => <<"application/json">>
        },
        Payload = json_utils:encode(#{
            <<"atmWorkflowExecutionId">> => AtmWorkflowExecutionId,
            <<"status">> => AtmWorkflowExecutionStatus
        }),
        try_to_notify(
            AtmWorkflowExecutionId, CallbackUrl, Headers, Payload,
            ?INITIAL_NOTIFICATION_INTERVAL(), ?MAX_NOTIFICATION_RETRIES + 1
        )
    end),
    ok.


%% @private
-spec try_to_notify(
    atm_workflow_execution:id(),
    http_client:url(),
    http_client:headers(),
    http_client:body(),
    non_neg_integer(),
    non_neg_integer()
) ->
    ok.
try_to_notify(AtmWorkflowExecutionId, CallbackUrl, _Headers, _Payload, _Interval, 0) ->
    ?error(
        "Failed to send atm workflow execution (~s) notification to '~s' (no retries left)",
        [AtmWorkflowExecutionId, CallbackUrl]
    );
try_to_notify(AtmWorkflowExecutionId, CallbackUrl, Headers, Payload, Interval, RetriesLeft) ->
    case send_notification(CallbackUrl, Headers, Payload) of
        ok ->
            ok;
        {error, _} = Error ->
            ?warning(
                "Failed to send atm workflow execution (~s) notification to ~s due to ~p.~n"
                "Next retry in ~p seconds. Number of retries left: ~p",
                [AtmWorkflowExecutionId, CallbackUrl, Error, Interval / 1000, RetriesLeft - 1]
            ),
            timer:sleep(Interval),
            NextInterval = min(2 * Interval, ?MAX_NOTIFICATION_INTERVAL),
            try_to_notify(AtmWorkflowExecutionId, CallbackUrl, Headers, Payload, NextInterval, RetriesLeft - 1)
    end.


%% @private
-spec send_notification(http_client:url(), http_client:headers(), http_client:body()) ->
    ok | {error, term()}.
send_notification(CallbackUrl, Headers, Payload) ->
    case http_client:post(CallbackUrl, Headers, Payload) of
        {ok, ResponseCode, _, ResponseBody} ->
            case http_utils:is_success_code(ResponseCode) of
                true -> ok;
                false -> {error, {http_response, ResponseCode, ResponseBody}}
            end;
        {error, _} = Error ->
            Error
    end.
