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
    start/3,
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
-define(JOB_TIMEOUT_SEC, op_worker:get_env(atm_workflow_job_timeout_sec, 1800)).
-define(JOB_TIMEOUT_CHECK_PERIOD_SEC, op_worker:get_env(atm_workflow_job_timeout_check_period_sec, 1800)).

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


-spec start(user_ctx:ctx(), atm_workflow_execution:id(), atm_workflow_execution_env:record()) ->
    ok.
start(UserCtx, AtmWorkflowExecutionId, AtmWorkflowExecutionEnv) ->
    ok = atm_workflow_execution_session:init(AtmWorkflowExecutionId, UserCtx),

    workflow_engine:execute_workflow(?ATM_WORKFLOW_EXECUTION_ENGINE, #{
        id => AtmWorkflowExecutionId,
        workflow_handler => ?MODULE,
        execution_context => AtmWorkflowExecutionEnv
    }).


-spec cancel(atm_workflow_execution:id()) -> ok | {error, already_ended}.
cancel(AtmWorkflowExecutionId) ->
    case atm_workflow_execution_status:handle_aborting(AtmWorkflowExecutionId, cancel) of
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
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(undefined, AtmWorkflowExecutionEnv),

    try
        prepare_internal(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx)
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
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(undefined, AtmWorkflowExecutionEnv),

    try
        {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
        AtmLaneSchema = get_lane_schema(AtmLaneIndex, AtmWorkflowExecutionDoc),
        AtmLaneExecution = get_lane_execution(AtmLaneIndex, AtmWorkflowExecutionDoc),

        freeze_lane_iteration_store(AtmWorkflowExecutionCtx, AtmLaneSchema),

        {ok, #{
            parallel_boxes => atm_lane_execution:get_parallel_box_execution_specs(
                AtmLaneExecution
            ),
            iterator => acquire_iterator_for_lane(AtmWorkflowExecutionCtx, AtmLaneSchema),
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
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(
        AtmTaskExecutionId, AtmWorkflowExecutionEnv
    ),
    % TODO VFS-8101 Better debug logs about items in processing
%%    AtmWorkflowExecutionLogger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
%%    log_item_processing(Item, AtmWorkflowExecutionLogger),

    try
        ok = atm_task_execution_handler:process_item(
            AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item,
            ReportResultUrl, HeartbeatUrl
        )
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO RUN TASK ~p DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmTaskExecutionId, Reason
        ]),
        report_task_execution_failed(AtmWorkflowExecutionCtx, AtmTaskExecutionId),
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
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(
        AtmTaskExecutionId, AtmWorkflowExecutionEnv
    ),
    % TODO VFS-7637 use audit log
    ?error("[~p] ASYNC TASK EXECUTION ~p FAILED DUE TO: ~p", [
        AtmWorkflowExecutionId, AtmTaskExecutionId, Error
    ]),

    report_task_execution_failed(AtmWorkflowExecutionCtx, AtmTaskExecutionId),
    error;

process_result(AtmWorkflowExecutionId, AtmWorkflowExecutionEnv, AtmTaskExecutionId, Results) ->
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(
        AtmTaskExecutionId, AtmWorkflowExecutionEnv
    ),

    try
        atm_task_execution_handler:process_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Results)
    catch _:Reason ->
        % TODO VFS-7637 use audit log
        ?error("[~p] FAILED TO PROCESS RESULTS FOR TASK EXECUTION ~p DUE TO: ~p", [
            AtmWorkflowExecutionId, AtmTaskExecutionId, Reason
        ]),
        report_task_execution_failed(AtmWorkflowExecutionCtx, AtmTaskExecutionId),
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
    AtmWorkflowExecutionCtx = atm_workflow_execution_ctx:acquire(undefined, AtmWorkflowExecutionEnv),

    try
        {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
        AtmLaneSchema = get_lane_schema(AtmLaneIndex, AtmWorkflowExecutionDoc),

        unfreeze_lane_iteration_store(AtmWorkflowExecutionCtx, AtmLaneSchema)
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
    atm_workflow_execution_ctx:record()
) ->
    ok | no_return().
prepare_internal(AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {ok, #document{value = #atm_workflow_execution{
        lanes = AtmLaneExecutions
    }}} = atm_workflow_execution_status:handle_preparing(AtmWorkflowExecutionId),

    try
        atm_lane_execution:prepare_all(AtmWorkflowExecutionCtx, AtmLaneExecutions)
    catch Type:Reason ->
        atm_workflow_execution_status:handle_aborting(AtmWorkflowExecutionId, failure),
        erlang:Type(Reason)
    end,

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
-spec freeze_lane_iteration_store(atm_workflow_execution_ctx:record(), atm_lane_schema:record()) ->
    ok | no_return().
freeze_lane_iteration_store(AtmWorkflowExecutionCtx, AtmLaneSchema) ->
    AtmStoreId = get_lane_iteration_store_id(AtmWorkflowExecutionCtx, AtmLaneSchema),
    ok = atm_store_api:freeze(AtmStoreId).


%% @private
-spec unfreeze_lane_iteration_store(atm_workflow_execution_ctx:record(), atm_lane_schema:record()) ->
    ok | no_return().
unfreeze_lane_iteration_store(AtmWorkflowExecutionCtx, AtmLaneSchema) ->
    AtmStoreId = get_lane_iteration_store_id(AtmWorkflowExecutionCtx, AtmLaneSchema),
    ok = atm_store_api:unfreeze(AtmStoreId).


%% @private
-spec get_lane_iteration_store_id(atm_workflow_execution_ctx:record(), atm_lane_schema:record()) ->
    atm_store:id().
get_lane_iteration_store_id(AtmWorkflowExecutionCtx, #atm_lane_schema{
    store_iterator_spec = #atm_store_iterator_spec{store_schema_id = AtmStoreSchemaId}
}) ->
    atm_workflow_execution_ctx:get_workflow_store_id(AtmStoreSchemaId, AtmWorkflowExecutionCtx).


%% @private
-spec acquire_iterator_for_lane(atm_workflow_execution_ctx:record(), atm_lane_schema:record()) ->
    atm_store_iterator:record() | no_return().
acquire_iterator_for_lane(AtmWorkflowExecutionCtx, #atm_lane_schema{
    store_iterator_spec = AtmStoreIteratorSpec = #atm_store_iterator_spec{
        store_schema_id = AtmStoreSchemaId
    }
}) ->
    AtmStoreId = atm_workflow_execution_ctx:get_workflow_store_id(
        AtmStoreSchemaId, AtmWorkflowExecutionCtx
    ),
    atm_store_api:acquire_iterator(AtmStoreId, AtmStoreIteratorSpec).


% TODO VFS-8101 Better debug logs about items in processing
%% @private
-spec log_item_processing(automation:item(), atm_workflow_execution_logger:record()) ->
    ok.
log_item_processing(Item, AtmWorkflowExecutionLogger) ->
    EncodedItem = try
        json_utils:encode(Item)
    catch _:_ ->
        str_utils:format_bin("~p", [Item])
    end,
    Entry = #{
        <<"description">> => <<"Processing item">>,
        %% TODO VFS-8101 replace magic numbers with size limit for each atm data type
        <<"item">> => case size(EncodedItem) > 1000 of
            true ->
                <<Chunk:996/binary, _/binary>> = EncodedItem,
                <<Chunk/binary, "...\"">>;
            false -> Item
        end
    },
    atm_workflow_execution_logger:task_debug(Entry, AtmWorkflowExecutionLogger).


%% @private
-spec report_task_execution_failed(atm_workflow_execution_ctx:record(), atm_task_execution:id()) ->
    ok.
report_task_execution_failed(AtmWorkflowExecutionCtx, AtmTaskExecutionId) ->
    catch atm_task_execution_handler:process_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, error),
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
