%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for performing operations on automation workflow executions.
%%% An execution is created according to specified automation workflow schema
%%% revision - to learn more about overall workflow concept @see automation.erl
%%% Those schemas (workflow and all used lambdas) are fetched from Onezone and
%%% saved at the beginning of execution (they can be changed so to ensure proper
%%% execution snapshot of concrete version must be made).
%%% Automation workflow execution consists of execution of lanes each of which
%%% is made of one or more runs. Lane execution *run* is an attempt of execution
%%% of particular lane schema. A run may fail and then a new run is created for
%%% this lane - up to max retries limit specified in schema. With this (new run
%%% is created rather than clearing and restarting the failed one) it is possible
%%% to view details of all lane execution runs.
%%% Beside automatic retries lanes can also be repeated after execution ended.
%%% Two types of manual repeat is supported:
%%% - retry - new lane run execution is created for retried lane which will
%%%           operate only on failed items.
%%% - rerun - new lane run execution is created for rerun lane which will
%%%           operate on all items from iterated store.
%%% When repeating lanes entire workflow execution is started anew (new workflow
%%% execution incarnation) but it is not cleared from accumulated data
%%% (e.g. store content).
%%% To learn more about stages of execution:
%%% - @see atm_workflow_execution_status.erl - for overall workflow execution
%%% - @see atm_lane_execution_status.erl - for particular lane execution
%%% - @see atm_task_execution_handler.erl - for particular task execution
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_api).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    init_engine/0
]).
-export([
    list/4,
    foreach/3,
    foldl/4
]).
-export([
    schedule/6,
    get/1, get_summary/2,
    init_cancel/2,
    init_pause/2,
    resume/2,
    repeat/4,
    discard/1
]).
-export([report_openfaas_down/2]).


-type listing_mode() :: basic | summary.

-type basic_entries() :: atm_workflow_executions_forest:entries().
-type summary_entries() :: [{atm_workflow_executions_forest:index(), atm_workflow_execution:summary()}].
-type entries() :: basic_entries() | summary_entries().

-type store_initial_content_overlay() :: #{AtmStoreSchemaId :: automation:id() => json_utils:json_term()}.

-export_type([listing_mode/0, basic_entries/0, summary_entries/0, entries/0]).
-export_type([store_initial_content_overlay/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_engine() -> ok.
init_engine() ->
    atm_workflow_execution_handler:init_engine().


-spec list(
    od_space:id(),
    atm_workflow_execution:phase(),
    listing_mode(),
    atm_workflow_executions_forest:listing_opts()
) ->
    {ok, entries(), IsLast :: boolean()}.
list(SpaceId, Phase, basic, ListingOpts) ->
    AtmWorkflowExecutionBasicEntries = list_basic_entries(SpaceId, Phase, ListingOpts),
    IsLast = maps:get(limit, ListingOpts) > length(AtmWorkflowExecutionBasicEntries),

    {ok, AtmWorkflowExecutionBasicEntries, IsLast};

list(SpaceId, Phase, summary, ListingOpts) ->
    AtmWorkflowExecutionBasicEntries = list_basic_entries(SpaceId, Phase, ListingOpts),
    IsLast = maps:get(limit, ListingOpts) > length(AtmWorkflowExecutionBasicEntries),

    AtmWorkflowExecutionSummaryEntries = lists_utils:pfiltermap(fun({Index, AtmWorkflowExecutionId}) ->
        {ok, #document{value = AtmWorkflowExecution}} = atm_workflow_execution:get(
            AtmWorkflowExecutionId
        ),
        case atm_workflow_execution_status:infer_phase(AtmWorkflowExecution) of
            Phase ->
                {true, {Index, get_summary(AtmWorkflowExecutionId, AtmWorkflowExecution)}};
            _ ->
                false
        end
    end, AtmWorkflowExecutionBasicEntries),

    {ok, AtmWorkflowExecutionSummaryEntries, IsLast}.


-spec foreach(
    od_space:id(),
    atm_workflow_execution:phase(),
    fun((atm_workflow_execution:id()) -> term())
) ->
    ok.
foreach(SpaceId, Phase, Callback) ->
    foldl(SpaceId, Phase, fun(AtmWorkflowExecutionId, _) -> Callback(AtmWorkflowExecutionId) end, ok),
    ok.


-spec foldl(
    od_space:id(),
    atm_workflow_execution:phase(),
    fun((atm_workflow_execution:id(), AccIn :: term()) -> AccOut :: term()),
    InitialAcc :: term()
) ->
    term().
foldl(SpaceId, Phase, Callback, InitialAcc) ->
    foldl(SpaceId, Phase, Callback, InitialAcc, #{limit => 1000, start_index => <<>>}).


-spec schedule(
    user_ctx:ctx(),
    od_space:id(),
    od_atm_workflow_schema:id(),
    atm_workflow_schema_revision:revision_number(),
    store_initial_content_overlay(),
    undefined | http_client:url()
) ->
    {atm_workflow_execution:id(), atm_workflow_execution:record()} | no_return().
schedule(
    UserCtx,
    SpaceId,
    AtmWorkflowSchemaId,
    AtmWorkflowSchemaRevisionNum,
    AtmStoreInitialContentOverlay,
    CallbackUrl
) ->
    {AtmWorkflowExecutionDoc, AtmWorkflowExecutionEnv} = atm_workflow_execution_factory:create(
        UserCtx,
        SpaceId,
        AtmWorkflowSchemaId,
        AtmWorkflowSchemaRevisionNum,
        AtmStoreInitialContentOverlay,
        CallbackUrl
    ),
    atm_workflow_execution_handler:start(UserCtx, AtmWorkflowExecutionEnv, AtmWorkflowExecutionDoc),

    {AtmWorkflowExecutionDoc#document.key, AtmWorkflowExecutionDoc#document.value}.


-spec get(atm_workflow_execution:id()) ->
    {ok, atm_workflow_execution:record()} | ?ERROR_NOT_FOUND.
get(AtmWorkflowExecutionId) ->
    case atm_workflow_execution:get(AtmWorkflowExecutionId) of
        {ok, #document{value = AtmWorkflowExecution}} ->
            {ok, AtmWorkflowExecution};
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND
    end.


-spec get_summary(atm_workflow_execution:id(), atm_workflow_execution:record()) ->
    atm_workflow_execution:summary().
get_summary(AtmWorkflowExecutionId, #atm_workflow_execution{
    name = Name,
    schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
    atm_inventory_id = AtmInventoryId,
    status = AtmWorkflowExecutionStatus,
    schedule_time = ScheduleTime,
    start_time = StartTime,
    suspend_time = SuspendTime,
    finish_time = FinishTime
}) ->
    {ok, #document{
        value = #atm_workflow_schema_snapshot{
            revision_number = RevisionNum
        }
    }} = atm_workflow_schema_snapshot:get(AtmWorkflowSchemaSnapshotId),

    #atm_workflow_execution_summary{
        atm_workflow_execution_id = AtmWorkflowExecutionId,
        name = Name,
        atm_workflow_schema_revision_num = RevisionNum,
        atm_inventory_id = AtmInventoryId,
        status = AtmWorkflowExecutionStatus,
        schedule_time = ScheduleTime,
        start_time = StartTime,
        suspend_time = SuspendTime,
        finish_time = FinishTime
    }.


-spec init_cancel(user_ctx:ctx(), atm_workflow_execution:id()) -> ok | errors:error().
init_cancel(UserCtx, AtmWorkflowExecutionId) ->
    atm_workflow_execution_handler:init_stop(UserCtx, AtmWorkflowExecutionId, cancel).


-spec init_pause(user_ctx:ctx(), atm_workflow_execution:id()) -> ok | errors:error().
init_pause(UserCtx, AtmWorkflowExecutionId) ->
    atm_workflow_execution_handler:init_stop(UserCtx, AtmWorkflowExecutionId, pause).


-spec resume(user_ctx:ctx(), atm_workflow_execution:id()) -> ok | errors:error().
resume(UserCtx, AtmWorkflowExecutionId) ->
    atm_workflow_execution_handler:resume(UserCtx, AtmWorkflowExecutionId).


-spec repeat(
    user_ctx:ctx(),
    atm_workflow_execution:repeat_type(),
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id()
) ->
    ok | errors:error().
repeat(UserCtx, Type, AtmLaneRunSelector, AtmWorkflowExecutionId) ->
    atm_workflow_execution_handler:repeat(
        UserCtx, Type, AtmLaneRunSelector, AtmWorkflowExecutionId
    ).


-spec discard(atm_workflow_execution:id()) -> ok | errors:error().
discard(AtmWorkflowExecutionId) ->
    atm_workflow_execution_status:handle_discard(AtmWorkflowExecutionId).


-spec report_openfaas_down(od_space:id(), errors:error()) -> ok.
report_openfaas_down(SpaceId, Error) ->
    CallbackFun = fun(AtmWorkflowExecutionId) ->
        try
            atm_workflow_execution_handler:on_openfaas_down(AtmWorkflowExecutionId, Error)
        catch Type:Reason:Stacktrace ->
            ?atm_examine_error(Type, Reason, Stacktrace)
        end
    end,

    foreach(SpaceId, ?WAITING_PHASE, CallbackFun),
    foreach(SpaceId, ?ONGOING_PHASE, CallbackFun).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec list_basic_entries(
    od_space:id(),
    atm_workflow_execution:phase(),
    atm_workflow_executions_forest:listing_opts()
) ->
    basic_entries().
list_basic_entries(SpaceId, ?WAITING_PHASE, ListingOpts) ->
    atm_waiting_workflow_executions:list(SpaceId, ListingOpts);
list_basic_entries(SpaceId, ?ONGOING_PHASE, ListingOpts) ->
    atm_ongoing_workflow_executions:list(SpaceId, ListingOpts);
list_basic_entries(SpaceId, ?SUSPENDED_PHASE, ListingOpts) ->
    atm_suspended_workflow_executions:list(SpaceId, ListingOpts);
list_basic_entries(SpaceId, ?ENDED_PHASE, ListingOpts) ->
    atm_ended_workflow_executions:list(SpaceId, ListingOpts).


%% @private
-spec foldl(
    od_space:id(),
    atm_workflow_execution:phase(),
    fun((atm_workflow_execution:id(), AccIn :: term()) -> AccOut :: term()),
    InitialAcc :: term(),
    atm_workflow_executions_forest:listing_opts()
) ->
    term().
foldl(SpaceId, Phase, Callback, InitialAcc, ListingOpts) ->
    {ok, AtmWorkflowExecutionBasicEntries, IsLast} = list(SpaceId, Phase, basic, ListingOpts),

    {LastEntryIndex, NewAcc} = lists:foldl(fun({Index, AtmWorkflowExecutionId}, {_, AccIn}) ->
        {Index, Callback(AtmWorkflowExecutionId, AccIn)}
    end, {<<>>, InitialAcc}, AtmWorkflowExecutionBasicEntries),

    case IsLast of
        true ->
            NewAcc;
        false ->
            foldl(SpaceId, Phase, Callback, NewAcc, ListingOpts#{
                start_index => LastEntryIndex, offset => 1
            })
    end.
