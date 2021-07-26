%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for performing operations on automation workflow executions.
%%% TODO VFS-7674 Describe automation workflow execution machinery
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_api).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([init_engine/0]).
-export([
    list/4,
    schedule/5,
    get/1, get_summary/2,
    cancel/1,
    terminate_not_ended/1
]).


-type listing_mode() :: basic | summary.

-type basic_entries() :: atm_workflow_executions_forest:entries().
-type summary_entries() :: [{atm_workflow_executions_forest:index(), atm_workflow_execution:summary()}].
-type entries() :: basic_entries() | summary_entries().

-type store_initial_values() :: #{
    AtmStoreSchemaId :: automation:id() => atm_store_api:initial_value()
}.

-export_type([listing_mode/0, basic_entries/0, summary_entries/0, entries/0]).
-export_type([store_initial_values/0]).


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


-spec schedule(
    user_ctx:ctx(),
    od_space:id(),
    od_atm_workflow_schema:id(),
    store_initial_values(),
    undefined | http_client:url()
) ->
    {atm_workflow_execution:id(), atm_workflow_execution:record()} | no_return().
schedule(UserCtx, SpaceId, AtmWorkflowSchemaId, StoreInitialValues, CallbackUrl) ->
    {AtmWorkflowExecutionDoc, AtmWorkflowExecutionEnv} = atm_workflow_execution_factory:create(
        UserCtx, SpaceId, AtmWorkflowSchemaId, StoreInitialValues, CallbackUrl
    ),
    AtmWorkflowExecutionId = AtmWorkflowExecutionDoc#document.key,

    atm_workflow_execution_handler:start(UserCtx, AtmWorkflowExecutionId, AtmWorkflowExecutionEnv),

    {AtmWorkflowExecutionId, AtmWorkflowExecutionDoc#document.value}.


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
    atm_inventory_id = AtmInventoryId,
    status = AtmWorkflowExecutionStatus,
    schedule_time = ScheduleTime,
    start_time = StartTime,
    finish_time = FinishTime
}) ->
    #atm_workflow_execution_summary{
        atm_workflow_execution_id = AtmWorkflowExecutionId,
        name = Name,
        atm_inventory_id = AtmInventoryId,
        status = AtmWorkflowExecutionStatus,
        schedule_time = ScheduleTime,
        start_time = StartTime,
        finish_time = FinishTime
    }.


-spec cancel(atm_workflow_execution:id()) -> ok | {error, already_ended}.
cancel(AtmWorkflowExecutionId) ->
    atm_workflow_execution_handler:cancel(AtmWorkflowExecutionId).


%%--------------------------------------------------------------------
%% @doc
%% Terminates all waiting and ongoing workflow executions for given space.
%% This function should be called only after provider restart to terminate
%% stale (processes handling execution no longer exists) workflows.
%% @end
%%--------------------------------------------------------------------
terminate_not_ended(SpaceId) ->
    TerminateFun = fun(AtmWorkflowExecutionId) ->
        {ok, #document{value = #atm_workflow_execution{
            store_registry = AtmStoreRegistry
        }}} = atm_workflow_execution:get(AtmWorkflowExecutionId),

        atm_workflow_execution_handler:handle_workflow_execution_ended(
            AtmWorkflowExecutionId,
            atm_workflow_execution_env:build(
                SpaceId, AtmWorkflowExecutionId, AtmStoreRegistry,
                undefined, undefined
            )
        )
    end,

    foreach_atm_workflow_execution(TerminateFun, SpaceId, ?WAITING_PHASE),
    foreach_atm_workflow_execution(TerminateFun, SpaceId, ?ONGOING_PHASE).


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
list_basic_entries(SpaceId, ?ENDED_PHASE, ListingOpts) ->
    atm_ended_workflow_executions:list(SpaceId, ListingOpts).


%% @private
-spec foreach_atm_workflow_execution(
    fun((atm_workflow_execution:id()) -> ok),
    od_space:id(),
    atm_workflow_execution:phase()
) ->
    ok.
foreach_atm_workflow_execution(Callback, SpaceId, Phase) ->
    foreach_workflow(Callback, SpaceId, Phase, #{limit => 1000, start_index => <<>>}).


%% @private
-spec foreach_workflow(
    fun((atm_workflow_execution:id()) -> ok),
    od_space:id(),
    atm_workflow_execution:phase(),
    atm_workflow_executions_forest:listing_opts()
) ->
    ok.
foreach_workflow(Callback, SpaceId, Phase, ListingOpts) ->
    {ok, AtmWorkflowExecutionBasicEntries, IsLast} = list(SpaceId, Phase, basic, ListingOpts),

    LastEntryIndex = lists:foldl(fun({Index, AtmWorkflowExecutionId}, _) ->
        Callback(AtmWorkflowExecutionId),
        Index
    end, <<>>, AtmWorkflowExecutionBasicEntries),

    case IsLast of
        true ->
            ok;
        false ->
            foreach_workflow(Callback, SpaceId, Phase, ListingOpts#{
                start_index => LastEntryIndex, offset => 1
            })
    end.
