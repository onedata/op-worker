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
    create/5,
    get/1, get_summary/2
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


-spec create(
    user_ctx:ctx(),
    od_space:id(),
    od_atm_workflow_schema:id(),
    store_initial_values(),
    undefined | http_client:url()
) ->
    {atm_workflow_execution:id(), atm_workflow_execution:record()} | no_return().
create(UserCtx, SpaceId, AtmWorkflowSchemaId, StoreInitialValues, CallbackUrl) ->
    SessionId = user_ctx:get_session_id(UserCtx),

    {ok, AtmWorkflowSchemaDoc = #document{value = #od_atm_workflow_schema{
        atm_lambdas = AtmLambdaIds
    }}} = atm_workflow_schema_logic:get(SessionId, AtmWorkflowSchemaId),

    AtmLambdaDocs = lists:foldl(fun(AtmLambdaId, Acc) ->
        {ok, AtmLambdaDoc} = atm_lambda_logic:get(SessionId, AtmLambdaId),
        Acc#{AtmLambdaId => AtmLambdaDoc}
    end, #{}, AtmLambdaIds),

    AtmWorkflowExecutionId = datastore_key:new(),

    AtmWorkflowExecutionDoc = atm_workflow_execution_factory:create(#atm_workflow_execution_creation_ctx{
        workflow_execution_ctx = atm_workflow_execution_ctx:build(
            SpaceId, AtmWorkflowExecutionId, UserCtx
        ),
        workflow_schema_doc = AtmWorkflowSchemaDoc,
        lambda_docs = AtmLambdaDocs,
        store_initial_values = StoreInitialValues,
        callback_url = CallbackUrl
    }),
    atm_workflow_execution_handler:start(UserCtx, AtmWorkflowExecutionDoc),

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
