%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for use in atm workflow execution tests.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_test_utils).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("modules/automation/atm_execution.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([
    cancel_workflow_execution/1,
    pause_workflow_execution/1,

    delete_offline_session/1,
    report_openfaas_unhealthy/1,
    interrupt_workflow_execution/1,

    stop_workflow_execution/2,

    resume_workflow_execution/1,
    repeat_workflow_execution/3
]).
-export([
    browse_store/2, browse_store/3,
    get_exception_store_content/2
]).
-export([
    assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed/1,
    assert_not_stopped_workflow_execution_can_be_neither_repeated_nor_resumed/2,
    assert_not_ended_workflow_execution_can_not_be_repeated/2
]).


-define(INFINITE_LOG_BASED_STORES_LISTING_OPTS, #{
    start_from => undefined,
    offset => 0,
    limit => 1000000000
}).


%%%===================================================================
%%% API
%%%===================================================================


-spec cancel_workflow_execution(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok | no_return().
cancel_workflow_execution(#atm_mock_call_ctx{
    provider = ProviderSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, mi_atm:cancel_workflow_execution(SessionId, AtmWorkflowExecutionId)).


-spec pause_workflow_execution(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok | no_return().
pause_workflow_execution(#atm_mock_call_ctx{
    provider = ProviderSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, mi_atm:pause_workflow_execution(SessionId, AtmWorkflowExecutionId)).


-spec delete_offline_session(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok | no_return().
delete_offline_session(#atm_mock_call_ctx{
    provider = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, offline_access_manager:close_session(AtmWorkflowExecutionId)).


-spec report_openfaas_unhealthy(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok | no_return().
report_openfaas_unhealthy(#atm_mock_call_ctx{
    provider = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, atm_workflow_execution_handler:on_openfaas_down(
        AtmWorkflowExecutionId, ?ERROR_ATM_OPENFAAS_UNHEALTHY
    )).


-spec interrupt_workflow_execution(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok | no_return().
interrupt_workflow_execution(AtmMockCallCtx) ->
    stop_workflow_execution(interrupt, AtmMockCallCtx).


-spec stop_workflow_execution(
    atm_lane_execution:run_stopping_reason(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok | errors:error().
stop_workflow_execution(Reason, #atm_mock_call_ctx{
    provider = ProviderSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, atm_workflow_execution_handler:stop(
        user_ctx:new(SessionId), AtmWorkflowExecutionId, Reason
    )).


-spec resume_workflow_execution(atm_workflow_execution_test_runner:mock_call_ctx()) ->
    ok.
resume_workflow_execution(#atm_mock_call_ctx{
    provider = ProviderSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, mi_atm:resume_workflow_execution(SessionId, AtmWorkflowExecutionId)).


-spec repeat_workflow_execution(
    atm_workflow_execution:repeat_type(),
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok.
repeat_workflow_execution(RepeatType, AtmLaneRunSelector, #atm_mock_call_ctx{
    provider = ProviderSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ?erpc(ProviderSelector, mi_atm:repeat_workflow_execution(
        SessionId, RepeatType, AtmWorkflowExecutionId, AtmLaneRunSelector
    )).


-spec browse_store(automation:id(), atm_workflow_execution_test_runner:mock_call_ctx()) ->
    json_utils:json_term().
browse_store(AtmStoreSchemaId, AtmMockCallCtx) ->
    browse_store(AtmStoreSchemaId, undefined, AtmMockCallCtx).


-spec browse_store
    (exception_store, atm_lane_execution:lane_run_selector(), atm_workflow_execution_test_runner:mock_call_ctx()) ->
        json_utils:json_term();
    (automation:id(), undefined | atm_task_execution:id(), atm_workflow_execution_test_runner:mock_call_ctx()) ->
        json_utils:json_term().
browse_store(AtmStoreSchemaId, AtmWorkflowExecutionComponentSelector, AtmMockCallCtx = #atm_mock_call_ctx{
    provider = ProviderSelector,
    space = SpaceSelector,
    session_id = SessionId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    SpaceId = oct_background:get_space_id(SpaceSelector),
    AtmStoreId = get_store_id(AtmStoreSchemaId, AtmWorkflowExecutionComponentSelector, AtmMockCallCtx),
    ?rpc(ProviderSelector, browse_store(SessionId, SpaceId, AtmWorkflowExecutionId, AtmStoreId)).


-spec get_exception_store_content(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    json_utils:json_term().
get_exception_store_content(AtmLaneRunSelector, AtmMockCallCtx) ->
    #{<<"items">> := Items, <<"isLast">> := true} = browse_store(
        exception_store, AtmLaneRunSelector, AtmMockCallCtx
    ),
    lists:map(fun(#{<<"value">> := Content}) -> Content end, Items).


-spec assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed(
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok.
assert_ended_workflow_execution_can_be_neither_stopped_nor_resumed(AtmMockCallCtx = #atm_mock_call_ctx{
    workflow_execution_exp_state = ExpState0
}) ->
    lists:foreach(fun(StoppingReason) ->
        ?assertEqual(?ERROR_ATM_WORKFLOW_EXECUTION_ENDED, stop_workflow_execution(StoppingReason, AtmMockCallCtx))
    end, ?STOPPING_REASONS),

    ?assertThrow(?ERROR_ATM_WORKFLOW_EXECUTION_NOT_RESUMABLE, resume_workflow_execution(AtmMockCallCtx)),

    ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpState0, 0)).


-spec assert_not_stopped_workflow_execution_can_be_neither_repeated_nor_resumed(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok.
assert_not_stopped_workflow_execution_can_be_neither_repeated_nor_resumed(AtmLaneRunSelector, AtmMockCallCtx) ->
    ?assertThrow(?ERROR_ATM_WORKFLOW_EXECUTION_NOT_RESUMABLE, resume_workflow_execution(AtmMockCallCtx)),
    assert_not_ended_workflow_execution_can_not_be_repeated(AtmLaneRunSelector, AtmMockCallCtx).


-spec assert_not_ended_workflow_execution_can_not_be_repeated(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution_test_runner:mock_call_ctx()
) ->
    ok.
assert_not_ended_workflow_execution_can_not_be_repeated(AtmLaneRunSelector, AtmMockCallCtx = #atm_mock_call_ctx{
    workflow_execution_exp_state = ExpState0
}) ->
    lists:foreach(fun(RepeatType) ->
        ?assertThrow(?ERROR_ATM_WORKFLOW_EXECUTION_NOT_ENDED, repeat_workflow_execution(
            RepeatType, AtmLaneRunSelector, AtmMockCallCtx
        ))
    end, [rerun, retry]),

    ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(ExpState0, 0)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_store_id
    (exception_store, atm_lane_execution:lane_run_selector(), atm_workflow_execution_test_runner:mock_call_ctx()) ->
        atm_store:id();
    (automation:id(), undefined | atm_task_execution:id(), atm_workflow_execution_test_runner:mock_call_ctx()) ->
        atm_store:id().
get_store_id(exception_store, AtmLaneRunSelector, #atm_mock_call_ctx{
    provider = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    {ok, #document{value = AtmWorkflowExecution}} = ?rpc(
        ProviderSelector, atm_workflow_execution:get(AtmWorkflowExecutionId)
    ),
    {ok, AtmLaneRun} = atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution),
    AtmLaneRun#atm_lane_execution_run.exception_store_id;

get_store_id(?WORKFLOW_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID, _, #atm_mock_call_ctx{
    provider = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    {ok, #document{value = #atm_workflow_execution{system_audit_log_store_id = AtmStoreId}}} = ?rpc(
        ProviderSelector, atm_workflow_execution:get(AtmWorkflowExecutionId)
    ),
    AtmStoreId;

get_store_id(?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID, AtmTaskExecutionId, #atm_mock_call_ctx{
    provider = ProviderSelector
}) ->
    {ok, #document{value = #atm_task_execution{system_audit_log_store_id = AtmStoreId}}} = ?rpc(
        ProviderSelector, atm_task_execution:get(AtmTaskExecutionId)
    ),
    AtmStoreId;

get_store_id(?CURRENT_TASK_TIME_SERIES_STORE_SCHEMA_ID, AtmTaskExecutionId, #atm_mock_call_ctx{
    provider = ProviderSelector
}) ->
    {ok, #document{value = #atm_task_execution{time_series_store_id = AtmStoreId}}} = ?rpc(
        ProviderSelector, atm_task_execution:get(AtmTaskExecutionId)
    ),
    AtmStoreId;

get_store_id(AtmStoreSchemaId, _, #atm_mock_call_ctx{
    provider = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    {ok, #document{value = #atm_workflow_execution{store_registry = AtmStoreRegistry}}} = ?rpc(
        ProviderSelector, atm_workflow_execution:get(AtmWorkflowExecutionId)
    ),
    maps:get(AtmStoreSchemaId, AtmStoreRegistry).


%% @private
-spec browse_store(session:id(), od_space:id(), atm_workflow_execution:id(), atm_store:id()) ->
    json_utils:json_term().
browse_store(SessionId, SpaceId, AtmWorkflowExecutionId, AtmStoreId) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_auth:build(SpaceId, AtmWorkflowExecutionId, SessionId),

    {ok, AtmStore = #atm_store{container = AtmStoreContainer}} = atm_store_api:get(AtmStoreId),
    AtmStoreBrowseOpts = build_browse_opts(atm_store_container:get_store_type(AtmStoreContainer)),

    AtmStoreContent = atm_store_api:browse_content(AtmWorkflowExecutionAuth, AtmStoreBrowseOpts, AtmStore),
    atm_store_content_browse_result:to_json(AtmStoreContent).


%% @private
-spec build_browse_opts(automation:store_type()) -> atm_store_content_browse_options:record().
build_browse_opts(audit_log) ->
    #atm_audit_log_store_content_browse_options{browse_opts = ?INFINITE_LOG_BASED_STORES_LISTING_OPTS};

build_browse_opts(list) ->
    #atm_list_store_content_browse_options{listing_opts = ?INFINITE_LOG_BASED_STORES_LISTING_OPTS};

build_browse_opts(tree_forest) ->
    #atm_tree_forest_store_content_browse_options{listing_opts = ?INFINITE_LOG_BASED_STORES_LISTING_OPTS};

build_browse_opts(range) ->
    #atm_range_store_content_browse_options{};

build_browse_opts(single_value) ->
    #atm_single_value_store_content_browse_options{};

build_browse_opts(time_series) ->
    #atm_time_series_store_content_browse_options{
        request = #time_series_slice_get_request{
            layout = #{?ALL_TIME_SERIES => [?ALL_METRICS]},
            start_timestamp = undefined,
            window_limit = 10000000000000000000000000000000000000000000000000000
        }
    }.
