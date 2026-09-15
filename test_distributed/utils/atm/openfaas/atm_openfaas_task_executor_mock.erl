%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements 'atm_openfaas_task_executor' mock for use in CT tests.
%%%
%%% The original executor record is left intact - only those callbacks that would
%%% reach out to the (nonexistent in tests) OpenFaaS service are substituted. Any
%%% contextual data needed by the mocked callbacks but available only at executor
%%% creation is kept in node cache under the executor's pod status registry id
%%% (the only field of the original record that is unique per executor, survives
%%% persistence and is accessible from the outside).
%%%
%%% NOTE: substituting the record itself must be avoided - it would force mocking
%%% of the persistence callbacks ('db_encode'/'db_decode') of a record nested in
%%% the 'atm_task_execution' model. Such mocks can not be safely unloaded (docs
%%% not yet flushed would then be encoded by the original callbacks, which freezes
%%% op-worker), meaning the deployment could never be restored to its original state.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_task_executor_mock).
-author("Bartosz Walkowicz").

-include("http/gui_paths.hrl").
-include("modules/automation/atm_execution.hrl").
-include("test_rpc.hrl").
-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([init/2, teardown/1]).
-export([mock_openfaas_health_check/1, unmock_openfaas_health_check/1]).
-export([mock_lane_initiation_result/4]).


-define(OPENFAAS_FEED_CONN_SECRET, <<"884d387220ec1359e3199361dd45d328779efc9a">>).
-define(STREAMER_ID, <<"10">>).

-define(MOCKED_MODULE, atm_openfaas_task_executor).

-define(MOCKED_LANE_INITIATION_RESULT_KEY(__ATM_WORKFLOW_EXECUTION_ID, __ATM_LANE_INDEX),
    {mocked_lane_initiation_result_key, __ATM_WORKFLOW_EXECUTION_ID, __ATM_LANE_INDEX}
).

-define(EXECUTOR_CTX_KEY(__ATM_TASK_EXECUTOR),
    {mocked_atm_openfaas_task_executor_ctx, ?MOCKED_MODULE:get_pod_status_registry_id(__ATM_TASK_EXECUTOR)}
).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(
    oct_background:entity_selector() | [oct_background:entity_selector()],
    module()
) ->
    ok.
init(ProviderSelectors, ModuleWithOpenfaasDockerMock) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),

    lists:foreach(fun(Worker) ->
        atm_openfaas_activity_feed_client_mock:set_secret_on_provider(
            Worker, ?OPENFAAS_FEED_CONN_SECRET
        )
    end, Workers),

    mock_openfaas_health_check(ProviderSelectors),

    test_utils:mock_new(Workers, ?MOCKED_MODULE, [passthrough, no_history]),

    % NOTE: 'create' is deliberately left unmocked - with the health check mocked
    % it does not reach out to OpenFaaS and builds a genuine executor record
    mock_initiate(Workers),
    mock_abort(Workers),
    mock_teardown(Workers),
    mock_delete(Workers),
    mock_run(Workers, ModuleWithOpenfaasDockerMock).


-spec teardown(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
teardown(ProviderSelectors) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),
    test_utils:mock_unload(Workers, ?MOCKED_MODULE),
    unmock_openfaas_health_check(ProviderSelectors).


%%--------------------------------------------------------------------
%% @doc
%% Makes the provider consider OpenFaaS service always available.
%% @end
%%--------------------------------------------------------------------
-spec mock_openfaas_health_check(
    oct_background:entity_selector() | [oct_background:entity_selector()]
) ->
    ok.
mock_openfaas_health_check(ProviderSelectors) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),
    test_utils:mock_new(Workers, atm_openfaas_monitor, [passthrough, no_history]),
    test_utils:mock_expect(Workers, atm_openfaas_monitor, assert_openfaas_healthy, fun() -> ok end).


%%--------------------------------------------------------------------
%% @doc
%% Restores the genuine OpenFaaS availability check (which, with no OpenFaaS
%% service deployed alongside the provider, reports it as not configured).
%% @end
%%--------------------------------------------------------------------
-spec unmock_openfaas_health_check(
    oct_background:entity_selector() | [oct_background:entity_selector()]
) ->
    ok.
unmock_openfaas_health_check(ProviderSelectors) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),
    test_utils:mock_unload(Workers, atm_openfaas_monitor).


-spec mock_lane_initiation_result(
    oct_background:entity_selector(),
    atm_workflow_execution:id(),
    atm_lane_execution:index(),
    success | exception
) ->
    ok.
mock_lane_initiation_result(ProviderSelector, AtmWorkflowExecutionId, AtmLaneIndex, MockedResult) ->
    ?rpc(ProviderSelector, node_cache:put(
        ?MOCKED_LANE_INITIATION_RESULT_KEY(AtmWorkflowExecutionId, AtmLaneIndex),
        MockedResult
    )).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_nodes([oct_background:entity_selector()]) -> [node()].
get_nodes(ProviderSelectors) ->
    lists:flatmap(fun(ProviderSelector) ->
        oct_background:get_provider_nodes(ProviderSelector)
    end, ProviderSelectors).


%% @private
-spec mock_initiate([node()]) -> ok.
mock_initiate(Workers) ->
    MockFun = fun(
        #atm_task_executor_initiation_ctx{
            workflow_execution_ctx = AtmWorkflowExecutionCtx,
            task_execution_id = AtmTaskExecutionId,
            lambda_revision = AtmLambdaRevision,
            uncorrelated_results = AtmTaskExecutionUncorrelatedResultNames
        },
        AtmTaskExecutor
    ) ->
        {ok, #document{value = #atm_task_execution{
            lane_index = AtmLaneIndex
        }}} = atm_task_execution:get(AtmTaskExecutionId),

        % executor ctx is built anew on each initiation - beside the original one
        % it also happens when resuming an execution, including after provider
        % restart, which leaves the node cache empty
        node_cache:put(?EXECUTOR_CTX_KEY(AtmTaskExecutor), #{
            operation_spec => AtmLambdaRevision#atm_lambda_revision.operation_spec,
            task_execution_uncorrelated_result_names => AtmTaskExecutionUncorrelatedResultNames
        }),

        AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
            AtmWorkflowExecutionCtx
        ),

        case node_cache:get(
            ?MOCKED_LANE_INITIATION_RESULT_KEY(AtmWorkflowExecutionId, AtmLaneIndex),
            success
        ) of
            success ->
                init_result_streamer_if_task_has_any_uncorrelated_results(
                    AtmTaskExecutor,
                    AtmWorkflowExecutionId,
                    AtmTaskExecutionId,
                    AtmTaskExecutionUncorrelatedResultNames
                ),

                #{
                    type => async,
                    data_stream_enabled => not lists_utils:is_empty(AtmTaskExecutionUncorrelatedResultNames)
                };
            exception ->
                throw(?ERR_ATM_OPENFAAS_FUNCTION_REGISTRATION_FAILED)
        end
    end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, initiate, MockFun).


%% @private
init_result_streamer_if_task_has_any_uncorrelated_results(
    _AtmTaskExecutor,
    _AtmWorkflowExecutionId,
    _AtmTaskExecutionId,
    []
) ->
    ok;

init_result_streamer_if_task_has_any_uncorrelated_results(
    AtmTaskExecutor,
    AtmWorkflowExecutionId,
    AtmTaskExecutionId,
    _AtmTaskExecutionUncorrelatedResultNames
) ->
    Path = string:replace(?OPENFAAS_ACTIVITY_FEED_WS_COWBOY_ROUTE, ":client_type", "result_streamer"),
    BasicAuthorization = base64:encode(?OPENFAAS_FEED_CONN_SECRET),

    {ok, ResultStreamerRef} = atm_openfaas_result_streamer_mock:connect_to_url(
        binary_to_list(oneprovider:build_url(wss, Path)),
        BasicAuthorization,
        [{cacerts, https_listener:get_cert_chain_ders()}]
    ),
    atm_openfaas_result_streamer_mock:deliver_registration_report(
        ResultStreamerRef, AtmWorkflowExecutionId, AtmTaskExecutionId, ?STREAMER_ID
    ),
    save_result_streamer_ref(ResultStreamerRef, AtmTaskExecutor).


%% @private
-spec mock_abort([node()]) -> ok.
mock_abort(Workers) ->
    MockFun = fun(_AtmWorkflowExecutionCtx, _AtmTaskExecutor) -> ok end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, abort, MockFun).


%% @private
-spec mock_teardown([node()]) -> ok.
mock_teardown(Workers) ->
    MockFun = fun(_AtmLaneExecutionRunTeardownCtx, _AtmTaskExecutor) -> ok end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, teardown, MockFun).


%% @private
-spec mock_delete([node()]) -> ok.
mock_delete(Workers) ->
    MockFun = fun(AtmTaskExecutor) ->
        node_cache:clear(?EXECUTOR_CTX_KEY(AtmTaskExecutor)),
        meck:passthrough([AtmTaskExecutor])
    end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, delete, MockFun).


%% @private
-spec mock_run([node()], module()) -> ok.
mock_run(Workers, ModuleWithOpenfaasDockerMock) ->
    MockFun = fun(AtmRunJobBatchCtx, AtmLambdaInput, AtmTaskExecutor) ->
        #{operation_spec := #atm_openfaas_operation_spec{docker_image = DockerImage}} = node_cache:get(
            ?EXECUTOR_CTX_KEY(AtmTaskExecutor)
        ),

        spawn(fun() ->
            Output = try
                AtmJobInputData = prepare_job_input_data(AtmRunJobBatchCtx, AtmLambdaInput),
                AtmJobOutputData = ModuleWithOpenfaasDockerMock:exec(DockerImage, AtmJobInputData),
                process_task_uncorrelated_results(AtmTaskExecutor, AtmJobOutputData)
            catch Type:Reason:Stacktrace ->
                errors:to_json(?examine_exception(Type, Reason, Stacktrace))
            end,

            {FunctionStatus, Response} = case Output == null orelse is_map(Output) of
                true -> {<<"200">>, json_utils:encode(Output)};
                false -> {<<"500">>, Output}
            end,

            http_client:post(
                build_job_callback_url(AtmLambdaInput),
                #{<<"x-function-status">> => FunctionStatus},
                Response,
                [{ssl_options, [{cacerts, https_listener:get_cert_chain_ders()}]}]
            )
        end),

        ok
    end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, run, MockFun).


%% @private
-spec prepare_job_input_data(atm_run_job_batch_ctx:record(), atm_task_executor:lambda_input()) ->
    json_utils:json_map().
prepare_job_input_data(AtmRunJobBatchCtx, #atm_lambda_input{
    workflow_execution_id = AtmWorkflowExecutionId,
    log_level = LogLevel,
    job_batch_id = AtmJobBatchId,
    config = Config,
    args_batch = ArgsBatch
}) ->
    HeartbeatUrl = atm_openfaas_task_callback_handler:build_job_batch_heartbeat_url(
        AtmWorkflowExecutionId, AtmJobBatchId
    ),

    #{
        <<"ctx">> => #{
            <<"heartbeatUrl">> => HeartbeatUrl,
            <<"oneproviderDomain">> => oneprovider:get_domain(),
            <<"accessToken">> => atm_run_job_batch_ctx:get_access_token(AtmRunJobBatchCtx),
            <<"atmWorkflowExecutionId">> => AtmWorkflowExecutionId,
            <<"logLevel">> => audit_log:severity_from_int(LogLevel),
            <<"config">> => Config
        },
        <<"argsBatch">> => ArgsBatch
    }.


%% @private
-spec process_task_uncorrelated_results(
    atm_task_executor:record(),
    atm_task_executor:job_batch_result()
) ->
    json_utils:json_map().
process_task_uncorrelated_results(AtmTaskExecutor, AtmTaskOutputData) ->
    case get_task_execution_uncorrelated_result_names(AtmTaskExecutor) of
        [] ->
            AtmTaskOutputData;
        AtmTaskExecutionUncorrelatedResultNames ->
            ResultStreamerRef = get_result_streamer_ref(AtmTaskExecutor),

            ResultsBatch = lists:map(fun(AllResults) ->
                Chunk = maps:map(
                    % Wrap results in array to simulate sidecar - openfaas feed server batch optimization
                    fun(_ResultName, Value) -> [Value] end,
                    maps:with(AtmTaskExecutionUncorrelatedResultNames, AllResults)
                ),
                atm_openfaas_result_streamer_mock:deliver_chunk_report(ResultStreamerRef, Chunk),
                maps:without(AtmTaskExecutionUncorrelatedResultNames, AllResults)
            end, maps:get(<<"resultsBatch">>, AtmTaskOutputData)),

            case {lists:all(fun maps_utils:is_empty/1, ResultsBatch), rand:uniform(3)} of
                {false, _} -> #{<<"resultsBatch">> => ResultsBatch};
                {true, 1} -> null;
                {true, 2} -> #{<<"resultsBatch">> => null};
                {true, 3} -> #{<<"resultsBatch">> => lists:map(fun(_) -> ?RAND_ELEMENT([null, #{}]) end, ResultsBatch)}
            end
    end.


%% @private
-spec build_job_callback_url(atm_task_executor:lambda_input()) -> binary().
build_job_callback_url(#atm_lambda_input{
    workflow_execution_id = AtmWorkflowExecutionId,
    job_batch_id = AtmJobBatchId
}) ->
    atm_openfaas_task_callback_handler:build_job_batch_output_url(
        AtmWorkflowExecutionId, AtmJobBatchId
    ).


%% @private
-spec get_task_execution_uncorrelated_result_names(atm_task_executor:record()) ->
    [automation:name()].
get_task_execution_uncorrelated_result_names(AtmTaskExecutor) ->
    #{task_execution_uncorrelated_result_names := AtmTaskExecutionUncorrelatedResultNames} =
        node_cache:get(?EXECUTOR_CTX_KEY(AtmTaskExecutor)),
    AtmTaskExecutionUncorrelatedResultNames.


%% @private
-spec save_result_streamer_ref(test_websocket_client:client_ref(), atm_task_executor:record()) ->
    ok.
save_result_streamer_ref(ResultStreamerRef, AtmTaskExecutor) ->
    extend_executor_ctx(AtmTaskExecutor, #{result_streamer_ref => ResultStreamerRef}).


%% @private
-spec get_result_streamer_ref(atm_task_executor:record()) -> test_websocket_client:client_ref().
get_result_streamer_ref(AtmTaskExecutor) ->
    #{result_streamer_ref := ResultStreamerRef} = node_cache:get(?EXECUTOR_CTX_KEY(AtmTaskExecutor)),
    ResultStreamerRef.


%% @private
-spec extend_executor_ctx(atm_task_executor:record(), map()) -> ok.
extend_executor_ctx(AtmTaskExecutor, Diff) ->
    {ok, _} = node_cache:update(?EXECUTOR_CTX_KEY(AtmTaskExecutor), fun(ExecutorCtx) ->
        {ok, maps:merge(ExecutorCtx, Diff), infinity}
    end),
    ok.
