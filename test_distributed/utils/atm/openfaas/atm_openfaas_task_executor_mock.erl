%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements 'atm_openfaas_task_executor' mock for use in CT tests.
%%%
%%% NOTE: to save and later use additional contextual data available only at
%%% executor creation the original executor model/record is also substituted.
%%% This in turn causes following:
%%% 1. mocked record must have the same name as original one as polymorphism
%%%    implemented in atm is based on record name.
%%% 2. not only required subset but all 'atm_openfaas_task_executor' functions
%%%    must be mocked to be able to operate on substituted record.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_task_executor_mock).
-author("Bartosz Walkowicz").

-include("http/gui_paths.hrl").
-include("modules/automation/atm_execution.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([init/2, teardown/1]).
-export([mock_lane_initiation_result/4]).


-record(atm_openfaas_task_executor, {
    node_cache_key :: binary(),
    workflow_execution_id :: atm_workflow_execution:id(),
    lane_index :: atm_lane_execution:index(),
    operation_spec :: atm_openfaas_operation_spec:record()
}).
-type record() :: #atm_openfaas_task_executor{}.


-define(OPENFAAS_FEED_CONN_SECRET, <<"884d387220ec1359e3199361dd45d328779efc9a">>).
-define(STREAMER_ID, <<"10">>).

-define(MOCKED_MODULE, atm_openfaas_task_executor).

-define(MOCKED_LANE_INITIATION_RESULT_KEY(__ATM_WORKFLOW_EXECUTION_ID, __ATM_LANE_INDEX),
    {mocked_lane_initiation_result_key, __ATM_WORKFLOW_EXECUTION_ID, __ATM_LANE_INDEX}
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

    % 'atm_task_execution' is mocked to be kept in memory only. This is necessary as
    % mocking of 'atm_openfaas_task_executor' makes it unable to dump docs containing
    % it ('atm_task_execution' is such model) to database when shutting down provider
    % (encoding/decoding mocks are removed automatically while some docs may not have
    % been flushed yet) which effectively freezes op-worker.
    test_utils:mock_new(Workers, atm_task_execution, [passthrough, no_history]),
    test_utils:mock_expect(Workers, atm_task_execution, get_ctx, fun() ->
        #{model => atm_task_execution, disc_driver => undefined}
    end),

    test_utils:mock_new(Workers, [?MOCKED_MODULE, atm_openfaas_monitor], [passthrough, no_history]),

    mock_assert_openfaas_healthy(Workers),

    mock_create(Workers),
    mock_initiate(Workers),
    mock_teardown(Workers),
    mock_delete(Workers),
    mock_is_in_readonly_mode(Workers),
    mock_run(Workers, ModuleWithOpenfaasDockerMock),

    mock_version(Workers),
    mock_db_encode(Workers),
    mock_db_decode(Workers).


-spec teardown(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
teardown(ProviderSelectors) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),
    test_utils:mock_unload(Workers, [?MOCKED_MODULE, atm_openfaas_monitor]).


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
-spec mock_assert_openfaas_healthy([node()]) -> ok.
mock_assert_openfaas_healthy(Workers) ->
    MockFun = fun() -> ok end,
    test_utils:mock_expect(Workers, atm_openfaas_monitor, assert_openfaas_healthy, MockFun).


%% @private
-spec mock_create([node()]) -> ok.
mock_create(Workers) ->
    MockFun = fun(AtmWorkflowExecutionCtx, AtmLaneIndex, _AtmTaskSchema, AtmLambdaRevision) ->
        AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
            AtmWorkflowExecutionCtx
        ),

        #atm_openfaas_task_executor{
            node_cache_key = ?RAND_STR(),
            workflow_execution_id = AtmWorkflowExecutionId,
            lane_index = AtmLaneIndex,
            operation_spec = AtmLambdaRevision#atm_lambda_revision.operation_spec
        }
    end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, create, MockFun).


%% @private
-spec mock_initiate([node()]) -> ok.
mock_initiate(Workers) ->
    MockFun = fun(
        #atm_task_executor_initiation_ctx{
            workflow_execution_ctx = AtmWorkflowExecutionCtx,
            task_execution_id = AtmTaskExecutionId,
            uncorrelated_results = AtmTaskExecutionUncorrelatedResultNames
        },
        AtmTaskExecutor
    ) ->
        save_task_execution_uncorrelated_result_names(
            AtmTaskExecutionUncorrelatedResultNames,
            AtmTaskExecutor
        ),

        AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
            AtmWorkflowExecutionCtx
        ),
        AtmLaneIndex = AtmTaskExecutor#atm_openfaas_task_executor.lane_index,

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
                throw(?ERROR_ATM_OPENFAAS_FUNCTION_REGISTRATION_FAILED)
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
-spec mock_teardown([node()]) -> ok.
mock_teardown(Workers) ->
    MockFun = fun(_AtmLaneExecutionRunTeardownCtx, _AtmTaskExecutor) -> ok end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, teardown, MockFun).


%% @private
-spec mock_delete(record()) -> ok.
mock_delete(Workers) ->
    MockFun = fun(_AtmTaskExecutor) -> ok end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, delete, MockFun).


%% @private
-spec mock_is_in_readonly_mode([node()]) -> ok.
mock_is_in_readonly_mode(Workers) ->
    MockFun = fun(#atm_openfaas_task_executor{
        operation_spec = #atm_openfaas_operation_spec{
            docker_execution_options = #atm_docker_execution_options{readonly = Readonly}
        }
    }) ->
        Readonly
    end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, is_in_readonly_mode, MockFun).


%% @private
-spec mock_run([node()], module()) -> ok.
mock_run(Workers, ModuleWithOpenfaasDockerMock) ->
    MockFun = fun(AtmRunJobBatchCtx, AtmLambdaInput, AtmTaskExecutor = #atm_openfaas_task_executor{
        operation_spec = #atm_openfaas_operation_spec{docker_image = DockerImage}
    }) ->
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
                Response
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
            <<"config">> => Config
        },
        <<"argsBatch">> => ArgsBatch
    }.


%% @private
-spec process_task_uncorrelated_results(record(), atm_task_executor:job_batch_result()) ->
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
-spec mock_version([node()]) -> ok.
mock_version(Workers) ->
    MockFun = fun() -> 1 end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, version, MockFun).


%% @private
-spec mock_db_encode([node()]) -> ok.
mock_db_encode(Workers) ->
    MockFun = fun(#atm_openfaas_task_executor{
        node_cache_key = NodeCacheKey,
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_index = AtmLaneIndex,
        operation_spec = OperationSpec
    }, NestedRecordEncoder) ->
        #{
            <<"nodeCacheKey">> => NodeCacheKey,
            <<"atmWorkflowExecutionId">> => AtmWorkflowExecutionId,
            <<"atmLaneIndex">> => AtmLaneIndex,
            <<"operationSpec">> => NestedRecordEncoder(OperationSpec, atm_openfaas_operation_spec)
        }
    end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, db_encode, MockFun).


%% @private
-spec mock_db_decode([node()]) -> ok.
mock_db_decode(Workers) ->
    MockFun = fun(#{
        <<"nodeCacheKey">> := NodeCacheKey,
        <<"atmWorkflowExecutionId">> := AtmWorkflowExecutionId,
        <<"atmLaneIndex">> := AtmLaneIndex,
        <<"operationSpec">> := OperationSpecJson
    }, NestedRecordDecoder) ->
        #atm_openfaas_task_executor{
            node_cache_key = NodeCacheKey,
            workflow_execution_id = AtmWorkflowExecutionId,
            lane_index = AtmLaneIndex,
            operation_spec = NestedRecordDecoder(OperationSpecJson, atm_openfaas_operation_spec)
        }
    end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, db_decode, MockFun).


%% @private
-spec save_task_execution_uncorrelated_result_names([automation:name()], record()) ->
    ok.
save_task_execution_uncorrelated_result_names(
    AtmTaskExecutionUncorrelatedResultNames,
    #atm_openfaas_task_executor{node_cache_key = NodeCacheKey}
) ->
    node_cache:put(
        {NodeCacheKey, task_execution_uncorrelated_result_names},
        AtmTaskExecutionUncorrelatedResultNames
    ).


%% @private
-spec get_task_execution_uncorrelated_result_names(record()) -> [automation:name()].
get_task_execution_uncorrelated_result_names(#atm_openfaas_task_executor{
    node_cache_key = NodeCacheKey
}) ->
    node_cache:get({NodeCacheKey, task_execution_uncorrelated_result_names}).


%% @private
-spec save_result_streamer_ref(test_websocket_client:client_ref(), record()) -> ok.
save_result_streamer_ref(ResultStreamerRef, #atm_openfaas_task_executor{
    node_cache_key = NodeCacheKey
}) ->
    node_cache:put({NodeCacheKey, result_streamer_ref}, ResultStreamerRef).


%% @private
-spec get_result_streamer_ref(record()) -> test_websocket_client:client_ref().
get_result_streamer_ref(#atm_openfaas_task_executor{node_cache_key = NodeCacheKey}) ->
    node_cache:get({NodeCacheKey, result_streamer_ref}).
