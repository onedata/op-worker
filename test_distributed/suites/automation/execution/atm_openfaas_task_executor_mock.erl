%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Mock for atm_openfaas_task_executor used in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_task_executor_mock).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/automation/automation.hrl").

%% API
-export([init/2, teardown/1]).
-export([set_exp_lane_initiation_result/3]).


-record(atm_openfaas_task_executor, {
    workflow_execution_id :: atm_workflow_execution:id(),
    lane_index :: atm_lane_execution:index(),
    operation_spec :: atm_openfaas_operation_spec:record()
}).
-type record() :: #atm_openfaas_task_executor{}.

-export_type([record/0]).


-define(MOCKED_MODULE, atm_openfaas_task_executor).

-define(EXP_LANE_INITIATION_RESULT_KEY(__ATM_WORKFLOW_EXECUTION_ID, __ATM_LANE_INDEX),
    {exp_lane_initiation_result_key, __ATM_WORKFLOW_EXECUTION_ID, __ATM_LANE_INDEX}
).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(
    oct_background:entity_selector() | [oct_background:entity_selector()],
    module()
) ->
    ok.
init(ProviderSelectors, TestDockerRegistryModule) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),

    test_utils:mock_new(Workers, ?MOCKED_MODULE, [passthrough, no_history]),

    mock_assert_openfaas_available(Workers),

    mock_create(Workers),
    mock_initiate(Workers),
    mock_teardown(Workers),
    mock_delete(Workers),
    mock_in_readonly_mode(Workers),
    mock_run(Workers, TestDockerRegistryModule),

    mock_version(Workers),
    mock_db_encode(Workers),
    mock_db_decode(Workers).


-spec teardown(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
teardown(ProviderSelectors) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),
    test_utils:mock_unload(Workers, ?MOCKED_MODULE).


-spec set_exp_lane_initiation_result(
    atm_workflow_execution:id(),
    atm_lane_execution:index(),
    success | failure
) ->
    ok.
set_exp_lane_initiation_result(AtmWorkflowExecutionId, AtmLaneIndex, ExpResult) ->
    node_cache:put(
        ?EXP_LANE_INITIATION_RESULT_KEY(AtmWorkflowExecutionId, AtmLaneIndex),
        ExpResult
    ).


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
-spec mock_assert_openfaas_available([node()]) -> ok.
mock_assert_openfaas_available(Workers) ->
    MockFun = fun() -> ok end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, assert_openfaas_available, MockFun).


%% @private
-spec mock_create([node()]) -> ok.
mock_create(Workers) ->
    MockFun = fun(AtmWorkflowExecutionCtx, AtmLaneIndex, _AtmTaskSchema, AtmLambdaRevision) ->
        AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
            AtmWorkflowExecutionCtx
        ),

        #atm_openfaas_task_executor{
            workflow_execution_id = AtmWorkflowExecutionId,
            lane_index = AtmLaneIndex,
            operation_spec = AtmLambdaRevision#atm_lambda_revision.operation_spec
        }
    end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, create, MockFun).


%% @private
-spec mock_initiate([node()]) -> ok.
mock_initiate(Workers) ->
    MockFun = fun(AtmWorkflowExecutionCtx, _AtmTaskSchema, _AtmLambdaRevision, AtmTaskExecutor) ->
        AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
            AtmWorkflowExecutionCtx
        ),
        AtmLaneIndex = AtmTaskExecutor#atm_openfaas_task_executor.lane_index,

        case node_cache:get(
            ?EXP_LANE_INITIATION_RESULT_KEY(AtmWorkflowExecutionId, AtmLaneIndex),
            success
        ) of
            success ->
                #{type => async};
            failure ->
                throw(?ERROR_ATM_OPENFAAS_FUNCTION_REGISTRATION_FAILED)
        end
    end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, initiate, MockFun).


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
-spec mock_in_readonly_mode([node()]) -> ok.
mock_in_readonly_mode(Workers) ->
    MockFun = fun(#atm_openfaas_task_executor{
        operation_spec = #atm_openfaas_operation_spec{
            docker_execution_options = #atm_docker_execution_options{readonly = Readonly}
        }
    }) ->
        Readonly
    end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, in_readonly_mode, MockFun).


%% @private
-spec mock_run([node()], module()) -> ok.
mock_run(Workers, TestDockerRegistryModule) ->
    MockFun = fun(AtmJobCtx, Input, #atm_openfaas_task_executor{
        operation_spec = #atm_openfaas_operation_spec{docker_image = DockerImage}
    }) ->
        spawn(fun() ->
            Result = try
                TestDockerRegistryModule:exec(DockerImage, Input)
            catch Type:Reason:Stacktrace ->
                errors:to_json(?atm_examine_error(Type, Reason, Stacktrace))
            end,

            Response = case is_map(Result) of
                true -> json_utils:encode(Result);
                false -> Result
            end,

            CallbackUrl = atm_job_ctx:get_report_result_url(AtmJobCtx),
            http_client:post(CallbackUrl, #{}, Response)
        end),

        ok
    end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, run, MockFun).


%% @private
-spec mock_version([node()]) -> ok.
mock_version(Workers) ->
    MockFun = fun() -> 1 end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, version, MockFun).


%% @private
-spec mock_db_encode([node()]) -> ok.
mock_db_encode(Workers) ->
    MockFun = fun(#atm_openfaas_task_executor{
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_index = AtmLaneIndex,
        operation_spec = OperationSpec
    }, NestedRecordEncoder) ->
        #{
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
        <<"atmWorkflowExecutionId">> := AtmWorkflowExecutionId,
        <<"atmLaneIndex">> := AtmLaneIndex,
        <<"operationSpec">> := OperationSpecJson
    }, NestedRecordDecoder) ->
        #atm_openfaas_task_executor{
            workflow_execution_id = AtmWorkflowExecutionId,
            lane_index = AtmLaneIndex,
            operation_spec = NestedRecordDecoder(OperationSpecJson, atm_openfaas_operation_spec)
        }
    end,
    test_utils:mock_expect(Workers, ?MOCKED_MODULE, db_decode, MockFun).
