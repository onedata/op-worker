%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation workflow execution behaviour when stopping and
%%% restarting op_worker.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_restart_tests).
-author("Bartosz Walkowicz").

-include("atm_workflow_execution_test.hrl").
-include("modules/automation/atm_execution.hrl").

-export([
    restart_op_worker_after_graceful_stop/1
]).


-define(ITERATED_STORE_SCHEMA_ID, <<"iterated_store_id">>).

-define(ATM_WORKFLOW_SCHEMA_DRAFT(__RELAY_METHOD), #atm_workflow_schema_dump_draft{
    name = str_utils:to_binary(?FUNCTION_NAME),
    revision_num = 1,
    revision = #atm_workflow_schema_revision_draft{
        stores = [
            ?ATM_SV_STORE_SCHEMA_DRAFT(?ITERATED_STORE_SCHEMA_ID, ?ATM_NUMBER_DATA_SPEC, ?RAND_INT(100))
        ],
        lanes = [#atm_lane_schema_draft{
            parallel_boxes = [#atm_parallel_box_schema_draft{
                id = <<"pb1">>,
                tasks = [#atm_task_schema_draft{
                    id = <<"task1">>,
                    lambda_id = ?ECHO_LAMBDA_ID,
                    lambda_revision_number = ?ECHO_LAMBDA_REVISION_NUM,
                    argument_mappings = [?ITERATED_ITEM_ARG_MAPPER(?ECHO_ARG_NAME)],
                    result_mappings = []
                }]
            }],
            store_iterator_spec = #atm_store_iterator_spec_draft{
                store_schema_id = ?ITERATED_STORE_SCHEMA_ID
            },
            max_retries = 2
        }]
    },
    supplementary_lambdas = #{
        ?ECHO_LAMBDA_ID => #{?ECHO_LAMBDA_REVISION_NUM => #atm_lambda_revision_draft{
            operation_spec = #atm_openfaas_operation_spec_draft{
                docker_image = ?ECHO_WITH_SLEEP_DOCKER_IMAGE_ID
            },
            argument_specs = [#atm_parameter_spec{
                name = ?ECHO_ARG_NAME,
                data_spec = ?ATM_NUMBER_DATA_SPEC,
                is_optional = false
            }],
            result_specs = [#atm_lambda_result_spec{
                name = ?ECHO_ARG_NAME,
                data_spec = ?ATM_NUMBER_DATA_SPEC,
                relay_method = __RELAY_METHOD
            }]
        }}
    }
}).

-define(TASK1_SELECTOR(__ATM_LANE_RUN_SELECTOR), {__ATM_LANE_RUN_SELECTOR, <<"pb1">>, <<"task1">>}).


%%%===================================================================
%%% Tests
%%%===================================================================


pause_workflow_execution(AtmWorkflowExecutionId) ->
    SessionId = oct_background:get_user_session_id(?USER_SELECTOR, ?PROVIDER_SELECTOR),
    ?rpc(?PROVIDER_SELECTOR, mi_atm:init_pause_workflow_execution(SessionId, AtmWorkflowExecutionId)).


restart_op_worker_after_graceful_stop(Config) ->
    mock_atm_supervision_worker(),
    %% TODO VFS-10266 SET LAMBDA SLEEP TO 6 SEC ??

    ReturnValueAtmWorkflowSchemaId = create_workflow_schema(return_value),
    ReturnValueAtmWorkflowSchemaRevision = get_workflow_schema_revision(ReturnValueAtmWorkflowSchemaId),

    FilePipeAtmWorkflowSchemaId = create_workflow_schema(file_pipe),
    FilePipeAtmWorkflowSchemaRevision = get_workflow_schema_revision(FilePipeAtmWorkflowSchemaId),

    UserPausedAtmWorkflowExecutionId = schedule_workflow_execution(ReturnValueAtmWorkflowSchemaId),
    SystemPausedAtmWorkflowExecutionId = schedule_workflow_execution(ReturnValueAtmWorkflowSchemaId),

    % Wait until first item begins execution
    timer:sleep(timer:seconds(1)),

    UserPausedAtmWorkflowExecutionExpState0 = expect_workflow_execution_active(
        UserPausedAtmWorkflowExecutionId, ReturnValueAtmWorkflowSchemaRevision
    ),
    SystemPausedAtmWorkflowExecutionExpState0 = expect_workflow_execution_active(
        SystemPausedAtmWorkflowExecutionId, ReturnValueAtmWorkflowSchemaRevision
    ),
    assert_all_match_with_backend([
        UserPausedAtmWorkflowExecutionExpState0, SystemPausedAtmWorkflowExecutionExpState0
    ]),

    % Make some time gap so that previous executions would be gracefully stopped
    % but the following ones not
    timer:sleep(timer:seconds(1)),

    InterruptedAtmWorkflowExecutionId = schedule_workflow_execution(ReturnValueAtmWorkflowSchemaId),
    FailedAtmWorkflowExecutionId = schedule_workflow_execution(FilePipeAtmWorkflowSchemaId),

    % Wait until first item begins execution
    timer:sleep(timer:seconds(1)),

    InterruptedAtmWorkflowExecutionExpState0 = expect_workflow_execution_active(
        InterruptedAtmWorkflowExecutionId, ReturnValueAtmWorkflowSchemaRevision
    ),
    FailedAtmWorkflowExecutionExpState0 = expect_workflow_execution_active(
        FailedAtmWorkflowExecutionId, FilePipeAtmWorkflowSchemaRevision
    ),
    assert_all_match_with_backend([
        UserPausedAtmWorkflowExecutionExpState0, SystemPausedAtmWorkflowExecutionExpState0,
        InterruptedAtmWorkflowExecutionExpState0, FailedAtmWorkflowExecutionExpState0
    ]),

    pause_workflow_execution(UserPausedAtmWorkflowExecutionId),
    UserPausedAtmWorkflowExecutionExpState1 = expect_workflow_execution_stopping(
        UserPausedAtmWorkflowExecutionExpState0
    ),
    assert_all_match_with_backend([
        UserPausedAtmWorkflowExecutionExpState1, SystemPausedAtmWorkflowExecutionExpState0,
        InterruptedAtmWorkflowExecutionExpState0, FailedAtmWorkflowExecutionExpState0
    ]),

    OpWorkerStopRequestId = init_op_worker_stop(),

    BeforeCleanupRef = receive {Ref, before_cleanup} -> Ref end,
    reply(BeforeCleanupRef, proceed),

    timer:sleep(timer:seconds(1)),

    ct:pal("~p", [?LINE]),
    SystemPausedAtmWorkflowExecutionExpState1 = expect_workflow_execution_stopping(
        SystemPausedAtmWorkflowExecutionExpState0
    ),
    InterruptedAtmWorkflowExecutionExpState1 = expect_workflow_execution_stopping(
        InterruptedAtmWorkflowExecutionExpState0
    ),
    FailedAtmWorkflowExecutionExpState1 = expect_workflow_execution_stopping(
        FailedAtmWorkflowExecutionExpState0
    ),
    assert_all_match_with_backend([
        UserPausedAtmWorkflowExecutionExpState1, SystemPausedAtmWorkflowExecutionExpState1,
        InterruptedAtmWorkflowExecutionExpState1, FailedAtmWorkflowExecutionExpState1
    ]),
    ct:pal("~p", [?LINE]),

    %% TODO VFS-10266 cleanup ended
    ct:pal("~p", [?LINE]),
    AfterCleanupRef = receive {Ref, after_cleanup} -> Ref end,
    ct:pal("~p", [?LINE]),

    UserPausedAtmWorkflowExecutionExpState2 = expect_workflow_execution_paused(
        UserPausedAtmWorkflowExecutionExpState1
    ),
    SystemPausedAtmWorkflowExecutionExpState2 = expect_workflow_execution_paused(
        SystemPausedAtmWorkflowExecutionExpState1
    ),
    assert_all_match_with_backend([
        UserPausedAtmWorkflowExecutionExpState2, SystemPausedAtmWorkflowExecutionExpState2,
        InterruptedAtmWorkflowExecutionExpState1, FailedAtmWorkflowExecutionExpState1
    ]),
    ct:pal("~p", [?LINE]),

    reply(AfterCleanupRef, proceed),
    ct:pal("~p", [?LINE]),
    finalize_op_worker_stop(OpWorkerStopRequestId),
    ct:pal("~p", [?LINE]),
    restart_op_worker(Config),
    %% TODO VFS-10266 op_worker restart

    % Wait until automation restart procedure finishes
    timer:sleep(timer:seconds(1)),

    ct:pal("~p", [?LINE]),
    % Workflow paused due to op_worker stopping should be resumed after op_worker restart
    SystemPausedAtmWorkflowExecutionExpState2 = atm_workflow_execution_exp_state_builder:expect(
        SystemPausedAtmWorkflowExecutionExpState2, [
            {task, ?TASK1_SELECTOR({1, 1}), finished},
            {lane_run, {1, 1}, finished},
            workflow_finished
        ]
    ),
    % Workflow interrupted due to op_worker stopping should be restarted after op_worker restart
    InterruptedAtmWorkflowExecutionExpState2 = atm_workflow_execution_exp_state_builder:expect(
        InterruptedAtmWorkflowExecutionExpState1, [
            % interruption
            {all_tasks, {1, 1}, abruptly, interrupted},
            {lane_run, {1, 1}, interrupted},
            workflow_interrupted,

            % resume
            workflow_active,
            {lane_run, {1, 1}, active},
            {task, ?TASK1_SELECTOR({1, 1}), active},
            % Item execution was interrupted and as such it should be scheduled once again
            {task, ?TASK1_SELECTOR({1, 1}), items_scheduled, 1}
        ]
    ),
    % Workflow interrupted due to op_worker stopping but with uncorrelated results
    % should fail and not be restarted
    FailedAtmWorkflowExecutionExpState2 = atm_workflow_execution_exp_state_builder:expect(
        FailedAtmWorkflowExecutionExpState1, [
            {all_tasks, {1, 1}, abruptly, failed},
            {lane_run, {1, 1}, failed},
            workflow_failed
        ]
    ),
    ct:pal("~p", [?LINE]),
    assert_all_match_with_backend([
        UserPausedAtmWorkflowExecutionExpState2, SystemPausedAtmWorkflowExecutionExpState2,
        InterruptedAtmWorkflowExecutionExpState2, FailedAtmWorkflowExecutionExpState2
    ]).



%% @private
-spec mock_atm_supervision_worker() -> ok.
mock_atm_supervision_worker() ->
    TestProcPid = self(),
    Workers = oct_background:get_provider_nodes(?PROVIDER_SELECTOR),

    Module = atm_supervision_worker,
    test_utils:mock_new(Workers, Module, [passthrough, no_history]),

    test_utils:mock_expect(Workers, Module, cleanup, fun() ->
        call_test_process(TestProcPid, before_cleanup),
        meck:passthrough(),
        call_test_process(TestProcPid, after_cleanup)
    end).


%% @private
-spec call_test_process(pid(), term()) -> term() | no_return().
call_test_process(TestProcPid, Msg) ->
    MRef = erlang:monitor(process, TestProcPid),
    TestProcPid ! {{self(), MRef}, Msg},
    receive
        {MRef, Reply} ->
            erlang:demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, _, _, Reason} ->
            exit(Reason)
    end.


%% @private
-spec reply({pid(), reference()}, term()) -> ok.
reply({Pid, MRef}, Reply) ->
    Pid ! {MRef, Reply},
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec set_env(atom(), term()) -> ok.
set_env(EnvVar, EnvValue) ->
    ?rpc(?PROVIDER_SELECTOR, op_worker:set_env(EnvVar, EnvValue)).


%% @private
-spec init_op_worker_stop() -> erpc:request_id().
init_op_worker_stop() ->
    Node = oct_background:get_random_provider_node(?PROVIDER_SELECTOR),
    erpc:send_request(Node, application, stop, [?APP_NAME]).


%% @private
-spec finalize_op_worker_stop(erpc:request_id()) -> ok.
finalize_op_worker_stop(OpWorkerStopRequestId) ->
    ?assertEqual(ok, erpc:receive_response(OpWorkerStopRequestId)).


%% @private
restart_op_worker(Config) ->
    Node = oct_background:get_random_provider_node(?PROVIDER_SELECTOR),
    failure_test_utils:kill_nodes(Config, Node),
    failure_test_utils:restart_nodes(Config, Node),
    ok.


%% @private
-spec create_workflow_schema(return_value | file_pipe) ->
    od_atm_workflow_schema:id().
create_workflow_schema(RelayMethod) ->
    atm_test_inventory:add_workflow_schema(?ATM_WORKFLOW_SCHEMA_DRAFT(RelayMethod)).


%% @private
-spec get_workflow_schema_revision(od_atm_workflow_schema:id()) ->
    atm_workflow_schema_revision:record().
get_workflow_schema_revision(AtmWorkflowSchemaId) ->
    atm_test_inventory:get_workflow_schema_revision(1, AtmWorkflowSchemaId).


%% @private
-spec schedule_workflow_execution(od_atm_workflow_schema:id()) ->
    atm_workflow_execution:id().
schedule_workflow_execution(AtmWorkflowSchemaId) ->
    SessionId = oct_background:get_user_session_id(?USER_SELECTOR, ?PROVIDER_SELECTOR),
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),

    {AtmWorkflowExecutionId, _} = ?rpc(?PROVIDER_SELECTOR, mi_atm:schedule_workflow_execution(
        SessionId, SpaceId, AtmWorkflowSchemaId, 1, #{}, ?LOGGER_DEBUG, undefined
    )),
    AtmWorkflowExecutionId.


%% @private
-spec expect_workflow_execution_active(atm_workflow_execution:id(), atm_workflow_schema_revision:record()) ->
    atm_workflow_execution_exp_state_builder:ctx().
expect_workflow_execution_active(AtmWorkflowExecutionId, AtmWorkflowSchemaRevision) ->
    SpaceId = oct_background:get_space_id(?SPACE_SELECTOR),

    ExpInitialState = atm_workflow_execution_exp_state_builder:init(
        ?PROVIDER_SELECTOR, SpaceId, normal, AtmWorkflowExecutionId, AtmWorkflowSchemaRevision
    ),
    atm_workflow_execution_exp_state_builder:expect(ExpInitialState, [
        {lane_run, {1, 1}, started_preparing},
        {lane_run, {1, 1}, created},
        {lane_run, {1, 1}, enqueued},
        {task, ?TASK1_SELECTOR({1, 1}), items_scheduled, 1}
    ]).


%% @private
-spec expect_workflow_execution_stopping(atm_workflow_execution_exp_state_builder:ctx()) ->
    atm_workflow_execution_exp_state_builder:ctx().
expect_workflow_execution_stopping(ExpState) ->
    atm_workflow_execution_exp_state_builder:expect(ExpState, [
        {task, ?TASK1_SELECTOR({1, 1}), stopping},
        {lane_run, {1, 1}, stopping},
        workflow_stopping
    ]).


%% @private
-spec expect_workflow_execution_paused(atm_workflow_execution_exp_state_builder:ctx()) ->
    atm_workflow_execution_exp_state_builder:ctx().
expect_workflow_execution_paused(ExpState) ->
    atm_workflow_execution_exp_state_builder:expect(ExpState, [
        {task, ?TASK1_SELECTOR({1, 1}), items_finished, 1},
        {task, ?TASK1_SELECTOR({1, 1}), paused},
        {lane_run, {1, 1}, paused},
        workflow_paused
    ]).


%% @private
-spec assert_all_match_with_backend([atm_workflow_execution_exp_state_builder:exp_state()]) ->
    ok.
assert_all_match_with_backend(ExpAtmWorkflowExecutionStates) ->
    lists:foreach(fun(ExpAtmWorkflowExecutionState) ->
        ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(
            ExpAtmWorkflowExecutionState
        ))
    end, ExpAtmWorkflowExecutionStates).
