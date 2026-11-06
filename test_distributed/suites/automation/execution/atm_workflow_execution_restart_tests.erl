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

% This module indirectly includes eunit.hrl, whose parse transform would
% otherwise auto-export every arity 0 function named *_test - clashing with
% the export list below.
-define(EUNIT_NOAUTO, 1).

-include("atm/atm_workflow_execution_test.hrl").
-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").

-export([
    restart_op_worker_after_graceful_stop/1
]).
-export([
    clean_up/1
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
            % no automatic retries - the point of this test is what a restart does
            % to an execution, not what the execution does on its own afterwards
            max_retries = 0
        }]
    },
    supplementary_lambdas = #{
        ?ECHO_LAMBDA_ID => #{?ECHO_LAMBDA_REVISION_NUM => #atm_lambda_revision_draft{
            operation_spec = #atm_openfaas_operation_spec_draft{
                % the job must outlive everything this test does to its execution,
                % and must keep reporting heartbeats while it does - the suite sets
                % 'atm_workflow_job_timeout_sec' to 1 second, so a silent job would
                % be timed out right away
                docker_image = ?ECHO_UNTIL_STOPPING_DOCKER_IMAGE_ID
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

% every state transition awaited by this test is asynchronous (graceful stop of
% executions, node restart, the restart procedure delayed by
% 'atm_workflow_executions_restart_retry_delay') - hence assertions must retry,
% and for long enough to cover the last of those, which is counted down from the
% boot of the restarted node rather than from the moment the test starts waiting
-define(ATTEMPTS, 90).

% Ids of the executions whose jobs are exempted from the workflow engine keepalive
% timeouts, kept on the provider node so that the mock installed before any of them
% exists can consult it (@see exempt_jobs_from_timeouts/1)
-define(JOB_TIMEOUT_EXEMPTIONS_KEY, atm_restart_test_job_timeout_exemptions).


%%%===================================================================
%%% Tests
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Checks how op_worker stop and the subsequent restart are survived by atm
%% workflow executions, given the 4 states they can be caught in:
%%
%%   1. suspended by the user beforehand - must be left alone by the restart,
%%   2. suspended with the 'op_worker_stopping' reason - must be resumed by the
%%      restart,
%%   3. still running when the provider went down - must be interrupted and then
%%      resumed by the restart,
%%   4. as above, but with uncorrelated task results - such execution can not be
%%      resumed and must be failed instead.
%%
%% Two properties of the shutdown shape the way those states are set up:
%%
%%   * op_worker brings its https listener down BEFORE asking atm executions to
%%     stop (see the ordering in node_manager_plugin:after_listeners_stop/0), and
%%     that listener serves both the job heartbeats and the job answers. No job in
%%     flight can therefore ever report again - it can only be timed out, taking
%%     its item down with it. Hence 2. is put into its state before the shutdown,
%%     with the listener still up: the state it models is that of an execution
%%     which had no job in flight when the provider began stopping, the only kind
%%     the graceful stop procedure can actually suspend.
%%   * an execution can be caught by the shutdown only for as long as its jobs
%%     keep it from stopping. With the 1 second job timeout this suite sets they
%%     would all be given up on within the graceful stop window, so 3. and 4. have
%%     their jobs exempted from timeouts - which is what the production timeout
%%     (30 min) amounts to anyway.
%% @end
%%--------------------------------------------------------------------
restart_op_worker_after_graceful_stop(Config) ->
    mock_job_timeouts(),

    ReturnValueAtmWorkflowSchemaId = create_workflow_schema(return_value),
    ReturnValueAtmWorkflowSchemaRevision = get_workflow_schema_revision(ReturnValueAtmWorkflowSchemaId),

    FilePipeAtmWorkflowSchemaId = create_workflow_schema(file_pipe),
    FilePipeAtmWorkflowSchemaRevision = get_workflow_schema_revision(FilePipeAtmWorkflowSchemaId),

    UserPausedAtmWorkflowExecutionId = schedule_workflow_execution(ReturnValueAtmWorkflowSchemaId),
    SystemPausedAtmWorkflowExecutionId = schedule_workflow_execution(ReturnValueAtmWorkflowSchemaId),
    InterruptedAtmWorkflowExecutionId = schedule_workflow_execution(ReturnValueAtmWorkflowSchemaId),
    FailedAtmWorkflowExecutionId = schedule_workflow_execution(FilePipeAtmWorkflowSchemaId),

    UserPausedAtmWorkflowExecutionExpState0 = expect_workflow_execution_active(
        UserPausedAtmWorkflowExecutionId, ReturnValueAtmWorkflowSchemaRevision
    ),
    SystemPausedAtmWorkflowExecutionExpState0 = expect_workflow_execution_active(
        SystemPausedAtmWorkflowExecutionId, ReturnValueAtmWorkflowSchemaRevision
    ),
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

    exempt_jobs_from_timeouts([InterruptedAtmWorkflowExecutionId, FailedAtmWorkflowExecutionId]),

    % Both executions are suspended while the listeners are still up, so their jobs
    % do manage to answer the stop request and the items are accounted for as
    % finished. The two differ only in the reason recorded, which is precisely what
    % the restart procedure keys on.
    pause_workflow_execution(UserPausedAtmWorkflowExecutionId),
    UserPausedAtmWorkflowExecutionExpState1 = expect_workflow_execution_suspended(
        UserPausedAtmWorkflowExecutionExpState0
    ),
    stop_workflow_execution_as_op_worker_stopping(SystemPausedAtmWorkflowExecutionId),
    SystemPausedAtmWorkflowExecutionExpState1 = expect_workflow_execution_suspended(
        SystemPausedAtmWorkflowExecutionExpState0
    ),
    assert_all_match_with_backend([
        UserPausedAtmWorkflowExecutionExpState1, SystemPausedAtmWorkflowExecutionExpState1,
        InterruptedAtmWorkflowExecutionExpState0, FailedAtmWorkflowExecutionExpState0
    ]),

    mock_graceful_stop_procedure(),
    OpWorkerStopRequestId = init_op_worker_stop(),

    % the node is held here until answered, so that the states left behind by the
    % graceful stop procedure can be examined before op_worker finishes stopping
    GracefulStopFinishedRef = receive {Ref, graceful_stop_finished} -> Ref end,

    % These jobs are never timed out and so keep their executions from stopping
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

    reply(GracefulStopFinishedRef, proceed),
    finalize_op_worker_stop(OpWorkerStopRequestId),
    restart_op_worker(Config),

    % Expectations below must be built only once the restart procedure has run - the
    % timestamps they carry are validated against the moment of their building, with
    % a tolerance far shorter than 'atm_workflow_executions_restart_retry_delay'
    await_atm_workflow_executions_restarted(SystemPausedAtmWorkflowExecutionId),

    % Execution suspended by the user is not resumed by the restart procedure
    UserPausedAtmWorkflowExecutionExpState2 = UserPausedAtmWorkflowExecutionExpState1,

    % Execution suspended due to op_worker stopping is resumed, and ends right away
    % as its sole item had been processed before the suspension
    SystemPausedAtmWorkflowExecutionExpState2 = atm_workflow_execution_exp_state_builder:expect(
        SystemPausedAtmWorkflowExecutionExpState1, [
            {lane_run, {1, 1}, resuming},
            workflow_resuming,
            {all_tasks, {1, 1}, pending},
            {lane_run, {1, 1}, enqueued},
            workflow_active,
            {all_tasks, {1, 1}, finished},
            {lane_run, {1, 1}, finished},
            {lane_runs, [{1, 1}], rerunable},
            workflow_finished
        ]
    ),
    % Execution that was still running when op_worker went down is interrupted
    % and then resumed
    InterruptedAtmWorkflowExecutionExpState2 = atm_workflow_execution_exp_state_builder:expect(
        InterruptedAtmWorkflowExecutionExpState1, [
            % interruption
            {all_tasks, {1, 1}, abruptly, interrupted},
            {lane_run, {1, 1}, interrupted},
            workflow_interrupted,

            % resume
            {lane_run, {1, 1}, resuming},
            workflow_resuming,
            {all_tasks, {1, 1}, pending},
            {lane_run, {1, 1}, enqueued},
            workflow_active,
            % Item execution was interrupted and as such it should be scheduled once again
            {task, ?TASK1_SELECTOR({1, 1}), items_scheduled, 1}
        ]
    ),
    % Execution with uncorrelated results can not be resumed, as the results
    % streamed so far were lost along with the provider
    FailedAtmWorkflowExecutionExpState2 = atm_workflow_execution_exp_state_builder:expect(
        FailedAtmWorkflowExecutionExpState1, [
            {all_tasks, {1, 1}, abruptly, failed},
            {lane_run, {1, 1}, failed},
            % rerunable but NOT retriable - a retry re-runs the items that failed,
            % and this run has no such set to speak of: it was stopped rather than
            % having gone through all of its items (@see
            % atm_lane_execution_status:is_lane_run_repeatable/2)
            {lane_runs, [{1, 1}], rerunable},
            workflow_failed
        ]
    ),
    assert_all_match_with_backend([
        UserPausedAtmWorkflowExecutionExpState2, SystemPausedAtmWorkflowExecutionExpState2,
        InterruptedAtmWorkflowExecutionExpState2, FailedAtmWorkflowExecutionExpState2
    ]).


%%--------------------------------------------------------------------
%% @doc
%% Undoes what the testcase does to the provider itself, so that neither the
%% following testcases nor the following runs inherit it:
%%
%%   * op_worker left stopped - the case of a testcase that failed anywhere
%%     between initiating the stop and completing the restart. The restart wipes
%%     the mocks and the node cache along with everything below.
%%   * jobs exempted from timeouts - executions left behind by such a testcase
%%     could never be stopped, and would keep a job running until the end of time.
%% @end
%%--------------------------------------------------------------------
-spec clean_up(test_config:config()) -> ok.
clean_up(Config) ->
    Node = oct_background:get_random_provider_node(?ATM_PROVIDER_SELECTOR),

    case rpc:call(Node, application, which_applications, []) of
        RunningApps when is_list(RunningApps) ->
            case lists:keymember(?APP_NAME, 1, RunningApps) of
                true -> exempt_jobs_from_timeouts([]);
                false -> restart_op_worker(Config)
            end;
        {badrpc, _} ->
            restart_op_worker(Config)
    end.


%%%===================================================================
%%% Mocks
%%%===================================================================


%% @private
%% @doc
%% Holds the node right after the graceful stop procedure has run, so that the
%% states it left behind can be examined while op_worker is still able to report
%% them (it is stopping, and with it goes everything the assertions query).
%% @end
-spec mock_graceful_stop_procedure() -> ok.
mock_graceful_stop_procedure() ->
    TestProcPid = self(),
    Workers = oct_background:get_provider_nodes(?ATM_PROVIDER_SELECTOR),

    Module = atm_supervision_worker,
    test_utils:mock_new(Workers, Module, [passthrough, no_history]),

    test_utils:mock_expect(Workers, Module, try_to_gracefully_stop_atm_workflow_executions, fun() ->
        % NOTE: 'meck:passthrough/0' would NOT do - it is syntactic sugar building a
        % ret_spec to be used as the value of an expect, and called from within one
        % it silently does nothing, skipping the mocked procedure altogether
        meck:passthrough([]),
        call_test_process(TestProcPid, graceful_stop_finished)
    end).


%% @private
%% @doc
%% Withholds the workflow engine keepalive timeouts from the executions listed
%% under ?JOB_TIMEOUT_EXEMPTIONS_KEY - their jobs are then never given up on and
%% the executions can not be stopped, gracefully or otherwise. This is what the
%% provider would face with the production job timeout (30 min) rather than the
%% 1 second this suite sets.
%% @end
-spec mock_job_timeouts() -> ok.
mock_job_timeouts() ->
    Workers = oct_background:get_provider_nodes(?ATM_PROVIDER_SELECTOR),
    ExemptionsKey = ?JOB_TIMEOUT_EXEMPTIONS_KEY,

    Module = workflow_execution_state,
    test_utils:mock_new(Workers, Module, [passthrough, no_history]),

    test_utils:mock_expect(Workers, Module, check_timeouts, fun(ExecutionId) ->
        case lists:member(ExecutionId, node_cache:get(ExemptionsKey, [])) of
            true -> false;
            false -> meck:passthrough([ExecutionId])
        end
    end).


%% @private
-spec exempt_jobs_from_timeouts([atm_workflow_execution:id()]) -> ok.
exempt_jobs_from_timeouts(AtmWorkflowExecutionIds) ->
    % NOTE: called also during cleanup, when this module may not be loaded on the
    % node yet - hence a plain mfa call rather than the closure '?rpc' would ship
    opw_test_rpc:call(?ATM_PROVIDER_SELECTOR, node_cache, put, [
        ?JOB_TIMEOUT_EXEMPTIONS_KEY, AtmWorkflowExecutionIds
    ]).


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
-spec init_op_worker_stop() -> erpc:request_id().
init_op_worker_stop() ->
    Node = oct_background:get_random_provider_node(?ATM_PROVIDER_SELECTOR),
    erpc:send_request(Node, application, stop, [?APP_NAME]).


%% @private
-spec finalize_op_worker_stop(erpc:request_id()) -> ok.
finalize_op_worker_stop(OpWorkerStopRequestId) ->
    ?assertEqual(ok, erpc:receive_response(OpWorkerStopRequestId)).


%% @private
-spec restart_op_worker(test_config:config()) -> ok.
restart_op_worker(Config) ->
    Node = oct_background:get_random_provider_node(?ATM_PROVIDER_SELECTOR),
    failure_test_utils:kill_nodes(Config, Node),
    failure_test_utils:restart_nodes(Config, Node),

    % Mocks, alongside the modules defining them, died with the node while the
    % workflow executions restarted by the provider still need them to run tasks.
    % This module goes with them - the '?rpc' macro ships a closure, which the node
    % can not apply unless the module defining it is loaded there.
    test_node_starter:load_modules([Node], [?MODULE | ?ATM_WORKFLOW_EXECUTION_TEST_UTILS]),
    atm_workflow_execution_test_runner:init(?ATM_PROVIDER_SELECTOR).


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
    SessionId = oct_background:get_user_session_id(?ATM_USER_SELECTOR, ?ATM_PROVIDER_SELECTOR),
    SpaceId = oct_background:get_space_id(?ATM_SPACE_SELECTOR),

    {AtmWorkflowExecutionId, _} = ?rpc(?ATM_PROVIDER_SELECTOR, mi_atm:schedule_workflow_execution(
        SessionId, SpaceId, AtmWorkflowSchemaId, 1, #{}, ?DEBUG_AUDIT_LOG_SEVERITY_INT, undefined
    )),
    AtmWorkflowExecutionId.


%% @private
-spec pause_workflow_execution(atm_workflow_execution:id()) -> ok.
pause_workflow_execution(AtmWorkflowExecutionId) ->
    SessionId = oct_background:get_user_session_id(?ATM_USER_SELECTOR, ?ATM_PROVIDER_SELECTOR),
    ?rpc(?ATM_PROVIDER_SELECTOR, mi_atm:init_pause_workflow_execution(SessionId, AtmWorkflowExecutionId)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops the execution exactly as the graceful stop procedure would - and for the
%% very same reason - only while op_worker is still fully up. This is the state of
%% an execution that had nothing in flight when the provider began stopping; one
%% that had can not reach it, as its job would need the https listener that is
%% already down by then to report anything.
%% @end
%%--------------------------------------------------------------------
-spec stop_workflow_execution_as_op_worker_stopping(atm_workflow_execution:id()) -> ok.
stop_workflow_execution_as_op_worker_stopping(AtmWorkflowExecutionId) ->
    ?rpc(?ATM_PROVIDER_SELECTOR, atm_workflow_execution_handler:init_stop(
        user_ctx:new(?ROOT_SESS_ID), AtmWorkflowExecutionId, op_worker_stopping
    )).


%% @private
-spec await_atm_workflow_executions_restarted(atm_workflow_execution:id()) -> ok.
await_atm_workflow_executions_restarted(SuspendedAtmWorkflowExecutionId) ->
    % suspended executions are the last ones the restart procedure goes through,
    % so this one leaving its status behind means the procedure has run its course
    ?assertNotEqual(?PAUSED_STATUS, get_workflow_execution_status(
        SuspendedAtmWorkflowExecutionId
    ), ?ATTEMPTS).


%% @private
-spec get_workflow_execution_status(atm_workflow_execution:id()) ->
    atm_workflow_execution:status().
get_workflow_execution_status(AtmWorkflowExecutionId) ->
    {ok, #document{value = #atm_workflow_execution{status = Status}}} = ?rpc(
        ?ATM_PROVIDER_SELECTOR, atm_workflow_execution:get(AtmWorkflowExecutionId)
    ),
    Status.


%% @private
-spec expect_workflow_execution_active(atm_workflow_execution:id(), atm_workflow_schema_revision:record()) ->
    atm_workflow_execution_exp_state_builder:ctx().
expect_workflow_execution_active(AtmWorkflowExecutionId, AtmWorkflowSchemaRevision) ->
    SpaceId = oct_background:get_space_id(?ATM_SPACE_SELECTOR),

    await_lane_run_created(AtmWorkflowExecutionId),

    ExpInitialState = atm_workflow_execution_exp_state_builder:init(
        ?ATM_PROVIDER_SELECTOR, SpaceId, normal, AtmWorkflowExecutionId, AtmWorkflowSchemaRevision
    ),
    atm_workflow_execution_exp_state_builder:expect(ExpInitialState, [
        {lane_run, {1, 1}, started_preparing},
        {lane_run, {1, 1}, created},
        {lane_run, {1, 1}, enqueued},
        {task, ?TASK1_SELECTOR({1, 1}), items_scheduled, 1}
    ]).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% The exp state builder is no pure model - the '{lane_run, _, created}'
%% expectation reads the lane run back from op, as the ids generated for its task
%% executions can not be told beforehand. Asking for it before op has created the
%% lane run crashes the builder on a length mismatch between the parallel boxes of
%% the schema and the (still absent) executed ones, and no amount of retrying the
%% assertion afterwards can help - it is the building of the expectation, not the
%% checking of it, that comes too early.
%% @end
%%--------------------------------------------------------------------
-spec await_lane_run_created(atm_workflow_execution:id()) -> ok.
await_lane_run_created(AtmWorkflowExecutionId) ->
    ?assertEqual(true, is_lane_run_created(AtmWorkflowExecutionId), ?ATTEMPTS).


%% @private
-spec is_lane_run_created(atm_workflow_execution:id()) -> boolean().
is_lane_run_created(AtmWorkflowExecutionId) ->
    {ok, #document{value = AtmWorkflowExecution}} = ?rpc(?ATM_PROVIDER_SELECTOR, atm_workflow_execution:get(
        AtmWorkflowExecutionId
    )),
    case atm_lane_execution:get_run({1, 1}, AtmWorkflowExecution) of
        {ok, #atm_lane_execution_run{parallel_boxes = AtmParallelBoxExecutions}} ->
            AtmParallelBoxExecutions =/= [];
        {error, _} ->
            false
    end.


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
-spec expect_workflow_execution_suspended(atm_workflow_execution_exp_state_builder:ctx()) ->
    atm_workflow_execution_exp_state_builder:ctx().
expect_workflow_execution_suspended(ExpState) ->
    atm_workflow_execution_exp_state_builder:expect(
        expect_workflow_execution_stopping(ExpState), [
            {task, ?TASK1_SELECTOR({1, 1}), items_finished, 1},
            {task, ?TASK1_SELECTOR({1, 1}), paused},
            {lane_run, {1, 1}, paused},
            workflow_paused
        ]
    ).


%% @private
-spec assert_all_match_with_backend([atm_workflow_execution_exp_state_builder:ctx()]) ->
    ok.
assert_all_match_with_backend(ExpAtmWorkflowExecutionStates) ->
    lists:foreach(fun(ExpAtmWorkflowExecutionState) ->
        ?assert(atm_workflow_execution_exp_state_builder:assert_matches_with_backend(
            ExpAtmWorkflowExecutionState, ?ATTEMPTS
        ))
    end, ExpAtmWorkflowExecutionStates).
