%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for mocking automation workflow execution steps (e.g. callbacks).
%%% When each step begin execution (by op execution process) a call to test process
%%% informing it which step begins execution is made. Test process may then execute
%%% hooks or check expected atm workflow execution state changes (this allows to check
%%% incremental changes after each step rather than final expectation after execution
%%% ends). Next, it responds to execution process by sending it step execution strategy
%%% (e.g. simple passthrough or passthrough with delay, etc.) to be carry out. Finally,
%%% after step ends execution process makes synchronous call to test process informing
%%% it that step execution ended.
%%%
%%% Should the test process die (e.g. testcase failed or was killed by timetrap) the
%%% workflow execution is detached from it and left to run unperturbed. Otherwise, all
%%% its steps would keep dying alongside the test process and the execution would never
%%% reach any stopped status - such execution can not be discarded and blocks provider
%%% shutdown (see atm_supervision_worker:try_to_gracefully_stop_atm_workflow_executions/0),
%%% effectively poisoning the deployment for any subsequent test run.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_test_mocks).
-author("Bartosz Walkowicz").

-include("atm/atm_workflow_execution_test.hrl").
-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([init/1, teardown/1]).
-export([schedule_workflow_execution_as_test_process/8]).
-export([reply_to_execution_process/2]).
-export([detach_from_test_process/1]).


-opaque reply_to() :: {pid(), reference()}.
-export_type([reply_to/0]).


-define(TEST_PROC_PID_KEY(__ATM_WORKFLOW_EXECUTION_ID),
    {atm_test_runner_process, __ATM_WORKFLOW_EXECUTION_ID}
).

% reply substitute returned when the test process died before answering
-define(TEST_PROCESS_DOWN, test_process_down).

% Mirrors the private ?ATM_WORKFLOW_EXECUTION_ENGINE of atm_workflow_execution_handler
-define(ATM_WORKFLOW_EXECUTION_ENGINE_ID, <<"atm_workflow_execution_engine">>).
-define(JOB_ENQUEUING_TIMEOUT_SEC, 10).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
init(ProviderSelectors) ->
    atm_openfaas_task_executor_mock:init(ProviderSelectors, atm_openfaas_docker_mock),

    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),
    set_job_enqueuing_timeout(Workers, ?JOB_ENQUEUING_TIMEOUT_SEC),
    mock_workflow_execution_factory(Workers),
    mock_workflow_execution_handler_steps(Workers),
    mock_lane_execution_stop_handler_steps(Workers),
    mock_lane_execution_factory_steps(Workers),
    mock_task_execution_status_steps(Workers).


-spec teardown(oct_background:entity_selector() | [oct_background:entity_selector()]) ->
    ok.
teardown(ProviderSelectors) ->
    Workers = get_nodes(utils:ensure_list(ProviderSelectors)),
    unmock_task_execution_status_steps(Workers),
    unmock_lane_execution_factory_steps(Workers),
    unmock_lane_execution_handler_steps(Workers),
    unmock_workflow_execution_handler_steps(Workers),
    unmock_workflow_execution_factory(Workers),
    set_job_enqueuing_timeout(Workers, infinity),

    atm_openfaas_task_executor_mock:teardown(ProviderSelectors).


%% @private
%% @doc
%% Bounds how long a job may stay registered without reporting a heartbeat.
%%
%% The atm engine sets no such bound ('enqueuing_timeout => infinity'), which leaves
%% a freshly registered job with no keepalive timer at all - the timer is armed only
%% by the first heartbeat that finds the job already registered, and any heartbeat
%% reaching op earlier is silently dropped (TODO VFS-10550). Registration happens
%% only after the mocked 'run_task_for_item' step returns, and the test runner holds
%% that step back for as long as it needs, so a lambda that heartbeats once and goes
%% silent can end up immune to timeouts altogether.
%% @end
-spec set_job_enqueuing_timeout([node()], time:seconds() | infinity) -> ok.
set_job_enqueuing_timeout(Workers, Timeout) ->
    lists:foreach(fun(Worker) ->
        % Sanity check on the engine id, which is private to the product - any other
        % id (or an engine that was never initiated on this node) would yield the
        % 'undefined' default rather than a setting of its own
        ?assertNotEqual(undefined, rpc:call(
            Worker, workflow_engine, get_enqueuing_timeout, [?ATM_WORKFLOW_EXECUTION_ENGINE_ID]
        )),
        ok = rpc:call(Worker, workflow_engine, set_enqueuing_timeout, [
            ?ATM_WORKFLOW_EXECUTION_ENGINE_ID, Timeout
        ])
    end, Workers).


-spec schedule_workflow_execution_as_test_process(
    oct_background:entity_selector(),
    session:id(),
    od_space:id(),
    od_atm_workflow_schema:id(),
    atm_workflow_schema_revision:revision_number(),
    atm_workflow_execution_api:store_initial_content_overlay(),
    audit_log:entry_severity_int(),
    undefined | http_client:url()
) ->
    {atm_workflow_execution:id(), atm_workflow_execution:record()}.
schedule_workflow_execution_as_test_process(
    ProviderSelector,
    SessionId,
    SpaceId,
    AtmWorkflowSchemaId,
    AtmWorkflowSchemaRevisionNum,
    AtmStoreInitialContentOverlay,
    LogLevel,
    CallbackUrl
) ->
    TestProcPid = self(),

    ?rpc(ProviderSelector, mi_atm:schedule_workflow_execution(
        SessionId, SpaceId, AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum,
        AtmStoreInitialContentOverlay#{test_process => TestProcPid},
        LogLevel, CallbackUrl
    )).


-spec reply_to_execution_process
    % when replying to 'before_step' report
    (reply_to(), atm_workflow_execution_test_runner:mock_strategy()) -> ok;
    % when replying to 'after_step' report
    (reply_to(), ok) -> ok.
reply_to_execution_process({ExecutionProcPid, MRef}, Reply) ->
    ExecutionProcPid ! {MRef, Reply}.


%%--------------------------------------------------------------------
%% @doc
%% Makes the mocks stop reporting steps of specified workflow execution to the
%% test process (from then on it runs unperturbed).
%%
%% NOTE: must be called on the provider node.
%% @end
%%--------------------------------------------------------------------
-spec detach_from_test_process(atm_workflow_execution:id()) -> ok.
detach_from_test_process(AtmWorkflowExecutionId) ->
    node_cache:clear(?TEST_PROC_PID_KEY(AtmWorkflowExecutionId)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_nodes([oct_background:entity_selector()]) -> [node()].
get_nodes(ProviderSelectors) ->
    lists:flatmap(
        fun(ProviderSelector) -> oct_background:get_provider_nodes(ProviderSelector) end,
        ProviderSelectors
    ).


%% @private
-spec mock_workflow_execution_factory([node()]) -> ok.
mock_workflow_execution_factory(Workers) ->
    test_utils:mock_new(Workers, atm_workflow_execution_factory, [passthrough, no_history]),

    test_utils:mock_expect(Workers, atm_workflow_execution_factory, create, fun(
        UserCtx,
        SpaceId,
        AtmWorkflowSchemaId,
        AtmWorkflowSchemaRevisionNum,
        StoreInitialValues,
        LoggingSeverity,
        CallbackUrl
    ) ->
        Result = {#document{key = AtmWorkflowExecutionId}, _} = meck:passthrough([
            UserCtx, SpaceId, AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum,
            StoreInitialValues, LoggingSeverity, CallbackUrl
        ]),
        case maps:get(test_process, StoreInitialValues, undefined) of
            undefined -> ok;
            TestProcPid -> node_cache:put(?TEST_PROC_PID_KEY(AtmWorkflowExecutionId), TestProcPid)
        end,
        Result
    end).


%% @private
-spec unmock_workflow_execution_factory([node()]) -> ok.
unmock_workflow_execution_factory(Workers) ->
    test_utils:mock_unload(Workers, atm_workflow_execution_factory).


%% @private
-spec mock_workflow_execution_handler_steps([node()]) -> ok.
mock_workflow_execution_handler_steps(Workers) ->
    test_utils:mock_new(Workers, atm_workflow_execution_handler, [passthrough, no_history]),

    test_utils:mock_expect(Workers, atm_workflow_execution_handler, handle_task_results_processed_for_all_items, fun(
        AtmWorkflowExecutionId,
        _AtmWorkflowExecutionEnv,
        AtmTaskExecutionId
    ) ->
        workflow_engine:report_task_data_streaming_concluded(AtmWorkflowExecutionId, AtmTaskExecutionId, success)
    end),

    mock_workflow_execution_handler_step(Workers, prepare_lane, 3),
    mock_workflow_execution_handler_step(Workers, resume_lane, 3),
    mock_workflow_execution_handler_step(Workers, run_task_for_item, 5),
    mock_workflow_execution_handler_step(Workers, process_task_result_for_item, 5),
    mock_workflow_execution_handler_step(Workers, process_streamed_task_data, 4),
    mock_workflow_execution_handler_step(Workers, handle_task_results_processed_for_all_items, 3),
    mock_workflow_execution_handler_step(Workers, report_item_error, 3),
    mock_workflow_execution_handler_step(Workers, handle_task_execution_stopped, 3),
    mock_workflow_execution_handler_step(Workers, handle_exception, 5),
    mock_workflow_execution_handler_step(Workers, handle_workflow_abruptly_stopped, 3),
    mock_workflow_execution_handler_step(Workers, handle_workflow_execution_stopped, 2).


%% @private
-spec unmock_workflow_execution_handler_steps([node()]) -> ok.
unmock_workflow_execution_handler_steps(Workers) ->
    test_utils:mock_unload(Workers, atm_workflow_execution_handler).


%% @private
-spec mock_workflow_execution_handler_step([node()], atom(), 1..6) -> ok.
mock_workflow_execution_handler_step(Workers, FunName, FunArity) ->
    MockFun = build_workflow_execution_handler_step_function_mock(FunArity, FunName),
    test_utils:mock_expect(Workers, atm_workflow_execution_handler, FunName, MockFun).


%% @private
-spec build_workflow_execution_handler_step_function_mock(1..6, atom()) ->
    function().
build_workflow_execution_handler_step_function_mock(1, Label) ->
    fun(AtmWorkflowExecutionId) ->
        Args = [AtmWorkflowExecutionId],
        exec_mock(AtmWorkflowExecutionId, Label, Args)
    end;

build_workflow_execution_handler_step_function_mock(2, Label) ->
    fun(AtmWorkflowExecutionId, Arg2) ->
        Args = [AtmWorkflowExecutionId, Arg2],
        exec_mock(AtmWorkflowExecutionId, Label, Args)
    end;

build_workflow_execution_handler_step_function_mock(3, Label) ->
    fun(AtmWorkflowExecutionId, Arg2, Arg3) ->
        Args = [AtmWorkflowExecutionId, Arg2, Arg3],
        exec_mock(AtmWorkflowExecutionId, Label, Args)
    end;

build_workflow_execution_handler_step_function_mock(4, Label) ->
    fun(AtmWorkflowExecutionId, Arg2, Arg3, Arg4) ->
        Args = [AtmWorkflowExecutionId, Arg2, Arg3, Arg4],
        exec_mock(AtmWorkflowExecutionId, Label, Args)
    end;

build_workflow_execution_handler_step_function_mock(5, Label) ->
    fun(AtmWorkflowExecutionId, Arg2, Arg3, Arg4, Arg5) ->
        Args = [AtmWorkflowExecutionId, Arg2, Arg3, Arg4, Arg5],
        exec_mock(AtmWorkflowExecutionId, Label, Args)
    end;

build_workflow_execution_handler_step_function_mock(6, Label) ->
    fun(AtmWorkflowExecutionId, Arg2, Arg3, Arg4, Arg5, Arg6) ->
        Args = [AtmWorkflowExecutionId, Arg2, Arg3, Arg4, Arg5, Arg6],
        exec_mock(AtmWorkflowExecutionId, Label, Args)
    end.


%% @private
-spec mock_lane_execution_stop_handler_steps([node()]) -> ok.
mock_lane_execution_stop_handler_steps(Workers) ->
    test_utils:mock_new(Workers, atm_lane_execution_stop_handler, [passthrough, no_history]),

    test_utils:mock_expect(Workers, atm_lane_execution_stop_handler, handle_stopped, fun(
        AtmLaneRunSelector,
        AtmWorkflowExecutionId,
        AtmWorkflowExecutionCtx
    ) ->
        exec_mock(
            AtmWorkflowExecutionId,
            handle_lane_execution_stopped,
            [AtmLaneRunSelector, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx]
        )
    end).


%% @private
-spec unmock_lane_execution_handler_steps([node()]) -> ok.
unmock_lane_execution_handler_steps(Workers) ->
    test_utils:mock_unload(Workers, atm_lane_execution_stop_handler).


%% @private
-spec mock_lane_execution_factory_steps([node()]) -> ok.
mock_lane_execution_factory_steps(Workers) ->
    test_utils:mock_new(Workers, atm_lane_execution_factory, [passthrough, no_history]),

    test_utils:mock_expect(Workers, atm_lane_execution_factory, create_run, fun(
        AtmLaneRunSelector,
        AtmWorkflowExecutionDoc,
        AtmWorkflowExecutionCtx
    ) ->
        exec_mock(
            AtmWorkflowExecutionDoc#document.key,
            create_run,
            [AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx]
        )
    end).


%% @private
-spec unmock_lane_execution_factory_steps([node()]) -> ok.
unmock_lane_execution_factory_steps(Workers) ->
    test_utils:mock_unload(Workers, atm_lane_execution_factory).


%% @private
-spec mock_task_execution_status_steps([node()]) -> ok.
mock_task_execution_status_steps(Workers) ->
    Module = atm_task_execution_status,
    test_utils:mock_new(Workers, Module, [passthrough, no_history]),

    test_utils:mock_expect(Workers, Module, handle_resuming, fun(AtmTaskExecutionId, CurrentIncarnation) ->
        AtmWorkflowExecutionId = get_workflow_execution_id(AtmTaskExecutionId),
        exec_mock(AtmWorkflowExecutionId, handle_task_resuming, [AtmTaskExecutionId, CurrentIncarnation])
    end),
    test_utils:mock_expect(Workers, Module, handle_resumed, fun(AtmTaskExecutionId) ->
        AtmWorkflowExecutionId = get_workflow_execution_id(AtmTaskExecutionId),
        exec_mock(AtmWorkflowExecutionId, handle_task_resumed, [AtmTaskExecutionId])
    end).


%% @private
-spec get_workflow_execution_id(atm_task_execution:id()) -> atm_workflow_execution:id().
get_workflow_execution_id(AtmTaskExecutionId) ->
    {ok, #document{value = AtmTaskExecution}} = atm_task_execution:get(AtmTaskExecutionId),
    AtmTaskExecution#atm_task_execution.workflow_execution_id.


%% @private
-spec unmock_task_execution_status_steps([node()]) -> ok.
unmock_task_execution_status_steps(Workers) ->
    test_utils:mock_unload(Workers, atm_task_execution_status).


%% @private
-spec exec_mock(atm_workflow_execution:id(), atom(), [term()]) -> term().
exec_mock(AtmWorkflowExecutionId, Step, Args) ->
    case node_cache:get(?TEST_PROC_PID_KEY(AtmWorkflowExecutionId), undefined) of
        undefined ->
            meck:passthrough(Args);
        TestProcPid ->
            MockCallReport = #mock_call_report{
                timing = before_step,
                step = Step,
                args = Args,
                result = undefined
            },
            MockExecution = call_test_process(TestProcPid, MockCallReport),

            case MockExecution of
                ?TEST_PROCESS_DOWN ->
                    detach_from_test_process(AtmWorkflowExecutionId),
                    meck:passthrough(Args);

                passthrough ->
                    exec_original_function(AtmWorkflowExecutionId, Args, TestProcPid, MockCallReport);

                {passthrough_with_delay, DelayMilliseconds} ->
                    timer:sleep(DelayMilliseconds),
                    exec_original_function(AtmWorkflowExecutionId, Args, TestProcPid, MockCallReport);

                {passthrough_with_result_override, ResultOverride} ->
                    exec_original_function(AtmWorkflowExecutionId, Args, TestProcPid, MockCallReport),
                    apply_result_override(ResultOverride);

                {yield, ResultOverride} ->
                    apply_result_override(ResultOverride)
            end
    end.


%% @private
-spec exec_original_function(
    atm_workflow_execution:id(),
    [term()],
    pid(),
    atm_workflow_execution_test_runner:mock_call_report()
) ->
    term().
exec_original_function(AtmWorkflowExecutionId, Args, TestProcPid, MockCallReport) ->
    Result = meck:passthrough(Args),

    case call_test_process(TestProcPid, MockCallReport#mock_call_report{
        timing = after_step,
        result = Result
    }) of
        ok -> ok;
        ?TEST_PROCESS_DOWN -> detach_from_test_process(AtmWorkflowExecutionId)
    end,

    Result.


%% @private
-spec apply_result_override
    ({return, Result}) -> Result when Result :: term();
    ({error | throw, errors:error()}) -> no_return().
apply_result_override({return, Result}) -> Result;
apply_result_override({throw, Error}) -> throw(Error);
apply_result_override({error, Error}) -> error(Error).


%% @private
-spec call_test_process(pid(), term()) -> term().
call_test_process(TestProcPid, Msg) ->
    MRef = erlang:monitor(process, TestProcPid),
    TestProcPid ! {{self(), MRef}, Msg},
    receive
        {MRef, Reply} ->
            erlang:demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, _, _, _Reason} ->
            ?TEST_PROCESS_DOWN
    end.
