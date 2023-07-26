%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module allowing definition and modification of atm workflow execution
%%% exp state which consists of:
%%% - exp state of atm_workflow_execution model
%%% - exp state of each atm_task_execution model
%%% - exp state of each atm_store model (to be implemented)
%%%
%%% NOTE: exp states are created and stored as json object similar to responses
%%% send to clients via API endpoints. Model records definitions from op are
%%% not reused as they contain many irrelevant (to clients) fields considered
%%% as implementation details and omitted from said responses.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_exp_state_builder).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([
    init/5,
    expect/2,
    assert_matches_with_backend/1, assert_matches_with_backend/2,
    assert_deleted/1
]).
-export([
    get_workflow_status/1,

    set_current_lane_run/3,

    get_task_selector/2,
    get_task_schema_id/2,
    get_task_id/2,
    get_task_stats/2,
    adjust_abruptly_stopped_task_stats/1,
    get_task_status/2
]).

% json object similar in structure to translations returned via API endpoints
% (has the same keys but as for values instead of concrete values it may contain
% validator functions - e.g. timestamp fields should check approx time rather
% than concrete value)
-type workflow_execution_state() :: json_utils:json_map().
-type lane_run_state() :: json_utils:json_map().
-type parallel_box_execution_state() :: json_utils:json_map().
-type task_execution_state() :: json_utils:json_map().

-record(exp_task_execution_state_ctx, {
    lane_run_selector :: atm_lane_execution:lane_run_selector(),
    parallel_box_schema_id :: automation:id(),
    exp_state :: task_execution_state()
}).
-type exp_task_execution_state_ctx() :: #exp_task_execution_state_ctx{}.
-type exp_task_execution_state_ctx_registry() :: #{atm_task_execution:id() => exp_task_execution_state_ctx()}.

-record(exp_workflow_execution_state_ctx, {
    provider_selector :: oct_background:entity_selector(),
    lane_schemas :: [atm_lane_schema:record()],

    workflow_execution_id :: atm_workflow_execution:id(),
    current_lane_index :: atm_lane_execution:index(),
    current_run_num :: atm_lane_execution:run_num(),

    clock_status :: normal | frozen,

    exp_workflow_execution_state :: workflow_execution_state(),
    exp_task_execution_state_ctx_registry :: exp_task_execution_state_ctx_registry()
}).
-type ctx() :: #exp_workflow_execution_state_ctx{}.

-type task_registry() :: #{AtmTaskSchemaId :: automation:id() => atm_task_execution:id()}.
-type task_selector() :: {atm_lane_execution:lane_run_selector(), automation:id(), automation:id()}.


-type expectation() ::
    {task, atm_task_execution:id() | task_selector(), atm_task_execution:status()} |
    {task, atm_task_execution:id() | task_selector(), items_scheduled, non_neg_integer()} |
    {task, atm_task_execution:id() | task_selector(), items_finished, non_neg_integer()} |
    {task, atm_task_execution:id() | task_selector(), items_failed, non_neg_integer()} |
    {task, atm_task_execution:id() | task_selector(), fun((task_execution_state()) -> task_execution_state())} |

    {all_tasks, atm_lane_execution:lane_run_selector(), atm_task_execution:status()} |
    {all_tasks, atm_lane_execution:lane_run_selector(), abruptly, failed | cancelled | interrupted} |
    {all_tasks, atm_lane_execution:lane_run_selector(), stopping_due_to, pause | interrupt | cancel} |

    {lane_run, atm_lane_execution:lane_run_selector(), scheduled} |
    {lane_run, atm_lane_execution:lane_run_selector(), automatic_retry_scheduled} |
    {lane_run, atm_lane_execution:lane_run_selector(), manual_retry_scheduled, atm_lane_execution:run_num()} |
    {lane_run, atm_lane_execution:lane_run_selector(), manual_rerun_scheduled, atm_lane_execution:run_num()} |
    {lane_run, atm_lane_execution:lane_run_selector(), manual_repeat_scheduled, retry | rerun, atm_lane_execution:run_num()} |
    {lane_run, atm_lane_execution:lane_run_selector(), started_preparing} |
    {lane_run, atm_lane_execution:lane_run_selector(), started_preparing_as_current_lane_run} |
    {lane_run, atm_lane_execution:lane_run_selector(), started_preparing_in_advance} |
    {lane_run, atm_lane_execution:lane_run_selector(), created} |
    {lane_run, atm_lane_execution:lane_run_selector(), atm_lane_execution:run_status()} |
    {lane_run, atm_lane_execution:lane_run_selector(), run_num_set, atm_lane_execution:run_num()} |
    {lane_run, atm_lane_execution:lane_run_selector(), removed} |

    {lane_runs, atm_lane_execution:lane_run_selector() | [atm_lane_execution:lane_run_selector()], rerunable} |
    {lane_runs, atm_lane_execution:lane_run_selector() | [atm_lane_execution:lane_run_selector()], rerunable, boolean()} |
    {lane_runs, atm_lane_execution:lane_run_selector() | [atm_lane_execution:lane_run_selector()], retriable} |
    {lane_runs, atm_lane_execution:lane_run_selector() | [atm_lane_execution:lane_run_selector()], retriable, boolean()} |

    workflow_scheduled |
    workflow_resuming |
    workflow_active |
    workflow_stopping |
    workflow_paused |
    workflow_interrupted |
    workflow_finished |
    workflow_failed |
    workflow_cancelled |
    workflow_crashed.

-type log_fun() :: fun((binary(), [term()]) -> ok).

-export_type([ctx/0, task_selector/0, expectation/0]).


-define(JSON_PATH(__QUERY_BIN), binary:split(__QUERY_BIN, <<".">>, [global])).
-define(JSON_PATH(__FORMAT, __ARGS), ?JSON_PATH(str_utils:format_bin(__FORMAT, __ARGS))).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(
    oct_background:entity_selector(),
    od_space:id(),
    normal | frozen,
    atm_workflow_execution:id(),
    atm_workflow_schema_revision:record()
) ->
    ctx().
init(
    ProviderSelector,
    SpaceId,
    ClockStatus,
    AtmWorkflowExecutionId,
    #atm_workflow_schema_revision{
        stores = AtmStoreSchemas,
        lanes = AtmLaneSchemas = [FirstAtmLaneSchema | RestAtmLaneSchemas]
    }
) ->
    ExpFirstAtmLaneExecutionState = #{
        <<"schemaId">> => FirstAtmLaneSchema#atm_lane_schema.id,
        <<"runs">> => [build_initial_regular_lane_run_exp_state(1, <<"scheduled">>)]
    },
    ExpRestAtmLaneExecutionStates = lists:map(fun(#atm_lane_schema{id = AtmLaneSchemaId}) ->
        #{
            <<"schemaId">> => AtmLaneSchemaId,
            <<"runs">> => []
        }
    end, RestAtmLaneSchemas),

    % It is not possible to infer store ids and they must be fetched from created docs
    AtmWorkflowExecution = fetch_workflow_execution(ProviderSelector, AtmWorkflowExecutionId),
    AtmAuditLogStoreId = AtmWorkflowExecution#atm_workflow_execution.system_audit_log_store_id,
    case is_binary(AtmAuditLogStoreId) of
        true -> ok;
        false -> crash("ERROR - workflow audit log not created")
    end,

    #exp_workflow_execution_state_ctx{
        provider_selector = ProviderSelector,
        lane_schemas = AtmLaneSchemas,

        workflow_execution_id = AtmWorkflowExecutionId,
        current_lane_index = 1,
        current_run_num = 1,

        clock_status = ClockStatus,

        exp_workflow_execution_state = #{
            <<"spaceId">> => SpaceId,

            <<"storeRegistry">> => get_store_registry(AtmWorkflowExecution, AtmStoreSchemas),
            <<"systemAuditLogId">> => AtmAuditLogStoreId,

            <<"lanes">> => [ExpFirstAtmLaneExecutionState | ExpRestAtmLaneExecutionStates],

            <<"status">> => <<"scheduled">>,

            <<"scheduleTime">> => build_timestamp_field_validator(get_timestamp_seconds(
                ClockStatus
            )),
            <<"startTime">> => 0,
            <<"suspendTime">> => 0,
            <<"finishTime">> => 0
        },
        exp_task_execution_state_ctx_registry = #{}
    }.


-spec expect(ctx(), expectation() | [expectation()]) -> ctx().
expect(ExpStateCtx, {task, AtmTaskExecutionIdOrSelector, ExpStatus}) when
    ExpStatus =:= resuming;
    ExpStatus =:= pending;
    ExpStatus =:= active;
    ExpStatus =:= stopping;
    ExpStatus =:= skipped;
    ExpStatus =:= finished;
    ExpStatus =:= failed;
    ExpStatus =:= cancelled;
    ExpStatus =:= paused;
    ExpStatus =:= interrupted
->
    AtmTaskExecutionId = resolve_task_id(AtmTaskExecutionIdOrSelector, ExpStateCtx),
    update_task_execution_exp_state(ExpStateCtx, AtmTaskExecutionId, fun(ExpAtmTaskExecutionState) ->
        ExpAtmTaskExecutionState#{<<"status">> => str_utils:to_binary(ExpStatus)}
    end);

expect(ExpStateCtx0, {task, AtmTaskExecutionIdOrSelector, items_scheduled, ItemCount}) ->
    AtmTaskExecutionId = resolve_task_id(AtmTaskExecutionIdOrSelector, ExpStateCtx0),

    ExpStateCtx1 = lists:foldl(fun(ExpectFun, ExpStateCtxAcc) ->
        ExpectFun(ExpStateCtxAcc, AtmTaskExecutionId)
    end, ExpStateCtx0, [
        fun expect_task_transitioned_to_active_status_if_was_in_pending_status/2,
        fun expect_task_lane_run_transitioned_to_active_status_if_was_in_enqueued_status/2
    ]),
    case get_task_status(AtmTaskExecutionId, ExpStateCtx1) of
        <<"active">> ->
            ExpAtmTaskExecutionStateDiff = fun(AtmTaskExecution = #{<<"itemsInProcessing">> := IIP}) ->
                AtmTaskExecution#{<<"itemsInProcessing">> => IIP + ItemCount}
            end,
            update_task_execution_exp_state(ExpStateCtx1, AtmTaskExecutionId, ExpAtmTaskExecutionStateDiff);
        _ ->
            ExpStateCtx1
    end;

expect(ExpStateCtx, {task, AtmTaskExecutionIdOrSelector, items_finished, ItemCount}) ->
    AtmTaskExecutionId = resolve_task_id(AtmTaskExecutionIdOrSelector, ExpStateCtx),

    update_task_execution_exp_state(ExpStateCtx, AtmTaskExecutionId, fun(ExpAtmTaskExecutionState = #{
        <<"itemsInProcessing">> := IIP,
        <<"itemsProcessed">> := IP
    }) ->
        ExpAtmTaskExecutionState#{
            <<"itemsInProcessing">> => IIP - ItemCount,
            <<"itemsProcessed">> => IP + ItemCount
        }
    end);

expect(ExpStateCtx, {task, AtmTaskExecutionIdOrSelector, items_failed, ItemCount}) ->
    AtmTaskExecutionId = resolve_task_id(AtmTaskExecutionIdOrSelector, ExpStateCtx),

    update_task_execution_exp_state(ExpStateCtx, AtmTaskExecutionId, fun(ExpAtmTaskExecutionState = #{
        <<"itemsInProcessing">> := IIP,
        <<"itemsFailed">> := IF,
        <<"itemsProcessed">> := IP
    }) ->
        ExpAtmTaskExecutionState#{
            <<"itemsInProcessing">> => IIP - ItemCount,
            <<"itemsFailed">> => IF + ItemCount,
            <<"itemsProcessed">> => IP + ItemCount
        }
    end);

expect(ExpStateCtx, {task, AtmTaskExecutionIdOrSelector, ExpTaskStateDiff}) when is_function(ExpTaskStateDiff, 1) ->
    AtmTaskExecutionId = resolve_task_id(AtmTaskExecutionIdOrSelector, ExpStateCtx),
    update_task_execution_exp_state(ExpStateCtx, AtmTaskExecutionId, ExpTaskStateDiff);

expect(ExpStateCtx, {all_tasks, AtmLaneRunSelector, ExpStatus}) when
    ExpStatus =:= resuming;
    ExpStatus =:= pending;
    ExpStatus =:= active;
    ExpStatus =:= stopping;
    ExpStatus =:= skipped;
    ExpStatus =:= finished;
    ExpStatus =:= failed;
    ExpStatus =:= cancelled;
    ExpStatus =:= paused;
    ExpStatus =:= interrupted
->
    ExpAtmTaskExecutionDiff = fun(ExpAtmTaskExecution = #exp_task_execution_state_ctx{exp_state = ExpState}) ->
        ExpAtmTaskExecution#exp_task_execution_state_ctx{exp_state = ExpState#{
            <<"status">> => str_utils:to_binary(ExpStatus)
        }}
    end,
    expect_all_tasks_transitioned(ExpStateCtx, AtmLaneRunSelector, ExpAtmTaskExecutionDiff);

expect(ExpStateCtx, {all_tasks, AtmLaneRunSelector, abruptly, ExpStoppedStatus}) when
    ExpStoppedStatus =:= failed;
    ExpStoppedStatus =:= cancelled;
    ExpStoppedStatus =:= interrupted
->
    ExpAtmTaskExecutionDiff = fun(ExpAtmTaskExecution = #exp_task_execution_state_ctx{exp_state = ExpState}) ->
        ExpAtmTaskExecution#exp_task_execution_state_ctx{exp_state = adjust_abruptly_stopped_task_stats(ExpState#{
            <<"status">> => str_utils:to_binary(ExpStoppedStatus)
        })}
    end,
    expect_all_tasks_transitioned(ExpStateCtx, AtmLaneRunSelector, ExpAtmTaskExecutionDiff);

expect(ExpStateCtx, {all_tasks, AtmLaneRunSelector, stopping_due_to, Reason}) when
    Reason =:= pause;
    Reason =:= interrupt;
    Reason =:= cancel
->
    expect_all_tasks_stopping_due_to(ExpStateCtx, AtmLaneRunSelector, Reason);

expect(ExpStateCtx, {lane_run, AtmLaneRunSelector, scheduled}) ->
    expect_lane_run_scheduled(ExpStateCtx, AtmLaneRunSelector);

expect(ExpStateCtx, {lane_run, AtmLaneRunSelector, automatic_retry_scheduled}) ->
    expect_lane_run_automatic_retry_scheduled(ExpStateCtx, AtmLaneRunSelector);

expect(ExpStateCtx, {lane_run, AtmLaneRunSelector, manual_retry_scheduled, NewRunNum}) ->
    expect_lane_run_manual_repeat_scheduled(ExpStateCtx, retry, AtmLaneRunSelector, NewRunNum);

expect(ExpStateCtx, {lane_run, AtmLaneRunSelector, manual_rerun_scheduled, NewRunNum}) ->
    expect_lane_run_manual_repeat_scheduled(ExpStateCtx, rerun, AtmLaneRunSelector, NewRunNum);

expect(ExpStateCtx, {lane_run, AtmLaneRunSelector, manual_repeat_scheduled, RepeatType, NewRunNum}) ->
    expect_lane_run_manual_repeat_scheduled(ExpStateCtx, RepeatType, AtmLaneRunSelector, NewRunNum);

expect(ExpStateCtx, {lane_run, AtmLaneRunSelector, started_preparing}) ->
    case is_current_lane_run(AtmLaneRunSelector, ExpStateCtx) of
        true -> expect_current_lane_run_started_preparing(ExpStateCtx, AtmLaneRunSelector);
        false -> expect_lane_run_started_preparing_in_advance(ExpStateCtx, AtmLaneRunSelector)
    end;

expect(ExpStateCtx, {lane_run, AtmLaneRunSelector, started_preparing_as_current_lane_run}) ->
    expect_current_lane_run_started_preparing(ExpStateCtx, AtmLaneRunSelector);

expect(ExpStateCtx, {lane_run, AtmLaneRunSelector, started_preparing_in_advance}) ->
    expect_lane_run_started_preparing_in_advance(ExpStateCtx, AtmLaneRunSelector);

expect(ExpStateCtx, {lane_run, AtmLaneRunSelector, created}) ->
    expect_lane_run_created(ExpStateCtx, AtmLaneRunSelector);

expect(ExpStateCtx, {lane_run, AtmLaneRunSelector, ExpStatus}) when
    ExpStatus =:= resuming;
    ExpStatus =:= enqueued;
    ExpStatus =:= active;
    ExpStatus =:= stopping;
    ExpStatus =:= interrupted;
    ExpStatus =:= paused;
    ExpStatus =:= finished;
    ExpStatus =:= crashed;
    ExpStatus =:= cancelled;
    ExpStatus =:= failed
->
    update_exp_lane_run_state(ExpStateCtx, AtmLaneRunSelector, #{
        <<"status">> => str_utils:to_binary(ExpStatus)
    });

expect(ExpStateCtx, {lane_run, AtmLaneRunSelector, run_num_set, RunNum}) ->
    update_exp_lane_run_state(ExpStateCtx, AtmLaneRunSelector, #{<<"runNumber">> => RunNum});

expect(ExpStateCtx, {lane_run, AtmLaneRunSelector, removed}) ->
    expect_lane_run_removed(ExpStateCtx, AtmLaneRunSelector);

expect(ExpStateCtx, {lane_runs, AtmLaneRunSelectors, rerunable}) ->
    expect(ExpStateCtx, {lane_runs, AtmLaneRunSelectors, rerunable, true});

expect(ExpStateCtx, {lane_runs, AtmLaneRunSelectors, rerunable, IsRerunable}) ->
    update_exp_lane_runs_state(ExpStateCtx, as_list(AtmLaneRunSelectors), #{
        <<"isRerunable">> => IsRerunable
    });

expect(ExpStateCtx, {lane_runs, AtmLaneRunSelectors, retriable}) ->
    expect(ExpStateCtx, {lane_runs, AtmLaneRunSelectors, retriable, true});

expect(ExpStateCtx, {lane_runs, AtmLaneRunSelectors, retriable, IsRetriabla}) ->
    update_exp_lane_runs_state(ExpStateCtx, as_list(AtmLaneRunSelectors), #{
        <<"isRetriable">> => IsRetriabla
    });

expect(ExpStateCtx, workflow_scheduled) ->
    update_exp_workflow_execution_state(ExpStateCtx, #{
        <<"status">> => <<"scheduled">>,
        <<"scheduleTime">> => build_timestamp_field_validator(get_timestamp_seconds(ExpStateCtx))
    });

expect(ExpStateCtx, workflow_resuming) ->
    update_exp_workflow_execution_state(ExpStateCtx, #{
        <<"status">> => <<"resuming">>,
        <<"scheduleTime">> => build_timestamp_field_validator(get_timestamp_seconds(ExpStateCtx))
    });

expect(ExpStateCtx, workflow_active) ->
    update_exp_workflow_execution_state(ExpStateCtx, #{
        <<"status">> => <<"active">>,
        <<"startTime">> => build_timestamp_field_validator(get_timestamp_seconds(ExpStateCtx))
    });

expect(ExpStateCtx, workflow_stopping) ->
    update_exp_workflow_execution_state(ExpStateCtx, fun
        (ExpAtmWorkflowExecutionState = #{<<"status">> := <<"active">>}) ->
            ExpAtmWorkflowExecutionState#{<<"status">> => <<"stopping">>};
        (ExpAtmWorkflowExecutionState) ->
            ExpAtmWorkflowExecutionState#{
                <<"status">> => <<"stopping">>,
                <<"startTime">> => build_timestamp_field_validator(get_timestamp_seconds(
                    ExpStateCtx
                ))
            }
    end);

expect(ExpStateCtx, workflow_paused) ->
    update_exp_workflow_execution_state(ExpStateCtx, #{
        <<"status">> => <<"paused">>,
        <<"suspendTime">> => build_timestamp_field_validator(get_timestamp_seconds(ExpStateCtx))
    });

expect(ExpStateCtx, workflow_interrupted) ->
    update_exp_workflow_execution_state(ExpStateCtx, #{
        <<"status">> => <<"interrupted">>,
        <<"suspendTime">> => build_timestamp_field_validator(get_timestamp_seconds(ExpStateCtx))
    });

expect(ExpStateCtx, workflow_finished) ->
    update_exp_workflow_execution_state(ExpStateCtx, #{
        <<"status">> => <<"finished">>,
        <<"finishTime">> => build_timestamp_field_validator(get_timestamp_seconds(ExpStateCtx))
    });

expect(ExpStateCtx, workflow_failed) ->
    update_exp_workflow_execution_state(ExpStateCtx, #{
        <<"status">> => <<"failed">>,
        <<"finishTime">> => build_timestamp_field_validator(get_timestamp_seconds(ExpStateCtx))
    });

expect(ExpStateCtx, workflow_cancelled) ->
    update_exp_workflow_execution_state(ExpStateCtx, #{
        <<"status">> => <<"cancelled">>,
        <<"finishTime">> => build_timestamp_field_validator(get_timestamp_seconds(ExpStateCtx))
    });

expect(ExpStateCtx, workflow_crashed) ->
    update_exp_workflow_execution_state(ExpStateCtx, #{
        <<"status">> => <<"crashed">>,
        <<"finishTime">> => build_timestamp_field_validator(get_timestamp_seconds(ExpStateCtx))
    });

expect(ExpStateCtx, Expectations) when is_list(Expectations) ->
    lists:foldl(fun(Expectation, ExpStateCtxAcc) ->
        expect(ExpStateCtxAcc, Expectation)
    end, ExpStateCtx, Expectations).


-spec assert_matches_with_backend(ctx()) -> boolean().
assert_matches_with_backend(ExpStateCtx) ->
    assert_matches_with_backend(ExpStateCtx, 0).


-spec assert_matches_with_backend(ctx(), non_neg_integer()) -> boolean().
assert_matches_with_backend(ExpStateCtx, 0) ->
    assert_matches_with_backend_internal(ExpStateCtx, fun ct:pal/2);

assert_matches_with_backend(ExpStateCtx, Retries) ->
    case assert_matches_with_backend_internal(ExpStateCtx, fun(_, _) -> ok end) of
        true ->
            true;
        false ->
            timer:sleep(timer:seconds(1)),
            assert_matches_with_backend(ExpStateCtx, Retries - 1)
    end.


-spec assert_deleted(ctx()) -> ok | no_return().
assert_deleted(ExpStateCtx) ->
    assert_workflow_related_docs_deleted(ExpStateCtx),
    assert_global_store_related_docs_deleted(ExpStateCtx),
    assert_task_related_docs_deleted(ExpStateCtx).


-spec get_workflow_status(ctx()) -> binary().
get_workflow_status(#exp_workflow_execution_state_ctx{
    exp_workflow_execution_state = ExpAtmWorkflowExecutionState
}) ->
    maps:get(<<"status">>, ExpAtmWorkflowExecutionState).


-spec set_current_lane_run(non_neg_integer(), non_neg_integer(), ctx()) -> ctx().
set_current_lane_run(AtmLaneIndex, AtmRunNum, ExpState) ->
    ExpState#exp_workflow_execution_state_ctx{
        current_lane_index = AtmLaneIndex,
        current_run_num = AtmRunNum
    }.


-spec get_task_selector(atm_task_execution:id(), ctx()) -> task_selector().
get_task_selector(AtmTaskExecutionId, #exp_workflow_execution_state_ctx{
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
}) ->
    #exp_task_execution_state_ctx{
        lane_run_selector = AtmLaneRunSelector,
        parallel_box_schema_id = AtmParallelBoxSchemaId,
        exp_state = #{<<"schemaId">> := AtmTaskSchemaId}
    } = maps:get(AtmTaskExecutionId, ExpAtmTaskExecutionsRegistry),

    {AtmLaneRunSelector, AtmParallelBoxSchemaId, AtmTaskSchemaId}.


-spec get_task_schema_id(atm_task_execution:id(), ctx()) -> automation:id().
get_task_schema_id(AtmTaskExecutionId, #exp_workflow_execution_state_ctx{
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
}) ->
    #exp_task_execution_state_ctx{exp_state = #{<<"schemaId">> := AtmTaskSchemaId}} = maps:get(
        AtmTaskExecutionId, ExpAtmTaskExecutionsRegistry
    ),
    AtmTaskSchemaId.


-spec get_task_id(task_selector(), ctx()) -> atm_task_execution:id().
get_task_id({AtmLaneRunSelector, AtmParallelBoxSchemaId, AtmTaskSchemaId}, ExpStateCtx) ->
    {ok, {_, #{<<"parallelBoxes">> := ExpParallelBoxExecutionStates}}} = locate_lane_run(
        AtmLaneRunSelector, ExpStateCtx
    ),
    {ok, #{<<"taskRegistry">> := AtmTaskRegistry}} = lists_utils:find(
        fun(#{<<"schemaId">> := SchemaId}) -> SchemaId == AtmParallelBoxSchemaId end,
        ExpParallelBoxExecutionStates
    ),
    maps:get(AtmTaskSchemaId, AtmTaskRegistry).


-spec get_task_stats(atm_task_execution:id(), ctx()) -> {integer(), integer(), integer()}.
get_task_stats(AtmTaskExecutionId, #exp_workflow_execution_state_ctx{
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
}) ->
    #exp_task_execution_state_ctx{exp_state = #{
        <<"itemsInProcessing">> := IIP,
        <<"itemsFailed">> := IF,
        <<"itemsProcessed">> := IP
    }} = maps:get(AtmTaskExecutionId, ExpAtmTaskExecutionsRegistry),

    {IIP, IF, IP}.


-spec adjust_abruptly_stopped_task_stats(task_execution_state()) -> task_execution_state().
adjust_abruptly_stopped_task_stats(ExpAtmTaskExecutionState = #{
    <<"itemsInProcessing">> := ItemsInProcessing,
    <<"itemsProcessed">> := ItemsProcessed,
    <<"itemsFailed">> := ItemsFailed
}) ->
    ExpAtmTaskExecutionState#{
        <<"itemsInProcessing">> => 0,
        <<"itemsProcessed">> => ItemsProcessed + ItemsInProcessing,
        <<"itemsFailed">> => ItemsFailed + ItemsInProcessing
    }.


-spec get_task_status(atm_task_execution:id(), ctx()) -> binary().
get_task_status(AtmTaskExecutionId, #exp_workflow_execution_state_ctx{
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
}) ->
    #exp_task_execution_state_ctx{exp_state = #{<<"status">> := ExpStatus}} = maps:get(
        AtmTaskExecutionId, ExpAtmTaskExecutionsRegistry
    ),
    ExpStatus.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_timestamp_seconds(ctx() | normal | frozen) -> time:seconds().
get_timestamp_seconds(#exp_workflow_execution_state_ctx{clock_status = ClockStatus}) ->
    get_timestamp_seconds(ClockStatus);
get_timestamp_seconds(frozen) ->
    time_test_utils:get_frozen_time_seconds();
get_timestamp_seconds(normal) ->
    global_clock:timestamp_seconds().


%% @private
as_list(Term) when is_list(Term) -> Term;
as_list(Term) -> [Term].


%% @private
-spec crash(Format :: string(), Args :: [term()]) -> no_return().
crash(Format, Args) ->
    crash(str_utils:format_bin(Format, Args)).


%% @private
-spec crash(string() | binary()) -> no_return().
crash(ErrorMsg) ->
    ct:pal(str_utils:to_binary(ErrorMsg)),
    ?assert(false).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Store registry must be fetched from model stored in op as it is not
%% possible to tell beforehand (and later assert) what ids will be generated
%% for store instances.
%% @end
%%--------------------------------------------------------------------
-spec get_store_registry(atm_workflow_execution:record(), [atm_store_schema:record()]) ->
    atm_workflow_execution:store_registry().
get_store_registry(
    #atm_workflow_execution{store_registry = AtmStoreRegistry},
    AtmStoreSchemas
) ->
    ExpStoreSchemaId = lists:sort(lists:map(fun(#atm_store_schema{id = AtmStoreSchemaId}) ->
        AtmStoreSchemaId
    end, AtmStoreSchemas)),

    case lists:sort(maps:keys(AtmStoreRegistry)) of
        ExpStoreSchemaId -> ok;
        _ -> crash("ERROR - store instances not generated for every store schema")
    end,

    AtmStoreRegistry.


%% @private
-spec expect_all_tasks_stopping_due_to(
    ctx(),
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run_stopping_reason()
) ->
    ctx().
expect_all_tasks_stopping_due_to(ExpStateCtx, AtmLaneRunSelector, Reason) ->
    Status = case Reason of
        pause -> <<"paused">>;
        interrupt -> <<"interrupted">>;
        cancel -> <<"cancelled">>
    end,
    ExpAtmTaskExecutionDiff = fun(ExpAtmTaskExecution = #exp_task_execution_state_ctx{
        exp_state = ExpState
    }) ->
        ExpAtmTaskExecution#exp_task_execution_state_ctx{exp_state = ExpState#{
            <<"status">> => case maps:get(<<"status">>, ExpState) of
                <<"pending">> -> Status;
                <<"active">> -> <<"stopping">>;
                <<"stopping">> -> <<"stopping">>;
                Status -> Status
            end
        }}
    end,
    expect_all_tasks_transitioned(ExpStateCtx, AtmLaneRunSelector, ExpAtmTaskExecutionDiff).


%% @private
-spec expect_all_tasks_transitioned(
    ctx(),
    atm_lane_execution:lane_run_selector(),
    fun((exp_task_execution_state_ctx()) -> exp_task_execution_state_ctx())
) ->
    ctx().
expect_all_tasks_transitioned(
    ExpStateCtx = #exp_workflow_execution_state_ctx{
        exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionStateCtxRegistry0
    },
    AtmLaneRunSelector,
    ExpAtmTaskExecutionDiff
) ->
    {ok, {_AtmLaneRunPath, ExpAtmLaneRunState}} = locate_lane_run(AtmLaneRunSelector, ExpStateCtx),

    ExpStateCtx#exp_workflow_execution_state_ctx{exp_task_execution_state_ctx_registry = lists:foldl(
        fun(#{<<"taskRegistry">> := AtmTaskRegistry}, OuterAcc) ->
            maps:fold(fun(_AtmTaskSchemaId, AtmTaskExecutionId, InnerAcc) ->
                maps:update_with(AtmTaskExecutionId, ExpAtmTaskExecutionDiff, InnerAcc)
            end, OuterAcc, AtmTaskRegistry)
        end,
        ExpAtmTaskExecutionStateCtxRegistry0,
        maps:get(<<"parallelBoxes">>, ExpAtmLaneRunState)
    )}.


%% @private
-spec expect_task_transitioned_to_active_status_if_was_in_pending_status(
    ctx(),
    atm_task_execution:id()
) ->
    ctx().
expect_task_transitioned_to_active_status_if_was_in_pending_status(ExpStateCtx, AtmTaskExecutionId) ->
    Diff = build_transition_to_status_if_in_status_diff(<<"pending">>, <<"active">>),
    update_task_execution_exp_state(ExpStateCtx, AtmTaskExecutionId, Diff).


%% @private
-spec expect_task_lane_run_transitioned_to_active_status_if_was_in_enqueued_status(
    ctx(),
    atm_task_execution:id()
) ->
    ctx().
expect_task_lane_run_transitioned_to_active_status_if_was_in_enqueued_status(
    ExpStateCtx = #exp_workflow_execution_state_ctx{
        exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
    },
    AtmTaskExecutionId
) ->
    #exp_task_execution_state_ctx{lane_run_selector = AtmLaneRunSelector} = maps:get(
        AtmTaskExecutionId, ExpAtmTaskExecutionsRegistry
    ),
    Diff = build_transition_to_status_if_in_status_diff(<<"enqueued">>, <<"active">>),
    update_exp_lane_run_state(ExpStateCtx, AtmLaneRunSelector, Diff).


%% @private
-spec build_transition_to_status_if_in_status_diff(binary(), binary()) ->
    fun((json_utils:json_map()) -> json_utils:json_map()).
build_transition_to_status_if_in_status_diff(RequiredStatus, NewStatus) ->
    fun
        (ExpState = #{<<"status">> := Status}) when Status =:= RequiredStatus ->
            ExpState#{<<"status">> => NewStatus};
        (ExpState) ->
            ExpState
    end.


%% @private
-spec update_task_execution_exp_state(
    ctx(),
    atm_task_execution:id(),
    json_utils:json_map() | fun((task_execution_state()) -> task_execution_state())
) ->
    ctx().
update_task_execution_exp_state(ExpStateCtx, AtmTaskExecutionId, Diff) when is_map(Diff) ->
    update_task_execution_exp_state(AtmTaskExecutionId, fun(ExpAtmTaskExecutionState) ->
        maps:merge(ExpAtmTaskExecutionState, Diff)
    end, ExpStateCtx);

update_task_execution_exp_state(
    ExpStateCtx = #exp_workflow_execution_state_ctx{
        exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
    },
    AtmTaskExecutionId,
    ExpStateDiff
) ->
    Diff = fun(ExpAtmTaskExecution = #exp_task_execution_state_ctx{exp_state = ExpState}) ->
        ExpAtmTaskExecution#exp_task_execution_state_ctx{exp_state = ExpStateDiff(ExpState)}
    end,
    ExpStateCtx#exp_workflow_execution_state_ctx{exp_task_execution_state_ctx_registry = maps:update_with(
        AtmTaskExecutionId, Diff, ExpAtmTaskExecutionsRegistry
    )}.


%% @private
-spec resolve_task_id(atm_task_execution:id() | task_selector(), ctx()) ->
    atm_task_execution:id().
resolve_task_id(AtmTaskExecutionId, _ExpStateCtx) when is_binary(AtmTaskExecutionId) ->
    AtmTaskExecutionId;
resolve_task_id(AtmTaskExecutionSelector, ExpStateCtx) ->
    get_task_id(AtmTaskExecutionSelector, ExpStateCtx).


%% @private
-spec expect_current_lane_run_started_preparing(ctx(), atm_lane_execution:lane_run_selector()) ->
    ctx().
expect_current_lane_run_started_preparing(ExpStateCtx0, AtmLaneRunSelector) ->
    ExpStateCtx1 = update_exp_lane_run_state(ExpStateCtx0, AtmLaneRunSelector, #{
        <<"status">> => <<"preparing">>}
    ),
    update_exp_workflow_execution_state(ExpStateCtx1, #{
        <<"status">> => <<"active">>,
        <<"startTime">> => build_timestamp_field_validator(get_timestamp_seconds(ExpStateCtx1))
    }).


%% @private
-spec expect_lane_run_started_preparing_in_advance(ctx(), atm_lane_execution:lane_run_selector()) ->
    ctx().
expect_lane_run_started_preparing_in_advance(
    ExpStateCtx = #exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState0
    },
    AtmLaneRunSelector = {AtmLaneSelector, _}
) ->
    {ok, ExpAtmWorkflowExecutionState1} = case locate_lane_run(AtmLaneRunSelector, ExpStateCtx) of
        {ok, {LaneRunPath, Run = #{<<"status">> := <<"scheduled">>}}} ->
            json_utils:insert(
                ExpAtmWorkflowExecutionState0,
                Run#{<<"status">> => <<"preparing">>},
                LaneRunPath
            );
        ?ERROR_NOT_FOUND ->
            AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpStateCtx),
            AtmLanePath = ?JSON_PATH("lanes.[~B].runs", [AtmLaneIndex - 1]),
            {ok, PrevRuns} = json_utils:query(ExpAtmWorkflowExecutionState0, AtmLanePath),

            json_utils:insert(
                ExpAtmWorkflowExecutionState0,
                [build_initial_regular_lane_run_exp_state(undefined, <<"preparing">>) | PrevRuns],
                AtmLanePath
            )
    end,
    ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState1
    }.


%% @private
-spec expect_lane_run_created(ctx(), atm_lane_execution:lane_run_selector()) ->
    ctx().
expect_lane_run_created(
    ExpStateCtx = #exp_workflow_execution_state_ctx{
        provider_selector = ProviderSelector,
        workflow_execution_id = AtmWorkflowExecutionId,
        exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionStateCtxRegistry0
    },
    AtmLaneRunSelector
) ->
    AtmLaneSchema = get_lane_schema(ExpStateCtx, AtmLaneRunSelector),
    {ok, AtmLaneRun} = atm_lane_execution:get_run(AtmLaneRunSelector, fetch_workflow_execution(
        ProviderSelector, AtmWorkflowExecutionId
    )),

    {ExpAtmParallelBoxExecutionStates, ExpAtmTaskExecutionStateCtxRegistry1} = lists:mapfoldl(
        fun({AtmParallelBoxSchema, AtmParallelBoxExecution}, OuterAcc) ->
            AtmParallelBoxSchemaId = AtmParallelBoxSchema#atm_parallel_box_schema.id,
            AtmTaskSchemas = AtmParallelBoxSchema#atm_parallel_box_schema.tasks,
            AtmTasksRegistry = get_task_registry(AtmParallelBoxExecution, AtmParallelBoxSchema),

            ExpAtmParallelBoxExecutionState = build_exp_initial_parallel_box_execution_state(
                AtmParallelBoxSchemaId,
                AtmTasksRegistry
            ),
            UpdatedOuterAcc = maps:fold(fun(AtmTaskSchemaId, AtmTaskExecutionId, InnerAcc) ->
                {ok, AtmTaskSchema} = lists_utils:find(fun(#atm_task_schema{id = Id}) ->
                    Id == AtmTaskSchemaId
                end, AtmTaskSchemas),

                #atm_task_execution{
                    system_audit_log_store_id = AtmTaskAuditLogStoreId,
                    time_series_store_id = AtmTaskTSStoreId
                } = fetch_task_execution(ProviderSelector, AtmTaskExecutionId),

                case is_binary(AtmTaskAuditLogStoreId) of
                    true -> ok;
                    false -> crash("ERROR - task (id: ~s) audit log not created", [AtmTaskExecutionId])
                end,
                case {AtmTaskSchema#atm_task_schema.time_series_store_config, is_binary(AtmTaskTSStoreId)} of
                    {undefined, false} -> ok;
                    {_, true} -> ok;
                    _ -> crash("ERROR - task (id: ~s) time series store not created", [AtmTaskExecutionId])
                end,

                InnerAcc#{AtmTaskExecutionId => #exp_task_execution_state_ctx{
                    lane_run_selector = AtmLaneRunSelector,
                    parallel_box_schema_id = AtmParallelBoxSchemaId,
                    exp_state = build_task_execution_initial_exp_state(
                        AtmWorkflowExecutionId, AtmTaskSchemaId, AtmTaskAuditLogStoreId, AtmTaskTSStoreId
                    )
                }}
            end, OuterAcc, AtmTasksRegistry),

            {ExpAtmParallelBoxExecutionState, UpdatedOuterAcc}
        end,
        ExpAtmTaskExecutionStateCtxRegistry0,
        lists:zip(
            AtmLaneSchema#atm_lane_schema.parallel_boxes,
            AtmLaneRun#atm_lane_execution_run.parallel_boxes
        )
    ),

    ExpAtmLaneRunStateDiff = #{
        <<"iteratedStoreId">> => AtmLaneRun#atm_lane_execution_run.iterated_store_id,
        <<"exceptionStoreId">> => AtmLaneRun#atm_lane_execution_run.exception_store_id,
        <<"parallelBoxes">> => ExpAtmParallelBoxExecutionStates
    },
    update_exp_lane_run_state(ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionStateCtxRegistry1
    }, AtmLaneRunSelector, ExpAtmLaneRunStateDiff).


%% @private
-spec get_lane_schema(ctx(), atm_lane_execution:lane_run_selector()) ->
    atm_lane_schema:record().
get_lane_schema(
    ExpStateCtx = #exp_workflow_execution_state_ctx{lane_schemas = AtmLaneSchemas},
    {AtmLaneSelector, _}
) ->
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpStateCtx),
    lists:nth(AtmLaneIndex, AtmLaneSchemas).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Task registry must be fetched from model stored in op as it is not
%% possible to tell beforehand (and later assert) what ids will be generated
%% for task executions.
%% @end
%%--------------------------------------------------------------------
-spec get_task_registry(atm_parallel_box_execution:record(), atm_parallel_box_schema:record()) ->
    task_registry().
get_task_registry(AtmParallelBoxExecution, #atm_parallel_box_schema{tasks = AtmTaskSchemas}) ->
    AtmTasksRegistry = maps:get(
        <<"taskRegistry">>,
        atm_parallel_box_execution:to_json(AtmParallelBoxExecution)
    ),

    % Assert task executions are generated for all task schemas
    ExpTaskSchemaIds = lists:sort(lists:map(
        fun(#atm_task_schema{id = AtmTaskSchemaId}) -> AtmTaskSchemaId end,
        AtmTaskSchemas
    )),
    case lists:sort(maps:keys(AtmTasksRegistry)) of
        ExpTaskSchemaIds -> ok;
        _ -> crash("ERROR - task executions not generated for every task schema")
    end,

    AtmTasksRegistry.


%% @private
-spec fetch_task_execution(oct_background:entity_selector(), atm_task_execution:id()) ->
    atm_task_execution:record().
fetch_task_execution(ProviderSelector, AtmTaskExecutionId) ->
    {ok, #document{value = AtmTaskExecution}} = ?rpc(ProviderSelector, atm_task_execution:get(
        AtmTaskExecutionId
    )),
    AtmTaskExecution.


%% @private
-spec expect_lane_run_removed(ctx(), atm_lane_execution:lane_run_selector()) ->
    ctx().
expect_lane_run_removed(
    ExpStateCtx = #exp_workflow_execution_state_ctx{
        current_run_num = CurrentRunNum,
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState = #{<<"lanes">> := AtmLaneExecutions},
        exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
    },
    {AtmLaneSelector, AtmRunSelector}
) ->
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpStateCtx),
    TargetRunNum = resolve_run_selector(AtmRunSelector, ExpStateCtx),

    AtmLaneExecutionWithRun = #{<<"runs">> := AtmLaneRuns} = lists:nth(AtmLaneIndex, AtmLaneExecutions),
    {#{<<"parallelBoxes">> := AtmParallelBoxes}, RestAtmLaneRuns} = take_lane_run(
        TargetRunNum, CurrentRunNum, AtmLaneRuns, []
    ),
    AtmLaneExecutionWithoutRun = AtmLaneExecutionWithRun#{<<"runs">> => RestAtmLaneRuns},

    AtmTaskExecutionIds = lists:foldl(fun(#{<<"taskRegistry">> := AtmTaskRegistry}, Acc) ->
        maps:values(AtmTaskRegistry) ++ Acc
    end, [], AtmParallelBoxes),

    ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState#{
            <<"lanes">> => lists_utils:replace_at(
                AtmLaneExecutionWithoutRun, AtmLaneIndex, AtmLaneExecutions
            )
        },
        exp_task_execution_state_ctx_registry = maps:without(
            AtmTaskExecutionIds, ExpAtmTaskExecutionsRegistry
        )
    }.


%% @private
-spec expect_lane_run_scheduled(ctx(), atm_lane_execution:lane_run_selector()) ->
    ctx().
expect_lane_run_scheduled(
    ExpStateCtx = #exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState0
    },
    {AtmLaneSelector, AtmRunSelector}
) ->
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpStateCtx),
    AtmRunNum = resolve_run_selector(AtmRunSelector, ExpStateCtx),

    AtmLanePath = ?JSON_PATH("lanes.[~B].runs", [AtmLaneIndex - 1]),
    {ok, PrevRuns} = json_utils:query(ExpAtmWorkflowExecutionState0, AtmLanePath),

    {ok, ExpAtmWorkflowExecutionState1} = json_utils:insert(
        ExpAtmWorkflowExecutionState0,
        [build_initial_regular_lane_run_exp_state(AtmRunNum, <<"scheduled">>) | PrevRuns],
        AtmLanePath
    ),
    ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState1
    }.


%% @private
-spec expect_lane_run_automatic_retry_scheduled(ctx(), atm_lane_execution:lane_run_selector()) ->
    ctx().
expect_lane_run_automatic_retry_scheduled(
    ExpStateCtx = #exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState0
    },
    {AtmLaneSelector, _}
) ->
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpStateCtx),
    AtmLanePath = ?JSON_PATH("lanes.[~B].runs", [AtmLaneIndex - 1]),

    {ok, PrevLaneRuns = [FailedLaneRun | _]} = json_utils:query(
        ExpAtmWorkflowExecutionState0, AtmLanePath
    ),
    NewLaneRun = build_initial_regular_lane_run_exp_state(
        maps:get(<<"runNumber">>, FailedLaneRun) + 1,
        <<"scheduled">>,
        <<"retry">>,
        maps:get(<<"runNumber">>, FailedLaneRun),
        maps:get(<<"exceptionStoreId">>, FailedLaneRun)
    ),
    {ok, ExpAtmWorkflowExecutionState1} = json_utils:insert(
        ExpAtmWorkflowExecutionState0,
        [NewLaneRun | PrevLaneRuns],
        AtmLanePath
    ),
    ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState1
    }.


%% @private
-spec expect_lane_run_manual_repeat_scheduled(
    ctx(),
    atm_workflow_execution:repeat_type(),
    atm_lane_execution:lane_run_selector(),
    non_neg_integer()
) ->
    ctx().
expect_lane_run_manual_repeat_scheduled(
    ExpStateCtx = #exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState0
    },
    RepeatType,
    AtmLaneRunSelector,
    NewRunNum
) ->
    {AtmLaneSelector, AtmRunSelector} = AtmLaneRunSelector,
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpStateCtx),
    AtmRunNum = resolve_run_selector(AtmRunSelector, ExpStateCtx),

    AtmLaneExecutionPath = ?JSON_PATH("lanes.[~B].runs", [AtmLaneIndex - 1]),
    {ok, PrevAtmLaneExecutionRuns} = json_utils:query(ExpAtmWorkflowExecutionState0, AtmLaneExecutionPath),
    {ok, {_, PrevAtmLaneRunState}} = locate_lane_run(AtmLaneRunSelector, ExpStateCtx),

    {RepeatTypeBin, IteratedStoreId} = case RepeatType of
        retry -> {<<"retry">>, maps:get(<<"exceptionStoreId">>, PrevAtmLaneRunState)};
        rerun -> {<<"rerun">>, maps:get(<<"iteratedStoreId">>, PrevAtmLaneRunState)}
    end,
    NewLaneRun = build_initial_regular_lane_run_exp_state(
        NewRunNum,
        <<"scheduled">>,
        RepeatTypeBin,
        AtmRunNum,
        IteratedStoreId
    ),
    {ok, ExpAtmWorkflowExecutionState1} = json_utils:insert(
        ExpAtmWorkflowExecutionState0,
        [NewLaneRun | PrevAtmLaneExecutionRuns],
        AtmLaneExecutionPath
    ),
    ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState1
    }.


%% @private
-spec update_exp_lane_runs_state(
    ctx(),
    [atm_lane_execution:lane_run_selector()],
    json_utils:json_map() | fun((lane_run_state()) -> lane_run_state())
) ->
    ctx().
update_exp_lane_runs_state(ExpStateCtx, AtmLaneRunSelectors, ExpAtmLaneRunStateDiff) ->
    lists:foldl(fun(AtmLaneRunSelector, ExpStateCtxAcc) ->
        update_exp_lane_run_state(ExpStateCtxAcc, AtmLaneRunSelector, ExpAtmLaneRunStateDiff)
    end, ExpStateCtx, AtmLaneRunSelectors).


%% @private
-spec update_exp_lane_run_state(
    ctx(),
    atm_lane_execution:lane_run_selector(),
    json_utils:json_map() | fun((lane_run_state()) -> lane_run_state())
) ->
    ctx().
update_exp_lane_run_state(ExpStateCtx, AtmLaneRunSelector, Diff) when is_map(Diff) ->
    update_exp_lane_run_state(ExpStateCtx, AtmLaneRunSelector, fun(ExpAtmLaneRunState) ->
        maps:merge(ExpAtmLaneRunState, Diff)
    end);

update_exp_lane_run_state(
    ExpStateCtx = #exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState0
    },
    AtmLaneRunSelector,
    Diff
) ->
    {ok, {AtmLaneRunPath, AtmLaneRun}} = locate_lane_run(AtmLaneRunSelector, ExpStateCtx),
    {ok, ExpAtmWorkflowExecutionState1} = json_utils:insert(
        ExpAtmWorkflowExecutionState0,
        Diff(AtmLaneRun),
        AtmLaneRunPath
    ),
    ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = ExpAtmWorkflowExecutionState1
    }.


%% @private
take_lane_run(_TargetRunNum, _CurrentRunNum, [], _) ->
    error;

take_lane_run(TargetRunNum, CurrentRunNum, [Run = #{<<"runNumber">> := null} | RestRuns], NewerRunsReversed) when
    CurrentRunNum =< TargetRunNum
    ->
    {Run, lists:reverse(NewerRunsReversed) ++ RestRuns};

take_lane_run(TargetRunNum, _CurrentRunNum, [Run = #{<<"runNumber">> := TargetRunNum} | RestRuns], NewerRunsReversed) ->
    {Run, lists:reverse(NewerRunsReversed) ++ RestRuns};

take_lane_run(TargetRunNum, CurrentRunNum, [Run | RestRuns], NewerRunsReversed) ->
    take_lane_run(TargetRunNum, CurrentRunNum, RestRuns, [Run | NewerRunsReversed]).


%% @private
-spec locate_lane_run(atm_lane_execution:lane_run_selector(), ctx()) ->
    {ok, {json_utils:query(), json_utils:json_map()}} | ?ERROR_NOT_FOUND.
locate_lane_run({AtmLaneSelector, AtmRunSelector}, ExpStateCtx = #exp_workflow_execution_state_ctx{
    current_run_num = CurrentRunNum,
    exp_workflow_execution_state = #{<<"lanes">> := AtmLaneExecutions}
}) ->
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpStateCtx),
    TargetRunNum = resolve_run_selector(AtmRunSelector, ExpStateCtx),

    SearchResult = lists_utils:foldl_while(fun
        (Run = #{<<"runNumber">> := null}, Acc) when CurrentRunNum =< TargetRunNum ->
            {halt, {ok, Acc + 1, Run}};
        (Run = #{<<"runNumber">> := RunNum}, Acc) when RunNum =:= TargetRunNum ->
            {halt, {ok, Acc + 1, Run}};
        (#{<<"runNumber">> := RunNum}, _) when RunNum < TargetRunNum ->
            {halt, ?ERROR_NOT_FOUND};
        (_, Acc) ->
            {cont, Acc + 1}
    end, 0, maps:get(<<"runs">>, lists:nth(AtmLaneIndex, AtmLaneExecutions))),

    case SearchResult of
        {ok, AtmRunIndex, AtmLaneRun} ->
            Path = ?JSON_PATH("lanes.[~B].runs.[~B]", [AtmLaneIndex - 1, AtmRunIndex - 1]),
            {ok, {Path, AtmLaneRun}};
        _ ->
            ?ERROR_NOT_FOUND
    end.


%% @private
-spec is_current_lane_run(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:record()
) ->
    boolean().
is_current_lane_run({AtmLaneSelector, AtmRunSelector}, ExpStateCtx = #exp_workflow_execution_state_ctx{
    current_lane_index = CurrentAtmLaneIndex,
    current_run_num = CurrentRunNum
}) ->
    CurrentAtmLaneIndex == resolve_lane_selector(AtmLaneSelector, ExpStateCtx) andalso
        CurrentRunNum == resolve_run_selector(AtmRunSelector, ExpStateCtx).


%% @private
-spec resolve_lane_selector(atm_lane_execution:selector(), ctx()) ->
    atm_lane_execution:index().
resolve_lane_selector(current, #exp_workflow_execution_state_ctx{current_lane_index = CurrentAtmLaneIndex}) ->
    CurrentAtmLaneIndex;
resolve_lane_selector(AtmLaneIndex, _) ->
    AtmLaneIndex.


%% @private
-spec resolve_run_selector(atm_lane_execution:run_selector(), ctx()) ->
    atm_lane_execution:run_num().
resolve_run_selector(current, #exp_workflow_execution_state_ctx{current_run_num = CurrentRunNum}) ->
    CurrentRunNum;
resolve_run_selector(RunNum, _) ->
    RunNum.


%% @private
-spec update_exp_workflow_execution_state(
    ctx(),
    json_utils:json_map() | fun((workflow_execution_state()) -> workflow_execution_state())
) ->
    ctx().
update_exp_workflow_execution_state(ExpStateCtx, Diff) when is_map(Diff) ->
    update_exp_workflow_execution_state(ExpStateCtx, fun(ExpAtmWorkflowExecutionState) ->
        maps:merge(ExpAtmWorkflowExecutionState, Diff)
    end);

update_exp_workflow_execution_state(ExpStateCtx = #exp_workflow_execution_state_ctx{
    exp_workflow_execution_state = ExpAtmWorkflowExecutionState
}, Diff) ->
    ExpStateCtx#exp_workflow_execution_state_ctx{
        exp_workflow_execution_state = Diff(ExpAtmWorkflowExecutionState)
    }.


%% @private
-spec fetch_workflow_execution(oct_background:entity_selector(), atm_workflow_execution:id()) ->
    atm_workflow_execution:record().
fetch_workflow_execution(ProviderSelector, AtmWorkflowExecutionId) ->
    {ok, #document{value = AtmWorkflowExecution}} = opw_test_rpc:call(
        ProviderSelector, atm_workflow_execution, get, [AtmWorkflowExecutionId]
    ),
    AtmWorkflowExecution.


%% @private
-spec build_timestamp_field_validator(non_neg_integer()) ->
    fun((non_neg_integer()) -> boolean()).
build_timestamp_field_validator(ApproxTime) ->
    fun(RecordedTime) -> abs(RecordedTime - ApproxTime) < 15 end.


%% @private
-spec build_initial_regular_lane_run_exp_state(
    undefined | atm_lane_execution:run_num(),
    binary()
) ->
    lane_run_state().
build_initial_regular_lane_run_exp_state(ExpRunNum, ExpInitialStatus) ->
    build_initial_regular_lane_run_exp_state(
        ExpRunNum, ExpInitialStatus, <<"regular">>, undefined, undefined
    ).


%% @private
-spec build_initial_regular_lane_run_exp_state(
    undefined | atm_lane_execution:run_num(),
    binary(),
    binary(),
    undefined | atm_lane_execution:run_num(),
    undefined | atm_store:id()
) ->
    lane_run_state().
build_initial_regular_lane_run_exp_state(
    ExpRunNum,
    ExpInitialStatus,
    ExpRunType,
    ExpOriginalRunNum,
    ExpIteratedStoreId
) ->
    #{
        <<"runNumber">> => utils:undefined_to_null(ExpRunNum),
        <<"originRunNumber">> => utils:undefined_to_null(ExpOriginalRunNum),
        <<"status">> => ExpInitialStatus,
        <<"iteratedStoreId">> => utils:undefined_to_null(ExpIteratedStoreId),
        <<"exceptionStoreId">> => null,
        <<"parallelBoxes">> => [],
        <<"runType">> => ExpRunType,
        <<"isRetriable">> => false,
        <<"isRerunable">> => false
    }.


%% @private
-spec build_exp_initial_parallel_box_execution_state(automation:id(), task_registry()) ->
    parallel_box_execution_state().
build_exp_initial_parallel_box_execution_state(AtmParallelBoxSchemaId, AtmTasksRegistry) ->
    #{
        <<"schemaId">> => AtmParallelBoxSchemaId,
        <<"taskRegistry">> => AtmTasksRegistry
    }.


%% @private
-spec build_task_execution_initial_exp_state(
    atm_workflow_execution:id(),
    automation:id(),
    atm_store:id(),
    undefined | atm_store:id()
) ->
    task_execution_state().
build_task_execution_initial_exp_state(
    AtmWorkflowExecutionId,
    AtmTaskSchemaId,
    AtmTaskAuditLogStoreId,
    AtmTaskTSStoreId
) ->
    #{
        <<"atmWorkflowExecutionId">> => AtmWorkflowExecutionId,
        <<"schemaId">> => AtmTaskSchemaId,

        <<"systemAuditLogId">> => AtmTaskAuditLogStoreId,
        <<"timeSeriesStoreId">> => utils:undefined_to_null(AtmTaskTSStoreId),

        <<"status">> => <<"pending">>,

        <<"itemsInProcessing">> => 0,
        <<"itemsProcessed">> => 0,
        <<"itemsFailed">> => 0
    }.


%% @private
-spec assert_matches_with_backend_internal(ctx(), log_fun()) -> boolean().
assert_matches_with_backend_internal(ExpStateCtx, LogFun) ->
    assert_workflow_execution_expectations(ExpStateCtx, LogFun) and
        assert_task_execution_expectations(ExpStateCtx, LogFun).


%% @private
-spec assert_workflow_execution_expectations(ctx(), log_fun()) -> boolean().
assert_workflow_execution_expectations(ExpStateCtx = #exp_workflow_execution_state_ctx{
    provider_selector = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId,
    exp_workflow_execution_state = ExpAtmWorkflowExecutionState
}, LogFun) ->
    AtmWorkflowExecutionState = atm_workflow_execution_to_json(fetch_workflow_execution(
        ProviderSelector, AtmWorkflowExecutionId
    )),

    case catch assert_json_expectations(
        <<"atmWorkflowExecution">>, ExpAtmWorkflowExecutionState, AtmWorkflowExecutionState, LogFun
    ) of
        ok ->
            true andalso assert_workflow_execution_in_proper_links_tree(ExpStateCtx, LogFun);
        badmatch ->
            LogFun(
                "Error: mismatch between exp workflow execution state: ~n~p~n~nand model stored in op: ~n~p",
                [ExpAtmWorkflowExecutionState, AtmWorkflowExecutionState]
            ),
            false
    end.


%% @private
-spec assert_workflow_execution_in_proper_links_tree(ctx(), log_fun()) -> boolean().
assert_workflow_execution_in_proper_links_tree(#exp_workflow_execution_state_ctx{
    provider_selector = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId,
    exp_workflow_execution_state = #{<<"spaceId">> := SpaceId, <<"status">> := ExpStatus}
}, LogFun) ->
    Phase = atm_workflow_execution_status:status_to_phase(binary_to_atom(ExpStatus)),

    case lists:member(AtmWorkflowExecutionId, list_phase_links_tree(ProviderSelector, SpaceId, Phase)) of
        true ->
            true;
        false ->
            LogFun("Error: workflow execution (id: ~s) not present in expected links tree: ~p", [
                AtmWorkflowExecutionId, Phase
            ]),
            false
    end.


%% @private
-spec list_phase_links_tree(
    oct_background:entity_selector(),
    od_space:id(),
    atm_workflow_execution:phase()
) ->
    [atm_workflow_execution:id()].
list_phase_links_tree(ProviderSelector, SpaceId, Phase) ->
    TreeModule = case Phase of
        ?WAITING_PHASE -> atm_waiting_workflow_executions;
        ?ONGOING_PHASE -> atm_ongoing_workflow_executions;
        ?SUSPENDED_PHASE -> atm_suspended_workflow_executions;
        ?ENDED_PHASE -> atm_ended_workflow_executions
    end,
    ListOpts = #{offset => 0, limit => 100000000000000000},

    lists:map(
        fun({_Index, AtmWorkflowExecutionId}) -> AtmWorkflowExecutionId end,
        ?rpc(ProviderSelector, TreeModule:list(SpaceId, ListOpts))
    ).


%% @private
-spec assert_task_execution_expectations(ctx(), log_fun()) -> boolean().
assert_task_execution_expectations(#exp_workflow_execution_state_ctx{
    provider_selector = ProviderSelector,
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionStateCtxRegistry
}, LogFun) ->
    maps_utils:fold_while(fun(AtmTaskExecutionId, ExpAtmTaskExecution, true) ->
        AtmTaskExecution = fetch_task_execution(ProviderSelector, AtmTaskExecutionId),
        AtmTaskExecutionState = atm_task_execution_to_json(AtmTaskExecution),
        ExpAtmTaskExecutionState = ExpAtmTaskExecution#exp_task_execution_state_ctx.exp_state,

        case catch assert_json_expectations(
            <<"atmTaskExecution">>, ExpAtmTaskExecutionState, AtmTaskExecutionState, LogFun
        ) of
            ok ->
                {cont, true};
            badmatch ->
                LogFun(
                    "Error: mismatch between exp task execution state: ~n~p~n~nand model stored in op: ~n~p",
                    [ExpAtmTaskExecutionState, AtmTaskExecutionState]
                ),
                {halt, false}
        end
    end, true, ExpAtmTaskExecutionStateCtxRegistry).


%% @private
-spec assert_workflow_related_docs_deleted(ctx()) -> ok | no_return().
assert_workflow_related_docs_deleted(#exp_workflow_execution_state_ctx{
    provider_selector = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId,
    exp_workflow_execution_state = #{
        <<"spaceId">> := SpaceId,
        <<"systemAuditLogId">> := AtmAuditLogStoreId,
        <<"lanes">> := ExpAtmLaneExecutionStates,
        <<"status">> := ExpStatus
    }
}) ->
    try
        Phase = atm_workflow_execution_status:status_to_phase(binary_to_atom(ExpStatus)),
        ?assertNot(lists:member(
            AtmWorkflowExecutionId,
            list_phase_links_tree(ProviderSelector, SpaceId, Phase)
        )),
        ?assertNot(lists:member(
            AtmWorkflowExecutionId,
            ?rpc(ProviderSelector, atm_discarded_workflow_executions:list(<<>>, 100000000000000000))
        )),

        ?assertEqual(
            ?ERROR_NOT_FOUND,
            ?rpc(ProviderSelector, atm_workflow_execution:get(AtmWorkflowExecutionId, include_discarded))
        ),

        assert_store_related_docs_deleted(ProviderSelector, AtmAuditLogStoreId),

        lists:foreach(fun(#{<<"runs">> := AtmLaneRuns}) ->
            lists:foreach(fun(#{<<"exceptionStoreId">> := ExceptionStoreId}) ->
                assert_store_related_docs_deleted(ProviderSelector, ExceptionStoreId)
            end, AtmLaneRuns)
        end, ExpAtmLaneExecutionStates)
    catch Type:Reason ->
        ct:pal("ERROR: workflow (id: ~s) related docs are not deleted.", [AtmWorkflowExecutionId]),
        erlang:Type(Reason)
    end.


%% @private
-spec assert_global_store_related_docs_deleted(ctx()) -> ok | no_return().
assert_global_store_related_docs_deleted(#exp_workflow_execution_state_ctx{
    provider_selector = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId,
    exp_workflow_execution_state = #{<<"storeRegistry">> := AtmStoreRegistry}
}) ->
    try
        lists:foreach(fun(AtmStoreId) ->
            assert_store_related_docs_deleted(ProviderSelector, AtmStoreId)
        end, maps:values(AtmStoreRegistry))
    catch Type:Reason ->
        ct:pal("ERROR: global stores related docs are not deleted.", [AtmWorkflowExecutionId]),
        erlang:Type(Reason)
    end.


%% @private
-spec assert_task_related_docs_deleted(ctx()) -> ok | no_return().
assert_task_related_docs_deleted(#exp_workflow_execution_state_ctx{
    provider_selector = ProviderSelector,
    exp_task_execution_state_ctx_registry = ExpAtmTaskExecutionsRegistry
}) ->
    maps:foreach(fun(AtmTaskExecutionId, #exp_task_execution_state_ctx{exp_state = #{
        <<"systemAuditLogId">> := AtmTaskAuditLogStoreId,
        <<"timeSeriesStoreId">> := AtmTaskTSStoreId
    }}) ->
        try
            ?assertEqual(
                ?ERROR_NOT_FOUND,
                ?rpc(ProviderSelector, atm_task_execution:get(AtmTaskExecutionId))
            ),
            assert_store_related_docs_deleted(ProviderSelector, AtmTaskAuditLogStoreId),
            assert_store_related_docs_deleted(ProviderSelector, AtmTaskTSStoreId)
        catch Type:Reason ->
            ct:pal("ERROR: task (id: ~s) related docs are not deleted.", [AtmTaskExecutionId]),
            erlang:Type(Reason)
        end
    end, ExpAtmTaskExecutionsRegistry).


%% @private
-spec assert_store_related_docs_deleted(
    oct_background:entity_selector(),
    null | undefined | atm_store:id()
) ->
    ok | no_return().
assert_store_related_docs_deleted(_ProviderSelector, null) ->
    ok;
assert_store_related_docs_deleted(_ProviderSelector, undefined) ->
    ok;
assert_store_related_docs_deleted(ProviderSelector, AtmStoreId) ->
    ?assertEqual(?ERROR_NOT_FOUND, ?rpc(ProviderSelector, atm_store:get(AtmStoreId))).


%% @private
-spec atm_workflow_execution_to_json(atm_workflow_execution:record()) ->
    workflow_execution_state().
atm_workflow_execution_to_json(AtmWorkflowExecution = #atm_workflow_execution{
    space_id = SpaceId,

    store_registry = AtmStoreRegistry,
    system_audit_log_store_id = AtmAuditLogStoreId,

    lanes_count = AtmLanesCount,

    status = Status,

    schedule_time = ScheduleTime,
    start_time = StartTime,
    suspend_time = SuspendTime,
    finish_time = FinishTime
}) ->
    #{
        <<"spaceId">> => SpaceId,

        <<"storeRegistry">> => AtmStoreRegistry,
        <<"systemAuditLogId">> => AtmAuditLogStoreId,

        <<"lanes">> => lists:map(
            fun(AtmLaneIndex) -> atm_lane_execution:to_json(AtmLaneIndex, AtmWorkflowExecution) end,
            lists:seq(1, AtmLanesCount)
        ),

        <<"status">> => atom_to_binary(Status, utf8),

        <<"scheduleTime">> => ScheduleTime,
        <<"startTime">> => StartTime,
        <<"suspendTime">> => SuspendTime,
        <<"finishTime">> => FinishTime
    }.


%% @private
-spec atm_task_execution_to_json(atm_task_execution:record()) ->
    task_execution_state().
atm_task_execution_to_json(#atm_task_execution{
    workflow_execution_id = AtmWorkflowExecutionId,
    schema_id = AtmTaskSchemaId,

    status = AtmTaskExecutionStatus,

    system_audit_log_store_id = AtmTaskAuditLogStoreId,
    time_series_store_id = AtmTaskTSStoreId,

    items_in_processing = ItemsInProcessing,
    items_processed = ItemsProcessed,
    items_failed = ItemsFailed
}) ->
    #{
        <<"atmWorkflowExecutionId">> => AtmWorkflowExecutionId,
        <<"schemaId">> => AtmTaskSchemaId,

        <<"status">> => atom_to_binary(AtmTaskExecutionStatus, utf8),

        <<"systemAuditLogId">> => AtmTaskAuditLogStoreId,
        <<"timeSeriesStoreId">> => utils:undefined_to_null(AtmTaskTSStoreId),

        <<"itemsInProcessing">> => ItemsInProcessing,
        <<"itemsProcessed">> => ItemsProcessed,
        <<"itemsFailed">> => ItemsFailed
    }.


%% private
-spec assert_json_expectations(
    binary(),
    json_utils:json_term(),
    json_utils:json_term(),
    log_fun()
) ->
    ok | no_return().
assert_json_expectations(Path, Expected, Value, LogFun) when is_map(Expected), is_map(Value) ->
    ExpectedKeys = lists:sort(maps:keys(Expected)),
    ValueKeys = lists:sort(maps:keys(Value)),

    case ExpectedKeys == ValueKeys of
        true ->
            ok;
        false ->
            LogFun("Error: unmatching keys in objects at '~p'.~nExpected: ~p~nGot: ~p", [
                Path, Expected, Value
            ]),
            throw(badmatch)
    end,

    maps:foreach(fun(Key, ExpectedField) ->
        ValueField = maps:get(Key, Value),
        assert_json_expectations(<<Path/binary, ".", Key/binary>>, ExpectedField, ValueField, LogFun)
    end, Expected);

assert_json_expectations(Path, Expected, Value, LogFun) when is_list(Expected), is_list(Value) ->
    case length(Expected) == length(Value) of
        true ->
            lists:foreach(fun({Index, {ExpectedItem, ValueItem}}) ->
                assert_json_expectations(
                    str_utils:format_bin("~s.[~B]", [Path, Index - 1]),
                    ExpectedItem,
                    ValueItem,
                    LogFun
                )
            end, lists_utils:enumerate(lists:zip(Expected, Value)));
        false ->
            LogFun("Error: unmatching arrays at '~p'.~nExpected: ~p~nGot: ~p", [
                Path, Expected, Value
            ]),
            throw(badmatch)
    end;

assert_json_expectations(Path, Expected, Value, LogFun) when is_function(Expected, 1) ->
    case Expected(Value) of
        true ->
            ok;
        false ->
            LogFun("Error: predicate for '~p' failed.~nGot: ~p", [Path, Value]),
            throw(badmatch)
    end;

assert_json_expectations(Path, Expected, Value, LogFun) ->
    case Expected == Value of
        true ->
            ok;
        false ->
            LogFun("Error: unmatching items at '~p'.~nExpected: ~p~nGot: ~p", [
                Path, Expected, Value
            ]),
            throw(badmatch)
    end.
