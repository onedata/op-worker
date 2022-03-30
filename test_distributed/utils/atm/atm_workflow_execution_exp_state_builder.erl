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
%%% - expected state of atm_workflow_execution model
%%% - expected state of each atm_task_execution model (to be implemented)
%%% - expected state of each atm_store model (to be implemented)
%%%
%%% NOTE: exp state is created and stored as json object similar to responses
%%% send to clients via API endpoints. Model records definitions from op are
%%% not reused as they contain many irrelevant (to clients) fields considered
%%% as implementation details and omitted from said responses.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_exp_state_builder).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([
    init/5,

    report_lane_run_started_preparing/2,
    report_lane_run_created/2,
    report_lane_run_failed/2,
    report_workflow_execution_aborting/1,
    report_workflow_execution_failed/1,

    assert_matches_with_backend/1
]).

% json object similar in structure to translations returned via API endpoints
% (has the same keys but as for values instead of concrete values it may contain
% validator functions - e.g. timestamp fields should check approx time rather
% than concrete value)
-type exp_workflow_execution_state() :: json_utils:json_term().
-type exp_task_execution_state() :: json_utils:json_term().

-record(exp_state, {
    provider_selector :: oct_background:entity_selector(),
    lane_schemas :: [atm_lane_schema:record()],

    workflow_execution_id :: atm_workflow_execution:id(),
    current_lane_index :: atm_lane_execution:index(),
    current_run_num :: atm_lane_execution:run_num(),

    exp_workflow_execution_state :: exp_workflow_execution_state(),
    exp_task_execution_states_registry :: #{atm_task_execution:id() => exp_task_execution_state()}
}).
-type exp_state() :: #exp_state{}.

-export_type([exp_state/0]).


-define(JSON_PATH(__QUERY_BIN), binary:split(__QUERY_BIN, <<".">>, [global])).
-define(NOW(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(
    oct_background:entity_selector(),
    od_space:id(),
    atm_workflow_execution:id(),
    time:seconds(),
    [atm_lane_schema:record()]
) ->
    exp_state().
init(ProviderSelector, SpaceId, AtmWorkflowExecutionId, ApproxScheduleTime, AtmLaneSchemas) ->
    FirstAtmLaneSchema = hd(AtmLaneSchemas),

    FirstLane = #{
        <<"schemaId">> => FirstAtmLaneSchema#atm_lane_schema.id,
        <<"runs">> => [build_initial_regular_lane_run(1, <<"scheduled">>)]
    },
    LeftoverLanes = lists:map(fun(#atm_lane_schema{id = AtmLaneSchemaId}) ->
        #{
            <<"schemaId">> => AtmLaneSchemaId,
            <<"runs">> => []
        }
    end, tl(AtmLaneSchemas)),

    #exp_state{
        provider_selector = ProviderSelector,
        lane_schemas = AtmLaneSchemas,
        workflow_execution_id = AtmWorkflowExecutionId,
        current_lane_index = 1,
        current_run_num = 1,
        exp_workflow_execution_state = #{
            <<"spaceId">> => SpaceId,

            <<"lanes">> => [FirstLane | LeftoverLanes],

            <<"status">> => atom_to_binary(?SCHEDULED_STATUS, utf8),

            <<"scheduleTime">> => build_timestamp_field_validator(ApproxScheduleTime),
            <<"startTime">> => 0,
            <<"finishTime">> => 0
        },
        exp_task_execution_states_registry = #{}
    }.


-spec report_lane_run_started_preparing(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
report_lane_run_started_preparing(AtmLaneRunSelector, ExpState) ->
    case is_current_lane_run(AtmLaneRunSelector, ExpState) of
        true -> report_current_lane_run_started_preparing(AtmLaneRunSelector, ExpState);
        false -> report_lane_run_started_preparing_in_advance(AtmLaneRunSelector, ExpState)
    end.


-spec report_lane_run_failed(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
report_lane_run_failed(AtmLaneRunSelector, ExpState = #exp_state{
    exp_workflow_execution_state = ExpWorkflowExecutionState0
}) ->
    {ok, {AtmLaneRunPath, AtmLaneRun}} = locate_lane_run(AtmLaneRunSelector, ExpState),

    {ok, ExpWorkflowExecutionState1} = json_utils:insert(
        ExpWorkflowExecutionState0,
        AtmLaneRun#{<<"status">> => <<"failed">>, <<"isRerunable">> => true},
        AtmLaneRunPath
    ),
    ExpState#exp_state{exp_workflow_execution_state = ExpWorkflowExecutionState1}.


-spec report_workflow_execution_aborting(exp_state()) -> exp_state().
report_workflow_execution_aborting(ExpState = #exp_state{
    exp_workflow_execution_state = ExpWorkflowExecutionState
}) ->
    ExpState#exp_state{exp_workflow_execution_state = ExpWorkflowExecutionState#{
        <<"status">> => <<"aborting">>
    }}.


-spec report_workflow_execution_failed(exp_state()) -> exp_state().
report_workflow_execution_failed(ExpState = #exp_state{
    exp_workflow_execution_state = ExpWorkflowExecutionState
}) ->
    ExpState#exp_state{exp_workflow_execution_state = ExpWorkflowExecutionState#{
        <<"status">> => <<"failed">>,
        <<"finishTime">> => build_timestamp_field_validator(?NOW())
    }}.


-spec assert_matches_with_backend(exp_state()) -> boolean().
assert_matches_with_backend(ExpState) ->
    assert_workflow_execution_expectations(ExpState) and assert_task_execution_expectations(ExpState).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_timestamp_field_validator(non_neg_integer()) ->
    fun((non_neg_integer()) -> boolean()).
build_timestamp_field_validator(ApproxTime) ->
    fun(RecordedTime) -> abs(RecordedTime - ApproxTime) < 10 end.


%% @private
-spec build_initial_regular_lane_run(undefined | atm_lane_execution:run_num(), binary()) ->
    json_utils:json_map().
build_initial_regular_lane_run(RunNum, InitialStatus) ->
    #{
        <<"runNumber">> => utils:undefined_to_null(RunNum),
        <<"originRunNumber">> => null,
        <<"status">> => InitialStatus,
        <<"iteratedStoreId">> => null,
        <<"exceptionStoreId">> => null,
        <<"parallelBoxes">> => [],
        <<"runType">> => <<"regular">>,
        <<"isRetriable">> => false,
        <<"isRerunable">> => false
    }.


%% @private
-spec report_current_lane_run_started_preparing(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
report_current_lane_run_started_preparing(AtmLaneRunSelector, ExpState = #exp_state{
    exp_workflow_execution_state = ExpWorkflowExecutionState0
}) ->
    {ok, {AtmLaneRunPath, AtmLaneRun}} = locate_lane_run(AtmLaneRunSelector, ExpState),

    {ok, ExpWorkflowExecutionState1} = json_utils:insert(
        ExpWorkflowExecutionState0,
        AtmLaneRun#{<<"status">> => <<"preparing">>},
        AtmLaneRunPath
    ),
    ExpState#exp_state{exp_workflow_execution_state = case ExpWorkflowExecutionState1 of
        #{<<"status">> := <<"scheduled">>} ->
            ExpWorkflowExecutionState1#{
                <<"status">> => <<"active">>,
                <<"startTime">> => build_timestamp_field_validator(?NOW())
            };
        _ ->
            ExpWorkflowExecutionState1
    end}.


%% @private
-spec report_lane_run_started_preparing_in_advance(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
report_lane_run_started_preparing_in_advance(AtmLaneRunSelector, ExpState = #exp_state{
    exp_workflow_execution_state = ExpWorkflowExecutionState0
}) ->
    {Path, Run} = case locate_lane_run(AtmLaneRunSelector, ExpState) of
        {ok, {AtmLaneRunPath, AtmLaneRun}} ->
            {AtmLaneRunPath, AtmLaneRun#{<<"status">> => <<"preparing">>}};
        ?ERROR_NOT_FOUND ->
            AtmLaneIndex = resolve_lane_selector(element(1, AtmLaneRunSelector), ExpState),
            AtmLaneRunPath = str_utils:format_bin("lanes.[~B].runs.[0]", [AtmLaneIndex - 1]),
            {AtmLaneRunPath, build_initial_regular_lane_run(undefined, <<"preparing">>)}
    end,
    {ok, ExpWorkflowExecutionState1} = json_utils:insert(ExpWorkflowExecutionState0, Run, Path),
    ExpState#exp_state{exp_workflow_execution_state = ExpWorkflowExecutionState1}.


%% @private
-spec get_workflow_execution(exp_state()) -> atm_workflow_execution:id().
get_workflow_execution(#exp_state{
    provider_selector = ProviderSelector,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    {ok, #document{value = AtmWorkflowExecution}} = opw_test_rpc:call(
        ProviderSelector, atm_workflow_execution, get, [AtmWorkflowExecutionId]
    ),
    AtmWorkflowExecution.


%% @private
-spec get_lane_schema(atm_lane_execution:lane_run_selector(), exp_state()) ->
    atm_lane_schema:record().
get_lane_schema({AtmLaneSelector, _}, ExpState = #exp_state{lane_schemas = AtmLaneSchemas}) ->
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpState),
    lists:nth(AtmLaneIndex, AtmLaneSchemas).


%% @private
-spec locate_lane_run(atm_lane_execution:lane_run_selector(), exp_state()) ->
    {ok, {json_utils:query(), json_utils:json_map()}} | ?ERROR_NOT_FOUND.
locate_lane_run({AtmLaneSelector, AtmRunSelector}, ExpState = #exp_state{
    current_run_num = CurrentRunNum,
    exp_workflow_execution_state = #{<<"lanes">> := AtmLaneExecutions}
}) ->
    AtmLaneIndex = resolve_lane_selector(AtmLaneSelector, ExpState),
    TargetRunNum = resolve_run_selector(AtmRunSelector, ExpState),

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
            Path = ?JSON_PATH(str_utils:format_bin("lanes.[~B].runs.[~B]", [
                AtmLaneIndex - 1, AtmRunIndex - 1
            ])),
            {ok, {Path, AtmLaneRun}};
        _ ->
            ?ERROR_NOT_FOUND
    end.


%% @private
-spec is_current_lane_run(atm_lane_execution:lane_run_selector(), atm_workflow_execution:record()) ->
    boolean().
is_current_lane_run({AtmLaneSelector, AtmRunSelector}, ExpState = #exp_state{
    current_lane_index = CurrentAtmLaneIndex,
    current_run_num = CurrentRunNum
}) ->
    CurrentAtmLaneIndex == resolve_lane_selector(AtmLaneSelector, ExpState) andalso
        CurrentRunNum == resolve_run_selector(AtmRunSelector, ExpState).


%% @private
-spec resolve_lane_selector(atm_lane_execution:selector(), exp_state()) ->
    atm_lane_execution:index().
resolve_lane_selector(current, #exp_state{current_lane_index = CurrentAtmLaneIndex}) ->
    CurrentAtmLaneIndex;
resolve_lane_selector(AtmLaneIndex, _) ->
    AtmLaneIndex.


%% @private
-spec resolve_run_selector(atm_lane_execution:run_selector(), exp_state()) ->
    atm_lane_execution:run_num().
resolve_run_selector(current, #exp_state{current_run_num = CurrentRunNum}) ->
    CurrentRunNum;
resolve_run_selector(RunNum, _) ->
    RunNum.


%% @private
-spec assert_workflow_execution_expectations(exp_state()) -> boolean().
assert_workflow_execution_expectations(ExpState = #exp_state{
    exp_workflow_execution_state = ExpWorkflowExecutionJson
}) ->
    AtmWorkflowExecution = get_workflow_execution(ExpState),
    AtmWorkflowExecutionJson = atm_workflow_execution_to_json(AtmWorkflowExecution),

    case catch assert_json_expectations(
        <<"atmWorkflowExecution">>, ExpWorkflowExecutionJson, AtmWorkflowExecutionJson
    ) of
        ok ->
            true;
        badmatch ->
            ct:pal(
                "Error: mismatch between exp worklfow execution state: ~n~p~n~nand model stored in op: ~n~p",
                [ExpWorkflowExecutionJson, AtmWorkflowExecutionJson]
            ),
            false
    end.


%% @private
-spec assert_task_execution_expectations(exp_state()) -> boolean().
assert_task_execution_expectations(#exp_state{
    provider_selector = ProviderSelector,
    exp_task_execution_states_registry = ExpTaskExecutionStatesRegistry
}) ->
    maps_utils:fold_while(fun(AtmTaskExecutionId, ExpAtmTaskExecutionJson, true) ->
        {ok, #document{value = AtmTaskExecution}} = opw_test_rpc:call(
            ProviderSelector, atm_task_execution, get, [AtmTaskExecutionId]
        ),
        AtmTaskExecutionJson = atm_task_execution_to_json(AtmTaskExecution),

        case catch assert_json_expectations(
            <<"atmTaskExecution">>, ExpAtmTaskExecutionJson, AtmTaskExecutionJson
        ) of
            ok ->
                {cont, true};
            badmatch ->
                ct:pal(
                    "Error: mismatch between exp task execution state: ~n~p~n~nand model stored in op: ~n~p",
                    [ExpAtmTaskExecutionJson, AtmTaskExecutionJson]
                ),
                {halt, false}
        end
    end, true, ExpTaskExecutionStatesRegistry).


%% @private
-spec atm_workflow_execution_to_json(atm_workflow_execution:record()) ->
    json_utils:json_map().
atm_workflow_execution_to_json(#atm_workflow_execution{
    space_id = SpaceId,

    lanes = AtmLaneExecutions,
    lanes_count = AtmLanesCount,

    status = Status,

    schedule_time = ScheduleTime,
    start_time = StartTime,
    finish_time = FinishTime
}) ->
    #{
        <<"spaceId">> => SpaceId,

        <<"lanes">> => lists:map(
            fun(LaneIndex) -> atm_lane_execution:to_json(maps:get(LaneIndex, AtmLaneExecutions)) end,
            lists:seq(1, AtmLanesCount)
        ),

        <<"status">> => atom_to_binary(Status, utf8),

        <<"scheduleTime">> => ScheduleTime,
        <<"startTime">> => StartTime,
        <<"finishTime">> => FinishTime
    }.


%% @private
-spec atm_task_execution_to_json(atm_task_execution:record()) ->
    json_utils:json_map().
atm_task_execution_to_json(#atm_task_execution{
    workflow_execution_id = AtmWorkflowExecutionId,
    schema_id = AtmTaskSchemaId,

    status = AtmTaskExecutionStatus,

    items_in_processing = ItemsInProcessing,
    items_processed = ItemsProcessed,
    items_failed = ItemsFailed
}) ->
    #{
        <<"atmWorkflowExecutionId">> => AtmWorkflowExecutionId,
        <<"schemaId">> => AtmTaskSchemaId,

        <<"status">> => atom_to_binary(AtmTaskExecutionStatus, utf8),

        <<"itemsInProcessing">> => ItemsInProcessing,
        <<"itemsProcessed">> => ItemsProcessed,
        <<"itemsFailed">> => ItemsFailed
    }.


%% private
-spec assert_json_expectations(binary(), json_utils:json_term(), json_utils:json_term()) ->
    ok | no_return().
assert_json_expectations(Path, Expected, Value) when is_map(Expected), is_map(Value) ->
    ExpectedKeys = lists:sort(maps:keys(Expected)),
    ValueKeys = lists:sort(maps:keys(Value)),

    case ExpectedKeys == ValueKeys of
        true ->
            ok;
        false ->
            ct:pal("Error: unmatching keys in objects at '~p'.~nExpected: ~p~nGot: ~p", [
                Path, Expected, Value
            ]),
            throw(badmatch)
    end,

    maps:foreach(fun(Key, ExpectedField) ->
        ValueField = maps:get(Key, Value),
        assert_json_expectations(<<Path/binary, ".", Key/binary>>, ExpectedField, ValueField)
    end, Expected);

assert_json_expectations(Path, Expected, Value) when is_list(Expected), is_list(Value) ->
    case length(Expected) == length(Value) of
        true ->
            lists:foreach(fun({Index, {ExpectedItem, ValueItem}}) ->
                assert_json_expectations(
                    str_utils:format_bin("~s.[~B]", [Path, Index - 1]),
                    ExpectedItem,
                    ValueItem
                )
            end, lists_utils:enumerate(lists:zip(Expected, Value)));
        false ->
            ct:pal("Error: unmatching arrays at '~p'.~nExpected: ~p~nGot: ~p", [
                Path, Expected, Value
            ]),
            throw(badmatch)
    end;

assert_json_expectations(Path, Expected, Value) when is_function(Expected, 1) ->
    case Expected(Value) of
        true ->
            ok;
        false ->
            ct:pal("Error: predicate for '~p' failed.~nGot: ~p", [Path, Value]),
            throw(badmatch)
    end;

assert_json_expectations(Path, Expected, Value) ->
    case Expected == Value of
        true ->
            ok;
        false ->
            ct:pal("Error: unmatching items at '~p'.~nExpected: ~p~nGot: ~p", [
                Path, Expected, Value
            ]),
            throw(badmatch)
    end.
