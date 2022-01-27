%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module allowing definition and modification of atm workflow  execution
%%% test view which is json map of fields expected to be stored in op.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_test_view).
-author("Bartosz Walkowicz").

-include("atm_workflow_exeuction_test_runner.hrl").
-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([
    build/3,

    report_lane_run_begin_preparing/2,
    report_lane_run_failed/2,
    report_workflow_execution_aborting/1,
    report_workflow_execution_failed/1,

    assert_match_with_backend/3
]).

-type view() :: json_utils:json_term().

-export_type([view/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(od_space:id(), time:seconds(), [atm_lane_schema:record()]) ->
    view().
build(SpaceId, ApproxScheduleTime, AtmLaneSchemas) ->
    FirstAtmLaneSchema = hd(AtmLaneSchemas),

    FirstLane = #{
        <<"schemaId">> => FirstAtmLaneSchema#atm_lane_schema.id,
        <<"runs">> => [build_initial_regular_lane_run(1)]
    },
    LeftoverLanes = lists:map(fun(#atm_lane_schema{id = AtmLaneSchemaId}) ->
        #{
            <<"schemaId">> => AtmLaneSchemaId,
            <<"runs">> => []
        }
    end, tl(AtmLaneSchemas)),

    #{
        <<"spaceId">> => SpaceId,

        <<"lanes">> => [FirstLane | LeftoverLanes],

        <<"status">> => atom_to_binary(?SCHEDULED_STATUS, utf8),

        <<"scheduleTime">> => fun(ScheduleTime) ->
            ScheduleTime - 10 < ApproxScheduleTime andalso ApproxScheduleTime < ScheduleTime + 10
        end,
        <<"startTime">> => 0,
        <<"finishTime">> => 0
    }.


-spec report_lane_run_begin_preparing(atm_lane_execution:lane_run_selector(), view()) ->
    view().
report_lane_run_begin_preparing(AtmLaneRunSelector, TestView0) ->
    {AtmLaneRunPath, AtmLaneRun} = locate_lane_run(AtmLaneRunSelector, TestView0),

    {ok, TestView1} = json_utils:insert(
        TestView0,
        AtmLaneRun#{<<"status">> => <<"preparing">>},
        AtmLaneRunPath
    ),
    case maps:get(<<"status">>, TestView1) of
        <<"scheduled">> ->
            ApproxStartTime = global_clock:timestamp_seconds(),

            TestView1#{
                <<"status">> => <<"active">>,
                <<"startTime">> => fun(ScheduleTime) ->
                    ScheduleTime - 10 < ApproxStartTime andalso ApproxStartTime < ScheduleTime + 10
                end
            };
        _ ->
            TestView1
    end.


-spec report_lane_run_failed(atm_lane_execution:lane_run_selector(), view()) ->
    view().
report_lane_run_failed(AtmLaneRunSelector, TestView0) ->
    {AtmLaneRunPath, AtmLaneRun} = locate_lane_run(AtmLaneRunSelector, TestView0),

    {ok, TestView1} = json_utils:insert(
        TestView0,
        AtmLaneRun#{<<"status">> => <<"failed">>, <<"isRerunable">> => true},
        AtmLaneRunPath
    ),
    TestView1.


-spec report_workflow_execution_aborting(view()) -> view().
report_workflow_execution_aborting(TestView) ->
    TestView#{<<"status">> => <<"aborting">>}.


-spec report_workflow_execution_failed(view()) -> view().
report_workflow_execution_failed(TestView) ->
    ApproxFinishTime = global_clock:timestamp_seconds(),

    TestView#{
        <<"status">> => <<"failed">>,
        <<"finishTime">> => fun(ScheduleTime) ->
            ScheduleTime - 10 < ApproxFinishTime andalso ApproxFinishTime < ScheduleTime + 10
        end
    }.


-spec assert_match_with_backend(
    oct_background:entity_selector(),
    atm_workflow_execution:id(),
    view()
) ->
    ok | no_return().
assert_match_with_backend(ProviderSelector, AtmWorkflowExecutionId, TestView) ->
    {ok, #document{value = AtmWorkflowExecution}} = opw_test_rpc:call(
        ProviderSelector, atm_workflow_execution, get, [AtmWorkflowExecutionId]
    ),
    AtmWorkflowExecutionJson = atm_workflow_execution_to_json(AtmWorkflowExecution),

    case catch assert_matching_jsons(<<"atmWorkflowExecution">>, TestView, AtmWorkflowExecutionJson) of
        ok ->
            ok;
        badmatch ->
            %% Bad match reason was already printed so it is enough to just end test
            ?assertMatch(success, failure)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% private
-spec assert_matching_jsons(binary(), json_utils:json_term(), json_utils:json_term()) ->
    ok | no_return().
assert_matching_jsons(Path, Expected, Value) when is_map(Expected), is_map(Value) ->
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
        assert_matching_jsons(str_utils:format_bin("~s.~s", [Path, Key]), ExpectedField, ValueField)
    end, Expected);

assert_matching_jsons(Path, Expected, Value) when is_list(Expected), is_list(Value) ->
    case length(Expected) == length(Value) of
        true ->
            lists:foreach(fun({Index, {ExpectedItem, ValueItem}}) ->
                assert_matching_jsons(
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

assert_matching_jsons(Path, Expected, Value) when is_function(Expected, 1) ->
    case Expected(Value) of
        true ->
            ok;
        false ->
            ct:pal("Error: predicate for '~p' failed.~nGot: ~p", [Path, Value]),
            throw(badmatch)
    end;

assert_matching_jsons(Path, Expected, Value) ->
    case Expected == Value of
        true ->
            ok;
        false ->
            ct:pal("Error: unmatching items at '~p'.~nExpected: ~p~nGot: ~p", [
                Path, Expected, Value
            ]),
            throw(badmatch)
    end.


%% @private
-spec build_initial_regular_lane_run(atm_lane_execution:run_num()) -> json_utils:json_map().
build_initial_regular_lane_run(RunNum) ->
    #{
        <<"runNumber">> => RunNum,
        <<"originRunNumber">> => null,
        <<"status">> => atom_to_binary(?SCHEDULED_STATUS, utf8),
        <<"iteratedStoreId">> => null,
        <<"exceptionStoreId">> => null,
        <<"parallelBoxes">> => [],
        <<"runType">> => <<"regular">>,
        <<"isRetriable">> => false,
        <<"isRerunable">> => false
    }.


%% @private
-spec locate_lane_run(atm_lane_execution:lane_run_selector(), view()) ->
    {json_utils:query(), json_utils:json_map()}.
locate_lane_run({AtmLaneIndex, AtmRunNum}, TestView) ->
    AtmLaneRuns = maps:get(
        <<"runs">>,
        lists:nth(AtmLaneIndex, maps:get(<<"lanes">>, TestView))
    ),
    {AtmRunIndex, AtmLaneRun} = hd(lists:dropwhile(
        fun({_, #{<<"runNumber">> := RunNum}}) -> RunNum /= AtmRunNum end,
        lists_utils:enumerate(AtmLaneRuns)
    )),
    Path = ?JSON_PATH(str_utils:format_bin("lanes.[~B].runs.[~B]", [
        AtmLaneIndex - 1, AtmRunIndex - 1
    ])),
    {Path, AtmLaneRun}.


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
