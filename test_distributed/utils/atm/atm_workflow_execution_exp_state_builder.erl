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
    init/3,

    report_lane_run_started_preparing/2,
    report_lane_run_failed/2,
    report_workflow_execution_aborting/1,
    report_workflow_execution_failed/1,

    assert_matches_with_backend/3
]).

% json object similar in structure to translations returned via API endpoints
% (has the same keys but as for values instead of concrete values it may contain
% validator functions - e.g. timestamp fields should check approx time rather
% than concrete value)
-type exp_state() :: json_utils:json_term().

-export_type([exp_state/0]).


-define(JSON_PATH(__QUERY_BIN), binary:split(__QUERY_BIN, <<".">>, [global])).
-define(NOW(), global_clock:timestamp_seconds()).


%%%===================================================================
%%% API
%%%===================================================================


-spec init(od_space:id(), time:seconds(), [atm_lane_schema:record()]) ->
    exp_state().
init(SpaceId, ApproxScheduleTime, AtmLaneSchemas) ->
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

        <<"scheduleTime">> => build_timestamp_field_validator(ApproxScheduleTime),
        <<"startTime">> => 0,
        <<"finishTime">> => 0
    }.


-spec report_lane_run_started_preparing(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
report_lane_run_started_preparing(AtmLaneRunSelector, ExpState0) ->
    {AtmLaneRunPath, AtmLaneRun} = locate_lane_run(AtmLaneRunSelector, ExpState0),

    {ok, ExpState1} = json_utils:insert(
        ExpState0,
        AtmLaneRun#{<<"status">> => <<"preparing">>},
        AtmLaneRunPath
    ),
    case maps:get(<<"status">>, ExpState1) of
        <<"scheduled">> ->
            ExpState1#{
                <<"status">> => <<"active">>,
                <<"startTime">> => build_timestamp_field_validator(?NOW())
            };
        _ ->
            ExpState1
    end.


-spec report_lane_run_failed(atm_lane_execution:lane_run_selector(), exp_state()) ->
    exp_state().
report_lane_run_failed(AtmLaneRunSelector, ExpState0) ->
    {AtmLaneRunPath, AtmLaneRun} = locate_lane_run(AtmLaneRunSelector, ExpState0),

    {ok, ExpState1} = json_utils:insert(
        ExpState0,
        AtmLaneRun#{<<"status">> => <<"failed">>, <<"isRerunable">> => true},
        AtmLaneRunPath
    ),
    ExpState1.


-spec report_workflow_execution_aborting(exp_state()) -> exp_state().
report_workflow_execution_aborting(ExpState) ->
    ExpState#{<<"status">> => <<"aborting">>}.


-spec report_workflow_execution_failed(exp_state()) -> exp_state().
report_workflow_execution_failed(ExpState) ->
    ExpState#{
        <<"status">> => <<"failed">>,
        <<"finishTime">> => build_timestamp_field_validator(?NOW())
    }.


-spec assert_matches_with_backend(
    oct_background:entity_selector(),
    atm_workflow_execution:id(),
    exp_state()
) ->
    boolean().
assert_matches_with_backend(ProviderSelector, AtmWorkflowExecutionId, ExpState) ->
    {ok, #document{value = AtmWorkflowExecution}} = opw_test_rpc:call(
        ProviderSelector, atm_workflow_execution, get, [AtmWorkflowExecutionId]
    ),
    AtmWorkflowExecutionJson = atm_workflow_execution_to_json(AtmWorkflowExecution),

    case catch assert_matching_jsons(
        <<"atmWorkflowExecution">>, ExpState, AtmWorkflowExecutionJson
    ) of
        ok ->
            true;
        badmatch ->
            ct:pal("Error: mismatch between exp state: ~n~p~n~nand models stored in op: ~n~p", [
                ExpState, AtmWorkflowExecutionJson
            ]),
            false
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
-spec build_timestamp_field_validator(non_neg_integer()) ->
    fun((non_neg_integer()) -> boolean()).
build_timestamp_field_validator(ApproxTime) ->
    fun(RecordedTime) -> abs(RecordedTime - ApproxTime) < 10 end.


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
-spec locate_lane_run(atm_lane_execution:lane_run_selector(), exp_state()) ->
    {json_utils:query(), json_utils:json_map()}.
locate_lane_run({AtmLaneIndex, AtmRunNum}, ExpState) ->
    AtmLaneRuns = maps:get(
        <<"runs">>,
        lists:nth(AtmLaneIndex, maps:get(<<"lanes">>, ExpState))
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
