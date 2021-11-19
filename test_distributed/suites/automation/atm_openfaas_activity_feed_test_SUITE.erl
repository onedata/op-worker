%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of OpenFaaS activity feed, covering the WebSocket server
%%% and the communication protocol.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_activity_feed_test_SUITE).
-author("Lukasz Opiola").

-include("http/gui_paths.hrl").
-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include_lib("cluster_worker/include/modules/datastore/infinite_log.hrl").


%% exported for CT
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    connectivity_test/1,
    lifecycle_test/1
]).

all() -> [
    connectivity_test,
    lifecycle_test
].

-define(CORRECT_SECRET, <<"884d387220ec1359e3199361dd45d328779efc9a">>).

% reports are sent asynchronously, without confirmation from the server and hence they
% may be consumed and visible in the activity registry after some delay
-define(ATTEMPTS, 15).

% @TODO VFS-8002 test if the activity registry is correctly created alongside a
% workflow execution and clean up during its deletion.

%%%===================================================================
%%% API functions
%%%===================================================================

connectivity_test(_Config) ->
    InvalidSecret = str_utils:rand_hex(10),

    opw_test_rpc:set_env(krakow, openfaas_container_status_feed_secret, undefined),
    ?assertMatch({error, unauthorized}, try_connect(undefined)),
    ?assertMatch({error, unauthorized}, try_connect(<<"not-a-base-64">>)),
    ?assertMatch({error, unauthorized}, try_connect(base64:encode(InvalidSecret))),
    ?assertMatch({error, unauthorized}, try_connect(base64:encode(?CORRECT_SECRET))),

    opw_test_rpc:set_env(krakow, openfaas_container_status_feed_secret, ?CORRECT_SECRET),
    ?assertMatch({error, unauthorized}, try_connect(undefined)),
    ?assertMatch({error, unauthorized}, try_connect(<<"not-a-base-64">>)),
    ?assertMatch({error, unauthorized}, try_connect(base64:encode(InvalidSecret))),
    ?assertMatch({ok, _}, try_connect(base64:encode(?CORRECT_SECRET))).


lifecycle_test(_Config) ->
    Client = connect(),
    FunctionName = str_utils:rand_hex(10),
    {ok, ActivityRegistryId} = create_activity_registry(FunctionName),

    PodAlpha = str_utils:rand_hex(10),
    PodBeta = str_utils:rand_hex(10),
    PodGamma = str_utils:rand_hex(10),

    verify_recorded_activity(ActivityRegistryId, []),

    FirstStatusReport = gen_status_report(FunctionName, PodAlpha),
    submit_status_reports(Client, [FirstStatusReport]),
    verify_recorded_activity(ActivityRegistryId, [FirstStatusReport]),

    SecondStatusReport = gen_status_report(FunctionName, PodAlpha),
    ThirdStatusReport = gen_status_report(FunctionName, PodAlpha),
    submit_status_reports(Client, [SecondStatusReport, ThirdStatusReport]),
    verify_recorded_activity(ActivityRegistryId, [FirstStatusReport, SecondStatusReport, ThirdStatusReport]),

    FollowingReports = lists_utils:generate(fun() ->
        gen_status_report(FunctionName, lists_utils:random_element([PodAlpha, PodBeta, PodGamma]))
    end, 200 + rand:uniform(500)),

    submit_status_reports(Client, FollowingReports),
    verify_recorded_activity(ActivityRegistryId, [
        FirstStatusReport, SecondStatusReport, ThirdStatusReport | FollowingReports
    ]),

    % delete the registry and verify if everything is cleaned up
    {ok, #atm_openfaas_function_activity_registry{
        pod_event_logs = PodEventLogs
    }} = ?assertMatch({ok, _}, get_activity_registry(ActivityRegistryId)),
    ?assertEqual(ok, delete_activity_registry(ActivityRegistryId)),
    ?assertEqual({error, not_found}, get_activity_registry(ActivityRegistryId)),
    maps:foreach(fun(_PodId, LogId) ->
        ?assertEqual({error, not_found}, opw_test_rpc:call(
            krakow, atm_openfaas_function_activity_registry, list_pod_events, [LogId, #{}]
        ))
    end, PodEventLogs).

%%%===================================================================
%%% Helper functions
%%%===================================================================

%% @private
-spec connect() -> test_websocket_client:client().
connect() ->
    {ok, Client} = try_connect(base64:encode(?CORRECT_SECRET)),
    Client.


%% @private
-spec try_connect(undefined | binary()) -> {ok, test_websocket_client:client()} | {error, term()}.
try_connect(SecretB64) ->
    Headers = case SecretB64 of
        undefined -> [];
        _ -> [{?HDR_AUTHORIZATION, <<"Basic ", SecretB64/binary>>}]
    end,
    test_websocket_client:start(krakow, ?OPENFAAS_ACTIVITY_FEED_WS_PATH, Headers).


%% @private
-spec create_activity_registry(atm_openfaas_task_executor:function_name()) ->
    {ok, atm_openfaas_function_activity_registry:id()} | {error, term()}.
create_activity_registry(FunctionName) ->
    opw_test_rpc:call(krakow, atm_openfaas_function_activity_registry, ensure_for_function, [FunctionName]).


%% @private
-spec get_activity_registry(atm_openfaas_function_activity_registry:id()) ->
    {ok, atm_openfaas_function_activity_registry:record()} | {error, term()}.
get_activity_registry(RegistryId) ->
    opw_test_rpc:call(krakow, atm_openfaas_function_activity_registry, get, [RegistryId]).


%% @private
-spec delete_activity_registry(atm_openfaas_function_activity_registry:id()) ->
    ok | {error, term()}.
delete_activity_registry(RegistryId) ->
    opw_test_rpc:call(krakow, atm_openfaas_function_activity_registry, delete, [RegistryId]).


%% @private
-spec submit_status_reports(test_websocket_client:client(), [atm_openfaas_function_pod_status_report:record()]) -> ok.
submit_status_reports(_Client, []) ->
    ok;
submit_status_reports(Client, StatusChangeReports) ->
    % randomly split the reports into batches to test batch handling
    {RandomReportsSublist, RemainingReports} = lists:split(
        rand:uniform(length(StatusChangeReports)),
        StatusChangeReports
    ),

    test_websocket_client:send(Client, jsonable_record:to_json(#atm_openfaas_function_activity_report{
        type = atm_openfaas_function_pod_status_report,
        batch = RandomReportsSublist
    }, atm_openfaas_function_activity_report)),

    submit_status_reports(Client, RemainingReports).


%% @private
-spec verify_recorded_activity(
    atm_openfaas_function_activity_registry:id(),
    [atm_openfaas_function_pod_status_report:record()]
) -> boolean().
verify_recorded_activity(RegistryId, SubmittedReports) ->
    ExpectedPodStatuses = lists:foldl(fun(#atm_openfaas_function_pod_status_report{
        timestamp = Timestamp,
        pod_id = PodId,
        current_pod_status = CurrentPodStatus
    }, Acc) ->
        case maps:find(PodId, Acc) of
            {ok, {ObservedAt, _}} when ObservedAt >= Timestamp ->
                Acc;
            _ ->
                Acc#{PodId => {Timestamp, CurrentPodStatus}}
        end
    end, #{}, SubmittedReports),
    ?assertMatch(
        {ok, #atm_openfaas_function_activity_registry{pod_statuses = ExpectedPodStatuses}},
        get_activity_registry(RegistryId),
        ?ATTEMPTS
    ),

    {ok, #atm_openfaas_function_activity_registry{
        pod_event_logs = PodEventLogs
    }} = get_activity_registry(RegistryId),

    ExpectedReversedPodEventLogs = lists:foldl(fun(#atm_openfaas_function_pod_status_report{
        timestamp = Timestamp,
        pod_id = PodId,
        pod_event = PodEvent
    }, LogsByPodAcc) ->
        maps:update_with(PodId, fun([{PreviousIndex, _, _} | _] = AccumulatedLogs) ->
            [{PreviousIndex + 1, Timestamp, PodEvent} | AccumulatedLogs]
        end, [{0, Timestamp, PodEvent}], LogsByPodAcc)
    end, #{}, SubmittedReports),

    maps:foreach(fun(PodId, LogId) ->
        ?assertEqual(
            {ok, {done, lists:reverse(maps:get(PodId, ExpectedReversedPodEventLogs))}},
            opw_test_rpc:call(krakow, atm_openfaas_function_activity_registry, list_pod_events, [
                LogId, #{direction => ?FORWARD, limit => 1000}
            ])
        )
    end, PodEventLogs).


%% @private
-spec gen_status_report(
    atm_openfaas_task_executor:function_name(),
    atm_openfaas_function_activity_registry:pod_id()
) ->
    atm_openfaas_function_pod_status_report:record().
gen_status_report(FunctionName, PodId) ->
    Status = lists_utils:random_element([
        <<"Killing">>, <<"FailedKillPod">>,
        <<"Scheduled">>,
        <<"Pulled">>,
        <<"SuccessfulCreate">>, <<"FailedCreate">>, <<"Created">>,
        <<"ScalingReplicaSet">>,
        <<"Completed">>
    ]),
    #atm_openfaas_function_pod_status_report{
        timestamp = global_clock:timestamp_millis() + 50000 - rand:uniform(100000),
        function_name = FunctionName,
        pod_id = PodId,
        current_pod_status = Status,
        pod_event = #{
            <<"type">> => <<"Normal">>,
            <<"reason">> => Status,
            <<"source">> => #{
                <<"component">> => <<"kubelet">>,
                <<"host">> => <<"node.example.com">>
            },
            <<"message">> => str_utils:rand_hex(10)
        }
    }.

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    opw_test_rpc:set_env(krakow, openfaas_container_status_feed_secret, ?CORRECT_SECRET),
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
