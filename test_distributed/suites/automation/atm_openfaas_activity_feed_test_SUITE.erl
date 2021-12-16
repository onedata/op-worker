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
    % connectivity_test cannot be run in parallel with other tests as it
    % causes the openfaas activity feed secret to change
    connectivity_test,
    lifecycle_test
].

-define(CORRECT_SECRET, <<"884d387220ec1359e3199361dd45d328779efc9a">>).

% reports are sent asynchronously, without confirmation from the server and hence they
% may be consumed and visible in the activity registry after some delay
-define(ATTEMPTS, 30).

% @TODO VFS-8002 test if the activity registry is correctly created alongside a
% workflow execution and clean up during its deletion.

%%%===================================================================
%%% API functions
%%%===================================================================

connectivity_test(_Config) ->
    InvalidSecret = str_utils:rand_hex(10),

    opw_test_rpc:set_env(krakow, openfaas_activity_feed_secret, undefined),
    ?assertMatch({error, unauthorized}, try_connect(undefined)),
    ?assertMatch({error, unauthorized}, try_connect(<<"not-a-base-64">>)),
    ?assertMatch({error, unauthorized}, try_connect(base64:encode(InvalidSecret))),
    ?assertMatch({error, unauthorized}, try_connect(base64:encode(?CORRECT_SECRET))),

    opw_test_rpc:set_env(krakow, openfaas_activity_feed_secret, ?CORRECT_SECRET),
    ?assertMatch({error, unauthorized}, try_connect(undefined)),
    ?assertMatch({error, unauthorized}, try_connect(<<"not-a-base-64">>)),
    ?assertMatch({error, unauthorized}, try_connect(base64:encode(InvalidSecret))),
    ?assertMatch({ok, _}, try_connect(base64:encode(?CORRECT_SECRET))),

    opw_test_rpc:set_env(krakow, openfaas_activity_feed_secret, binary_to_list(?CORRECT_SECRET)),
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
        pod_status_registry = PodStatusRegistry
    }} = ?assertMatch({ok, _}, get_activity_registry(ActivityRegistryId)),
    ?assertEqual(ok, delete_activity_registry(ActivityRegistryId)),
    ?assertEqual({error, not_found}, get_activity_registry(ActivityRegistryId)),
    atm_openfaas_function_pod_status_registry:foreach_summary(fun(_PodId, #atm_openfaas_function_pod_status_summary{
        event_log = PodEventLogId
    }) ->
        ?assertEqual({error, not_found}, opw_test_rpc:call(
            krakow, atm_openfaas_function_activity_registry, browse_pod_event_log, [PodEventLogId, #{}]
        ))
    end, PodStatusRegistry).

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
    ExpectedPodStatusesByPod = lists:foldl(fun(#atm_openfaas_function_pod_status_report{
        pod_id = PodId,
        containers_readiness = ContainersReadiness,
        event_timestamp = Timestamp,
        event_reason = Reason
    }, Acc) ->
        case maps:find(PodId, Acc) of
            {ok, {_, _, ObservedAt}} when ObservedAt >= Timestamp ->
                Acc;
            _ ->
                Acc#{PodId => {Reason, ContainersReadiness, Timestamp}}
        end
    end, #{}, SubmittedReports),

    maps:foreach(fun(PodId, {ExpStatus, ExpContainersReadiness, ExpStatusChangeTimestamp}) ->
        ?assertMatch(
            #atm_openfaas_function_pod_status_summary{
                current_status = ExpStatus,
                current_containers_readiness = ExpContainersReadiness,
                last_status_change_timestamp = ExpStatusChangeTimestamp
            },
            get_pod_status_summary(RegistryId, PodId),
            ?ATTEMPTS
        )
    end, ExpectedPodStatusesByPod),

    ExpectedReversedPodEventLogsByPod = lists:foldl(fun(#atm_openfaas_function_pod_status_report{
        pod_id = PodId,

        event_timestamp = EventTimestamp,
        event_type = EventType,
        event_reason = EventReason,
        event_message = EventMessage
    }, LogsByPodAcc) ->
        EventData = #{
            <<"timestamp">> => EventTimestamp,
            <<"content">> => #{
                <<"type">> => EventType,
                <<"reason">> => EventReason,
                <<"message">> => EventMessage
            }
        },
        maps:update_with(PodId, fun([#{<<"index">> := PreviousIndexBin} | _] = AccumulatedLogs) ->
            [EventData#{
                <<"index">> => integer_to_binary(binary_to_integer(PreviousIndexBin) + 1)
            } | AccumulatedLogs]
        end, [EventData#{<<"index">> => <<"0">>}], LogsByPodAcc)
    end, #{}, SubmittedReports),

    maps:foreach(fun(PodId, ExpectedReversedPodEventLogs) ->
        #atm_openfaas_function_pod_status_summary{
            event_log = PodEventLogId
        } = get_pod_status_summary(RegistryId, PodId),
        ?assertEqual(
            {ok, #{
                <<"logEntries">> => lists:reverse(ExpectedReversedPodEventLogs),
                <<"isLast">> => true
            }},
            opw_test_rpc:call(krakow, atm_openfaas_function_activity_registry, browse_pod_event_log, [
                PodEventLogId, #{direction => ?FORWARD, limit => 1000}
            ]),
            ?ATTEMPTS
        )
    end, ExpectedReversedPodEventLogsByPod).


%% @private
-spec get_pod_status_summary(
    atm_openfaas_function_activity_registry:id(),
    atm_openfaas_function_activity_registry:pod_id()
) ->
    atm_openfaas_function_pod_status_summary:record() | undefined.
get_pod_status_summary(RegistryId, PodId) ->
    case get_activity_registry(RegistryId) of
        {ok, #atm_openfaas_function_activity_registry{pod_status_registry = PodStatusRegistry}} ->
            try
                atm_openfaas_function_pod_status_registry:get_summary(PodId, PodStatusRegistry)
            catch _:_ ->
                undefined
            end;
        _ ->
            undefined
    end.


%% @private
-spec gen_status_report(
    atm_openfaas_task_executor:function_name(),
    atm_openfaas_function_activity_registry:pod_id()
) ->
    atm_openfaas_function_pod_status_report:record().
gen_status_report(FunctionName, PodId) ->
    RandContainersCount = rand:uniform(10),
    RandReadyContainersCount = rand:uniform(RandContainersCount),

    #atm_openfaas_function_pod_status_report{
        % Simulate the fact that reports may not arrive in order.
        % Still, the original timestamps from the reports should be returned
        % during listing (which is checked in verify_recorded_activity/2).
        function_name = FunctionName,
        pod_id = PodId,

        containers_readiness = str_utils:format_bin("~B/~B", [RandReadyContainersCount, RandContainersCount]),

        event_timestamp = global_clock:timestamp_millis() + 50000 - rand:uniform(100000),
        event_type = lists_utils:random_element([<<"Normal">>, <<"Error">>]),
        event_reason = lists_utils:random_element([
            <<"Killing">>, <<"FailedKillPod">>,
            <<"Scheduled">>,
            <<"Pulled">>,
            <<"SuccessfulCreate">>, <<"FailedCreate">>, <<"Created">>,
            <<"ScalingReplicaSet">>, <<"Completed">>
        ]),
        event_message = str_utils:rand_hex(10)
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
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    opw_test_rpc:set_env(krakow, openfaas_activity_feed_secret, ?CORRECT_SECRET),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
