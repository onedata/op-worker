%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of OpenFaaS activity feed, covering the WebSocket server
%%% and the communication protocol. Tests integration with k8s-event-monitor
%%% and openfaas-lambda-result-streamer by simulating the behaviour of these clients.
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
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include_lib("cluster_worker/include/modules/datastore/infinite_log.hrl").


%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    connectivity_test/1,

    function_pod_status_lifecycle_test/1,

    result_streamer_registration_deregistration_test/1,
    result_streamer_chunk_reporting_test/1,
    result_stream_conclusion_with_already_deregistered_streamers_test/1,
    result_stream_conclusion_with_still_registered_streamers_test/1,
    result_stream_conclusion_mixed_test/1,
    result_stream_conclusion_with_no_registered_streamers_test/1,
    result_stream_conclusion_timeout_test/1,
    result_streamer_reregistration_test/1,
    result_streamer_stale_report_ignoring_test/1,
    result_streamer_batch_handling_test/1,
    result_streamer_error_handling_test/1
]).

groups() -> [
    {sequential, [sequential], [
        % connectivity_test cannot be run in parallel with other tests as it
        % causes the openfaas activity feed secret to change
        connectivity_test
    ]},
    {parallel, [parallel], [
        function_pod_status_lifecycle_test,

        result_streamer_registration_deregistration_test,
        result_streamer_chunk_reporting_test,
        result_stream_conclusion_with_already_deregistered_streamers_test,
        result_stream_conclusion_with_still_registered_streamers_test,
        result_stream_conclusion_mixed_test,
        result_stream_conclusion_with_no_registered_streamers_test,
        result_stream_conclusion_timeout_test,
        result_streamer_reregistration_test,
        result_streamer_stale_report_ignoring_test,
        result_streamer_batch_handling_test,
        result_streamer_error_handling_test
    ]}
].

all() -> [
    {group, sequential},
    {group, parallel}
].


-define(CORRECT_SECRET, <<"884d387220ec1359e3199361dd45d328779efc9a">>).

% reports are sent asynchronously, without confirmation from the server and hence they
% may be consumed and visible in the activity registry after some delay
-define(ATTEMPTS, 30).
-define(await(Term), ?assert(Term, ?ATTEMPTS)).
-define(awaitLong(Term), ?assert(Term, 120)).

-define(STREAM_CHUNK_ALPHA, #{<<"a">> => [1, 2, 3], <<"b">> => [<<"a">>, <<"b">>, <<"c">>]}).
-define(STREAM_CHUNK_BETA, #{<<"c">> => [9, 8, 7], <<"d">> => [<<"x">>, <<"y">>, <<"z">>]}).

% @TODO VFS-8002 test if the activity registry is correctly created alongside a
% workflow execution and clean up during its deletion.

%%%===================================================================
%%% Tests
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


function_pod_status_lifecycle_test(_Config) ->
    Client = connect(),
    FunctionName = str_utils:rand_hex(10),
    {ok, ActivityRegistryId} = create_activity_registry(FunctionName),

    PodAlpha = str_utils:rand_hex(10),
    PodBeta = str_utils:rand_hex(10),
    PodGamma = str_utils:rand_hex(10),

    verify_recorded_pod_status_changes(ActivityRegistryId, []),

    FirstStatusReport = gen_pod_status_report(FunctionName, PodAlpha),
    submit_pod_status_reports(Client, [FirstStatusReport]),
    verify_recorded_pod_status_changes(ActivityRegistryId, [FirstStatusReport]),

    SecondStatusReport = gen_pod_status_report(FunctionName, PodAlpha),
    ThirdStatusReport = gen_pod_status_report(FunctionName, PodAlpha),
    submit_pod_status_reports(Client, [SecondStatusReport, ThirdStatusReport]),
    verify_recorded_pod_status_changes(ActivityRegistryId, [FirstStatusReport, SecondStatusReport, ThirdStatusReport]),

    FollowingReports = lists_utils:generate(fun() ->
        gen_pod_status_report(FunctionName, lists_utils:random_element([PodAlpha, PodBeta, PodGamma]))
    end, 200 + rand:uniform(500)),

    submit_pod_status_reports(Client, FollowingReports),
    verify_recorded_pod_status_changes(ActivityRegistryId, [
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


result_streamer_registration_deregistration_test(_Config) ->
    {ClientAlpha, ClientBeta, ClientGamma} = {connect(), connect(), connect()},
    {WorkflowExecutionId, TaskExecutionId} = {?RAND_STR(), ?RAND_STR()},
    {StreamerIdAlpha, StreamerIdBeta, StreamerIdGamma} = {?RAND_STR(), ?RAND_STR(), ?RAND_STR()},

    send_result_streamer_registration_report(ClientAlpha, WorkflowExecutionId, TaskExecutionId, StreamerIdAlpha),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [StreamerIdAlpha])),

    send_result_streamer_registration_report(ClientBeta, WorkflowExecutionId, TaskExecutionId, StreamerIdBeta),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [StreamerIdAlpha, StreamerIdBeta])),

    % registration should be idempotent
    send_result_streamer_registration_report(ClientBeta, WorkflowExecutionId, TaskExecutionId, StreamerIdBeta),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [StreamerIdAlpha, StreamerIdBeta])),

    send_result_streamer_deregistration_report(ClientBeta),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [StreamerIdAlpha])),

    send_result_streamer_registration_report(ClientGamma, WorkflowExecutionId, TaskExecutionId, StreamerIdGamma),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [StreamerIdAlpha, StreamerIdGamma])),
    send_result_streamer_deregistration_report(ClientAlpha),
    send_result_streamer_deregistration_report(ClientGamma),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [])),

    % deregistration should be idempotent
    send_result_streamer_deregistration_report(ClientAlpha),
    send_result_streamer_deregistration_report(ClientGamma),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [])).


result_streamer_chunk_reporting_test(_Config) ->
    Client = connect(),
    {WorkflowExecutionId, TaskExecutionId, ResultStreamerId} = {?RAND_STR(), ?RAND_STR(), ?RAND_STR()},

    send_result_streamer_registration_report(Client, WorkflowExecutionId, TaskExecutionId, ResultStreamerId),

    send_result_streamer_chunk_report(Client, ?STREAM_CHUNK_ALPHA),
    ?await(compare_streamed_reports(WorkflowExecutionId, TaskExecutionId, [
        {chunk, ?STREAM_CHUNK_ALPHA}
    ])),

    send_result_streamer_chunk_report(Client, ?STREAM_CHUNK_BETA),
    ?await(compare_streamed_reports(WorkflowExecutionId, TaskExecutionId, [
        {chunk, ?STREAM_CHUNK_ALPHA},
        {chunk, ?STREAM_CHUNK_BETA}
    ])),

    send_result_streamer_chunk_report(Client, ?STREAM_CHUNK_ALPHA),
    send_result_streamer_chunk_report(Client, ?STREAM_CHUNK_ALPHA),
    ?await(compare_streamed_reports(WorkflowExecutionId, TaskExecutionId, [
        {chunk, ?STREAM_CHUNK_ALPHA},
        {chunk, ?STREAM_CHUNK_BETA},
        {chunk, ?STREAM_CHUNK_ALPHA},
        {chunk, ?STREAM_CHUNK_ALPHA}
    ])).


result_stream_conclusion_with_already_deregistered_streamers_test(_Config) ->
    {ClientAlpha, ClientBeta, ClientGamma} = {connect(), connect(), connect()},
    {WorkflowExecutionId, TaskExecutionId} = {?RAND_STR(), ?RAND_STR()},
    {StreamerIdAlpha, StreamerIdBeta, StreamerIdGamma} = {?RAND_STR(), ?RAND_STR(), ?RAND_STR()},

    send_result_streamer_registration_report(ClientAlpha, WorkflowExecutionId, TaskExecutionId, StreamerIdAlpha),
    send_result_streamer_registration_report(ClientBeta, WorkflowExecutionId, TaskExecutionId, StreamerIdBeta),
    ?assert(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, not_concluded)),

    send_result_streamer_deregistration_report(ClientAlpha),
    ?assert(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, not_concluded)),

    send_result_streamer_registration_report(ClientGamma, WorkflowExecutionId, TaskExecutionId, StreamerIdGamma),

    send_result_streamer_deregistration_report(ClientGamma),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [StreamerIdBeta])),
    ?assert(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, not_concluded)),

    send_result_streamer_deregistration_report(ClientBeta),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [])),
    % the task data stream is not concluded automatically, even if all steamers are deregistered;
    % it must be triggered implicitly
    ?assert(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, not_concluded)),

    trigger_result_stream_conclusion(WorkflowExecutionId, TaskExecutionId),
    ?await(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, success)),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, {error, not_found})).


result_stream_conclusion_with_still_registered_streamers_test(_Config) ->
    {ClientAlpha, ClientBeta, ClientGamma} = {connect(), connect(), connect()},
    {WorkflowExecutionId, TaskExecutionId} = {?RAND_STR(), ?RAND_STR()},
    {StreamerIdAlpha, StreamerIdBeta, StreamerIdGamma} = {?RAND_STR(), ?RAND_STR(), ?RAND_STR()},

    send_result_streamer_registration_report(ClientAlpha, WorkflowExecutionId, TaskExecutionId, StreamerIdAlpha),
    send_result_streamer_registration_report(ClientBeta, WorkflowExecutionId, TaskExecutionId, StreamerIdBeta),
    send_result_streamer_registration_report(ClientGamma, WorkflowExecutionId, TaskExecutionId, StreamerIdGamma),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [StreamerIdAlpha, StreamerIdBeta, StreamerIdGamma])),

    trigger_result_stream_conclusion(WorkflowExecutionId, TaskExecutionId),
    ?await(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, success)),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, {error, not_found})).


result_stream_conclusion_mixed_test(_Config) ->
    {ClientAlpha, ClientBeta, ClientGamma} = {connect(), connect(), connect()},
    {WorkflowExecutionId, TaskExecutionId} = {?RAND_STR(), ?RAND_STR()},
    {StreamerIdAlpha, StreamerIdBeta, StreamerIdGamma} = {?RAND_STR(), ?RAND_STR(), ?RAND_STR()},

    send_result_streamer_registration_report(ClientAlpha, WorkflowExecutionId, TaskExecutionId, StreamerIdAlpha),
    send_result_streamer_registration_report(ClientBeta, WorkflowExecutionId, TaskExecutionId, StreamerIdBeta),
    send_result_streamer_registration_report(ClientGamma, WorkflowExecutionId, TaskExecutionId, StreamerIdGamma),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [StreamerIdAlpha, StreamerIdBeta, StreamerIdGamma])),

    % ClientBeta is deregistered before conclusion, while the other two clients
    % should be prompted with a finalization signal
    send_result_streamer_deregistration_report(ClientBeta),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [StreamerIdAlpha, StreamerIdGamma])),

    trigger_result_stream_conclusion(WorkflowExecutionId, TaskExecutionId),
    ?await(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, success)),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, {error, not_found})).


result_stream_conclusion_with_no_registered_streamers_test(_Config) ->
    {WorkflowExecutionId, TaskExecutionId} = {?RAND_STR(), ?RAND_STR()},
    trigger_result_stream_conclusion(WorkflowExecutionId, TaskExecutionId),
    ?await(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, {failure, ?ERROR_INTERNAL_SERVER_ERROR})).


result_stream_conclusion_timeout_test(_Config) ->
    {ClientAlpha, ClientBeta, ClientGamma} = {connect(), connect(), connect()},
    {WorkflowExecutionId, TaskExecutionId} = {?RAND_STR(), ?RAND_STR()},
    {StreamerIdAlpha, StreamerIdBeta, StreamerIdGamma} = {?RAND_STR(), ?RAND_STR(), ?RAND_STR()},

    send_result_streamer_registration_report(ClientAlpha, WorkflowExecutionId, TaskExecutionId, StreamerIdAlpha),
    send_result_streamer_registration_report(ClientBeta, WorkflowExecutionId, TaskExecutionId, StreamerIdBeta),
    send_result_streamer_registration_report(ClientGamma, WorkflowExecutionId, TaskExecutionId, StreamerIdGamma),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [StreamerIdAlpha, StreamerIdBeta, StreamerIdGamma])),

    simulate_deregistration_failure_for_client(ClientBeta),

    trigger_result_stream_conclusion(WorkflowExecutionId, TaskExecutionId),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [StreamerIdBeta])),
    ?awaitLong(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, {failure, ?ERROR_TIMEOUT})),
    % the registry should be cleaned even if there were conclusion errors
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, {error, not_found})),

    % sending a late deregistration report should be ignored
    send_result_streamer_deregistration_report(ClientBeta),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, {error, not_found})).


% Clients may lose connection and reconnects, in such case they register under the same
% result streamer id, but the connection ref is different. The handler should recognize such situations.
result_streamer_reregistration_test(_Config) ->
    {ClientAlpha, ClientBeta, ClientGamma} = {connect(), connect(), connect()},
    {WorkflowExecutionId, TaskExecutionId, ResultStreamerId} = {?RAND_STR(), ?RAND_STR(), ?RAND_STR()},

    send_result_streamer_registration_report(ClientAlpha, WorkflowExecutionId, TaskExecutionId, ResultStreamerId),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [ResultStreamerId])),

    send_result_streamer_registration_report(ClientBeta, WorkflowExecutionId, TaskExecutionId, ResultStreamerId),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [ResultStreamerId])),

    % registration should be idempotent
    send_result_streamer_registration_report(ClientBeta, WorkflowExecutionId, TaskExecutionId, ResultStreamerId),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [ResultStreamerId])),

    send_result_streamer_registration_report(ClientGamma, WorkflowExecutionId, TaskExecutionId, ResultStreamerId),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [ResultStreamerId])),

    % a deregistration report from a previous incarnation of the result streamer should be ignored
    send_result_streamer_deregistration_report(ClientAlpha),
    send_result_streamer_deregistration_report(ClientBeta),
    timer:sleep(5000),
    ?assert(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [ResultStreamerId])),

    % but deregistration from the current incarnation should work
    send_result_streamer_deregistration_report(ClientGamma),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [])),

    trigger_result_stream_conclusion(WorkflowExecutionId, TaskExecutionId),
    ?await(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, success)),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, {error, not_found})).


% reports that are late (after deregistration) or from a previous incarnation of the streamer are
% ignored - such situation can only happen when there has been an anomaly and the stream will
% anyway conclude with failure, so no special handling of this situation is required
result_streamer_stale_report_ignoring_test(_Config) ->
    {ClientAlpha, ClientBeta, ClientGamma} = {connect(), connect(), connect()},
    {WorkflowExecutionId, TaskExecutionId} = {?RAND_STR(), ?RAND_STR()},
    {ResultStreamerId, DeregisteredResultStreamerId} = {?RAND_STR(), ?RAND_STR()},

    send_result_streamer_registration_report(ClientGamma, WorkflowExecutionId, TaskExecutionId, DeregisteredResultStreamerId),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [DeregisteredResultStreamerId])),
    send_result_streamer_deregistration_report(ClientGamma),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [])),

    send_result_streamer_registration_report(ClientAlpha, WorkflowExecutionId, TaskExecutionId, ResultStreamerId),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [ResultStreamerId])),

    send_result_streamer_registration_report(ClientBeta, WorkflowExecutionId, TaskExecutionId, ResultStreamerId),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [ResultStreamerId])),

    % ClientAlpha has been replaced by ClientBeta during re-register
    % ClientGamma has been deregistered sometime in the past
    % in both cases, their reports should be ignored
    send_result_streamer_chunk_report(ClientAlpha, ?STREAM_CHUNK_ALPHA),
    send_result_streamer_chunk_report(ClientGamma, ?STREAM_CHUNK_BETA),
    timer:sleep(5000),
    ?assert(compare_streamed_reports(WorkflowExecutionId, TaskExecutionId, [])),

    % report from active ClientBeta should be accepted
    send_result_streamer_chunk_report(ClientBeta, ?STREAM_CHUNK_BETA),
    ?await(compare_streamed_reports(WorkflowExecutionId, TaskExecutionId, [{chunk, ?STREAM_CHUNK_BETA}])).


result_streamer_batch_handling_test(_Config) ->
    Client = connect(),
    {WorkflowExecutionId, TaskExecutionId, ResultStreamerId} = {?RAND_STR(), ?RAND_STR(), ?RAND_STR()},

    send_result_streamer_report(Client, [
        #atm_openfaas_result_streamer_registration_report{
            workflow_execution_id = WorkflowExecutionId,
            task_execution_id = TaskExecutionId,
            result_streamer_id = ResultStreamerId
        },
        #atm_openfaas_result_streamer_chunk_report{chunk = ?STREAM_CHUNK_ALPHA},
        #atm_openfaas_result_streamer_registration_report{
            workflow_execution_id = WorkflowExecutionId,
            task_execution_id = TaskExecutionId,
            result_streamer_id = ResultStreamerId
        }
    ]),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [ResultStreamerId])),
    ?assert(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, not_concluded)),

    send_result_streamer_report(Client, [
        #atm_openfaas_result_streamer_chunk_report{chunk = ?STREAM_CHUNK_ALPHA},
        #atm_openfaas_result_streamer_chunk_report{chunk = ?STREAM_CHUNK_BETA},
        #atm_openfaas_result_streamer_chunk_report{chunk = ?STREAM_CHUNK_ALPHA},
        #atm_openfaas_result_streamer_chunk_report{chunk = ?STREAM_CHUNK_BETA},
        #atm_openfaas_result_streamer_deregistration_report{}
    ]),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [])),
    ?await(compare_streamed_reports(WorkflowExecutionId, TaskExecutionId, [
        {chunk, ?STREAM_CHUNK_ALPHA},
        {chunk, ?STREAM_CHUNK_ALPHA},
        {chunk, ?STREAM_CHUNK_BETA},
        {chunk, ?STREAM_CHUNK_ALPHA},
        {chunk, ?STREAM_CHUNK_BETA}
    ])),
    ?await(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, not_concluded)),

    trigger_result_stream_conclusion(WorkflowExecutionId, TaskExecutionId),
    ?await(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, success)),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, {error, not_found})).


result_streamer_error_handling_test(_Config) ->
    {ClientAlpha, ClientBeta, ClientGamma} = {connect(), connect(), connect()},
    {WorkflowExecutionId, TaskExecutionId} = {?RAND_STR(), ?RAND_STR()},
    {StreamerIdAlpha, StreamerIdBeta, StreamerIdGamma} = {?RAND_STR(), ?RAND_STR(), ?RAND_STR()},

    send_result_streamer_registration_report(ClientAlpha, WorkflowExecutionId, TaskExecutionId, StreamerIdAlpha),
    send_result_streamer_registration_report(ClientBeta, WorkflowExecutionId, TaskExecutionId, StreamerIdBeta),
    send_result_streamer_registration_report(ClientGamma, WorkflowExecutionId, TaskExecutionId, StreamerIdGamma),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, [StreamerIdAlpha, StreamerIdBeta, StreamerIdGamma])),

    test_websocket_client:send(ClientAlpha, <<"bad-message">>),
    ?await(compare_streamed_reports(WorkflowExecutionId, TaskExecutionId, [
        ?ERROR_BAD_MESSAGE(<<"bad-message">>)
    ])),

    simulate_failure_of_next_report_processing(WorkflowExecutionId, TaskExecutionId),
    send_result_streamer_chunk_report(ClientGamma, ?STREAM_CHUNK_ALPHA),
    ?await(compare_streamed_reports(WorkflowExecutionId, TaskExecutionId, [
        ?ERROR_BAD_MESSAGE(<<"bad-message">>),
        ?ERROR_INTERNAL_SERVER_ERROR
    ])),

    test_websocket_client:send(ClientBeta, <<"1254321">>),
    ?await(compare_streamed_reports(WorkflowExecutionId, TaskExecutionId, [
        ?ERROR_BAD_MESSAGE(<<"bad-message">>),
        ?ERROR_INTERNAL_SERVER_ERROR,
        ?ERROR_BAD_MESSAGE(<<"1254321">>)
    ])),

    % errors are streamed, but should not cause the stream to conclude by itself;
    % it must be triggered implicitly
    ?await(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, not_concluded)),
    trigger_result_stream_conclusion(WorkflowExecutionId, TaskExecutionId),
    ?await(compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, success)),
    ?await(compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, {error, not_found})).

%%%===================================================================
%%% Helper functions - websocket connection
%%% (simulating a k8s-event-monitor or openfaas-lambda-result-streamer)
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
    test_websocket_client:start(krakow, ?OPENFAAS_ACTIVITY_FEED_WS_PATH, Headers, fun handle_push_message/2).


%% @private
-spec send_activity_report(
    test_websocket_client:client(),
    atm_openfaas_activity_report:type(),
    atm_openfaas_result_streamer_report:batch()
) ->
    ok.
send_activity_report(Client, Type, Batch) ->
    test_websocket_client:send(Client, json_utils:encode(jsonable_record:to_json(
        #atm_openfaas_activity_report{type = Type, batch = Batch},
        atm_openfaas_activity_report
    ))).


%% @private
%% @doc Callback handling push messages received by the test_websocket_client. Currently, only one
%% type of message is expected (finalization signal sent to result streamers).
%% This callback is common for connections simulating k8s-event-monitor and openfaas-lambda-result-streamer,
%% though the k8s-event-monitor does not expect any push messages.
-spec handle_push_message(test_websocket_client:client(), binary()) ->
    no_reply | {reply, binary()}.
handle_push_message(_ClientRef, <<"Bad request: ", _/binary>>) ->
    % this push message is received when a bad request is performed (result_streamer_error_handling_test)
    no_reply;
handle_push_message(_ClientRef, <<"Internal server error while processing the request">>) ->
    % this push message is received when an error occurs during report processing (result_streamer_error_handling_test)
    no_reply;
handle_push_message(ClientRef, Payload) ->
    #atm_openfaas_result_streamer_finalization_signal{} = jsonable_record:from_json(
        json_utils:decode(Payload),
        atm_openfaas_result_streamer_finalization_signal
    ),
    handle_finalization_signal(ClientRef).

%%%===================================================================
%%% Helper functions - function pod status
%%%===================================================================

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


-spec submit_pod_status_reports(test_websocket_client:client(), [atm_openfaas_function_pod_status_report:record()]) -> ok.
submit_pod_status_reports(_Client, []) ->
    ok;
submit_pod_status_reports(Client, StatusChangeReports) ->
    % randomly split the reports into batches to test batch handling
    {RandomReportsSublist, RemainingReports} = lists:split(
        rand:uniform(length(StatusChangeReports)),
        StatusChangeReports
    ),
    send_activity_report(Client, atm_openfaas_function_pod_status_report, RandomReportsSublist),
    submit_pod_status_reports(Client, RemainingReports).


%% @private
-spec verify_recorded_pod_status_changes(
    atm_openfaas_function_activity_registry:id(),
    [atm_openfaas_function_pod_status_report:record()]
) -> boolean().
verify_recorded_pod_status_changes(RegistryId, SubmittedReports) ->
    ExpectedPodStatusesByPod = lists:foldl(fun(#atm_openfaas_function_pod_status_report{
        pod_id = PodId,
        pod_status = PodStatus,
        containers_readiness = ContainersReadiness,
        event_timestamp = Timestamp
    }, Acc) ->
        case maps:find(PodId, Acc) of
            {ok, {_, _, ObservedAt}} when Timestamp < ObservedAt ->
                Acc;
            {ok, {PreviousPodStatus, _, ObservedAt}} ->
                NewLastStatusChangeTimestamp = case PodStatus of
                    PreviousPodStatus -> ObservedAt;
                    _ -> Timestamp
                end,
                Acc#{PodId => {PodStatus, ContainersReadiness, NewLastStatusChangeTimestamp}};
            error ->
                Acc#{PodId => {PodStatus, ContainersReadiness, Timestamp}}
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
-spec gen_pod_status_report(
    atm_openfaas_task_executor:function_name(),
    atm_openfaas_function_activity_registry:pod_id()
) ->
    atm_openfaas_function_pod_status_report:record().
gen_pod_status_report(FunctionName, PodId) ->
    RandContainersCount = rand:uniform(10),
    RandReadyContainersCount = rand:uniform(RandContainersCount),

    #atm_openfaas_function_pod_status_report{
        % Simulate the fact that reports may not arrive in order.
        % Still, the original timestamps from the reports should be returned
        % during listing (which is checked in verify_recorded_activity/2).
        function_name = FunctionName,
        pod_id = PodId,

        pod_status = lists_utils:random_element([
            <<"FailedKillPod">>,
            <<"Scheduled">>,
            <<"SuccessfulCreate">>, <<"FailedCreate">>, <<"Created">>,
            <<"ScalingReplicaSet">>, <<"Completed">>
        ]),
        containers_readiness = str_utils:format_bin("~B/~B", [RandReadyContainersCount, RandContainersCount]),

        event_timestamp = global_clock:timestamp_millis() + 50000 - rand:uniform(100000),
        event_type = lists_utils:random_element([<<"Normal">>, <<"Error">>]),
        event_reason = lists_utils:random_element([
            <<"Killing">>,
            <<"Scheduled">>,
            <<"Pulled">>,
            <<"SuccessfulCreate">>, <<"FailedCreate">>, <<"Created">>,
            <<"ScalingReplicaSet">>, <<"Completed">>
        ]),
        event_message = str_utils:rand_hex(10)
    }.

%%%===================================================================
%%% Helper functions - result streamer
%%%===================================================================

%% @private
-spec send_result_streamer_report(test_websocket_client:client(), [atm_openfaas_result_streamer_report:record()]) ->
    ok.
send_result_streamer_report(Client, Batch) ->
    send_activity_report(Client, atm_openfaas_result_streamer_report, Batch).


%% @private
-spec send_result_streamer_registration_report(
    test_websocket_client:client(),
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    atm_openfaas_result_streamer_registry:result_streamer_id()
) ->
    ok.
send_result_streamer_registration_report(Client, WorkflowExecutionId, TaskExecutionId, ResultStreamerId) ->
    send_result_streamer_report(Client, [
        #atm_openfaas_result_streamer_registration_report{
            workflow_execution_id = WorkflowExecutionId,
            task_execution_id = TaskExecutionId,
            result_streamer_id = ResultStreamerId
        }
    ]).


%% @private
-spec send_result_streamer_chunk_report(test_websocket_client:client(), atm_openfaas_result_streamer_chunk_report:chunk()) ->
    ok.
send_result_streamer_chunk_report(Client, Chunk) ->
    send_result_streamer_report(Client, [
        #atm_openfaas_result_streamer_chunk_report{chunk = Chunk}
    ]).


%% @private
-spec send_result_streamer_deregistration_report(test_websocket_client:client()) -> ok.
send_result_streamer_deregistration_report(Client) ->
    send_result_streamer_report(Client, [
        #atm_openfaas_result_streamer_deregistration_report{}
    ]).


%% @private
-spec trigger_result_stream_conclusion(atm_workflow_execution:id(), atm_task_execution:id()) ->
    ok.
trigger_result_stream_conclusion(WorkflowExecutionId, TaskExecutionId) ->
    opw_test_rpc:call(krakow, atm_openfaas_result_stream_handler, trigger_conclusion, [
        WorkflowExecutionId, TaskExecutionId
    ]).


%% @private
-spec compare_result_streamer_registry(
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    [atm_openfaas_result_streamer_registry:result_streamer_id()]
) ->
    boolean().
compare_result_streamer_registry(WorkflowExecutionId, TaskExecutionId, ExpectedRegisteredStreamerIdsOrError) ->
    try
        AllSteamerIds = opw_test_rpc:insecure_call(krakow, atm_openfaas_result_streamer_registry, get_all, [
            WorkflowExecutionId, TaskExecutionId
        ]),
        lists:sort(ExpectedRegisteredStreamerIdsOrError) =:= lists:sort(AllSteamerIds)
    catch
        error:{exception, {badmatch, ExpectedRegisteredStreamerIdsOrError}, _} ->
            true;
        _:_:_ ->
            false
    end.


%% @private
-spec compare_streamed_reports(
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    [workflow_engine:streamed_task_data()]
) ->
    boolean().
compare_streamed_reports(WorkflowExecutionId, TaskExecutionId, ExpectedReports) ->
    try
        CollectedReports = opw_test_rpc:insecure_call(krakow, node_cache, get, [
            {stream_task_data_memory, WorkflowExecutionId, TaskExecutionId}, []
        ]),
        true = ExpectedReports =:= CollectedReports
    catch _:_:_ ->
        false
    end.


%% @private
%% @doc this function is run on the op-worker node
-spec mocked_stream_task_data(
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    workflow_engine:streamed_task_data()
) ->
    ok.
mocked_stream_task_data(WorkflowExecutionId, TaskExecutionId, TaskDataReport) ->
    ReportMemoryKey = {stream_task_data_memory, WorkflowExecutionId, TaskExecutionId},
    critical_section:run(ReportMemoryKey, fun() ->
        case node_cache:get({should_simulate_failure_of_next_report_processing, WorkflowExecutionId, TaskExecutionId}, false) of
            true ->
                node_cache:put({should_simulate_failure_of_next_report_processing, WorkflowExecutionId, TaskExecutionId}, false),
                error(fail);
            false ->
                PreviousReports = node_cache:get(ReportMemoryKey, []),
                node_cache:put(ReportMemoryKey, PreviousReports ++ [TaskDataReport])
        end
    end).


%% @private
-spec simulate_failure_of_next_report_processing(atm_workflow_execution:id(), atm_task_execution:id()) ->
    ok.
simulate_failure_of_next_report_processing(WorkflowExecutionId, TaskExecutionId) ->
    opw_test_rpc:call(krakow, node_cache, put, [
        {should_simulate_failure_of_next_report_processing, WorkflowExecutionId, TaskExecutionId}, true
    ]).


%% @private
-spec compare_result_stream_conclusion_status(
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    not_concluded | workflow_engine:stream_closing_result()
) ->
    boolean().
compare_result_stream_conclusion_status(WorkflowExecutionId, TaskExecutionId, ExpectedStatus) ->
    ExpectedStatus =:= opw_test_rpc:insecure_call(krakow, node_cache, get, [
        {result_stream_conclusion_status, WorkflowExecutionId, TaskExecutionId}, not_concluded
    ]).


%% @private
%% @doc this function is run on the op-worker node
-spec mocked_report_task_data_streaming_concluded(
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    workflow_engine:stream_closing_result()
) ->
    ok.
mocked_report_task_data_streaming_concluded(WorkflowExecutionId, TaskExecutionId, StreamClosingResult) ->
    node_cache:put({result_stream_conclusion_status, WorkflowExecutionId, TaskExecutionId}, StreamClosingResult).


%% @private
-spec handle_finalization_signal(test_websocket_client:client_ref()) -> no_reply | {reply, binary()}.
handle_finalization_signal(ClientRef) ->
    case node_cache:get({should_simulate_deregistration_failure, ClientRef}, false) of
        true ->
            % lack of response to finalization signal will cause a timeout of stream closing
            no_reply;
        false ->
            timer:sleep(?RAND_INT(0, 500)),
            {reply, json_utils:encode(jsonable_record:to_json(
                #atm_openfaas_activity_report{
                    type = atm_openfaas_result_streamer_report,
                    batch = [#atm_openfaas_result_streamer_deregistration_report{}]
                },
                atm_openfaas_activity_report
            ))}
    end.


%% @private
-spec simulate_deregistration_failure_for_client(test_websocket_client:client_ref()) -> ok.
simulate_deregistration_failure_for_client(ClientRef) ->
    node_cache:put({should_simulate_deregistration_failure, ClientRef}, true).

%%%===================================================================
%%% Setup and teardown
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    opw_test_rpc:set_env(krakow, openfaas_activity_feed_secret, ?CORRECT_SECRET),
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Workers, [workflow_engine]),
    ok = test_utils:mock_expect(Workers, workflow_engine, stream_task_data, fun mocked_stream_task_data/3),
    ok = test_utils:mock_expect(Workers, workflow_engine, report_task_data_streaming_concluded, fun mocked_report_task_data_streaming_concluded/3),
    Config.


end_per_group(_Group, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, [workflow_engine]).


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
