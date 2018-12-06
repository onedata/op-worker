%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module holds base cases for tests of reliability of events.
%%% @end
%%%--------------------------------------------------------------------
-module(events_reliability_test_base).
-author("Bartosz Walkowicz").

-include("fuse_utils.hrl").
-include("global_definitions.hrl").
-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    init_per_suite/1, init_per_testcase/2,
    end_per_testcase/2, end_per_suite/1
]).

%%tests
-export([
    events_aggregation_test_base/3,
    events_flush_test_base/3
]).

-define(TEST_DATA, <<"TEST_DATA">>).
-define(TEST_DATA_SIZE, size(?TEST_DATA)).

-define(PROVIDER_ID(__Node), rpc:call(__Node, oneprovider, get_id, [])).

-define(SMALL_NUM_OF_ATTEMPTS, 5).
-define(MEDIUM_NUM_OF_ATTEMPTS, 20).
-define(ATTEMPTS_INTERVAL, 50).

%%%===================================================================
%%% Test functions
%%%===================================================================


events_aggregation_test_base(Config, ConnectionWorker, AssertionWorker) ->
    UserId = <<"user1">>,
    MacaroonAuth = ?config({auth, UserId}, Config),
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(AssertionWorker)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    FilePath = list_to_binary(filename:join(["/", binary_to_list(SpaceName), ?FUNCTION_NAME])),
    {ok, FileGuid} = lfm_proxy:create(AssertionWorker, SessionId, FilePath, 8#700),

    % Mock function calls to check
    mock_events_utils(AssertionWorker),
    mock_aggregate_read_events(AssertionWorker),

    mock_event_handler(AssertionWorker),
    mock_handle_file_read_events(AssertionWorker),

    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(
        ConnectionWorker, [{active, true}], crypto:strong_rand_bytes(10), MacaroonAuth
    ),

    % Send 2 event with some delay and assert correct aggregation
    Block1 = #file_block{offset = 0, size = 4},
    Block2 = #file_block{offset = 10, size = 4},
    fuse_utils:emit_file_read_event(Sock, 0, 1, FileGuid, [Block1]),
    timer:sleep(100),
    fuse_utils:emit_file_read_event(Sock, 0, 0, FileGuid, [Block2]),
    assert_aggregate_read_events_called(AssertionWorker, FileGuid, Block1, Block2),

    % Assert that file read events handler was not called before aggregation time expires
    timer:sleep(500),
    assert_handle_file_read_events_not_called(AssertionWorker),

    % Assert that file read events handler was called after aggregation time expires
    timer:sleep(500),
    assert_handle_file_read_events_called(AssertionWorker, [#file_read_event{
        counter = 2,
        file_guid = FileGuid,
        size = 8,
        blocks = [Block1, Block2]}
    ]),

    unmock_events_utils(AssertionWorker),
    unmock_event_handler(AssertionWorker),

    ok = ssl:close(Sock).


events_flush_test_base(Config, ConnectionWorker, AssertionWorker) ->
    UserId = <<"user1">>,
    MacaroonAuth = ?config({auth, UserId}, Config),
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(AssertionWorker)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    FilePath = list_to_binary(filename:join(["/", binary_to_list(SpaceName), ?FUNCTION_NAME])),
    {ok, FileGuid} = lfm_proxy:create(AssertionWorker, SessionId, FilePath, 8#700),

    % Mock function calls to check
    mock_events_utils(AssertionWorker),
    mock_aggregate_read_events(AssertionWorker),

    mock_event_handler(AssertionWorker),
    mock_handle_file_read_events(AssertionWorker),

    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(
        ConnectionWorker, [{active, true}], crypto:strong_rand_bytes(10), MacaroonAuth
    ),

    [#'Subscription'{id = SubscriptionId}] = fuse_utils:get_subscriptions(Sock, [file_read]),

    % Send 2 event with some delay
    Block1 = #file_block{offset = 0, size = 4},
    Block2 = #file_block{offset = 10, size = 4},
    fuse_utils:emit_file_read_event(Sock, 0, 1, FileGuid, [Block1]),
    timer:sleep(100),
    fuse_utils:emit_file_read_event(Sock, 0, 0, FileGuid, [Block2]),
    assert_aggregate_read_events_called(AssertionWorker, FileGuid, Block1, Block2),

    % Assert that file read events handler was not called before aggregation time expires
    timer:sleep(100),
    assert_handle_file_read_events_not_called(AssertionWorker),

    % Assert that after forcing flush handler is called before aggregation time expires
    fuse_utils:flush_events(Sock, ?PROVIDER_ID(AssertionWorker), SubscriptionId),
    assert_handle_file_read_events_called(AssertionWorker, [#file_read_event{
        counter = 2,
        file_guid = FileGuid,
        size = 8,
        blocks = [Block1, Block2]}
    ]),

    unmock_events_utils(AssertionWorker),
    unmock_event_handler(AssertionWorker),

    ok = ssl:close(Sock).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        application:start(ssl),
        hackney:start(),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig2, "env_desc.json"), NewConfig2)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, events_reliability_test_base]} | Config].


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    initializer:remove_pending_messages(),
    lfm_proxy:init(Config).


end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config),
    ok.


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


mock_events_utils(Workers) ->
    test_utils:mock_new(Workers, event_utils, [passthrough]).


unmock_events_utils(Workers) ->
    test_utils:mock_unload(Workers, event_utils).


mock_event_handler(Workers) ->
    test_utils:mock_new(Workers, fslogic_event_handler, [passthrough]).


unmock_event_handler(Workers) ->
    test_utils:mock_unload(Workers, fslogic_event_handler).


mock_handle_file_read_events(Workers) ->
    test_utils:mock_expect(Workers, fslogic_event_handler, handle_file_read_events,
        fun(Evts, UserCtxMap) ->
            meck:passthrough([Evts, UserCtxMap])
        end
    ).


assert_handle_file_read_events_not_called(Worker) ->
    ?assertMatch({badrpc, {'EXIT', {not_found, _}}},
        rpc:call(Worker, meck, capture, [
            1, fslogic_event_handler, handle_file_read_events, '_', 1
        ])
    ),
    ok.


assert_handle_file_read_events_called(Worker, ExpEvents) ->
    ?assertMatch(ExpEvents,
        rpc:call(Worker, meck, capture, [
            1, fslogic_event_handler, handle_file_read_events, '_', 1
        ]), ?SMALL_NUM_OF_ATTEMPTS, ?ATTEMPTS_INTERVAL
    ),
    ok.


mock_aggregate_read_events(Workers) ->
    test_utils:mock_expect(Workers, event_utils, aggregate_file_read_events,
        fun(OldEvt, NewEvt) ->
            meck:passthrough([OldEvt, NewEvt])
        end
    ).


assert_aggregate_read_events_called(Worker, FileGuid, ExpBlock1, ExpBlock2) ->
    Mod = event_utils,
    Fun = aggregate_file_read_events,

    ExpBlocks = lists:sort([ExpBlock1, ExpBlock2]),
    ?assertMatch([{
        _, {Mod, Fun, _}, #file_read_event{file_guid = FileGuid, blocks = ExpBlocks}
    }], rpc:call(Worker, meck, history, [Mod]), ?MEDIUM_NUM_OF_ATTEMPTS, ?ATTEMPTS_INTERVAL),

    ok.
