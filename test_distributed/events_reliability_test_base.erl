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
    events_aggregation_test/1,
    events_flush_test/1,
    events_aggregation_stream_error_test/1,
    events_aggregation_stream_error_test2/1,
    events_aggregation_manager_error_test/1,
    events_aggregation_manager_error_test2/1,
    events_flush_stream_error_test/1,
    events_flush_handler_error_test/1
]).

-define(TEST_DATA, <<"TEST_DATA">>).
-define(TEST_DATA_SIZE, size(?TEST_DATA)).

-define(PROVIDER_ID(__Node), rpc:call(__Node, oneprovider, get_id, [])).

-define(SMALL_NUM_OF_ATTEMPTS, 5).
-define(MEDIUM_NUM_OF_ATTEMPTS, 20).
-define(ATTEMPTS_INTERVAL, 200).

%%%===================================================================
%%% Test functions
%%%===================================================================

events_aggregation_stream_error_test(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, event_stream, [passthrough]),

    test_utils:mock_expect(Workers, event_stream, send,
        fun(Stream, Message) ->
            case get(first_tested) of
                undefined ->
                    put(first_tested, true),
                    meck:passthrough([error, Message]);
                _ ->
                    meck:passthrough([Stream, Message])

            end
        end
    ),

    {ConnectionWorker, AssertionWorker} = get_nodes(Config),
    events_aggregation_test_base(Config, ConnectionWorker, AssertionWorker),
    test_utils:mock_unload(Workers, event_stream).

events_aggregation_stream_error_test2(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, event_stream, [passthrough]),

    test_utils:mock_expect(Workers, event_stream, handle_call, fun
        (#event{type = #file_read_event{}} = Request, From, State) ->
            case application:get_env(?APP_NAME, ?FUNCTION_NAME) of
                {ok, _} ->
                    meck:passthrough([Request, From, State]);
                _ ->
                    application:set_env(?APP_NAME, ?FUNCTION_NAME, true),
                    throw(test_error)

            end;
         (Request, From, State) ->
             meck:passthrough([Request, From, State])
    end),

    {ConnectionWorker, AssertionWorker} = get_nodes(Config),
    events_aggregation_test_base(Config, ConnectionWorker, AssertionWorker),
    test_utils:mock_unload(Workers, event_stream).

events_aggregation_manager_error_test(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, event_manager, [passthrough]),

    test_utils:mock_expect(Workers, event_manager, handle,
        fun(Stream, Message) ->
            case get(first_tested) of
                undefined ->
                    put(first_tested, true),
                    throw(error);
                _ ->
                    meck:passthrough([Stream, Message])

            end
        end
    ),

    {ConnectionWorker, AssertionWorker} = get_nodes(Config),
    events_aggregation_test_base(Config, ConnectionWorker, AssertionWorker),
    test_utils:mock_unload(Workers, event_manager).

events_aggregation_manager_error_test2(_Config) ->
    %TODO - test event manager test other way
    ok.
%%    Workers = ?config(op_worker_nodes, Config),
%%    test_utils:set_env(Workers, ?APP_NAME, fuse_session_ttl_seconds, 5),
%%    test_utils:mock_new(Workers, event_manager, [passthrough]),
%%
%%    test_utils:mock_expect(Workers, event_manager, handle_call, fun
%%        (#event{type = #file_read_event{}} = Request, From, State) ->
%%            case application:get_env(?APP_NAME, ?FUNCTION_NAME) of
%%                {ok, _} ->
%%                    meck:passthrough([Request, From, State]);
%%                _ ->
%%                    application:set_env(?APP_NAME, ?FUNCTION_NAME, true),
%%                    throw(test_error)
%%
%%            end;
%%        (Request, From, State) ->
%%            meck:passthrough([Request, From, State])
%%    end),
%%
%%    {ConnectionWorker, AssertionWorker} = get_nodes(Config),
%%    events_aggregation_failed_test_base(Config, ConnectionWorker, AssertionWorker),
%%    test_utils:mock_unload(Workers, event_manager),
%%    test_utils:set_env(Workers, ?APP_NAME, fuse_session_ttl_seconds, 300).

events_aggregation_test(Config) ->
    {ConnectionWorker, AssertionWorker} = get_nodes(Config),
    events_aggregation_test_base(Config, ConnectionWorker, AssertionWorker).


events_flush_stream_error_test(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, event_stream, [passthrough]),

    test_utils:mock_expect(Workers, event_stream, send,
        fun(Stream, Message) ->
            case get(first_tested) of
                undefined ->
                    put(first_tested, true),
                    meck:passthrough([error, Message]);
                _ ->
                    meck:passthrough([Stream, Message])

            end
        end
    ),

    {ConnectionWorker, AssertionWorker} = get_nodes(Config),
    events_flush_test_base(Config, ConnectionWorker, AssertionWorker, false, ok),
    test_utils:mock_unload(Workers, event_stream).

events_flush_handler_error_test(Config) ->
    {ConnectionWorker, AssertionWorker} = get_nodes(Config),
    events_flush_test_base(Config, ConnectionWorker, AssertionWorker, true, eagain).

events_flush_test(Config) ->
    {ConnectionWorker, AssertionWorker} = get_nodes(Config),
    events_flush_test_base(Config, ConnectionWorker, AssertionWorker, false, ok).

%%%===================================================================
%%% Test bases
%%%===================================================================


events_aggregation_test_base(Config, ConnectionWorker, AssertionWorker) ->
    UserId = <<"user1">>,
    MacaroonAuth = ?config({auth, UserId}, Config),
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(AssertionWorker)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    FilePath = list_to_binary(filename:join(["/", binary_to_list(SpaceName), binary_to_list(generator:gen_name())])),
    {ok, FileGuid} = lfm_proxy:create(AssertionWorker, SessionId, FilePath, 8#700),

    % Mock function calls to check
    mock_event_handler(AssertionWorker),
    mock_aggregate_read_events(AssertionWorker),
    mock_handle_file_read_events(AssertionWorker),

    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(
        ConnectionWorker, [{active, true}], crypto:strong_rand_bytes(10), MacaroonAuth
    ),

    % Send 2 event with some delay and assert correct aggregation
    Block1 = #file_block{offset = 0, size = 4},
    Block2 = #file_block{offset = 10, size = 4},
    fuse_utils:emit_file_read_event(Sock, 0, 1, FileGuid, [Block2]),
    timer:sleep(100),
    fuse_utils:emit_file_read_event(Sock, 0, 0, FileGuid, [Block1]),
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

    unmock_event_handler(AssertionWorker),

    ok = ssl:close(Sock).

events_aggregation_failed_test_base(Config, ConnectionWorker, AssertionWorker) ->
    UserId = <<"user1">>,
    MacaroonAuth = ?config({auth, UserId}, Config),
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(AssertionWorker)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    FilePath = list_to_binary(filename:join(["/", binary_to_list(SpaceName), binary_to_list(generator:gen_name())])),
    {ok, FileGuid} = lfm_proxy:create(AssertionWorker, SessionId, FilePath, 8#700),

    {ok, {Sock, TestSessionID}} = fuse_utils:connect_via_macaroon(
        ConnectionWorker, [{active, true}], crypto:strong_rand_bytes(10), MacaroonAuth
    ),

    ?assertMatch({ok, _}, rpc:call(ConnectionWorker, session, get, [TestSessionID])),
    Block1 = #file_block{offset = 0, size = 4},
    fuse_utils:emit_file_read_event(Sock, 0, 0, FileGuid, [Block1]),
    timer:sleep(10000),

    ?assertEqual({error, not_found}, rpc:call(ConnectionWorker, session, get, [TestSessionID])),
    ?assertEqual({error, closed}, ssl:send(Sock, <<"test">>)),
    ok = ssl:close(Sock).

events_flush_test_base(Config, ConnectionWorker, AssertionWorker, MockError, FlushCode) ->
    UserId = <<"user1">>,
    MacaroonAuth = ?config({auth, UserId}, Config),
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(AssertionWorker)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    FilePath = list_to_binary(filename:join(["/", binary_to_list(SpaceName), binary_to_list(generator:gen_name())])),
    {ok, FileGuid} = lfm_proxy:create(AssertionWorker, SessionId, FilePath, 8#700),

    % Mock function calls to check
    mock_event_handler(AssertionWorker),
    mock_aggregate_written_events(AssertionWorker),
    mock_handle_file_written_events(AssertionWorker, MockError),

    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(
        ConnectionWorker, [{active, true}], crypto:strong_rand_bytes(10), MacaroonAuth
    ),

    [#'Subscription'{id = SubscriptionId}] = fuse_utils:get_subscriptions(Sock, [file_written]),

    % Send 2 event with some delay
    Block1 = #file_block{offset = 0, size = 4},
    Block2 = #file_block{offset = 10, size = 4},
    fuse_utils:emit_file_written_event(Sock, 0, 1, FileGuid, [Block2]),
    timer:sleep(100),
    fuse_utils:emit_file_written_event(Sock, 0, 0, FileGuid, [Block1]),
    assert_aggregate_written_events_called(AssertionWorker, FileGuid, Block1, Block2),

    % Assert that file read events handler was not called before aggregation time expires
    timer:sleep(100),
    assert_handle_file_written_events_not_called(AssertionWorker),

    % Assert that after forcing flush handler is called before aggregation time expires
    fuse_utils:flush_events(Sock, ?PROVIDER_ID(AssertionWorker), SubscriptionId, FlushCode),
    assert_handle_file_written_events_called(AssertionWorker, [#file_written_event{
        counter = 2,
        file_guid = FileGuid,
        size = 8,
        blocks = [Block1, Block2]}
    ]),

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

get_nodes(Config) ->
    case ?config(op_worker_nodes, Config) of
        [WorkerP1] -> {WorkerP1, WorkerP1};
        [WorkerP2, WorkerP1] -> {WorkerP2, WorkerP1}
    end.

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

mock_handle_file_written_events(Workers, false = _MockError) ->
    test_utils:mock_expect(Workers, fslogic_event_handler, handle_file_written_events,
        fun(Evts, UserCtxMap) ->
            meck:passthrough([Evts, UserCtxMap])
        end
    );
mock_handle_file_written_events(Workers, _MockError) ->
    test_utils:mock_expect(Workers, fslogic_event_handler, handle_file_written_events,
        fun
            (Evts, #{notify := _NotifyFun} = UserCtxMap) ->
                case application:get_env(?APP_NAME, ?FUNCTION_NAME) of
                    {ok, _} ->
                        meck:passthrough([Evts, UserCtxMap]);
                    _ ->
                        application:set_env(?APP_NAME, ?FUNCTION_NAME, true),
                        throw(test_error)

                end;
            (Evts, UserCtxMap) ->
                meck:passthrough([Evts, UserCtxMap])
        end
    ).


assert_handle_file_written_events_not_called(Worker) ->
    ?assertMatch({badrpc, {'EXIT', {not_found, _}}},
        rpc:call(Worker, meck, capture, [
            1, fslogic_event_handler, handle_file_written_events, '_', 1
        ])
    ),
    ok.


assert_handle_file_written_events_called(Worker, ExpEvents) ->
    ?assertMatch(ExpEvents,
        rpc:call(Worker, meck, capture, [
            1, fslogic_event_handler, handle_file_written_events, '_', 1
        ]), ?SMALL_NUM_OF_ATTEMPTS, ?ATTEMPTS_INTERVAL
    ),
    ok.


mock_aggregate_read_events(Workers) ->
    test_utils:mock_expect(Workers, fslogic_event_handler, aggregate_file_read_events,
        fun(OldEvt, NewEvt) ->
            meck:passthrough([OldEvt, NewEvt])
        end
    ).


assert_aggregate_read_events_called(Worker, FileGuid, ExpBlock1, ExpBlock2) ->
    Mod = fslogic_event_handler,
    Fun = aggregate_file_read_events,

    ?assertMatch([{
        _, {Mod, Fun, [
            #file_read_event{file_guid = FileGuid, blocks = [ExpBlock1]},
            #file_read_event{file_guid = FileGuid, blocks = [ExpBlock2]}
        ]}, #file_read_event{file_guid = FileGuid, blocks = [ExpBlock1, ExpBlock2]}
    }], rpc:call(Worker, meck, history, [Mod]), ?MEDIUM_NUM_OF_ATTEMPTS, ?ATTEMPTS_INTERVAL).


mock_aggregate_written_events(Workers) ->
    test_utils:mock_expect(Workers, fslogic_event_handler, aggregate_file_written_events,
        fun(OldEvt, NewEvt) ->
            meck:passthrough([OldEvt, NewEvt])
        end
    ).


assert_aggregate_written_events_called(Worker, FileGuid, ExpBlock1, ExpBlock2) ->
    Mod = fslogic_event_handler,
    Fun = aggregate_file_written_events,

    ?assertMatch([{
        _, {Mod, Fun, [
            #file_written_event{file_guid = FileGuid, blocks = [ExpBlock1]},
            #file_written_event{file_guid = FileGuid, blocks = [ExpBlock2]}
        ]}, #file_written_event{file_guid = FileGuid, blocks = [ExpBlock1, ExpBlock2]}
    }], rpc:call(Worker, meck, history, [Mod]), ?MEDIUM_NUM_OF_ATTEMPTS, ?ATTEMPTS_INTERVAL).
