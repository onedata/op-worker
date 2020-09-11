%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests functionality of entire connection layer
%%% (modules/communication/connection/) such as sending messages or
%%% answering requests.
%%% @end
%%%--------------------------------------------------------------------
-module(connection_layer_test_SUITE).
-author("Bartosz Walkowicz").

-include("fuse_test_utils.hrl").
-include("global_definitions.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("test_utils/initializer.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, init_per_testcase/2,
    end_per_testcase/2, end_per_suite/1
]).

%% tests
-export([
    response_test/1,

    % tests fallback when sending immediate responses
    fallback_during_sending_response_because_of_connection_close_test/1,
    fallback_during_sending_response_because_of_connection_error_test/1,

    % tests fallback when sending fulfilled promise
    fulfill_promises_after_connection_close_test/1,
    fulfill_promises_after_connection_error_test/1,

    send_test/1,
    communicate_test/1,

    client_keepalive_test/1,
    heartbeats_test/1,
    async_request_manager_memory_management_test/1,

    socket_timeout_test/1,
    closing_last_connection_should_cancel_all_session_transfers_test/1
]).

-define(NORMAL_CASES, [
    response_test,

    fallback_during_sending_response_because_of_connection_close_test,
    fallback_during_sending_response_because_of_connection_error_test,

    fulfill_promises_after_connection_close_test,
    fulfill_promises_after_connection_error_test,

    send_test,
    communicate_test,

    client_keepalive_test,
    heartbeats_test,
    async_request_manager_memory_management_test,

%%    socket_timeout_test,
    closing_last_connection_should_cancel_all_session_transfers_test
]).

-define(PERFORMANCE_CASES, []).

all() -> ?ALL(?NORMAL_CASES, ?PERFORMANCE_CASES).

-record(file_handle, {
    handle :: helpers_nif:file_handle(),
    timeout :: timeout()
}).

-define(ATTEMPTS, 90).

%%%===================================================================
%%% Test functions
%%%===================================================================


% Without any errors response should be send via the same connection
% as request was send.
response_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    Nonce = crypto:strong_rand_bytes(10),
    {ok, {Sock1, SessId}} = fuse_test_utils:connect_via_token(Worker1, [{active, once}], Nonce),
    {ok, {_Sock2, SessId}} = fuse_test_utils:connect_via_token(Worker1, [{active, false}], Nonce),

    MsgId = <<"1">>,
    Ping = fuse_test_utils:generate_ping_message(MsgId),
    ssl:send(Sock1, Ping),

    ?assertMatch(#'ServerMessage'{
        message_id = MsgId,
        message_body = {pong, #'Pong'{}}
    }, fuse_test_utils:receive_server_message()),

    ok.


fallback_during_sending_response_because_of_connection_close_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    Nonce = crypto:strong_rand_bytes(10),
    % Create a couple of connections within the same session
    {ok, {Sock1, SessId}} = fuse_test_utils:connect_via_token(Worker1, [{active, true}], Nonce),
    {ok, {Sock2, SessId}} = fuse_test_utils:connect_via_token(Worker1, [{active, false}], Nonce),
    {ok, {Sock3, SessId}} = fuse_test_utils:connect_via_token(Worker1, [{active, false}], Nonce),
    {ok, {Sock4, SessId}} = fuse_test_utils:connect_via_token(Worker1, [{active, false}], Nonce),
    {ok, {Sock5, SessId}} = fuse_test_utils:connect_via_token(Worker1, [{active, false}], Nonce),

    Ping = fuse_test_utils:generate_ping_message(),

    % Send requests via each of 5 connections, immediately close four of them
    ok = ssl:send(Sock1, Ping),
    lists:foreach(fun(Sock) ->
        ok = ssl:send(Sock, Ping),
        ok = ssl:close(Sock)
    end, [Sock2, Sock3, Sock4, Sock5]),

    % Expect five responses for the ping, they should be received anyway
    % (from Sock1), given that the fallback mechanism works.
    lists:foreach(fun(_) ->
        ?assertMatch(#'ServerMessage'{
            message_body = {pong, #'Pong'{}}
        }, fuse_test_utils:receive_server_message())
    end, lists:seq(1, 5)),

    ok = ssl:close(Sock1).


fallback_during_sending_response_because_of_connection_error_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    Nonce = crypto:strong_rand_bytes(10),
    {ok, {Sock1, SessId}} = fuse_test_utils:connect_via_token(Worker1, [{active, false}], Nonce),
    {ok, {Sock2, SessId}} = fuse_test_utils:connect_via_token(Worker1, [{active, once}], Nonce),

    mock_ranch_ssl_to_fail_once(Worker1),
    MsgId = <<"1">>,
    Ping = fuse_test_utils:generate_ping_message(MsgId),
    ssl:send(Sock1, Ping),

    ?assertMatch(#'ServerMessage'{
        message_id = MsgId,
        message_body = {pong, #'Pong'{}}
    }, fuse_test_utils:receive_server_message()),

    unmock_ranch_ssl(Worker1),
    ok = ssl:close(Sock1),
    ok = ssl:close(Sock2).


fulfill_promises_after_connection_close_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    User = <<"user1">>,
    {ok, {Sock1, SessId}} = fuse_test_utils:connect_as_user(Config, Worker1, User, [{active, true}]),
    {ok, {Sock2, SessId}} = fuse_test_utils:connect_as_user(Config, Worker1, User, [{active, false}]),
    {ok, {Sock3, SessId}} = fuse_test_utils:connect_as_user(Config, Worker1, User, [{active, false}]),
    {ok, {Sock4, SessId}} = fuse_test_utils:connect_as_user(Config, Worker1, User, [{active, false}]),
    {ok, {Sock5, SessId}} = fuse_test_utils:connect_as_user(Config, Worker1, User, [{active, false}]),

    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Path = <<"/", SpaceName/binary, "/test_file">>,
    {ok, _} = lfm_proxy:create(Worker1, SessId, Path, 8#770),

    % Send 2 requests via each of 5 connections, immediately close four of them
    ok = ssl:send(Sock1, create_resolve_guid_req(Path)),
    ok = ssl:send(Sock1, create_resolve_guid_req(Path)),
    lists:foreach(fun(Sock) ->
        ok = ssl:send(Sock, create_resolve_guid_req(Path)),
        ok = ssl:send(Sock, create_resolve_guid_req(Path)),
        ok = ssl:close(Sock)
    end, [Sock2, Sock3, Sock4, Sock5]),

    % Fuse requests are handled using promises - connection manager holds info
    % about pending requests. Even after connection close, the promises should
    % be handled and responses sent to other connection within the session.
    GatherResponses = fun Fun(Counter) ->
        case fuse_test_utils:receive_server_message() of
            #'ServerMessage'{
                message_body = {processing_status, #'ProcessingStatus'{
                    code = 'IN_PROGRESS'
                }}
            } ->
                Fun(Counter);
            #'ServerMessage'{
                message_body = {fuse_response, #'FuseResponse'{
                    status = #'Status'{code = ?OK}
                }}
            } ->
                Fun(Counter + 1);
            {error, timeout} ->
                Counter
        end
    end,

    ?assertEqual(10, GatherResponses(0)),

    ok = ssl:close(Sock1).


fulfill_promises_after_connection_error_test(Config) ->
    Workers = [Worker1 | _] = ?config(op_worker_nodes, Config),

    User = <<"user1">>,
    {ok, {Sock1, SessId}} = fuse_test_utils:connect_as_user(Config, Worker1, User, [{active, true}]),
    {ok, {Sock2, SessId}} = fuse_test_utils:connect_as_user(Config, Worker1, User, [{active, false}]),

    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Path = <<"/", SpaceName/binary, "/test_file">>,
    {ok, _} = lfm_proxy:create(Worker1, SessId, Path, 8#770),

    mock_ranch_ssl_to_fail_once(Workers),
    ok = ssl:send(Sock2, create_resolve_guid_req(Path)),

    % Fuse requests are handled using promises - connection process holds info
    % about pending requests. Even after connection close, the promises should
    % be handled and responses sent to other connection within the session.
    GatherResponses = fun Fun(Counter) ->
        case fuse_test_utils:receive_server_message() of
            #'ServerMessage'{
                message_body = {processing_status, #'ProcessingStatus'{
                    code = 'IN_PROGRESS'
                }}
            } ->
                Fun(Counter);
            #'ServerMessage'{
                message_body = {fuse_response, #'FuseResponse'{
                    status = #'Status'{code = ?OK}
                }}
            } ->
                Fun(Counter + 1);
            {error, timeout} ->
                Counter
        end
    end,

    ?assertEqual(1, GatherResponses(0)),

    unmock_ranch_ssl(Worker1),
    ok = ssl:close(Sock1),
    ok = ssl:close(Sock2).


send_test(Config) ->
    Workers = [Worker1 | _] = ?config(op_worker_nodes, Config),

    Nonce = crypto:strong_rand_bytes(10),
    {ok, {Sock1, SessionId}} = fuse_test_utils:connect_via_token(Worker1, [{active, once}], Nonce),
    {ok, {Sock2, SessionId}} = fuse_test_utils:connect_via_token(Worker1, [{active, once}], Nonce),

    % In case of errors send_sync should try sending via other connections
    mock_ranch_ssl_to_fail_once(Workers),
    Description = <<"desc">>,
    ServerMsgInternal = #server_message{message_body = #status{
        code = ?OK,
        description = Description
    }},
    ?assertMatch(ok, send_msg(Worker1, SessionId, ServerMsgInternal)),

    ?assertMatch(#'ServerMessage'{
        message_id = undefined,
        message_body = {status, #'Status'{
            code = ?OK,
            description = Description
        }}
    }, fuse_test_utils:receive_server_message()),

    unmock_ranch_ssl(Worker1),
    ok = ssl:close(Sock1),
    ok = ssl:close(Sock2).


communicate_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    Status = #status{
        code = ?OK,
        description = <<"desc">>
    },
    {ok, MsgId} = rpc:call(Worker1, clproto_message_id, generate, [self()]),
    ServerMsgInternal = #server_message{
        message_id = MsgId,
        message_body = Status
    },

    {ok, {Sock, SessionId}} = spawn_ssl_echo_client(Worker1),

    % await until connection is added to session or timeout
    ?assertMatch(
        {ok, _, [_]},
        rpc:call(Worker1, session_connections, list, [SessionId]),
        ?ATTEMPTS
    ),

    % when sending msg with id to peer
    ?assertMatch(ok, send_msg(Worker1, SessionId, ServerMsgInternal)),

    % then response should be sent back
    ?assertReceivedMatch(#client_message{
        message_id = MsgId, message_body = Status
    }, ?TIMEOUT),

    ok = ssl:close(Sock).


client_keepalive_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    Nonce = crypto:strong_rand_bytes(10),

    initializer:remove_pending_messages(),
    {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1, [{active, true}], Nonce),
    % send keepalive msg and assert it will not end in decoding error
    % on provider side (following communication should succeed)
    ok = ssl:send(Sock, ?CLIENT_KEEPALIVE_MSG),
    fuse_test_utils:ping(Sock),
    ok = ssl:close(Sock).


heartbeats_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    {ok, {Sock, SessId}} = fuse_test_utils:connect_as_user(Config, Worker1, <<"user1">>, [{active, true}]),
    RootGuid = get_guid(Worker1, SessId, <<"/space_name1">>),
    initializer:remove_pending_messages(),

    create_timeouts_test(Config, Sock, RootGuid),
    ls_timeouts_test(Config, Sock, RootGuid),
    fsync_timeouts_test(Config, Sock, RootGuid),
    ok = ssl:close(Sock).


create_timeouts_test(Config, Sock, RootGuid) ->
    configure_cp(Config, helper_timeout),
    % send
    ok = ssl:send(Sock, fuse_test_utils:generate_create_file_message(RootGuid, <<"2">>, <<"ctt2">>)),
    % receive & validate
    await_status_answer(eagain, <<"2">>),

    configure_cp(Config, helper_delay),
    ok = ssl:send(Sock, fuse_test_utils:generate_create_file_message(RootGuid, <<"3">>, <<"ctt3">>)),
    % receive & validate
    await_status_answer(ok, <<"3">>, 3).

ls_timeouts_test(Config, Sock, RootGuid) ->
    configure_cp(Config, attr_delay),
    ok = ssl:send(Sock, fuse_test_utils:generate_get_children_attrs_message(RootGuid, <<"ls2">>)),
    % receive & validate
    await_status_answer(ok, <<"ls2">>, 2).

fsync_timeouts_test(Config, Sock, RootGuid) ->
    configure_cp(Config, events_delay),
    ok = ssl:send(Sock, fuse_test_utils:generate_fsync_message(RootGuid, <<"fs2">>)),
    % receive & validate
    await_status_answer(ok, <<"fs2">>, 2).


async_request_manager_memory_management_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    {ok, {Sock, SessionId}} = fuse_test_utils:connect_via_token(
        Worker1, [{active, true}], crypto:strong_rand_bytes(10)
    ),
    {ok, AsyncReqManager} = rpc:call(
        Worker1, session_connections, get_async_req_manager, [SessionId]
    ),

    ReqId = <<1>>,
    ReportRequest = fun() ->
        gen_server2:cast(AsyncReqManager, {report_pending_req, self(), ReqId})
    end,
    WithholdHeartBeats = fun() ->
        gen_server2:call(AsyncReqManager, {withhold_heartbeats, self(), ReqId})
    end,
    ReportResponseSent = fun() ->
        gen_server2:call(AsyncReqManager, {response_sent, ReqId})
    end,

    lists:foreach(fun({Op1, Op2, Op3}) ->
        assert_empty_async_req_manager_state(AsyncReqManager),
        Op1(),
        Op2(),
        Op3(),
        assert_empty_async_req_manager_state(AsyncReqManager)
    end, [
        {ReportRequest, WithholdHeartBeats, ReportResponseSent},
        {WithholdHeartBeats, ReportResponseSent, ReportRequest},
        {WithholdHeartBeats, ReportRequest, ReportResponseSent}
    ]),
    ok = ssl:close(Sock).


socket_timeout_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),

    RootGuid = get_guid(Worker1, SessId, <<"/space_name1">>),
    initializer:remove_pending_messages(),

    lists:foreach(fun(MainNum) ->
        {ok, {Sock2, SessId}} = fuse_test_utils:connect_as_user(Config, Worker1, <<"user1">>, [{active, true}]),

        lists:foreach(fun(Num) ->
            NumBin = integer_to_binary(Num),
            % send
            ok = ssl:send(Sock2, fuse_test_utils:generate_create_file_message(RootGuid, NumBin, NumBin))
        end, lists:seq(MainNum * 100 + 1, MainNum * 100 + 100)),

        AnsNums = lists:foldl(fun(_Num, Acc) ->
            % receive & validate
            M = fuse_test_utils:receive_server_message(),
            ?assertMatch(#'ServerMessage'{message_body = {
                fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
            }}, M),
            #'ServerMessage'{message_id = NumBin} = M,
            [NumBin | Acc]
        end, [], lists:seq(MainNum * 100 + 1, MainNum * 100 + 100)),

        lists:foreach(fun(Num) ->
            NumBin = integer_to_binary(Num),
            ?assert(lists:member(NumBin, AnsNums))
        end, lists:seq(MainNum * 100 + 1, MainNum * 100 + 100)),

        lists:foreach(fun(_Num) ->
            fuse_test_utils:ls(Sock2, RootGuid)
        end, lists:seq(MainNum * 100 + 1, MainNum * 100 + 100)),

        timer:sleep(timer:seconds(15)),
        ?assertMatch({error, closed}, ssl:send(Sock2, fuse_test_utils:generate_ping_message()))

    end, lists:seq(1, 10)).


closing_last_connection_should_cancel_all_session_transfers_test(Config) ->
    % given
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    RootGuid = get_guid(Worker1, SessionId, <<"/space_name1">>),

    Mod = replica_synchronizer,
    Fun = cancel_transfers_of_session,
    test_utils:mock_new(Workers, Mod, [passthrough]),
    test_utils:mock_expect(Workers, Mod, Fun, fun(FileUuid, SessId) ->
        meck:passthrough([FileUuid, SessId])
    end),

    {ok, {Sock, SessionId}} = fuse_test_utils:connect_as_user(Config, Worker1, <<"user1">>, [{active, true}]),

    % when
    fuse_test_utils:create_file(Sock, RootGuid, <<"f1">>),
    FileGuid = get_guid(Worker1, SessionId, <<"/space_name1/f1">>),
    FileUuid = rpc:call(Worker1, file_id, guid_to_uuid, [FileGuid]),
    fuse_test_utils:open(Sock, FileGuid),
    ok = ssl:close(Sock),

    % then
    timer:sleep(timer:seconds(90)),

    % File was opened 2 times (create and following open) but since it's one file
    % cancel should be called once. Unfortunately due to how session is terminated
    % (session_manager:remove_session may be called several times) it is possible
    % that cancel will be called more than once.
    CallsNum = rpc:call(Worker1, meck, num_calls, [Mod, Fun, '_'], timer:seconds(60)),
    ?assert(CallsNum >= 1),

    lists:foreach(fun(Num) ->
        ?assertMatch(FileUuid,
            rpc:call(Worker1, meck, capture, [Num, Mod, Fun, '_', 1])
        ),
        ?assertMatch(SessionId,
            rpc:call(Worker1, meck, capture, [Num, Mod, Fun, '_', 2])
        )
    end, lists:seq(1, CallsNum)),

    ok = test_utils:mock_unload(Workers, [Mod]).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    clproto_serializer:load_msg_defs(),
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(_) ->
    ok.


init_per_testcase(Case, Config) when
    Case =:= fallback_during_sending_response_because_of_connection_close_test;
    Case =:= fallback_during_sending_response_because_of_connection_error_test;
    Case =:= fulfill_promises_after_connection_close_test;
    Case =:= fulfill_promises_after_connection_error_test
->
    Workers = ?config(op_worker_nodes, Config),
    ssl:start(),
    initializer:remove_pending_messages(),

    % Artificially prolong message handling to avoid races between response from
    % the server and connection close.
    prolong_msg_routing(Workers),

    % Artificially prolong resolve_guid request processing to check if
    % multiple long-lasting promises are filled properly.
    test_utils:mock_new(Workers, guid_req),
    test_utils:mock_expect(Workers, guid_req, resolve_guid,
        fun(UserCtx, FileCtx) ->
            timer:sleep(rand:uniform(5000) + 10000),
            meck:passthrough([UserCtx, FileCtx])
        end
    ),

    NewConfig = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    mock_auth_manager(Config),
    lfm_proxy:init(NewConfig);

init_per_testcase(heartbeats_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:remove_pending_messages(),
    ssl:start(),

    mock_auth_manager(Config),
    CP_Pid = spawn_control_proc(),

    test_utils:mock_new(Workers, helpers),
    test_utils:mock_expect(Workers, helpers, apply_helper_nif, fun
        (#helper_handle{handle = Handle, timeout = Timeout}, Function, Args) ->
            apply_helper(Handle, Timeout, Function, Args, CP_Pid);
        (#file_handle{handle = Handle, timeout = Timeout}, Function, Args) ->
            apply_helper(Handle, Timeout, Function, Args, CP_Pid)
    end),

    test_utils:mock_new(Workers, attr_req),
    test_utils:mock_expect(Workers, attr_req, get_file_attr_insecure, fun
        (UserCtx, FileCtx, Opts) ->
            case get_cp_settings(CP_Pid) of
                attr_delay ->
                    timer:sleep(timer:seconds(70)),
                    meck:passthrough([UserCtx, FileCtx, Opts]);
                _ ->
                    meck:passthrough([UserCtx, FileCtx, Opts])
            end
    end),

    test_utils:mock_new(Workers, fslogic_event_handler),
    test_utils:mock_expect(Workers, fslogic_event_handler, handle_file_written_events, fun
        (Evts, UserCtxMap) ->
            handle_file_written_events(CP_Pid),
            meck:passthrough([Evts, UserCtxMap])
    end),

    [{control_proc, CP_Pid} |
        initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config)];

init_per_testcase(socket_timeout_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?APP_NAME,
            proto_connection_timeout, timer:seconds(10))
    end, Workers),
    init_per_testcase(heartbeats_test, Config);

init_per_testcase(closing_last_connection_should_cancel_all_session_transfers_test, Config) ->
    % Shorten ttl to force quicker client session removal
    init_per_testcase(heartbeats_test, [{fuse_session_grace_period_seconds, 10} | Config]);

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ssl:start(),
    initializer:remove_pending_messages(),
    mock_auth_manager(Config),

    initializer:mock_provider_id(
        Workers, <<"providerId">>, <<"access-token">>, <<"identity-token">>
    ),
    Config.


end_per_testcase(Case, Config) when
    Case =:= fallback_during_sending_response_because_of_connection_close_test;
    Case =:= fallback_during_sending_response_because_of_connection_error_test;
    Case =:= fulfill_promises_after_connection_close_test;
    Case =:= fulfill_promises_after_connection_error_test
->
    Workers = ?config(op_worker_nodes, Config),
    initializer:unmock_auth_manager(Config),
    test_utils:mock_validate_and_unload(Workers, [router, guid_req]),
    ssl:stop(),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config);

end_per_testcase(Case, Config) when
    Case =:= heartbeats_test;
    Case =:= closing_last_connection_should_cancel_all_session_transfers_test
->
    Workers = ?config(op_worker_nodes, Config),
    CP_Pid = ?config(control_proc, Config),
    ssl:stop(),
    stop_control_proc(CP_Pid),
    initializer:unmock_auth_manager(Config),
    test_utils:mock_validate_and_unload(Workers, [
        helpers, attr_req, fslogic_event_handler
    ]),
    initializer:clean_test_users_and_spaces_no_validate(Config);

end_per_testcase(socket_timeout_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?APP_NAME,
            proto_connection_timeout, timer:hours(24))
    end, Workers),
    end_per_testcase(heartbeats_test, Config);

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:unmock_provider_ids(Workers),
    initializer:unmock_auth_manager(Config),
    ssl:stop().


%%%===================================================================
%%% Internal functions
%%%===================================================================


mock_ranch_ssl_to_fail_once(Workers) ->
    Ref = make_ref(),
    test_utils:mock_new(Workers, ranch_ssl, [passthrough]),
    test_utils:mock_expect(Workers, ranch_ssl, send,
        fun(Msg, VerifyMsg) ->
            case simple_cache:get(Ref) of
                {ok, _} ->
                    meck:passthrough([Msg, VerifyMsg]);
                {error, not_found} ->
                    simple_cache:put(Ref, true),
                    {error, you_shall_not_send}
            end
        end
    ).


unmock_ranch_ssl(Node) ->
    test_utils:mock_unload(Node, [ranch_ssl]).


mock_auth_manager(Config) ->
    initializer:mock_auth_manager(Config).


prolong_msg_routing(Workers) ->
    test_utils:mock_new(Workers, router),
    test_utils:mock_expect(Workers, router, route_message,
        fun(Msg, ReplyTo) ->
            timer:sleep(timer:seconds(5)),
            meck:passthrough([Msg, ReplyTo])
        end
    ).


create_resolve_guid_req(Path) ->
    {ok, FuseReq} = clproto_serializer:serialize_client_message(#client_message{
        message_id = #message_id{id = crypto:strong_rand_bytes(5)},
        message_body = #fuse_request{
            fuse_request = #resolve_guid{
                path = Path
            }
        }
    }, true),
    FuseReq.


send_msg(Node, SessionId, Msg) ->
    rpc:call(Node, connection_api, send, [SessionId, Msg]).


%%--------------------------------------------------------------------
%% @doc
%% Connect to given node, and send back each message received from the server
%% @end
%%--------------------------------------------------------------------
-spec spawn_ssl_echo_client(NodeToConnect :: node()) ->
    {ok, {Sock :: ssl:sslsocket(), SessId :: session:id()}}.
spawn_ssl_echo_client(NodeToConnect) ->
    {ok, {Sock, SessionId}} = fuse_test_utils:connect_via_token(NodeToConnect, []),
    SslEchoClient =
        fun Loop() ->
            % receive data from server
            case ssl:recv(Sock, 0) of
                {ok, Data} ->
                    % decode
                    #'ServerMessage'{message_id = Id, message_body = Body} =
                        messages:decode_msg(Data, 'ServerMessage'),

                    % respond with the same data to the server (excluding stream_reset)
                    case Body of
                        {message_stream_reset, _} -> ok;
                        {subscription, _} -> ok;
                        _ ->
                            ClientAnsProtobuf = #'ClientMessage'{message_id = Id, message_body = Body},
                            ClientAnsRaw = messages:encode_msg(ClientAnsProtobuf),
                            ?assertEqual(ok, ssl:send(Sock, ClientAnsRaw))
                    end,

                    %loop back
                    Loop();
                {error, closed} -> ok;
                Error -> ?error("ssl_echo_client error: ~p", [Error])
            end
        end,
    spawn_link(SslEchoClient),
    {ok, {Sock, SessionId}}.


%%%===================================================================
%%% Test helper functions
%%%===================================================================


assert_empty_async_req_manager_state(AsyncReqManager) ->
    {state,
        _, PendingReqs, UnreportedReqs, WithheldHeartbeats, _
    } = get_gen_server_state(AsyncReqManager),

    ?assertMatch(0, maps:size(PendingReqs)),
    ?assertMatch(0, maps:size(UnreportedReqs)),
    ?assertMatch(0, maps:size(WithheldHeartbeats)).


get_gen_server_state(Pid) ->
    {status, Pid, _, [_PDict, _PStatus, _PParent, _PDbg, PMisc]} = sys:get_status(Pid),
    [_Header, _Status, {data, [{"State", State}]}] = PMisc,
    State.


await_status_answer(ExpStatus, MsgId) ->
    await_status_answer(ExpStatus, MsgId, 0).


await_status_answer(ExpStatus, MsgId, MinHeartbeatNum) ->
    timer:sleep(100), % wait for pending messages
    initializer:remove_pending_messages(),
    await_status_answer(ExpStatus, MsgId, MinHeartbeatNum, 0).


await_status_answer(ExpStatus, MsgId, MinHeartbeatsNum, HeartbeatsNum) ->
    case fuse_test_utils:receive_server_message([message_stream_reset, subscription,
        message_request]) of
        #'ServerMessage'{
            message_id = MsgId,
            message_body = {processing_status, #'ProcessingStatus'{code = 'IN_PROGRESS'}}
        } ->
            await_status_answer(ExpStatus, MsgId, MinHeartbeatsNum, HeartbeatsNum + 1);
        #'ServerMessage'{
            message_id = MsgId,
            message_body = {fuse_response, #'FuseResponse'{status = #'Status'{code = ExpStatus}}}
        } ->
            ?assert(MinHeartbeatsNum =< HeartbeatsNum);
        UnExpectedMsg ->
            ct:pal("Receive unexpected msg ~p while waiting for ~p", [
                UnExpectedMsg, ExpStatus
            ]),
            erlang:error(assertMatch)
    end.


get_guid(Worker, SessId, Path) ->
    Req = #fuse_request{fuse_request = #resolve_guid{path = Path}},
    {ok, #fuse_response{fuse_response = #guid{guid = Guid}}} =
        ?assertMatch(
            {ok, #fuse_response{status = #status{code = ?OK}}},
            rpc:call(Worker, worker_proxy, call, [fslogic_worker, {fuse_request, SessId, Req}]),
            30
        ),
    Guid.

spawn_control_proc() ->
    spawn(fun() ->
        control_proc(ok)
    end).

stop_control_proc(Pid) ->
    Pid ! stop.

control_proc(Value) ->
    receive
        {get_value, Pid} ->
            Pid ! {value, Value},
            control_proc(Value);
        {set_value, NewValue, Pid} ->
            Pid ! value_changed,
            control_proc(NewValue);
        stop ->
            ok
    end.

configure_cp(CP_Pid, Value) when is_pid(CP_Pid) ->
    CP_Pid ! {set_value, Value, self()},
    receive
        value_changed -> ok
    end;
configure_cp(Config, Value) ->
    CP_Pid = ?config(control_proc, Config),
    configure_cp(CP_Pid, Value).

get_cp_settings(CP_Pid) ->
    CP_Pid ! {get_value, self()},
    receive
        {value, Value} -> Value
    end.

apply_helper(Handle, Timeout, Function, Args, CP_Pid) ->
    case get_cp_settings(CP_Pid) of
        helper_timeout ->
            helpers:receive_loop(make_ref(), Timeout);
        helper_delay ->
            configure_cp(CP_Pid, ok),
            Master = self(),
            spawn(fun() ->
                {ok, ResponseRef} = apply(helpers_nif, Function, [Handle | Args]),
                Master ! {ref, ResponseRef},
                send_helpers_heartbeats_with_delay(10, Master, ResponseRef),
                Ans = helpers:receive_loop(ResponseRef, Timeout),
                Master ! {ResponseRef, Ans}
            end),
            receive
                {ref, ResponseRef} ->
                    helpers:receive_loop(ResponseRef, Timeout)
            end;
        _ ->
            {ok, ResponseRef} = apply(helpers_nif, Function, [Handle | Args]),
            helpers:receive_loop(ResponseRef, Timeout)
    end.

handle_file_written_events(CP_Pid) ->
    case get_cp_settings(CP_Pid) of
        events_delay ->
            timer:sleep(timer:seconds(70));
        _ ->
            ok
    end.

send_helpers_heartbeats_with_delay(0, _Master, _ResponseRef) ->
    ok;
send_helpers_heartbeats_with_delay(N, Master, ResponseRef) ->
    timer:sleep(timer:seconds(10)),
    Master ! {ResponseRef, heartbeat},
    send_helpers_heartbeats_with_delay(N - 1, Master, ResponseRef).
