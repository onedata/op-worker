%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests connection_manager.
%%% @end
%%%--------------------------------------------------------------------
-module(connection_manager_test_SUITE).
-author("Bartosz Walkowicz").

-include("fuse_utils.hrl").
-include("global_definitions.hrl").
-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/common/handshake_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, init_per_testcase/2,
    end_per_testcase/2, end_per_suite/1
]).

%%tests
-export([




    fallback_during_sending_response_test/1,
    fulfill_promises_after_connection_close_test/1,
    client_communicate_test/1,
    client_communicate_async_test/1,
    timeouts_test/1,
    client_keepalive_test/1,
    socket_timeout_test/1,
    closing_connection_should_cancel_all_session_transfers_test/1
]).

-define(NORMAL_CASES_NAMES, [



    socket_timeout_test,
    timeouts_test,
    client_keepalive_test,
    fallback_during_sending_response_test,
    fulfill_promises_after_connection_close_test,
    client_communicate_test,
    client_communicate_async_test,
    closing_connection_should_cancel_all_session_transfers_test
]).

-define(PERFORMANCE_CASES_NAMES, []).

all() -> ?ALL(?NORMAL_CASES_NAMES, ?PERFORMANCE_CASES_NAMES).

-define(CORRECT_PROVIDER_ID, <<"correct-iden-mac">>).
-define(INCORRECT_PROVIDER_ID, <<"incorrect-iden-mac">>).
-define(CORRECT_NONCE, <<"correct-nonce">>).
-define(INCORRECT_NONCE, <<"incorrect-nonce">>).

-define(assertMatchTwo(Guard1, Guard2, Expr),
    begin
        ((fun() ->
            case (Expr) of
                Guard1 -> 1;
                Guard2 -> 2;
                __V ->
                    erlang:error({assertMatch,
                        [{module, ?MODULE},
                            {line, ?LINE},
                            {expression, (??Expr)},
                            {pattern, (??Guard1), (??Guard2)},
                            {value, __V}]})
            end
        end)())
    end).

-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).

-record(file_handle, {
    handle :: helpers_nif:file_handle(),
    timeout :: timeout()
}).

%%%===================================================================
%%% Test functions
%%%===================================================================


























































































socket_timeout_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SID = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),

    RootGuid = get_guid(Worker1, SID, <<"/space_name1">>),
    initializer:remove_pending_messages(),
    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SID),

    % send
    ok = ssl:send(Sock, fuse_utils:generate_create_file_message(RootGuid, <<"1">>, <<"f1">>)),
    % receive & validate
    ?assertMatch(#'ServerMessage'{message_body = {
        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
    }, message_id = <<"1">>}, fuse_utils:receive_server_message()),

    lists:foreach(fun(MainNum) ->
        timer:sleep(timer:seconds(20)),
        {ok, {Sock2, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SID),

        lists:foreach(fun(Num) ->
            NumBin = integer_to_binary(Num),
            % send
            ok = ssl:send(Sock2, fuse_utils:generate_create_file_message(RootGuid, NumBin, NumBin))
        end, lists:seq(MainNum * 100 + 1, MainNum * 100 + 100)),

        AnsNums = lists:foldl(fun(_Num, Acc) ->
            % receive & validate
            M = fuse_utils:receive_server_message(),
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

        lists:foreach(fun(Num) ->
            LSBin = list_to_binary(integer_to_list(Num) ++ "ls"),
            ok = ssl:send(Sock2, fuse_utils:generate_get_children_attrs_message(RootGuid, LSBin)),
            % receive & validate
            ?assertMatch(#'ServerMessage'{message_body = {
                fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
            }, message_id = LSBin}, fuse_utils:receive_server_message())
        end, lists:seq(MainNum * 100 + 1, MainNum * 100 + 100))
    end, lists:seq(1, 10)).

timeouts_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SID = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),

    RootGuid = get_guid(Worker1, SID, <<"/space_name1">>),
    initializer:remove_pending_messages(),
    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SID),

    create_timeouts_test(Config, Sock, RootGuid),
    ls_timeouts_test(Config, Sock, RootGuid),
    fsync_timeouts_test(Config, Sock, RootGuid).

client_keepalive_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SID = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),

    RootGuid = get_guid(Worker1, SID, <<"/space_name1">>),
    initializer:remove_pending_messages(),
    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SID),
    % send keepalive msg and assert it will not end in decoding error
    % on provider side (following communication should succeed)
    ok = ssl:send(Sock, ?CLIENT_KEEPALIVE_MSG),
    create_timeouts_test(Config, Sock, RootGuid).

create_timeouts_test(Config, Sock, RootGuid) ->
    % send
    ok = ssl:send(Sock, fuse_utils:generate_create_file_message(RootGuid, <<"1">>, <<"f1">>)),
    % receive & validate
    ?assertMatch(#'ServerMessage'{message_body = {
        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
    }, message_id = <<"1">>}, fuse_utils:receive_server_message()),

    configure_cp(Config, helper_timeout),
    % send
    ok = ssl:send(Sock, fuse_utils:generate_create_file_message(RootGuid, <<"2">>, <<"f2">>)),
    % receive & validate
    check_answer(fun() -> ?assertMatchTwo(
        #'ServerMessage'{message_body = {
            fuse_response, #'FuseResponse'{status = #'Status'{code = eagain}
            }
        }, message_id = <<"2">>},
        #'ServerMessage'{message_body = {
            processing_status, #'ProcessingStatus'{code = 'IN_PROGRESS'}
        }, message_id = <<"2">>},
        fuse_utils:receive_server_message()) end
    ),

    configure_cp(Config, helper_delay),
    ok = ssl:send(Sock, fuse_utils:generate_create_file_message(RootGuid, <<"3">>, <<"f3">>)),
    % receive & validate
    check_answer(fun() -> ?assertMatchTwo(
        #'ServerMessage'{message_body = {
            fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
        }, message_id = <<"3">>},
        #'ServerMessage'{message_body = {
            processing_status, #'ProcessingStatus'{code = 'IN_PROGRESS'}
        }, message_id = <<"3">>},
        fuse_utils:receive_server_message()) end,
        3
    ).

ls_timeouts_test(Config, Sock, RootGuid) ->
    % send
    configure_cp(Config, ok),
    ok = ssl:send(Sock, fuse_utils:generate_get_children_attrs_message(RootGuid, <<"ls1">>)),
    % receive & validate
    ?assertMatch(#'ServerMessage'{message_body = {
        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
    }, message_id = <<"ls1">>}, fuse_utils:receive_server_message()),

    configure_cp(Config, attr_delay),
    ok = ssl:send(Sock, fuse_utils:generate_get_children_attrs_message(RootGuid, <<"ls2">>)),
    % receive & validate
    check_answer(fun() -> ?assertMatchTwo(
        #'ServerMessage'{message_body = {
            fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
        }, message_id = <<"ls2">>},
        #'ServerMessage'{message_body = {
            processing_status, #'ProcessingStatus'{code = 'IN_PROGRESS'}
        }, message_id = <<"ls2">>},
        fuse_utils:receive_server_message()) end,
        2
    ).

fsync_timeouts_test(Config, Sock, RootGuid) ->
    configure_cp(Config, ok),
    ok = ssl:send(Sock, fuse_utils:generate_fsync_message(RootGuid, <<"fs1">>)),
    % receive & validate
    ?assertMatch(#'ServerMessage'{message_body = {
        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
    }, message_id = <<"fs1">>}, fuse_utils:receive_server_message()),

    configure_cp(Config, events_delay),
    ok = ssl:send(Sock, fuse_utils:generate_fsync_message(RootGuid, <<"fs2">>)),
    % receive & validate
    check_answer(fun() -> ?assertMatchTwo(
        #'ServerMessage'{message_body = {
            fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
        }, message_id = <<"fs2">>},
        #'ServerMessage'{message_body = {
            processing_status, #'ProcessingStatus'{code = 'IN_PROGRESS'}
        }, message_id = <<"fs2">>},
        fuse_utils:receive_server_message()) end,
        2
    ).


fallback_during_sending_response_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    SessionId = <<"12345">>,
    % Create a couple of connections within the same session
    {ok, {Sock1, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),
    {ok, {Sock2, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),
    {ok, {Sock3, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),
    {ok, {Sock4, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),
    {ok, {Sock5, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),

    Ping = messages:encode_msg(#'ClientMessage'{
        message_id = crypto:strong_rand_bytes(5), message_body = {ping, #'Ping'{}}
    }),

    [ssl:setopts(Sock, [{active, once}]) || Sock <- [Sock1, Sock2, Sock3, Sock4, Sock5]],

    % Send requests via each of 5 connections, immediately close four of them
    ok = ssl:send(Sock1, Ping),
    lists:foreach(fun(Sock) ->
        ok = ssl:send(Sock, Ping),
        ok = ssl:close(Sock)
    end, [Sock2, Sock3, Sock4, Sock5]),

    % Expect five responses for the ping, they should be received anyway
    % (from Sock1), given that the fallback mechanism works.
    lists:foreach(fun(_) ->
        ssl:setopts(Sock1, [{active, once}]),
        ?assertMatch(#'ServerMessage'{
            message_body = {pong, #'Pong'{}}
        }, fuse_utils:receive_server_message())
    end, lists:seq(1, 5)),

    ok = ssl:close(Sock1),
    ok.


fulfill_promises_after_connection_close_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    {ok, {Sock1, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),
    {ok, {Sock2, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),
    {ok, {Sock3, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),
    {ok, {Sock4, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),
    {ok, {Sock5, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),

    [ssl:setopts(Sock, [{active, once}]) || Sock <- [Sock1, Sock2, Sock3, Sock4, Sock5]],

    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Path = <<"/", SpaceName/binary, "/test_file">>,
    {ok, _} = lfm_proxy:create(Worker1, SessionId, Path, 8#770),

    % Fuse requests are handled using promises - connection process holds info
    % about pending requests. Even after connection close, the promises should
    % be handled and responses sent to other connection within the session.
    GenFuseReq = fun() ->
        {ok, FuseReq} = serializer:serialize_client_message(#client_message{
            message_id = #message_id{id = crypto:strong_rand_bytes(5)},
            message_body = #fuse_request{
                fuse_request = #resolve_guid{
                    path = Path
                }
            }
        }, true),
        FuseReq
    end,

    % Send 2 requests via each of 5 connections, immediately close four of them
    ok = ssl:send(Sock1, GenFuseReq()),
    ok = ssl:send(Sock1, GenFuseReq()),
    lists:foreach(fun(Sock) ->
        ok = ssl:send(Sock, GenFuseReq()),
        ok = ssl:send(Sock, GenFuseReq()),
        ok = ssl:close(Sock)
    end, [Sock2, Sock3, Sock4, Sock5]),

    GatherResponses = fun Fun(Counter) ->
        ssl:setopts(Sock1, [{active, once}]),
        case fuse_utils:receive_server_message() of
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

    ok = ssl:close(Sock1),
    ok.

client_communicate_test(Config) ->
    % given
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    Status = #status{code = ?OK, description = <<"desc">>},
    ServerMsgInternal = #server_message{message_body = Status},

    % when
    {ok, {Sock, SessionId}} = spawn_ssl_echo_client(Worker1),
    CommunicateResult = rpc:call(Worker1, communicator, communicate,
        [ServerMsgInternal, SessionId, #{wait_for_ans => true}]),

    % then
    ?assertMatch({ok, #client_message{message_body = Status}}, CommunicateResult),
    ok = ssl:close(Sock).

client_communicate_async_test(Config) ->
    % given
    [Worker1 | _] = Workers = ?config(op_worker_nodes, Config),
    Status = #status{
        code = ?OK,
        description = <<"desc">>
    },
    ServerMsgInternal = #server_message{message_body = Status},
    Self = self(),
    {ok, {Sock, SessionId}} = spawn_ssl_echo_client(Worker1),

    % when
    {ok, MsgId} = rpc:call(Worker1, communicator, communicate,
        [ServerMsgInternal, SessionId, #{use_msg_id => {true, Self}}]),

    % then
    ?assertReceivedMatch(#client_message{
        message_id = MsgId, message_body = Status
    }, ?TIMEOUT),

    % given
    test_utils:mock_expect(Workers, router, route_message, fun
        (#client_message{message_id = Id = #message_id{issuer = Issuer,
            recipient = undefined}}) ->
            Issuer = oneprovider:get_id(),
            Self ! {router_message_called, Id},
            ok
    end),

    % when
    {ok, MsgId2} = rpc:call(Worker1, communicator, communicate,
        [ServerMsgInternal, SessionId, #{use_msg_id => true}]),

    % then
    ?assertReceivedMatch({router_message_called, MsgId2}, ?TIMEOUT),
    ok = ssl:close(Sock).

closing_connection_should_cancel_all_session_transfers_test(Config) ->
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

    % Create a couple of connections within the same session
    {ok, {Sock, _}} = fuse_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),

    % when
    ok = ssl:send(Sock, fuse_utils:generate_create_file_message(RootGuid, <<"1">>, <<"f1">>)),
    ?assertMatch(#'ServerMessage'{message_body = {
        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
    }, message_id = <<"1">>}, fuse_utils:receive_server_message()),

    FileGuid = get_guid(Worker1, SessionId, <<"/space_name1/f1">>),
    FileUuid = rpc:call(Worker1, fslogic_uuid, guid_to_uuid, [FileGuid]),

    ok = ssl:send(Sock, fuse_utils:generate_open_file_message(FileGuid, <<"2">>)),
    ?assertMatch(#'ServerMessage'{message_body = {
        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
    }, message_id = <<"2">>}, fuse_utils:receive_server_message()),

    ok = ssl:close(Sock),

    % then
    timer:sleep(timer:seconds(60)),

    CallsNum = lists:sum(meck_get_num_calls([Worker1], Mod, Fun, '_')),
    ?assertMatch(2, CallsNum),

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
    serializer:load_msg_defs(),
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(_) ->
    ok.




















init_per_testcase(client_communicate_async_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, router),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= fallback_during_sending_response_test;
    Case =:= fulfill_promises_after_connection_close_test ->
    Workers = ?config(op_worker_nodes, Config),
    ssl:start(),
    initializer:remove_pending_messages(),

    test_utils:mock_new(Workers, user_identity),
    test_utils:mock_expect(Workers, user_identity, get_or_fetch,
        fun(#macaroon_auth{macaroon = ?MACAROON, disch_macaroons = ?DISCH_MACAROONS}) ->
            {ok, #document{value = #user_identity{user_id = <<"user1">>}}}
        end
    ),

    % Artificially prolong message handling to avoid races between response from
    % the server and connection close.
    test_utils:mock_new(Workers, router),
    test_utils:mock_expect(Workers, router, route_message,
        fun(Msg) ->
            timer:sleep(2000),
            meck:passthrough([Msg])
        end
    ),

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
    lfm_proxy:init(NewConfig);

init_per_testcase(timeouts_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:remove_pending_messages(),
    ssl:start(),

    test_utils:mock_new(Workers, user_identity),
    test_utils:mock_expect(Workers, user_identity, get_or_fetch,
        fun(#macaroon_auth{macaroon = ?MACAROON, disch_macaroons = ?DISCH_MACAROONS}) ->
            {ok, #document{value = #user_identity{user_id = <<"user1">>}}}
        end
    ),

    CP_Pid = spawn_control_proc(),

    test_utils:mock_new(Workers, helpers),
    test_utils:mock_expect(Workers, helpers, apply_helper_nif, fun
        (#helper_handle{handle = Handle, timeout = Timeout}, Function, Args) ->
            aplly_helper(Handle, Timeout, Function, Args, CP_Pid);
        (#file_handle{handle = Handle, timeout = Timeout}, Function, Args) ->
            aplly_helper(Handle, Timeout, Function, Args, CP_Pid)
    end),

    test_utils:mock_new(Workers, attr_req),
    test_utils:mock_expect(Workers, attr_req, get_file_attr_insecure, fun
        (UserCtx, FileCtx) ->
            get_file_attr_insecure(UserCtx, FileCtx, CP_Pid)
    end),

    test_utils:mock_new(Workers, fslogic_event_handler),
    test_utils:mock_expect(Workers, fslogic_event_handler, handle_file_written_events, fun
        (Evts, UserCtxMap) ->
            handle_file_written_events(CP_Pid),
            meck:passthrough([Evts, UserCtxMap])
    end),

    [{control_proc, CP_Pid} |
        initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config)];

init_per_testcase(client_keepalive_test, Config) ->
    init_per_testcase(timeouts_test, Config);

init_per_testcase(closing_connection_should_cancel_all_session_transfers_test, Config) ->
    % Shorten ttl to force quicker client session removal
    init_per_testcase(timeouts_test, [{fuse_session_ttl_seconds, 10} | Config]);

init_per_testcase(socket_timeout_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?APP_NAME,
            proto_connection_timeout, timer:seconds(10))
    end, Workers),
    init_per_testcase(timeouts_test, Config);

init_per_testcase(default, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ssl:start(),
    initializer:remove_pending_messages(),
    mock_identity(Workers),

    initializer:mock_provider_id(
        Workers, <<"providerId">>, <<"auth-macaroon">>, <<"identity-macaroon">>
    ),
    Config;

init_per_testcase(_Case, Config) ->
    init_per_testcase(default, Config).











end_per_testcase(client_communicate_async_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [router]),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case =:= fulfill_promises_after_connection_close_test;
    Case =:= fallback_during_sending_response_test ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [user_identity, router, guid_req]),
    ssl:stop(),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config);


end_per_testcase(timeouts_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    CP_Pid = ?config(control_proc, Config),
    ssl:stop(),
    stop_control_proc(CP_Pid),
    test_utils:mock_validate_and_unload(Workers, [user_identity, helpers,
        attr_req, fslogic_event_handler]),
    initializer:clean_test_users_and_spaces_no_validate(Config);

end_per_testcase(client_keepalive_test, Config) ->
    end_per_testcase(timeouts_test, Config);

end_per_testcase(socket_timeout_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?APP_NAME,
            proto_connection_timeout, timer:minutes(10))
    end, Workers),
    end_per_testcase(timeouts_test, Config);

end_per_testcase(closing_connection_should_cancel_all_session_transfers_test, Config) ->
    end_per_testcase(timeouts_test, Config);
    
end_per_testcase(default, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:unmock_provider_ids(Workers),
    test_utils:mock_validate_and_unload(Workers, [user_identity]),
    ssl:stop();

end_per_testcase(_Case, Config) ->
    end_per_testcase(default, Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Connect to given node, and send back each message received from the server
%% @end
%%--------------------------------------------------------------------
-spec spawn_ssl_echo_client(NodeToConnect :: node()) ->
    {ok, {Sock :: ssl:socket(), SessId :: session:id()}}.
spawn_ssl_echo_client(NodeToConnect) ->
    {ok, {Sock, SessionId}} = fuse_utils:connect_via_macaroon(NodeToConnect, []),
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

mock_identity(Workers) ->
    test_utils:mock_new(Workers, user_identity),
    test_utils:mock_expect(Workers, user_identity, get_or_fetch,
        fun(#macaroon_auth{macaroon = ?MACAROON, disch_macaroons = ?DISCH_MACAROONS}) ->
            {ok, #document{value = #user_identity{}}}
        end
    ).

%%%===================================================================
%%% Test helper functions
%%%===================================================================

check_answer(AsertFun) ->
    check_answer(AsertFun, 0).

check_answer(AsertFun, MinHeartbeatNum) ->
    timer:sleep(100), % wait for pending messages
    initializer:remove_pending_messages(),
    check_answer(AsertFun, 0, MinHeartbeatNum).

check_answer(AsertFun, HeartbeatNum, MinHeartbeatNum) ->
    AnsNum = AsertFun(),
    case AnsNum of
        1 ->
            ?assert(HeartbeatNum >= MinHeartbeatNum);
        _ ->
            check_answer(AsertFun, HeartbeatNum + 1, MinHeartbeatNum)
    end.

get_guid(Worker, SessId, Path) ->
    #fuse_response{fuse_response = #guid{guid = Guid}} =
        ?assertMatch(
            #fuse_response{status = #status{code = ?OK}},
            ?req(Worker, SessId, #resolve_guid{path = Path}),
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

aplly_helper(Handle, Timeout, Function, Args, CP_Pid) ->
    case get_cp_settings(CP_Pid) of
        helper_timeout ->
            helpers:receive_loop(make_ref(), Timeout);
        helper_delay ->
            configure_cp(CP_Pid, ok),
            Master = self(),
            spawn(fun() ->
                {ok, ResponseRef} = apply(helpers_nif, Function, [Handle | Args]),
                Master ! {ref, ResponseRef},
                heartbeat(10, Master, ResponseRef),
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

get_file_attr_insecure(UserCtx, FileCtx, CP_Pid) ->
    case get_cp_settings(CP_Pid) of
        attr_delay ->
            timer:sleep(timer:seconds(70)),
            attr_req:get_file_attr_insecure(UserCtx, FileCtx, false);
        _ ->
            attr_req:get_file_attr_insecure(UserCtx, FileCtx, false)
    end.

handle_file_written_events(CP_Pid) ->
    case get_cp_settings(CP_Pid) of
        events_delay ->
            timer:sleep(timer:seconds(70));
        _ ->
            ok
    end.

heartbeat(0, _Master, _ResponseRef) ->
    ok;
heartbeat(N, Master, ResponseRef) ->
    timer:sleep(timer:seconds(10)),
    Master ! {ResponseRef, heartbeat},
    heartbeat(N - 1, Master, ResponseRef).

meck_get_num_calls(Nodes, Module, Fun, Args) ->
    lists:map(fun(Node) ->
        rpc:call(Node, meck, num_calls, [Module, Fun, Args], timer:seconds(60))
    end, Nodes).

