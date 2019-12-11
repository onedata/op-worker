%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests events management.
%%% @end
%%%--------------------------------------------------------------------
-module(client_events_test_SUITE).
-author("Michal Wrzeszcz").

-include("fuse_test_utils.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2,
    end_per_testcase/2, end_per_suite/1]).

-export([
    subscribe_on_dir_test/1,
    subscribe_on_user_root_test/1,
    subscribe_on_user_root_filter_test/1,
    subscribe_on_new_space_test/1,
    subscribe_on_new_space_filter_test/1
]).

all() ->
    ?ALL([
        subscribe_on_dir_test,
        subscribe_on_user_root_test,
        subscribe_on_user_root_filter_test,
        subscribe_on_new_space_test,
        subscribe_on_new_space_filter_test
    ]).

%%%===================================================================
%%% Tests
%%%===================================================================

subscribe_on_dir_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    SpaceGuid = client_simulation_test_base:get_guid(Worker1, SessionId, <<"/space_name1">>),

    {ok, {_, RootHandle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(Worker1, <<"0">>, SpaceGuid,
        generator:gen_name(), 8#755)),
    ?assertEqual(ok, lfm_proxy:close(Worker1, RootHandle)),

    {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1, [{active, true}], SessionId),

    Filename = generator:gen_name(),
    Dirname = generator:gen_name(),

    DirId = fuse_test_utils:create_directory(Sock, SpaceGuid, Dirname),
    Seq1 = get_seq(Config, 1),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_file_removed_subscription_message(0, Seq1, -Seq1, DirId))),
    timer:sleep(2000), % there is no sync between subscription and unlink

    {FileGuid, HandleId} = fuse_test_utils:create_file(Sock, DirId, Filename),
    fuse_test_utils:close(Sock, FileGuid, HandleId),

    ?assertEqual(ok, lfm_proxy:unlink(Worker1, <<"0">>, {guid, FileGuid})),
    ?assertEqual(ok, receive_file_removed_event()),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, 1), -Seq1))),
    ?assertEqual(ok, ssl:close(Sock)),
    ok.

subscribe_on_user_root_test(Config) ->
    subscribe_on_user_root_test_base(Config, 1, ok).

subscribe_on_user_root_filter_test(Config) ->
    subscribe_on_user_root_test_base(Config, 2, {error,timeout}).

subscribe_on_user_root_test_base(Config, UserNum, ExpectedAns) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user", (integer_to_binary(UserNum))/binary>>, ?GET_DOMAIN(Worker1)}}, Config),
    EmmiterSessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    SpaceGuid = client_simulation_test_base:get_guid(Worker1, EmmiterSessionId, <<"/space_name1">>),

    UserCtx = rpc:call(Worker1, user_ctx, new, [SessionId]),
    UserId = rpc:call(Worker1, user_ctx, get_user_id, [UserCtx]),
    DirId = fslogic_uuid:user_root_dir_guid(UserId),

    {ok, {Sock, _}} = fuse_test_utils:connect_via_custom_token(Worker1, [{active, true}], SessionId, UserNum),

    Seq1 = get_seq(Config, UserNum),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_file_attr_changed_subscription_message(0, Seq1, -Seq1, DirId, 500))),
    timer:sleep(2000), % there is no sync between subscription and unlink

    rpc:call(Worker1, fslogic_event_emitter, emit_file_attr_changed, [file_ctx:new_by_guid(SpaceGuid), []]),
    ?assertEqual(ExpectedAns, receive_file_attr_changed_event()),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, UserNum), -Seq1))),
    ?assertEqual(ok, ssl:close(Sock)),
    ok.

subscribe_on_new_space_test(Config) ->
    subscribe_on_new_space_test_base(Config, 1, ok).

subscribe_on_new_space_filter_test(Config) ->
    subscribe_on_new_space_test_base(Config, 2, {error,timeout}).

subscribe_on_new_space_test_base(Config, UserNum, ExpectedAns) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user", (integer_to_binary(UserNum))/binary>>, ?GET_DOMAIN(Worker1)}}, Config),
    EmmiterSessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),

    SpaceGuid = client_simulation_test_base:get_guid(Worker1, EmmiterSessionId, <<"/space_name1">>),
    SpaceDirUuid = file_id:guid_to_uuid(SpaceGuid),
    ?assertEqual(ok, rpc:call(Worker1, file_meta, delete, [SpaceDirUuid])),

    UserCtx = rpc:call(Worker1, user_ctx, new, [SessionId]),
    UserId = rpc:call(Worker1, user_ctx, get_user_id, [UserCtx]),
    DirId = fslogic_uuid:user_root_dir_guid(UserId),

    {ok, {Sock, _}} = fuse_test_utils:connect_via_custom_token(Worker1, [{active, true}], SessionId, UserNum),

    Seq1 = get_seq(Config, UserNum),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_file_attr_changed_subscription_message(0, Seq1, -Seq1, DirId, 500))),
    timer:sleep(2000), % there is no sync between subscription and unlink

    rpc:call(Worker1, file_meta, make_space_exist, [<<"space_id1">>]),
    ?assertEqual(ExpectedAns, receive_file_attr_changed_event()),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, UserNum), -Seq1))),
    ?assertEqual(ok, ssl:close(Sock)),
    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(Config2) ->
        Config3 = initializer:setup_storage(init_seq_counter(Config2)),
        Workers = ?config(op_worker_nodes, Config3),
        test_utils:mock_new(Workers, user_identity),
        test_utils:mock_expect(Workers, user_identity, get_or_fetch,
            fun
                (#token_auth{token = ?TOKEN}) ->
                    {ok, #document{value = #user_identity{user_id = <<"user1">>}}};
                (#token_auth{token = ?TOKEN2}) ->
                    {ok, #document{value = #user_identity{user_id = <<"user2">>}}}
            end
        ),
        initializer:create_test_users_and_spaces(?TEST_FILE(Config3, "env_desc.json"), Config3)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, pool_utils, ?MODULE]} | Config].

init_per_testcase(_Case, Config) ->
    timer:sleep(1000),
    initializer:remove_pending_messages(),
    ssl:start(),
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    ssl:stop().

end_per_suite(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, [user_identity]),
    initializer:clean_test_users_and_spaces_no_validate(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

receive_file_removed_event() ->
    case fuse_test_utils:receive_server_message([message_stream_reset, subscription, message_request,
        message_acknowledgement, processing_status]) of
        #'ServerMessage'{
            message_body = {events, #'Events'{events = [#'Event'{
                type = {file_removed, #'FileRemovedEvent'{}}
            }]}}
        } -> ok;
        Msg -> Msg
    end.

receive_file_attr_changed_event() ->
    case fuse_test_utils:receive_server_message([message_stream_reset, subscription, message_request,
        message_acknowledgement, processing_status]) of
        #'ServerMessage'{
            message_body = {events, #'Events'{events = [#'Event'{
                type = {file_attr_changed, #'FileAttrChangedEvent'{}}
            }]}}
        } -> ok;
        Msg -> Msg
    end.

get_seq(Config, User) ->
    Pid = ?config(seq_counter, Config),
    Pid ! {get_seq, User, self()},
    receive
        {seq, Seq} -> Seq
    end.

init_seq_counter(Config) ->
    Pid = spawn(fun() -> seq_counter(#{}) end),
    [{seq_counter, Pid} | Config].

seq_counter(Map) ->
    receive
        {get_seq, User, Pid} ->
            Seq = maps:get(User, Map, 0),
            Pid ! {seq, Seq},
            seq_counter(Map#{User => Seq + 1})
    end.