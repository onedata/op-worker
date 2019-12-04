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

    {ok, {Sock, _}} = fuse_test_utils:connect_via_macaroon(Worker1, [{active, true}], SessionId),

    Filename = generator:gen_name(),
    Dirname = generator:gen_name(),

    DirId = fuse_test_utils:create_directory(Sock, SpaceGuid, Dirname),
    client_simulation_test_base:create_new_file_subscriptions(Sock, DirId, 0),
    timer:sleep(2000), % there is no sync between subscription and unlink

    {FileGuid, HandleId} = fuse_test_utils:create_file(Sock, DirId, Filename),
    fuse_test_utils:close(Sock, FileGuid, HandleId),

    ?assertEqual(ok, lfm_proxy:unlink(Worker1, <<"0">>, {guid, FileGuid})),
    ?assertEqual(ok, receive_file_removed_event()),
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

    {ok, {Sock, _}} = fuse_test_utils:connect_via_custom_macaroon(Worker1, [{active, true}], SessionId, UserNum),

    client_simulation_test_base:create_new_file_subscriptions(Sock, DirId, 0),
    timer:sleep(2000), % there is no sync between subscription and unlink

    rpc:call(Worker1, fslogic_event_emitter, emit_file_attr_changed, [file_ctx:new_by_guid(SpaceGuid), []]),
    ?assertEqual(ExpectedAns, receive_file_attr_changed_event()),
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

    {ok, {Sock, _}} = fuse_test_utils:connect_via_custom_macaroon(Worker1, [{active, true}], SessionId, UserNum),

    client_simulation_test_base:create_new_file_subscriptions(Sock, DirId, 0),
    timer:sleep(2000), % there is no sync between subscription and unlink

    rpc:call(Worker1, file_meta, make_space_exist, [<<"space_id1">>]),
    ?assertEqual(ExpectedAns, receive_file_attr_changed_event()),
    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    client_simulation_test_base:init_per_suite(Config).

init_per_testcase(_Case, Config) ->
    client_simulation_test_base:init_per_testcase(Config).

end_per_testcase(_Case, Config) ->
    client_simulation_test_base:end_per_testcase(Config).

end_per_suite(_Case) ->
    ok.

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