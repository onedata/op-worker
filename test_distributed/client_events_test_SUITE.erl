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
    subscribe_on_new_space_filter_test/1,
    events_on_conflicts_test/1,
    subscribe_on_replication_info_test/1
]).

all() ->
    ?ALL([
        subscribe_on_dir_test,
        subscribe_on_user_root_test,
        subscribe_on_user_root_filter_test,
        subscribe_on_new_space_test,
        subscribe_on_new_space_filter_test,
        events_on_conflicts_test,
        subscribe_on_replication_info_test
    ]).

-define(CONFLICTING_FILE_NAME, <<"abc">>).
-define(CONFLICTING_FILE_AFTER_RENAME, <<"xyz">>).
-define(TEST_TREE_ID, <<"q">>).

%%%===================================================================
%%% Tests
%%%===================================================================

subscribe_on_dir_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    AccessToken = ?config({access_token, <<"user1">>}, Config),
    SpaceGuid = client_simulation_test_base:get_guid(Worker1, SessionId, <<"/space_name1">>),

    {ok, {_, RootHandle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(Worker1, <<"0">>, SpaceGuid,
        generator:gen_name(), 8#755)),
    ?assertEqual(ok, lfm_proxy:close(Worker1, RootHandle)),

    {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1, [{active, true}], SessionId, AccessToken),

    Filename = generator:gen_name(),
    Dirname = generator:gen_name(),

    DirId = fuse_test_utils:create_directory(Sock, SpaceGuid, Dirname),
    Seq1 = get_seq(Config, <<"user1">>),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_file_removed_subscription_message(0, Seq1, -Seq1, DirId))),
    {ok, SubscriptionRoutingKey} = subscription_type:get_routing_key(#file_removed_subscription{file_guid = DirId}),
    ?assertMatch({ok, [_]},
        rpc:call(Worker1, subscription_manager, get_subscribers, [SubscriptionRoutingKey, undefined]), 10),

    {FileGuid, HandleId} = fuse_test_utils:create_file(Sock, DirId, Filename),
    fuse_test_utils:close(Sock, FileGuid, HandleId),

    ?assertEqual(ok, lfm_proxy:unlink(Worker1, <<"0">>, {guid, FileGuid})),
    ?assertEqual(ok, receive_file_removed_event()),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, <<"user1">>), -Seq1))),
    ?assertEqual(ok, ssl:close(Sock)),
    ok.

subscribe_on_user_root_test(Config) ->
    subscribe_on_user_root_test_base(Config, <<"user1">>, ok).

subscribe_on_user_root_filter_test(Config) ->
    subscribe_on_user_root_test_base(Config, <<"user2">>, {error,timeout}).

subscribe_on_user_root_test_base(Config, User, ExpectedAns) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {User, ?GET_DOMAIN(Worker1)}}, Config),
    AccessToken = ?config({access_token, User}, Config),
    EmitterSessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    SpaceGuid = client_simulation_test_base:get_guid(Worker1, EmitterSessionId, <<"/space_name1">>),

    UserCtx = rpc:call(Worker1, user_ctx, new, [SessionId]),
    UserId = rpc:call(Worker1, user_ctx, get_user_id, [UserCtx]),
    DirId = fslogic_uuid:user_root_dir_guid(UserId),

    {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1, [{active, true}], SessionId, AccessToken),

    Seq1 = get_seq(Config, User),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_file_attr_changed_subscription_message(0, Seq1, -Seq1, DirId, 500))),
    {ok, SubscriptionRoutingKey} = subscription_type:get_routing_key(#file_attr_changed_subscription{file_guid = DirId}),
    ?assertMatch({ok, [_]},
        rpc:call(Worker1, subscription_manager, get_subscribers, [SubscriptionRoutingKey, undefined]), 10),

    rpc:call(Worker1, fslogic_event_emitter, emit_file_attr_changed, [file_ctx:new_by_guid(SpaceGuid), []]),
    ?assertEqual(ExpectedAns, receive_file_attr_changed_event()),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, User), -Seq1))),
    ?assertEqual(ok, ssl:close(Sock)),
    ok.

subscribe_on_new_space_test(Config) ->
    subscribe_on_new_space_test_base(Config, <<"user3">>, <<"user3">>, <<"4">>, ok).

subscribe_on_new_space_filter_test(Config) ->
    subscribe_on_new_space_test_base(Config, <<"user4">>, <<"user3">>, <<"3">>, {error,timeout}).

subscribe_on_new_space_test_base(Config, User, DomainUser, SpaceNum, ExpectedAns) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {User, ?GET_DOMAIN(Worker1)}}, Config),
    AccessToken = ?config({access_token, User}, Config),
    EmitterSessionId = ?config({session_id, {DomainUser, ?GET_DOMAIN(Worker1)}}, Config),

    SpaceGuid = client_simulation_test_base:get_guid(Worker1, EmitterSessionId, <<"/space_name", SpaceNum/binary>>),
    SpaceDirUuid = file_id:guid_to_uuid(SpaceGuid),
    ?assertEqual(ok, rpc:call(Worker1, file_meta, delete, [SpaceDirUuid])),

    UserCtx = rpc:call(Worker1, user_ctx, new, [SessionId]),
    UserId = rpc:call(Worker1, user_ctx, get_user_id, [UserCtx]),
    DirId = fslogic_uuid:user_root_dir_guid(UserId),

    {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1, [{active, true}], SessionId, AccessToken),

    Seq1 = get_seq(Config, User),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_file_attr_changed_subscription_message(0, Seq1, -Seq1, DirId, 500))),
    {ok, SubscriptionRoutingKey} = subscription_type:get_routing_key(#file_attr_changed_subscription{file_guid = DirId}),
    ?assertMatch({ok, [_]},
        rpc:call(Worker1, subscription_manager, get_subscribers, [SubscriptionRoutingKey, undefined]), 10),

    rpc:call(Worker1, file_meta, make_space_exist, [<<"space_id", SpaceNum/binary>>]),
    ?assertEqual(ExpectedAns, receive_file_attr_changed_event()),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, User), -Seq1))),
    ?assertEqual(ok, ssl:close(Sock)),
    ok.

% Create file names' conflict using mocks (see init_per_testcase)
% Additional events should appear
events_on_conflicts_test(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    SpaceGuid = client_simulation_test_base:get_guid(Worker1, SessionId, <<"/space_name1">>),
    AccessToken = ?config({access_token, <<"user1">>}, Config),

    {ok, {_, RootHandle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(Worker1, <<"0">>, SpaceGuid,
        generator:gen_name(), 8#755)),
    ?assertEqual(ok, lfm_proxy:close(Worker1, RootHandle)),

    {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1, [{active, true}], SessionId, AccessToken),

    Filename = ?CONFLICTING_FILE_NAME,
    Dirname = generator:gen_name(),

    DirId = fuse_test_utils:create_directory(Sock, SpaceGuid, Dirname),
    Seq1 = get_seq(Config, <<"user1">>),
    Seq2 = get_seq(Config, <<"user1">>),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_file_removed_subscription_message(0, Seq1, -Seq1, DirId))),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_file_renamed_subscription_message(0, Seq2, -Seq2, DirId))),
    {ok, SubscriptionRoutingKey} = subscription_type:get_routing_key(#file_removed_subscription{file_guid = DirId}),
    {ok, SubscriptionRoutingKey2} = subscription_type:get_routing_key(#file_renamed_subscription{file_guid = DirId}),
    ?assertMatch({ok, [_]},
        rpc:call(Worker1, subscription_manager, get_subscribers, [SubscriptionRoutingKey, undefined]), 10),
    ?assertMatch({ok, [_]},
        rpc:call(Worker1, subscription_manager, get_subscribers, [SubscriptionRoutingKey2, undefined]), 10),

    {FileGuid, HandleId} = fuse_test_utils:create_file(Sock, DirId, Filename),
    fuse_test_utils:close(Sock, FileGuid, HandleId),
    ?assertMatch({ok, _}, lfm_proxy:mv(Worker1, SessionId, {guid, FileGuid}, <<"/space_name1/", Dirname/binary, "/xyz">>)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker1, SessionId, {guid, FileGuid})),

    % Events triggered by mv and unlink
    ?assertEqual(ok, receive_file_renamed_event()),
    ?assertEqual(ok, receive_file_renamed_event()),

    lists:foreach(fun(Seq) ->
        ?assertEqual(ok, ssl:send(Sock,
            fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, <<"user1">>), -Seq)))
    end, [Seq1, Seq2]),
    ?assertEqual(ok, ssl:close(Sock)),
    ok.

subscribe_on_replication_info_test(Config) ->
    User = <<"user1">>,
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {User, ?GET_DOMAIN(Worker1)}}, Config),
    AccessToken = ?config({access_token, User}, Config),
    EmitterSessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    SpaceGuid = client_simulation_test_base:get_guid(Worker1, EmitterSessionId, <<"/space_name1">>),

    {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1, [{active, true}], SessionId, AccessToken),
    Filename = generator:gen_name(),
    Dirname = generator:gen_name(),

    % Create test dir and file
    DirId = fuse_test_utils:create_directory(Sock, SpaceGuid, Dirname),
    {FileGuid, HandleId} = fuse_test_utils:create_file(Sock, DirId, Filename),
    fuse_test_utils:close(Sock, FileGuid, HandleId),

    % Check if event is produced on subscription
    Seq1 = get_seq(Config, User),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_replica_status_changed_subscription_message(0, Seq1, -Seq1, DirId, 500))),
    ?assertMatch([{ok, []}, {ok, [_]}],
        rpc:call(Worker1, subscription_manager, get_attr_event_subscribers, [FileGuid, undefined]), 10),
    rpc:call(Worker1, fslogic_event_emitter, emit_file_attr_changed_with_replication_status,
        [file_ctx:new_by_guid(FileGuid)]),
    ?assertEqual(ok, receive_file_attr_changed_event()),

    % Check if single event is produced on overlapping subscriptions
    Seq2 = get_seq(Config, User),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_file_attr_changed_subscription_message(0, Seq2, -Seq2, DirId, 500))),
    ?assertMatch([{ok, [_]}, {ok, [_]}],
        rpc:call(Worker1, subscription_manager, get_attr_event_subscribers, [FileGuid, undefined]), 10),
    rpc:call(Worker1, fslogic_event_emitter, emit_file_attr_changed_with_replication_status,
        [file_ctx:new_by_guid(FileGuid)]),
    ?assertEqual(ok, receive_file_attr_changed_event()),
    ?assertEqual({error,timeout}, receive_file_attr_changed_event()),

    % Check if event is produced by standard emission function
    rpc:call(Worker1, fslogic_event_emitter, emit_file_attr_changed, [file_ctx:new_by_guid(FileGuid), []]),
    ?assertEqual(ok, receive_file_attr_changed_event()),

    % Check if event is produced on subscription without replication status
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, User), -Seq1))),
    ?assertMatch([{ok, [_]}, {ok, []}],
        rpc:call(Worker1, subscription_manager, get_attr_event_subscribers, [FileGuid, undefined]), 10),
    rpc:call(Worker1, fslogic_event_emitter, emit_file_attr_changed_with_replication_status,
        [file_ctx:new_by_guid(FileGuid)]),
    ?assertEqual(ok, receive_file_attr_changed_event()),

    % Cleanup subscription
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, User), -Seq2))),
    ?assertEqual(ok, ssl:close(Sock)),
    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(Config2) ->
        Config3 = initializer:setup_storage(init_seq_counter(Config2)),
        initializer:mock_auth_manager(Config3),
        initializer:create_test_users_and_spaces(?TEST_FILE(Config3, "env_desc.json"), Config3)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, pool_utils, ?MODULE]} | Config].

init_per_testcase(events_on_conflicts_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, file_meta),
    test_utils:mock_expect(Workers, file_meta, get_all_links, fun
        (Uuid, Name) when Name =:= ?CONFLICTING_FILE_NAME orelse Name =:= ?CONFLICTING_FILE_AFTER_RENAME ->
            case meck:passthrough([Uuid, Name]) of
                {ok, List} -> {ok, [#link{name = Name, target = <<>>, tree_id = ?TEST_TREE_ID} | List]};
                {error, not_found} -> {ok, [#link{name = Name, target = <<>>, tree_id = ?TEST_TREE_ID}]}
            end;
        (Uuid, Name) ->
            meck:passthrough([Uuid, Name])
    end),
    init_per_testcase(default, Config);
init_per_testcase(_Case, Config) ->
    initializer:remove_pending_messages(),
    ssl:start(),
    lfm_proxy:init(Config).

end_per_testcase(events_on_conflicts_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, file_meta),
    end_per_testcase(default, Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    ssl:stop().

end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unmock_auth_manager(Config).

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

receive_file_renamed_event() ->
    case fuse_test_utils:receive_server_message([message_stream_reset, subscription, message_request,
        message_acknowledgement, processing_status]) of
        #'ServerMessage'{
            message_body = {events, #'Events'{events = [#'Event'{
                type = {file_renamed, #'FileRenamedEvent'{top_entry = #'FileRenamedEntry'{}}}
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