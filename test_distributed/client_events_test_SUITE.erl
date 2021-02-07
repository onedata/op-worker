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
    subscribe_on_replication_info_test/1,
    subscribe_on_replication_info_multiprovider_test/1
]).

all() ->
    ?ALL([
        subscribe_on_dir_test,
        subscribe_on_user_root_test,
        subscribe_on_user_root_filter_test,
        subscribe_on_new_space_test,
        subscribe_on_new_space_filter_test,
        events_on_conflicts_test,
        subscribe_on_replication_info_test,
        subscribe_on_replication_info_multiprovider_test
    ]).

-define(CONFLICTING_FILE_NAME, <<"abc">>).
-define(CONFLICTING_FILE_AFTER_RENAME_STR, "xyz").
-define(CONFLICTING_FILE_AFTER_RENAME, <<?CONFLICTING_FILE_AFTER_RENAME_STR>>).
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
    receive_events_and_check(file_removed, FileGuid),

    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, <<"user1">>), -Seq1))),
    ?assertMatch({ok, []},
        rpc:call(Worker1, subscription_manager, get_subscribers, [SubscriptionRoutingKey, undefined]), 10),
    ?assertEqual(ok, ssl:close(Sock)),
    ok.

subscribe_on_user_root_test(Config) ->
    subscribe_on_user_root_test_base(Config, <<"user1">>, file_attr_changed).

subscribe_on_user_root_filter_test(Config) ->
    subscribe_on_user_root_test_base(Config, <<"user2">>, {not_received, file_attr_changed}).

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
    receive_events_and_check(ExpectedAns, SpaceGuid),

    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, User), -Seq1))),
    ?assertMatch({ok, []},
        rpc:call(Worker1, subscription_manager, get_subscribers, [SubscriptionRoutingKey, undefined]), 10),
    ?assertEqual(ok, ssl:close(Sock)),
    ok.

subscribe_on_new_space_test(Config) ->
    subscribe_on_new_space_test_base(Config, <<"user3">>, <<"user3">>, <<"3">>, file_attr_changed).

subscribe_on_new_space_filter_test(Config) ->
    subscribe_on_new_space_test_base(Config, <<"user4">>, <<"user3">>, <<"2">>, {not_received, file_attr_changed}).

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
    receive_events_and_check(ExpectedAns, SpaceGuid),

    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, User), -Seq1))),
    ?assertMatch({ok, []},
        rpc:call(Worker1, subscription_manager, get_subscribers, [SubscriptionRoutingKey, undefined]), 10),
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
    ?assertMatch({ok, _}, lfm_proxy:mv(Worker1, SessionId, {guid, FileGuid},
        <<"/space_name1/", Dirname/binary, "/", ?CONFLICTING_FILE_AFTER_RENAME_STR>>)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker1, SessionId, {guid, FileGuid})),

    % Events triggered by mv and unlink
    CheckList = receive_events_and_check(file_renamed, FileGuid),
    % Check for DirId as mock forces name conflict using uuid of parent
    case lists:member({file_renamed, DirId}, CheckList) of
        true -> ok; % Event has been already received with previous event
        false -> receive_events_and_check(file_renamed, DirId)
    end,

    lists:foreach(fun(Seq) ->
        ?assertEqual(ok, ssl:send(Sock,
            fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, <<"user1">>), -Seq)))
    end, [Seq1, Seq2]),
    ?assertMatch({ok, []},
        rpc:call(Worker1, subscription_manager, get_subscribers, [SubscriptionRoutingKey, undefined]), 10),
    ?assertMatch({ok, []},
        rpc:call(Worker1, subscription_manager, get_subscribers, [SubscriptionRoutingKey2, undefined]), 10),
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
        rpc:call(Worker1, subscription_manager, get_attr_event_subscribers, [FileGuid, undefined, true]), 10),
    ?assertMatch([{ok, [_]}],
        rpc:call(Worker1, subscription_manager, get_attr_event_subscribers, [FileGuid, undefined, false]), 10),
    rpc:call(Worker1, fslogic_event_emitter, emit_file_attr_changed_with_replication_status,
        [file_ctx:new_by_guid(FileGuid), true, []]),
    receive_events_and_check({receive_replication_changed, true}, FileGuid),
    rpc:call(Worker1, fslogic_event_emitter, emit_file_attr_changed_with_replication_status,
        [file_ctx:new_by_guid(FileGuid), false, []]),
    receive_events_and_check({receive_replication_changed, true}, FileGuid),

    % Check if single event is produced on overlapping subscriptions
    Seq2 = get_seq(Config, User),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_file_attr_changed_subscription_message(0, Seq2, -Seq2, DirId, 500))),
    ?assertMatch([{ok, [_]}, {ok, [_]}],
        rpc:call(Worker1, subscription_manager, get_attr_event_subscribers, [FileGuid, undefined, true]), 10),
    ?assertMatch([{ok, [_]}],
        rpc:call(Worker1, subscription_manager, get_attr_event_subscribers, [FileGuid, undefined, false]), 10),
    rpc:call(Worker1, fslogic_event_emitter, emit_file_attr_changed_with_replication_status,
        [file_ctx:new_by_guid(FileGuid), true, []]),
    receive_events_and_check({receive_replication_changed, true}, FileGuid),
    receive_events_and_check({not_received, file_attr_changed}, FileGuid),
    rpc:call(Worker1, fslogic_event_emitter, emit_file_attr_changed_with_replication_status,
        [file_ctx:new_by_guid(FileGuid), false, []]),
    receive_events_and_check({receive_replication_changed, true}, FileGuid),
    receive_events_and_check({not_received, file_attr_changed}, FileGuid),

    % Check if event is produced by standard emission function
    rpc:call(Worker1, fslogic_event_emitter, emit_file_attr_changed, [file_ctx:new_by_guid(FileGuid), []]),
    receive_events_and_check({receive_replication_changed, undefined}, FileGuid),

    % Check if event is produced on subscription without replication status
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, User), -Seq1))),
    ?assertMatch([{ok, [_]}, {ok, []}],
        rpc:call(Worker1, subscription_manager, get_attr_event_subscribers, [FileGuid, undefined, true]), 10),
    ?assertMatch([{ok, []}],
        rpc:call(Worker1, subscription_manager, get_attr_event_subscribers, [FileGuid, undefined, false]), 10),
    rpc:call(Worker1, fslogic_event_emitter, emit_file_attr_changed_with_replication_status,
        [file_ctx:new_by_guid(FileGuid), true, []]),
    receive_events_and_check({receive_replication_changed, undefined}, FileGuid),

    % Check if event is not produced on subscription without replication status if size is not changed
    rpc:call(Worker1, fslogic_event_emitter, emit_file_attr_changed_with_replication_status,
        [file_ctx:new_by_guid(FileGuid), false, []]),
    receive_events_and_check({not_received, file_attr_changed}, FileGuid),

    % Cleanup subscription
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_subscription_cancellation_message(0, get_seq(Config, User), -Seq2))),
    ?assertMatch([{ok, []}, {ok, []}],
        rpc:call(Worker1, subscription_manager, get_attr_event_subscribers, [FileGuid, undefined, true]), 10),
    ?assertEqual(ok, ssl:close(Sock)),
    ok.

subscribe_on_replication_info_multiprovider_test(Config) ->
    User = <<"user1">>,
    User2 = <<"user2">>,
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {User, ?GET_DOMAIN(Worker1)}}, Config),
    SessionIdUser2 = ?config({session_id, {User2, ?GET_DOMAIN(Worker1)}}, Config),
    SessionIdWorker2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker2)}}, Config),
    AccessToken = ?config({access_token, User}, Config),
    EmitterSessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    SpaceGuid = client_simulation_test_base:get_guid(Worker1, EmitterSessionId, <<"/space_name2">>),

    {ok, {Sock, _}} = fuse_test_utils:connect_via_token(Worker1, [{active, true}], SessionId, AccessToken),
    Filename = generator:gen_name(),
    Dirname = generator:gen_name(),

    % Create test dir and file
    DirId = fuse_test_utils:create_directory(Sock, SpaceGuid, Dirname),
    {FileGuid, HandleId} = fuse_test_utils:create_777_mode_file(Sock, DirId, Filename),
    fuse_test_utils:close(Sock, FileGuid, HandleId),

    % Subscribe and check if subscription has been created
    Seq1 = get_seq(Config, User),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_replica_status_changed_subscription_message(0, Seq1, -Seq1, DirId, 500))),
    ?assertMatch([{ok, []}, {ok, [_]}],
        rpc:call(Worker1, subscription_manager, get_attr_event_subscribers, [FileGuid, undefined, true]), 10),
    ?assertMatch([{ok, [_]}],
        rpc:call(Worker1, subscription_manager, get_attr_event_subscribers, [FileGuid, undefined, false]), 10),

    % Prepare handles to edit file
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker2, SessionIdWorker2, {guid, FileGuid}), 30),
    {ok, Worker1Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker1, SessionIdUser2, {guid, FileGuid}, rdwr)),
    {ok, Worker2Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker2, SessionIdWorker2, {guid, FileGuid}, rdwr)),

    % Test status change after write on remote provider
    ?assertEqual({ok, 3}, lfm_proxy:write(Worker2, Worker2Handle, 0, <<"xxx">>)),
    ?assertEqual(ok, lfm_proxy:fsync(Worker2, Worker2Handle)),
    receive_events_and_check({receive_replication_changed, false}, FileGuid),

    % Test status change after local write that does not change replication status
    ?assertEqual({ok, 3}, lfm_proxy:write(Worker1, Worker1Handle, 3, <<"xxx">>)),
    ?assertEqual(ok, lfm_proxy:fsync(Worker1, Worker1Handle)),
    receive_events_and_check({not_received, file_attr_changed}, FileGuid),

    % Test status change after local write that changes replication status
    ?assertEqual({ok, 3}, lfm_proxy:write(Worker1, Worker1Handle, 0, <<"xxx">>)),
    ?assertEqual(ok, lfm_proxy:fsync(Worker1, Worker1Handle)),
    receive_events_and_check({receive_replication_changed, true}, FileGuid),

    % Subscribe for standard file_attrs event and check if subscription has been created
    Seq2 = get_seq(Config, User),
    ?assertEqual(ok, ssl:send(Sock,
        fuse_test_utils:generate_file_attr_changed_subscription_message(0, Seq2, -Seq2, DirId, 500))),
    ?assertMatch([{ok, [_]}, {ok, [_]}],
        rpc:call(Worker1, subscription_manager, get_attr_event_subscribers, [FileGuid, undefined, true]), 10),
    ?assertMatch([{ok, [_]}],
        rpc:call(Worker1, subscription_manager, get_attr_event_subscribers, [FileGuid, undefined, false]), 10),

    % Test status change after local write that does not change replication status
    ?assertEqual({ok, 3}, lfm_proxy:write(Worker1, Worker1Handle, 6, <<"xxx">>)),
    ?assertEqual(ok, lfm_proxy:fsync(Worker1, Worker1Handle)),
    receive_events_and_check({receive_replication_changed, undefined}, FileGuid),

    % Change status to verify read operations
    ?assertMatch({ok, #file_attr{size = 9}}, lfm_proxy:stat(Worker2, SessionIdWorker2, {guid, FileGuid}), 30),
    ?assertEqual({ok, 3}, lfm_proxy:write(Worker2, Worker2Handle, 0, <<"xxx">>)),
    ?assertEqual(ok, lfm_proxy:fsync(Worker2, Worker2Handle)),
    receive_events_and_check({receive_replication_changed, false}, FileGuid),

    % Test status change after local read that does not change replication status
    ?assertEqual({ok, <<"x">>}, lfm_proxy:read(Worker1, Worker1Handle, 5, 1)),
    ?assertEqual(ok, lfm_proxy:fsync(Worker1, Worker1Handle)),
    receive_events_and_check({receive_replication_changed, undefined}, FileGuid),

    % Test status change after local read that changes replication status
    ?assertEqual({ok, <<"xxx">>}, lfm_proxy:read(Worker1, Worker1Handle, 0, 3)),
    ?assertEqual(ok, lfm_proxy:fsync(Worker1, Worker1Handle)),
    receive_events_and_check({receive_replication_changed, true}, FileGuid),

    % Release handles
    ?assertEqual(ok, lfm_proxy:close(Worker1, Worker1Handle)),
    ?assertEqual(ok, lfm_proxy:close(Worker2, Worker2Handle)),
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
    test_utils:mock_new(Workers, file_meta_links),
    test_utils:mock_expect(Workers, file_meta_links, get_all, fun
        (Uuid, Name) when Name =:= ?CONFLICTING_FILE_NAME orelse Name =:= ?CONFLICTING_FILE_AFTER_RENAME ->
            case meck:passthrough([Uuid, Name]) of
                {ok, List} -> {ok, [#link{name = Name, target = Uuid, tree_id = ?TEST_TREE_ID} | List]};
                {error, not_found} -> {ok, [#link{name = Name, target = Uuid, tree_id = ?TEST_TREE_ID}]}
            end;
        (Uuid, Name) ->
            meck:passthrough([Uuid, Name])
    end),
    init_per_testcase(default, Config);
init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    initializer:remove_pending_messages(),
    ssl:start(),
    lfm_proxy:init(Config).

end_per_testcase(events_on_conflicts_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, file_meta_links),
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

receive_events_and_check({not_received, Type}, Guid) ->
    {_, CheckList} = ?assertMatch({not_found, _}, receive_and_check_loop({Type, Guid})),
    CheckList;
receive_events_and_check(ExpectedType, Guid) ->
    {_, CheckList} = ?assertMatch({found, _}, receive_and_check_loop({ExpectedType, Guid})),
    CheckList.

receive_and_check_loop(Expected) ->
    case fuse_test_utils:receive_server_message([message_stream_reset, subscription, message_request,
        message_acknowledgement, processing_status]) of
        #'ServerMessage'{
            message_body = {events, #'Events'{events = EventsList}}} ->
            CheckList = lists:foldl(fun
                (#'Event'{type = {file_removed, #'FileRemovedEvent'{file_uuid = Guid}}}, Acc) ->
                    [{file_removed, Guid} | Acc];
                (#'Event'{type = {file_attr_changed, #'FileAttrChangedEvent'{
                        file_attr = #'FileAttr'{uuid = Guid, fully_replicated = Status}}}}, Acc) ->
                    [{file_attr_changed, Guid}, {{receive_replication_changed, Status}, Guid} | Acc];
                (#'Event'{type = {file_renamed, #'FileRenamedEvent'{
                    top_entry = #'FileRenamedEntry'{old_uuid = Guid}}}}, Acc) ->
                    [{file_renamed, Guid} | Acc]
            end, [], EventsList),

            case lists:member(Expected, CheckList) of
                true ->
                    {found, CheckList};
                false ->
                    {Ans, CheckList2} = receive_and_check_loop(Expected),
                    {Ans, CheckList ++ CheckList2}
            end;
        {error, timeout} -> {not_found, []};
        _ -> receive_and_check_loop(Expected)
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