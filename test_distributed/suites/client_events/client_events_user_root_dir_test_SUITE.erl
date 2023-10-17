%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests events management on user root dir.
%%%--------------------------------------------------------------------
-module(client_events_user_root_dir_test_SUITE).
-author("Michal Stanisz").


-include("proto/oneclient/event_messages.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2,
    end_per_testcase/2, end_per_suite/1]).

-export([
    add_space/1,
    add_space_proxy/1,
    add_space_duplicate_name/1,
    add_space_duplicate_name_proxy/1,
    add_user_to_existing_space/1,
    add_user_to_existing_space_proxy/1,
    add_support_to_space_with_duplicated_name/1,
    add_support_to_space_with_duplicated_name_proxy/1,
    remove_space/1,
    remove_space_proxy/1,
    remove_space_duplicate_name/1,
    remove_space_duplicate_name_proxy/1,
    rename_space/1,
    rename_space_proxy/1,
    rename_space_to_existing_name/1,
    rename_space_to_existing_name_proxy/1,
    rename_duplicated_space_name/1,
    rename_duplicated_space_name_proxy/1,
    remove_space_during_broken_zone_connection/1,
    remove_space_proxy_during_broken_zone_connection/1,
    remove_space_duplicate_name_during_broken_zone_connection/1,
    remove_space_duplicate_name_proxy_during_broken_zone_connection/1,
    rename_space_during_broken_zone_connection/1,
    rename_space_proxy_during_broken_zone_connection/1,
    rename_space_to_existing_name_during_broken_zone_connection/1,
    rename_space_to_existing_name_proxy_during_broken_zone_connection/1,
    rename_duplicated_space_name_during_broken_zone_connection/1,
    rename_duplicated_space_name_proxy_during_broken_zone_connection/1
]).

all() -> ?ALL([
    add_space,
    add_space_proxy,
    add_space_duplicate_name,
    add_space_duplicate_name_proxy,
    add_user_to_existing_space,
    add_user_to_existing_space_proxy,
    add_support_to_space_with_duplicated_name,
    add_support_to_space_with_duplicated_name_proxy,
    remove_space,
    remove_space_proxy,
    remove_space_duplicate_name,
    remove_space_duplicate_name_proxy,
    rename_space,
    rename_space_proxy,
    rename_space_to_existing_name,
    rename_space_to_existing_name_proxy,
    rename_duplicated_space_name,
    rename_duplicated_space_name_proxy,
    remove_space_during_broken_zone_connection,
    remove_space_proxy_during_broken_zone_connection,
    remove_space_duplicate_name_during_broken_zone_connection,
    remove_space_duplicate_name_proxy_during_broken_zone_connection,
    rename_space_during_broken_zone_connection,
    rename_space_proxy_during_broken_zone_connection,
    rename_space_to_existing_name_during_broken_zone_connection,
    rename_space_to_existing_name_proxy_during_broken_zone_connection,
    rename_duplicated_space_name_during_broken_zone_connection,
    rename_duplicated_space_name_proxy_during_broken_zone_connection
]).

-define(ATTEMPTS, 30).

-define(CLIENT_USER, user2).
-define(OTHER_USER, user1).

-define(SUPPORTING_PROVIDER, krakow).

-record(space, {
    id :: binary(),
    name :: binary()
}).

-type event() ::
    {file_attr_changed, file_id:guid(), file_meta:name()} |
    {file_renamed, file_id:guid(), file_meta:name()} |
    {file_removed, file_id:guid()}.

-record(test_config, {
    zone_connection_status = alive :: alive | broken,
    setup_fun = fun() -> ok end :: fun(() -> SetupState),
    test_fun :: fun((SetupState) -> TestState),
    expected_events_fun :: fun((TestState) -> [event()])
}).

%%%===================================================================
%%% Tests
%%%===================================================================

add_space(_Config) ->
    add_space_test_base(krakow).

add_space_proxy(_Config) ->
    add_space_test_base(paris).

add_space_duplicate_name(_Config) ->
    add_space_duplicate_name_test_base(krakow).

add_space_duplicate_name_proxy(_Config) ->
    add_space_duplicate_name_test_base(paris).

add_user_to_existing_space(_Config) ->
    add_user_to_existing_space_test_base(krakow).

add_user_to_existing_space_proxy(_Config) ->
    add_user_to_existing_space_test_base(paris).

add_support_to_space_with_duplicated_name(_Config) ->
    add_support_to_space_with_duplicated_name_test_base(krakow).

add_support_to_space_with_duplicated_name_proxy(_Config) ->
    add_support_to_space_with_duplicated_name_test_base(paris).

remove_space(_Config) ->
    remove_space_test_base(krakow, alive).

remove_space_proxy(_Config) ->
    remove_space_test_base(paris, alive).

remove_space_duplicate_name(_Config) ->
    remove_space_duplicate_name_test_base(krakow, alive).

remove_space_duplicate_name_proxy(_Config) ->
    remove_space_duplicate_name_test_base(paris, alive).

rename_space(_Config) ->
    rename_space_test_base(krakow, alive).

rename_space_proxy(_Config) ->
    rename_space_test_base(paris, alive).

rename_space_to_existing_name(_Config) ->
    rename_space_to_existing_name_test_base(krakow, alive).

rename_space_to_existing_name_proxy(_Config) ->
    rename_space_to_existing_name_test_base(paris, alive).

rename_duplicated_space_name(_Config) ->
    rename_duplicated_space_name_test_base(krakow, alive).

rename_duplicated_space_name_proxy(_Config) ->
    rename_duplicated_space_name_test_base(paris, alive).

remove_space_during_broken_zone_connection(_Config) ->
    remove_space_test_base(krakow, broken).

remove_space_proxy_during_broken_zone_connection(_Config) ->
    remove_space_test_base(paris, broken).

remove_space_duplicate_name_during_broken_zone_connection(_Config) ->
    remove_space_duplicate_name_test_base(krakow, broken).

remove_space_duplicate_name_proxy_during_broken_zone_connection(_Config) ->
    remove_space_duplicate_name_test_base(paris, broken).

rename_space_during_broken_zone_connection(_Config) ->
    rename_space_test_base(krakow, broken).

rename_space_proxy_during_broken_zone_connection(_Config) ->
    rename_space_test_base(paris, broken).

rename_space_to_existing_name_during_broken_zone_connection(_Config) ->
    rename_space_to_existing_name_test_base(krakow, broken).

rename_space_to_existing_name_proxy_during_broken_zone_connection(_Config) ->
    rename_space_to_existing_name_test_base(paris, broken).

rename_duplicated_space_name_during_broken_zone_connection(_Config) ->
    rename_duplicated_space_name_test_base(krakow, broken).

rename_duplicated_space_name_proxy_during_broken_zone_connection(_Config) ->
    rename_duplicated_space_name_test_base(paris, broken).


%%%===================================================================
%%% Test bases
%%%===================================================================

add_space_test_base(ClientProvider) ->
    test_base(ClientProvider, #test_config{
        test_fun = fun(_) ->
            SpaceName = str_utils:rand_hex(8),
            #space{name = SpaceName, id = create_supported_space(?SUPPORTING_PROVIDER, SpaceName)}
        end,
        expected_events_fun = fun(Space) ->
            [{file_attr_changed, guid(Space), Space#space.name}]
        end
    }).


add_space_duplicate_name_test_base(ClientProvider) ->
    test_base(ClientProvider, #test_config{
        setup_fun = fun setup_space/0,
        test_fun = fun(PreexistingSpace) ->
            SpaceId2 = create_supported_space(?SUPPORTING_PROVIDER, PreexistingSpace#space.name),
            {PreexistingSpace, #space{name = PreexistingSpace#space.name, id = SpaceId2}}
        end,
        expected_events_fun = fun({PreexistingSpace, AddedSpace}) -> [
            {file_attr_changed, guid(AddedSpace), extended_name(AddedSpace)},
            {file_renamed, guid(PreexistingSpace), extended_name(PreexistingSpace)}
        ] end
    }).
    

add_user_to_existing_space_test_base(ClientProvider) ->
    test_base(ClientProvider, #test_config{
        setup_fun = fun() ->
            SpaceName = str_utils:rand_hex(8),
            Space = #space{name = SpaceName, id = create_supported_space(?SUPPORTING_PROVIDER, SpaceName, ?OTHER_USER)},
            % ensure that space doc is already created
            ?assertMatch({ok, _}, opw_test_rpc:call(ClientProvider,
                space_logic, get, [oct_background:get_user_session_id(?OTHER_USER, ClientProvider), Space#space.id])),
            ?assertMatch({ok, _}, opw_test_rpc:call(ClientProvider,
                file_meta, get, [fslogic_file_id:spaceid_to_space_dir_uuid(Space#space.id)]), ?ATTEMPTS),
            Space
        end,
        test_fun = fun(#space{id = SpaceId} = Space) ->
            ok = ozw_test_rpc:add_user_to_space(SpaceId, oct_background:get_user_id(?CLIENT_USER)),
            Space
        end,
        expected_events_fun = fun(Space) -> [
            {file_attr_changed, guid(Space), Space#space.name}
        ] end
    }).


add_support_to_space_with_duplicated_name_test_base(ClientProvider) ->
    test_base(ClientProvider, #test_config{
        setup_fun = fun() ->
            SpaceName = str_utils:rand_hex(8),
            {setup_space(SpaceName), setup_space_without_support(SpaceName)}
        end,
        test_fun = fun({Space1, #space{id = SpaceId2} = Space2}) ->
            support_space(ClientProvider, ?CLIENT_USER, SpaceId2),
            {Space1, Space2}
        end,
        expected_events_fun = fun({Space1, Space2}) -> [
            {file_renamed, guid(Space1), extended_name(Space1)},
            {file_attr_changed, guid(Space2), extended_name(Space2)}
        ] end
    }).


remove_space_test_base(ClientProvider, ZoneConnectionStatus) ->
    test_base(ClientProvider, #test_config{
        setup_fun = fun setup_space/0,
        test_fun = fun(#space{id = SpaceId} = Space) ->
            ok = ozw_test_rpc:delete_space(SpaceId),
            Space
        end,
        expected_events_fun = fun(Space) -> [
            {file_removed, guid(Space)}
        ] end,
        zone_connection_status = ZoneConnectionStatus
    }).


remove_space_duplicate_name_test_base(ClientProvider, ZoneConnectionStatus) ->
    test_base(ClientProvider, #test_config{
        setup_fun = fun setup_2_spaces_with_conflicting_names/0,
        test_fun = fun({#space{id = SpaceId1} = Space1, Space2}) ->
            ok = ozw_test_rpc:delete_space(SpaceId1),
            {Space1, Space2}
        end,
        expected_events_fun = fun({Space1, _Space2}) -> [
            %% @TODO VFS-10923 check file_renamed event after resolving issue
%%            {file_renamed, guid(Space2), Space2#space.name},
            {file_removed, guid(Space1)}
        ] end,
        zone_connection_status = ZoneConnectionStatus
    }).


rename_duplicated_space_name_test_base(ClientProvider, ZoneConnectionStatus) ->
    test_base(ClientProvider, #test_config{
        setup_fun = fun setup_2_spaces_with_conflicting_names/0,
        test_fun = fun({#space{id = SpaceId1} = Space1, Space2}) ->
            NewName = str_utils:rand_hex(8),
            ok = ozw_test_rpc:update_space_name(SpaceId1, NewName),
            {Space1#space{name = NewName}, Space2}
        end,
        expected_events_fun = fun({Space1, Space2}) -> [
            {file_renamed, guid(Space1), Space1#space.name},
            {file_renamed, guid(Space2), Space2#space.name}
        ] end,
        zone_connection_status = ZoneConnectionStatus
    }).


rename_space_test_base(ClientProvider, ZoneConnectionStatus) ->
    test_base(ClientProvider, #test_config{
        setup_fun = fun setup_space/0,
        test_fun = fun(#space{id = SpaceId} = Space) ->
            NewSpaceName = str_utils:rand_hex(8),
            ok = ozw_test_rpc:update_space_name(SpaceId, NewSpaceName),
            Space#space{name = NewSpaceName}
        end,
        expected_events_fun = fun(Space) -> [
            {file_renamed, guid(Space), Space#space.name}
        ] end,
        zone_connection_status = ZoneConnectionStatus
    }).


rename_space_to_existing_name_test_base(ClientProvider, ZoneConnectionStatus) ->
    test_base(ClientProvider, #test_config{
        setup_fun = fun() -> {setup_space(), setup_space()} end,
        test_fun = fun({#space{name = SpaceName1} = Space1, #space{id = SpaceId2} = Space2}) ->
            ok = ozw_test_rpc:update_space_name(SpaceId2, SpaceName1),
            {Space1, Space2#space{name = SpaceName1}}
        end,
        expected_events_fun = fun({Space1, Space2}) -> [
            {file_renamed, guid(Space1), extended_name(Space1)},
            {file_renamed, guid(Space2), extended_name(Space2)}
        ] end,
        zone_connection_status = ZoneConnectionStatus
    }).
    

test_base(ClientProvider, #test_config{
    zone_connection_status = ZoneConnectionStatus,
    setup_fun = SetupFun,
    test_fun = TestFun,
    expected_events_fun = ExpectedEventsFun
}) ->
    put(conn, setup_client_connection(ClientProvider)), % closed in end_per_testcase
    SetupState = SetupFun(),
    maybe_break_connection_to_zone(ZoneConnectionStatus, ClientProvider),
    TestState = TestFun(SetupState),
    maybe_start_connection_to_zone(ZoneConnectionStatus, ClientProvider),
    assert_events_received(ExpectedEventsFun(TestState)).

%%%===================================================================
%%% Helper functions
%%%===================================================================

setup_client_connection(ClientProvider) ->
    UserId = oct_background:get_user_id(?CLIENT_USER),
    GetSessionId = fun(P) -> oct_background:get_user_session_id(?CLIENT_USER, P) end,
    {ok, {Sock, ConnSessId}} = fuse_test_utils:connect_via_token(oct_background:get_random_provider_node(ClientProvider),
        [{active, true}], GetSessionId(ClientProvider), oct_background:get_user_access_token(?CLIENT_USER)),
    UserRootDirGuid = fslogic_file_id:user_root_dir_guid(UserId),
    client_simulation_test_base:create_new_file_subscriptions(Sock, UserRootDirGuid, 0),
    lists:foreach(fun(Sub) ->
        assert_subscribed(ClientProvider, ConnSessId, Sub)
    end, [
        #file_removed_subscription{file_guid = UserRootDirGuid},
        #file_attr_changed_subscription{file_guid = UserRootDirGuid},
        #file_renamed_subscription{file_guid = UserRootDirGuid}
    ]),
    Sock.


setup_space() ->
    SpaceName = str_utils:rand_hex(8),
    setup_space(SpaceName).


setup_space(SpaceName) ->
    Space = #space{name = SpaceName, id = create_supported_space(?SUPPORTING_PROVIDER, SpaceName)},
    assert_event_received({file_attr_changed, guid(Space), Space#space.name}),
    Space.


setup_space_without_support(SpaceName) ->
    UserId = oct_background:get_user_id(?CLIENT_USER),
    Space = #space{name = SpaceName, id = ozw_test_rpc:create_space(UserId, SpaceName)},
    lists:foreach(fun(Provider) ->
        SessId = oct_background:get_user_session_id(UserId, Provider),
        ?assertMatch({ok, _}, opw_test_rpc:call(Provider, space_logic, get, [SessId, Space#space.id]))
    end, oct_background:get_provider_ids()),
    Space.


setup_2_spaces_with_conflicting_names() ->
    SpaceName = str_utils:rand_hex(8),
    Space1 = #space{name = SpaceName, id = create_supported_space(?SUPPORTING_PROVIDER, SpaceName)},
    assert_event_received({file_attr_changed, guid(Space1), SpaceName}),
    Space2 = #space{name = SpaceName, id = create_supported_space(?SUPPORTING_PROVIDER, SpaceName)},
    assert_events_received([
        {file_renamed, guid(Space1), extended_name(Space1)},
        {file_attr_changed, guid(Space2), extended_name(Space2)}
    ]),
    {Space1, Space2}.


assert_subscribed(ClientProvider, SessId, Subscription) ->
    ClientProviderNode = oct_background:get_random_provider_node(ClientProvider),
    {ok, SubscriptionRoutingKey} = subscription_type:get_routing_key(Subscription),
    GetSubs = fun() ->
        {ok, Subs} = rpc:call(ClientProviderNode, subscription_manager, get_subscribers, [SubscriptionRoutingKey]),
        Subs
    end,
    ?assert(lists:member(SessId, GetSubs()), ?ATTEMPTS).


assert_events_received(Events) ->
    lists:foreach(fun assert_event_received/1, Events).


assert_event_received(ExpectedEvent) ->
    GetSavedEventsFun = fun() ->
        receive
            {events, SavedEvents} ->
                SavedEvents
        after 0 ->
                []
        end
    end,
    GetEventsFun = fun() ->
        Events = receive_events() ++ GetSavedEventsFun(),
        self() ! {events, Events -- [ExpectedEvent]},
        Events
    end,
    try
        ?assert(lists:member(ExpectedEvent, GetEventsFun()), ?ATTEMPTS)
    catch C:R:S ->
        ct:pal("Expected event not received: ~p~nAll events: ~p~nStacktrace:~n~s", [ExpectedEvent, GetSavedEventsFun(), lager:pr_stacktrace(S)]),
        erlang:apply(erlang, C, [R])
    end.


receive_events() ->
    receive_events([]).

receive_events(Acc) ->
    case fuse_test_utils:receive_server_message([message_stream_reset, subscription, message_request,
        message_acknowledgement, processing_status], 0)
    of
        {error, timeout} ->
            Acc;
        #'ServerMessage'{message_body = {events, #'Events'{events = EventsList}}} ->
            receive_events([extract_event_data(Event) || #'Event'{type = {_, Event}} <- EventsList] ++ Acc)
    end.


extract_event_data(#'FileAttrChangedEvent'{file_attr = #'FileAttr'{uuid = Guid, name = Name}}) ->
    {file_attr_changed, Guid, Name};
extract_event_data(#'FileRenamedEvent'{top_entry = #'FileRenamedEntry'{old_uuid = Guid, new_name = NewName}}) ->
    {file_renamed, Guid, NewName};
extract_event_data(#'FileRemovedEvent'{file_uuid = Guid}) ->
    {file_removed, Guid}.


create_supported_space(NodeSelector, Name) ->
    create_supported_space(NodeSelector, Name, ?CLIENT_USER).
    
create_supported_space(NodeSelector, Name, UserPlaceholder) ->
    UserId = oct_background:get_user_id(UserPlaceholder),
    SpaceId = ozw_test_rpc:create_space(UserId, Name),
    support_space(NodeSelector, UserId, SpaceId).

support_space(NodeSelector, UserPlaceholder, SpaceId) ->
    UserId = oct_background:get_user_id(UserPlaceholder),
    {ok, SerializedToken} = tokens:serialize(ozw_test_rpc:create_space_support_token(UserId, SpaceId)),
    [StorageId | _] = opw_test_rpc:get_storages(NodeSelector),
    opw_test_rpc:support_space(NodeSelector, StorageId, SerializedToken, 1234321).


maybe_break_connection_to_zone(alive, _) ->
    ok;
maybe_break_connection_to_zone(broken, ProviderSelector) ->
    stop_zone_connection(ProviderSelector),
    ?assert(not opw_test_rpc:call(ProviderSelector, gs_channel_service, is_connected, []), ?ATTEMPTS).


maybe_start_connection_to_zone(alive, _) ->
    ok;
maybe_start_connection_to_zone(broken, ProviderSelector) ->
    start_zone_connection(ProviderSelector).


stop_zone_connection(ProviderSelector) ->
    opw_test_rpc:call(ProviderSelector, gs_channel_service, stop_service, []).


start_zone_connection(ProviderSelector) ->
    opw_test_rpc:call(ProviderSelector, gs_channel_service, start_service, []),
    ?assert(opw_test_rpc:call(ProviderSelector, gs_channel_service, is_connected, []), ?ATTEMPTS).


guid(#space{id = SpaceId}) ->
    fslogic_file_id:spaceid_to_space_dir_guid(SpaceId).

extended_name(#space{id = SpaceId, name = Name}) ->
    <<Name/binary, "@", SpaceId/binary>>.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {provider_token_ttl_sec, 24 * 60 * 60},
            {ignore_async_subscriptions, false}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_, Config) ->
    lists:foreach(fun(Provider) ->
        opw_test_rpc:call(Provider, gs_channel_service, is_connected, []) orelse
            start_zone_connection(Provider)
    end, oct_background:get_provider_ids()),
    lfm_proxy:init(Config).

end_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    ssl:close(get(conn)),
    clean_message_queue(),
    client_simulation_test_base:reset_sequence_counter(),
    test_utils:mock_unload(Nodes).


clean_message_queue() ->
    receive
        _ ->
            clean_message_queue()
    after 0 ->
        ok
    end.