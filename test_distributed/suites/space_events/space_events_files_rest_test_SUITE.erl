%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests for space file events mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(space_events_files_rest_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include("storage_files_test_SUITE.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([
    all/0, groups/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    non_existing_space_test/1,
    invalid_args_test/1,
    deleted_events_test/1,
    changed_or_created_events_test/1,
    nested_directory_not_observed_test/1,
    observe_space_root_test/1,

    unauthorized_client_test/1,
    token_caveats_test/1,
    observer_loses_access_during_monitoring_test/1,
    catching_monitor_with_authorization_changes_test/1,

    reconnect_without_catching_test/1,
    reconnect_with_old_last_event_id_test/1,
    takeover_is_seamless_test/1,

    heartbeat_event_during_inactivity_test/1,
    no_heartbeat_when_receiving_regular_events_test/1,

    multiple_clients_same_directory_test/1,
    multiple_clients_different_directories_test/1,
    multiple_clients_different_attributes_test/1
]).

groups() -> [
    {basic_tests, [sequential], [
        non_existing_space_test,
        invalid_args_test,
        deleted_events_test,
        changed_or_created_events_test,
        nested_directory_not_observed_test,
        observe_space_root_test
    ]},
    {auth_tests, [sequential], [
        unauthorized_client_test,
        token_caveats_test,
        observer_loses_access_during_monitoring_test,
        catching_monitor_with_authorization_changes_test
    ]},
    {reconnect_tests, [sequential], [
        reconnect_without_catching_test,
        reconnect_with_old_last_event_id_test,
        takeover_is_seamless_test
    ]},
    {heartbeat_tests, [sequential], [
        heartbeat_event_during_inactivity_test,
        no_heartbeat_when_receiving_regular_events_test
    ]},
    {multiple_clients_tests, [sequential], [
        multiple_clients_same_directory_test,
        multiple_clients_different_directories_test,
        multiple_clients_different_attributes_test
    ]}
].

all() -> [
    {group, basic_tests},
    {group, auth_tests},
    {group, reconnect_tests},
    {group, heartbeat_tests},
    {group, multiple_clients_tests}
].


-type test_env() :: #{
    setup_provider => oct_background:entity_selector(),
    events_provider => oct_background:entity_selector(),

    space => oct_background:entity_selector(),
    space_id => od_space:id(),
    space_guid => file_id:file_guid(),

    file_owner => oct_background:entity_selector(),
    file_owner_user_id => od_user:id(),
    file_owner_session_id => session:id(),

    file_tree => onenv_file_test_utils:object_spec() | [onenv_file_test_utils:object_spec()],
    observed_dir_guid => file_id:file_guid(),
    work_dir_guid => file_id:file_guid(),

    connecting_user => oct_background:entity_selector(),
    observed_attrs => [onedata_file:attr_name()],
    client_args => map()
}.


-define(ATTEMPTS, 30).

-define(assert_attr_changed_or_created_events(__EXP_ATTR_DATA, __SSE_CLIENT_PID, __FILE_GUID),
    ?assertEqual(
        lists:usort(__EXP_ATTR_DATA),
        lists:usort(get_data_attributes_from_changed_or_created_events(get_events_for_file(__SSE_CLIENT_PID, __FILE_GUID))),
        ?ATTEMPTS
    )
).


%%%===================================================================
%%% Test functions
%%%===================================================================


non_existing_space_test(_Config) ->
    NonExistingSpaceId = <<"dummy_id">>,

    ClientArgs = #{
        node => oct_background:get_random_provider_node(krakow),
        space_id => NonExistingSpaceId,
        token => oct_background:get_user_access_token(user2),
        observed_dirs => []
    },

    ?assertMatch(
        {error, {400, ?ERR_SPACE_NOT_SUPPORTED_BY(NonExistingSpaceId, _)}},
        space_file_events_test_sse_client:start(ClientArgs)
    ).


invalid_args_test(_Config) ->
    SpaceKrkId = oct_background:get_space_id(space_krk_p),
    SpaceKrkGuid = space_dir:guid(SpaceKrkId),
    SpaceKrkObjectId = ?check(file_id:guid_to_objectid(SpaceKrkGuid)),
    FileOwnerUserId = oct_background:get_user_id(user1),

    #object{
        children = [
            #object{guid = ForbiddenDirGuid},
            #object{guid = FileGuid}
        ]
    } = onenv_file_test_utils:create_file_tree(
        FileOwnerUserId, SpaceKrkGuid, krakow, #dir_spec{
            mode = ?FILE_MODE(8#777),
            children = [
                #dir_spec{mode = ?FILE_MODE(8#700)},
                #file_spec{mode = ?FILE_MODE(8#777)}
            ]
        }
    ),

    AllowedAttrs = [
        <<"name">>, <<"index">>, <<"type">>, <<"activePermissionsType">>,
        <<"posixPermissions">>, <<"acl">>,
        <<"parentFileId">>, <<"originProviderId">>, <<"directShareIds">>, <<"ownerUserId">>,
        <<"hardlinkCount">>, <<"symlinkValue">>, <<"creationTime">>, <<"atime">>, <<"mtime">>,
        <<"ctime">>, <<"size">>, <<"isFullyReplicatedLocally">>, <<"localReplicationRate">>
    ],

    ClientArgs = #{
        node => oct_background:get_random_provider_node(krakow),
        space_id => SpaceKrkId,
        token => oct_background:get_user_access_token(user3),
        observed_dirs => [SpaceKrkGuid]
    },

    lists:foreach(fun({Index, {InvalidArgs, ExpError}}) ->
        Args = maps:merge(ClientArgs, InvalidArgs),

        ?assertEqual(
            {Index, {error, {400, ExpError}}},
            {Index, space_file_events_test_sse_client:start(Args)}
        )
    end, lists:enumerate([
        {#{body_bin => <<"ASD">>}, ?ERR_MALFORMED_DATA},
        {#{body_json => #{}}, ?ERR_MISSING_REQUIRED_VALUE(<<"observedDirectories">>)},
        {
            #{body_json => #{<<"observedDirectories">> => <<"ASD">>}},
            ?ERR_BAD_VALUE_LIST_OF_STRINGS(<<"observedDirectories">>)
        },
        {
            #{body_json => #{<<"observedDirectories">> => [1]}},
            ?ERR_BAD_VALUE_LIST_OF_STRINGS(<<"observedDirectories">>)
        },
        {
            #{body_json => #{<<"observedDirectories">> => []}},
            ?ERR_BAD_VALUE_EMPTY(<<"observedDirectories">>)
        },
        {
            #{body_json => #{<<"observedDirectories">> => [<<"ASD">>]}},
            ?ERR_BAD_VALUE_IDENTIFIER(<<"observedDirectories[1]">>)
        },
        {
            #{observed_dirs => [SpaceKrkGuid, FileGuid]},
            ?ERR_BAD_DATA(<<"observedDirectories[2]">>, ?ERR_POSIX(?ENOTDIR))
        },
        {
            #{observed_dirs => [SpaceKrkGuid, ForbiddenDirGuid]},
            ?ERR_BAD_DATA(<<"observedDirectories[2]">>, ?ERR_POSIX(?EACCES))
        },
        {
            #{body_json => #{<<"observedDirectories">> => [SpaceKrkObjectId], <<"observedAttributes">> => <<"ASD">>}},
            ?ERR_BAD_VALUE_NOT_ALLOWED(<<"observedAttributes">>, AllowedAttrs)
        },
        {
            #{body_json => #{<<"observedDirectories">> => [SpaceKrkObjectId], <<"observedAttributes">> => []}},
            ?ERR_BAD_VALUE_EMPTY(<<"observedAttributes">>)
        },
        {
            #{body_json => #{<<"observedDirectories">> => [SpaceKrkObjectId], <<"observedAttributes">> => [<<"ASD">>]}},
            ?ERR_BAD_VALUE_NOT_ALLOWED(<<"observedAttributes">>, AllowedAttrs)
        },
        {
            #{headers => [{<<"last-event-id">>, <<"last">>}]},
            ?ERR_BAD_VALUE_INTEGER(<<"last-event-id">>)
        },
        {
            #{headers => [{<<"last-event-id">>, <<"-1">>}]},
            ?ERR_BAD_VALUE_TOO_LOW(<<"last-event-id">>, 0)
        }
    ])).


deleted_events_test(_Config) ->
    TestEnv = create_multi_provider_test_env(#{
        file_tree_spec => #dir_spec{
            mode = ?FILE_MODE(8#777),
            children = [#dir_spec{}, #file_spec{}]
        }
    }),

    #{
        file_owner_user_id := FileOwnerUserId,
        file_tree := #object{children = [
            #object{guid = ChildDirGuid},
            #object{guid = ChildFileGuid}
        ]}
    } = TestEnv,

    ClientPid = start_client(TestEnv),

    % Removing file in observed dir should result in event
    % NOTE: rm will choose random provider for removal (not necessarily krakow)
    onenv_file_test_utils:rm_and_sync_file(FileOwnerUserId, ChildFileGuid),
    ?assert(count_file_deleted_events(get_events_for_file(ClientPid, ChildFileGuid)) > 0, ?ATTEMPTS),

    %% TODO VFS-12134 Assert deleted child event
    % while deleting dir, at least for now, does not produce events
    % NOTE: rm will choose random provider for removal (not necessarily krakow)
    onenv_file_test_utils:rm_and_sync_file(FileOwnerUserId, ChildDirGuid),
    ?assertEqual(0, count_file_deleted_events(get_events_for_file(ClientPid, ChildDirGuid)), ?ATTEMPTS),

    ok = space_file_events_test_sse_client:stop(ClientPid).


changed_or_created_events_test(_Config) ->
    ClientProvider = krakow,
    ModifyingProvider = ?RAND_ELEMENT([krakow, paris]),
    ct:pal("Provider with SSE client: ~ts~nProvider modifying data: ~ts", [ClientProvider, ModifyingProvider]),

    TestEnv = create_multi_provider_test_env(#{
        setup_provider => ModifyingProvider,
        events_provider => ClientProvider,
        file_tree_spec => #dir_spec{
            mode = ?FILE_MODE(8#777),
            children = [#dir_spec{}, #file_spec{}]
        },
        observed_attrs => [?attr_name, ?attr_mode, ?attr_size]
    }),
    FileOwnerUserId = maps:get(file_owner_user_id, TestEnv),
    ObservedDirGuid = maps:get(observed_dir_guid, TestEnv),

    SSEClientPid = start_client(TestEnv),

    % Creating new files in observed dir should result in its events for all observed documents
    ChildFileName = ?RAND_STR(),
    #object{guid = ChildFileGuid} = onenv_file_test_utils:create_file_tree(
        FileOwnerUserId, ObservedDirGuid, ModifyingProvider, #file_spec{name = ChildFileName}
    ),

    ExpAttrsForAttrChangedEvents1 = [
        #{<<"name">> => ChildFileName, <<"posixPermissions">> => <<"664">>},
        #{<<"size">> => 0}
    ],
    ?assert_attr_changed_or_created_events(ExpAttrsForAttrChangedEvents1, SSEClientPid, ChildFileGuid),

    % mode change should result in event
    Node = oct_background:get_random_provider_node(ModifyingProvider),
    FileOwnerSessionId = oct_background:get_user_session_id(FileOwnerUserId, ModifyingProvider),
    ?assertMatch(ok, lfm_proxy:set_perms(Node, FileOwnerSessionId, ?FILE_REF(ChildFileGuid), 8#740)),

    ExpAttrsForAttrChangedEvents2 = ExpAttrsForAttrChangedEvents1 ++ [
        #{<<"name">> => ChildFileName, <<"posixPermissions">> => <<"740">>}
    ],
    ?assert_attr_changed_or_created_events(ExpAttrsForAttrChangedEvents2, SSEClientPid, ChildFileGuid),

    ok = space_file_events_test_sse_client:stop(SSEClientPid).


nested_directory_not_observed_test(_Config) ->
    % Create test env with directory structure: observed_dir/subdir
    TestEnv = create_single_provider_test_env(#{
        file_tree_spec => #dir_spec{
            mode = ?FILE_MODE(8#777),
            children = [#dir_spec{mode = ?FILE_MODE(8#777)}]
        }
    }),
    #object{children = [#object{guid = SubDirGuid}]} = maps:get(file_tree, TestEnv),

    % Start client observing parent directory only
    ClientPid = start_client(TestEnv),

    % Create file in subdirectory → should NOT receive event (not recursive)
    FileOwnerUserId = maps:get(file_owner_user_id, TestEnv),
    SetupProvider = maps:get(setup_provider, TestEnv),
    #object{guid = FileInSubDirGuid} = onenv_file_test_utils:create_file_tree(
        FileOwnerUserId, SubDirGuid, SetupProvider, #file_spec{name = ?RAND_STR()}
    ),

    % Create file directly in observed directory → should receive event
    [FileInParentGuid] = generate_n_events(1, TestEnv, ClientPid),

    % ASSERTIONS: Only direct children events, NOT recursive
    ?assert(length(get_events_for_file(ClientPid, FileInParentGuid)) > 0, ?ATTEMPTS),
    ?assertEqual([], get_events_for_file(ClientPid, FileInSubDirGuid)),

    % Cleanup
    ok = space_file_events_test_sse_client:stop(ClientPid).


observe_space_root_test(_Config) ->
    % Create test env observing space root instead of subdirectory
    SpaceId = oct_background:get_space_id(space_krk_p),
    SpaceGuid = space_dir:guid(SpaceId),
    
    TestEnv = create_single_provider_test_env(#{}),

    % Override client to observe space root instead
    ClientArgs = maps:get(client_args, TestEnv),
    ClientArgsForRoot = ClientArgs#{observed_dirs => [SpaceGuid]},
    ClientPid = start_client(TestEnv#{client_args => ClientArgsForRoot}),

    % Create file directly in space root → should receive event
    FileOwnerUserId = maps:get(file_owner_user_id, TestEnv),
    SetupProvider = maps:get(setup_provider, TestEnv),
    
    FileSpecInRoot = #file_spec{name = ?RAND_STR()},
    #object{guid = FileInRootGuid} = onenv_file_test_utils:create_file_tree(
        FileOwnerUserId, SpaceGuid, SetupProvider, FileSpecInRoot
    ),
    await_event_for_file(ClientPid, FileInRootGuid),

    % Cleanup
    ok = space_file_events_test_sse_client:stop(ClientPid).


unauthorized_client_test(_Config) ->
    SpaceKrkId = oct_background:get_space_id(space_krk_p),
    SpaceKrkGuid = space_dir:guid(SpaceKrkId),

    ClientArgs = #{
        node => oct_background:get_random_provider_node(krakow),
        space_id => SpaceKrkId,
        observed_dirs => [SpaceKrkGuid]
    },

    % no token == guest auth
    ?assertMatch(
        {error, {401, ?ERR_UNAUTHORIZED(undefined)}},
        space_file_events_test_sse_client:start(ClientArgs)
    ),
    % user not belonging to space
    ?assertMatch(
        {error, {403, ?ERR_FORBIDDEN}},
        space_file_events_test_sse_client:start(ClientArgs#{
            token => oct_background:get_user_access_token(user2)
        })
    ).


token_caveats_test(_Config) ->
    SpaceKrkId = oct_background:get_space_id(space_krk_p),
    SpaceKrkGuid = space_dir:guid(SpaceKrkId),
    Token = oct_background:get_user_access_token(user3),

    ClientArgs = #{
        node => oct_background:get_random_provider_node(krakow),
        space_id => SpaceKrkId,
        token => Token,
        observed_dirs => [SpaceKrkGuid]
    },

    % Request containing data caveats should succeed
    DataCaveat = #cv_data_path{whitelist = [<<"/", SpaceKrkId/binary>>]},
    TokenWithDataCaveat = tokens:confine(Token, DataCaveat),
    {ok, ClientWithDataCaveat} = ?assertMatch(
        {ok, _},
        space_file_events_test_sse_client:start(ClientArgs#{token => TokenWithDataCaveat})
    ),
    ok = space_file_events_test_sse_client:stop(ClientWithDataCaveat),

    % Request containing invalid api caveat should be rejected
    InvalidApiCaveat = #cv_api{whitelist = [
        % valid caveat - operation check user perms and as such permission to get user record is required
        {all, all, ?GRI_PATTERN(od_user, <<"*">>, <<"instance">>, '*')},
        % invalid caveat
        {all, all, ?GRI_PATTERN(op_space, <<"ASD">>, <<"changes">>)}
    ]},
    TokenWithInvalidApiCaveat = tokens:confine(Token, InvalidApiCaveat),
    ?assertMatch(
        {error, {401, ?ERR_UNAUTHORIZED(?ERR_TOKEN_CAVEAT_UNVERIFIED(InvalidApiCaveat))}},
        space_file_events_test_sse_client:start(ClientArgs#{token => TokenWithInvalidApiCaveat})
    ),

    % Request containing valid api caveat should succeed
    ValidApiCaveat = #cv_api{whitelist = [
        {all, all, ?GRI_PATTERN(od_user, <<"*">>, <<"instance">>, '*')},
        {all, all, ?GRI_PATTERN(op_space, SpaceKrkId, <<"file_events">>)}
    ]},
    TokenWithValidApiCaveat = tokens:confine(Token, ValidApiCaveat),
    {ok, ClientWithApiCaveat} = ?assertMatch(
        {ok, _},
        space_file_events_test_sse_client:start(ClientArgs#{token => TokenWithValidApiCaveat})
    ),
    ok = space_file_events_test_sse_client:stop(ClientWithApiCaveat).


observer_loses_access_during_monitoring_test(_Config) ->
    % Create file with mode=777 so connecting user (non-owner) can access it initially
    TestEnv = create_single_provider_test_env(#{}),
    ObservedDirGuid = maps:get(observed_dir_guid, TestEnv),
    ProviderNode = oct_background:get_random_provider_node(maps:get(setup_provider, TestEnv)),
    FileOwnerSessionId = oct_background:get_user_session_id(maps:get(file_owner_user_id, TestEnv), maps:get(setup_provider, TestEnv)),

    ClientArgs = maps:get(client_args, TestEnv),
    ControlClientPid = start_client(TestEnv#{client_args => ClientArgs#{
        token => oct_background:get_user_access_token(maps:get(file_owner, TestEnv))
    }}),
    ClientPid = start_client(TestEnv),

    FileGuid = create_file_and_await_sync(TestEnv, <<"file1.txt">>, ControlClientPid),
    await_event_for_file(ClientPid, FileGuid),
    
    % Change file permissions to 700 (only owner has access)
    ok = lfm_proxy:set_perms(ProviderNode, FileOwnerSessionId, ?FILE_REF(ObservedDirGuid), 8#700),

    % No event should be received after losing access
    FileGuid2 = create_file_and_await_sync(TestEnv, <<"file2.txt">>, ControlClientPid),
    ?assertEqual([], get_events_for_file(ClientPid, FileGuid2)),
    FileGuid2Events = get_events_for_file(ControlClientPid, FileGuid2),

    % Change file permissions to 770 (owner and connecting user have access)
    ok = lfm_proxy:set_perms(ProviderNode, FileOwnerSessionId, ?FILE_REF(ObservedDirGuid), 8#770),

    % After regaining access, client should receive new event (but omitted events will not be received)
    FileGuid3 = create_file_and_await_sync(TestEnv, <<"file3.txt">>, ControlClientPid),
    await_event_for_file(ClientPid, FileGuid3),
    ?assertEqual(FileGuid2Events, FileGuid2Events -- ensure_events(ClientPid)),

    % Cleanup
    ok = space_file_events_test_sse_client:stop(ControlClientPid),
    ok = space_file_events_test_sse_client:stop(ClientPid).


catching_monitor_with_authorization_changes_test(_Config) ->
    % Goal: Verify authorization is checked LIVE during catching replay (not stale)
    % When catching monitor replays events, it should use CURRENT permissions,
    % not permissions from when events were generated

    TestEnv = create_single_provider_test_env(#{
        observed_attrs => [?attr_name, ?attr_acl]
    }),

    ObservedDirGuid = maps:get(observed_dir_guid, TestEnv),
    ProviderNode = oct_background:get_random_provider_node(maps:get(setup_provider, TestEnv)),
    FileOwnerSessionId = maps:get(file_owner_session_id, TestEnv),

    ClientArgs = maps:get(client_args, TestEnv),
    ControlClientPid = start_client(TestEnv#{client_args => ClientArgs#{
        token => oct_background:get_user_access_token(maps:get(file_owner, TestEnv))
    }}),
    ClientPid = start_client(TestEnv),

    ok = lfm_proxy:set_perms(ProviderNode, FileOwnerSessionId, ?FILE_REF(ObservedDirGuid), 8#700),

    FileGuid = create_file_and_await_sync(TestEnv, #file_spec{mode = ?FILE_MODE(8#777)}, ControlClientPid),
    assert_no_event_for_file(ClientPid, FileGuid),

    LastEventId = get_event_id(hd(ensure_events(ControlClientPid))),

    ok = space_file_events_test_sse_client:stop(ClientPid),

    ok = lfm_proxy:set_perms(ProviderNode, FileOwnerSessionId, ?FILE_REF(ObservedDirGuid), 8#777),

    ct:pal("Reconnecting with old Last-Event-Id (catching monitor should replay with LIVE auth)..."),
    ReconnectedPid = start_client(TestEnv, LastEventId - 1),

    await_event_for_file(ReconnectedPid, FileGuid),

    % Cleanup
    ok = space_file_events_test_sse_client:stop(ControlClientPid),
    ok = space_file_events_test_sse_client:stop(ReconnectedPid).


reconnect_without_catching_test(_Config) ->
    TestEnv = create_single_provider_test_env(#{}),

    % Start control client (for synchronization) and client that will reconnect
    ControlClientPid = start_client(TestEnv),
    Client2Pid = start_client(TestEnv),

    % Create File1 and ensure both clients receive it
    File1Guid = create_file_and_await_sync(TestEnv, <<"file1.txt">>, ControlClientPid),
    await_event_for_file(Client2Pid, File1Guid),

    % Get Last-Event-Id for randomization
    LastEventId = get_last_event_id(Client2Pid),

    % Disconnect Client2
    ok = space_file_events_test_sse_client:stop(Client2Pid),

    % Create File2 and File3 while Client2 is disconnected
    File2Guid = create_file_and_await_sync(TestEnv, <<"file2.txt">>, ControlClientPid),
    File3Guid = create_file_and_await_sync(TestEnv, <<"file3.txt">>, ControlClientPid),

    % Reconnect WITHOUT catching monitor (randomize scenario)
    Client2ReconnectedPid = case rand:uniform(2) of
        1 ->
            % Scenario 1: No Last-Event-Id header (fresh connection)
            ct:pal("Testing reconnect WITHOUT Last-Event-Id header"),
            start_client(TestEnv);
        2 ->
            % Scenario 2: Future Last-Event-Id (client claims to be ahead)
            FutureEventId = LastEventId + 1000,
            ct:pal("Testing reconnect WITH future Last-Event-Id: ~B (current was ~B)", [FutureEventId, LastEventId]),
            start_client(TestEnv, FutureEventId)
    end,

    % Create File4 after reconnection
    File4Guid = create_file_and_await_sync(TestEnv, <<"file4.txt">>, ControlClientPid),
    await_event_for_file(Client2ReconnectedPid, File4Guid),

    % CRITICAL: Verify Client2 does NOT have historical events (File2, File3)
    % This confirms no catching monitor was started
    assert_no_event_for_file(Client2ReconnectedPid, File2Guid),
    assert_no_event_for_file(Client2ReconnectedPid, File3Guid),

    % Cleanup
    ok = space_file_events_test_sse_client:stop(ControlClientPid),
    ok = space_file_events_test_sse_client:stop(Client2ReconnectedPid).


reconnect_with_old_last_event_id_test(_Config) ->
    TestEnv = create_single_provider_test_env(#{}),

    % Start control client for synchronization
    ControlClientPid = start_client(TestEnv),

    % Start Client2 that will reconnect
    Client2Pid = start_client(TestEnv),

    % Create File1 - both clients receive it
    File1Guid = create_file_and_await_sync(TestEnv, <<"file1.txt">>, ControlClientPid),
    await_event_for_file(Client2Pid, File1Guid),

    % Extract Last-Event-Id from Client2
    LastEventId = get_last_event_id(Client2Pid),

    % Disconnect Client2
    ok = space_file_events_test_sse_client:stop(Client2Pid),

    % Generate 3 events while Client2 is disconnected (advancing sequence)
    File2Guid = create_file_and_await_sync(TestEnv, <<"file2.txt">>, ControlClientPid),
    File3Guid = create_file_and_await_sync(TestEnv, <<"file3.txt">>, ControlClientPid),
    File4Guid = create_file_and_await_sync(TestEnv, <<"file4.txt">>, ControlClientPid),

    % Reconnect Client2 with old Last-Event-Id
    Client2ReconnectedPid = start_client(TestEnv, LastEventId),

    % Wait for Client2 to receive all historical events (File2, File3, File4)
    await_event_for_file(Client2ReconnectedPid, File2Guid),
    await_event_for_file(Client2ReconnectedPid, File3Guid),
    await_event_for_file(Client2ReconnectedPid, File4Guid),

    % Generate new event after reconnection
    File5Guid = create_file_and_await_sync(TestEnv, <<"file5.txt">>, ControlClientPid),

    % Verify Client2 receives new event from main monitor
    await_event_for_file(Client2ReconnectedPid, File5Guid),

    % Verify event IDs are sequential (no gaps, no duplicates)
    assert_all_client_events_sequential(Client2ReconnectedPid),
    assert_all_client_events_sequential(ControlClientPid),

    % Cleanup
    ok = space_file_events_test_sse_client:stop(ControlClientPid),
    ok = space_file_events_test_sse_client:stop(Client2ReconnectedPid).


takeover_is_seamless_test(_Config) ->
    % Goal: Verify takeover from catching monitor to main monitor is seamless:
    % - No gaps in received events (all expected files received)
    % - No duplicate events
    % - Event IDs in ascending order
    % - Events generated DURING catching are also received (concurrent writes test)
    
    TestEnv = create_single_provider_test_env(#{}),
    
    % 1. Start client, receive initial event
    ClientPid = start_client(TestEnv),
    ControlClientPid = start_client(TestEnv),
    
    InitialFileGuid = create_file_and_await_sync(TestEnv, <<"initial.txt">>, ControlClientPid),
    await_event_for_file(ClientPid, InitialFileGuid),
    
    % Get Last-Event-Id before disconnecting
    LastEventIdBeforeDisconnect = get_last_event_id(ClientPid),
    
    % 2. Stop client
    ok = space_file_events_test_sse_client:stop(ClientPid),
    
    % 3. Generate MANY events while disconnected (ensures catching monitor needed)
    ct:pal("Generating 150 missed events while client disconnected..."),
    MissedFileGuids = generate_n_events(150, TestEnv, ControlClientPid),
    
    % 4. Reconnect with old Last-Event-Id (catching monitor starts)
    ct:pal("Reconnecting with old Last-Event-Id=~B (catching monitor should start)...", 
        [LastEventIdBeforeDisconnect]),
    ReconnectedPid = start_client(TestEnv, LastEventIdBeforeDisconnect),
    
    % 5. While catching up, generate MORE events (tests concurrent writes during catching/takeover)
    ct:pal("Generating 30 additional events DURING catching phase..."),
    ConcurrentFileGuids = generate_n_events(30, TestEnv, ControlClientPid),

    % 6. Wait for ALL events to arrive (catching completes, takeover happens, main continues)
    AllExpectedFileGuids = MissedFileGuids ++ ConcurrentFileGuids,
    ct:pal("Waiting for all ~B events to be received (takeover should complete)...", 
        [length(AllExpectedFileGuids)]),
    
    lists:foreach(fun(Guid) -> 
        await_event_for_file(ReconnectedPid, Guid)
    end, AllExpectedFileGuids),
    
    % 7. ASSERTIONS: Seamless takeover
    % skip first event as it concerns initial.txt file not replayed after reconnection
    ControlFileEvents = tl([E || E <- ensure_events(ControlClientPid), is_file_event(E)]),
    ReceivedFileEvents = [E || E <- ensure_events(ReconnectedPid), is_file_event(E)],
        
    ct:pal("Received ~B file events total", [length(ReceivedFileEvents)]),
    
    assert_all_client_events_sequential(ReconnectedPid),

    ?assertEqual(ControlFileEvents, ReceivedFileEvents),

    % Cleanup
    ok = space_file_events_test_sse_client:stop(ControlClientPid),
    ok = space_file_events_test_sse_client:stop(ReconnectedPid).


heartbeat_event_during_inactivity_test(_Config) ->
    % Create test env with two directories - observed and unobserved
    TestEnv = create_single_provider_test_env(#{
        file_tree_spec => [
            #dir_spec{mode = ?FILE_MODE(8#777)},
            #dir_spec{mode = ?FILE_MODE(8#777)}
        ]
    }),
    [_ObservedDirObject, UnobservedDirObject] = maps:get(file_tree, TestEnv),
    UnobservedDirGuid = UnobservedDirObject#object.guid,

    % Start control client observing unobserved dir (for sync) and test client observing ONLY "observed" directory
    ClientArgs = maps:get(client_args, TestEnv),
    ClientPid = start_client(TestEnv),
    ControlClientArgs = ClientArgs#{observed_dirs => [UnobservedDirGuid]},
    ControlClientPid = start_client(TestEnv#{client_args => ControlClientArgs}),

    % Generate 150 file changes in UNOBSERVED directory
    % This advances space sequence but ClientPid doesn't receive file events
    generate_n_events(150, TestEnv#{work_dir_guid => UnobservedDirGuid}, ControlClientPid),

    % - Client receives at least one heartbeat event (gap > 100 threshold)
    ?assert(count_heartbeat_events(ClientPid) >= 1, ?ATTEMPTS),
    % - No file change events received (files were in unobserved directory)
    ?assertEqual(0, count_file_events(ClientPid)),

    % Verify heartbeat event has correct structure
    {ok, ControlClientEvents} = space_file_events_test_sse_client:get_events(ControlClientPid),
    FirstControlClientEventId = get_event_id(hd(ControlClientEvents)),
    LastControlClientEventId = get_event_id(lists:last(ControlClientEvents)),

    LastHeartbeatEventId = get_event_id(lists:last(get_heartbeat_events(ClientPid))),

    ?assert(LastHeartbeatEventId - FirstControlClientEventId >= 100),
    ?assert(LastControlClientEventId - LastHeartbeatEventId < 100),

    % Cleanup
    ok = space_file_events_test_sse_client:stop(ControlClientPid),
    ok = space_file_events_test_sse_client:stop(ClientPid).


no_heartbeat_when_receiving_regular_events_test(_Config) ->
    TestEnv = create_single_provider_test_env(#{}),

    ClientPid = start_client(TestEnv),

    % Generate 50 file changes in OBSERVED directory
    % Each change updates client's last_seen_seq
    generate_n_events(50, TestEnv, ClientPid),

    % - Client receives at least 50 file change events
    ?assert(count_file_events(ClientPid) >= 50, ?ATTEMPTS),
    % - NO heartbeat events received (last_seen_seq stays current)
    ?assertEqual(0, count_heartbeat_events(ClientPid)),

    % Cleanup
    ok = space_file_events_test_sse_client:stop(ClientPid).


multiple_clients_same_directory_test(_Config) ->
    TestEnv = create_single_provider_test_env(#{}),

    % Start 3 clients observing the same directory
    Client1Pid = start_client(TestEnv),
    Client2Pid = start_client(TestEnv),
    Client3Pid = start_client(TestEnv),

    % Generate a file change event
    generate_n_events(1, TestEnv, Client1Pid),

    % ASSERTIONS: All 3 clients receive identical event
    ?assertEqual(ensure_events(Client1Pid), ensure_events(Client2Pid), ?ATTEMPTS),
    ?assertEqual(ensure_events(Client2Pid), ensure_events(Client3Pid)),

    % Cleanup
    ok = space_file_events_test_sse_client:stop(Client1Pid),
    ok = space_file_events_test_sse_client:stop(Client2Pid),
    ok = space_file_events_test_sse_client:stop(Client3Pid).


multiple_clients_different_directories_test(_Config) ->
    % Create test env with two separate directories
    TestEnv = create_single_provider_test_env(#{
        file_tree_spec => [
            #dir_spec{mode = ?FILE_MODE(8#777)},
            #dir_spec{mode = ?FILE_MODE(8#777)}
        ]
    }),
    [Dir1Object, Dir2Object] = maps:get(file_tree, TestEnv),
    Dir1Guid = Dir1Object#object.guid,
    Dir2Guid = Dir2Object#object.guid,

    % Start Client1 observing Dir1, Client2 observing Dir2
    ClientArgs = maps:get(client_args, TestEnv),
    Client1Pid = start_client(TestEnv#{client_args => ClientArgs#{observed_dirs => [Dir1Guid]}}),
    Client2Pid = start_client(TestEnv#{client_args => ClientArgs#{observed_dirs => [Dir2Guid]}}),

    % Create file in Dir1
    [File1Guid] = generate_n_events(1, TestEnv#{work_dir_guid => Dir1Guid}, Client1Pid),

    % Create file in Dir2
    [File2Guid] = generate_n_events(1, TestEnv#{work_dir_guid => Dir2Guid}, Client2Pid),

    % ASSERTIONS: Clients receive only events for their observed directories
    ?assert(length(get_events_for_file(Client1Pid, File1Guid)) > 0),
    ?assertEqual([], get_events_for_file(Client1Pid, File2Guid)),

    ?assert(length(get_events_for_file(Client2Pid, File2Guid)) > 0),
    ?assertEqual([], get_events_for_file(Client2Pid, File1Guid)),

    Client1Events = ensure_events(Client1Pid),
    Client2Events = ensure_events(Client2Pid),
    ?assertEqual(Client1Events, Client1Events -- Client2Events),

    % Cleanup
    ok = space_file_events_test_sse_client:stop(Client1Pid),
    ok = space_file_events_test_sse_client:stop(Client2Pid).


multiple_clients_different_attributes_test(_Config) ->
    TestEnv = create_single_provider_test_env(#{
        file_tree_spec => #dir_spec{
            mode = ?FILE_MODE(8#777),
            children = [#file_spec{mode = ?FILE_MODE(8#644)}]
        },
        observed_attrs => [?attr_mode]  % Default for Client1
    }),
    #object{children = [#object{guid = FileGuid}]} = maps:get(file_tree, TestEnv),
    SetupProvider = maps:get(setup_provider, TestEnv),

    % Start Client1 with observed_attrs=[size]
    Client1Pid = start_client(TestEnv),

    % Start Client2 with observed_attrs=[mode]
    ClientArgs = maps:get(client_args, TestEnv),
    Client2Args = ClientArgs#{observed_attrs => [?attr_size]},
    Client2Pid = start_client(TestEnv#{client_args => Client2Args}),

    % Change file mode (Client2 should see this)
    ProviderNode = oct_background:get_random_provider_node(SetupProvider),
    FileOwnerSessionId = maps:get(file_owner_session_id, TestEnv),
    ?assertMatch(ok, lfm_proxy:set_perms(ProviderNode, FileOwnerSessionId, ?FILE_REF(FileGuid), 8#755)),

    % Write data to file (Client1 should see size change)
    {ok, Handle} = lfm_proxy:open(ProviderNode, FileOwnerSessionId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(ProviderNode, Handle, 0, <<"test data">>),
    ok = lfm_proxy:close(ProviderNode, Handle),

    ?assert_attr_changed_or_created_events([#{<<"posixPermissions">> => <<"755">>}], Client1Pid, FileGuid),
    ?assert_attr_changed_or_created_events([#{<<"size">> => 9}], Client2Pid, FileGuid),

    FileGuid2 = create_file_and_await_sync(TestEnv, ?RAND_STR(), Client1Pid),

    ?assert_attr_changed_or_created_events([#{<<"posixPermissions">> => <<"664">>}], Client1Pid, FileGuid2),
    ?assert_attr_changed_or_created_events([#{<<"size">> => 0}], Client2Pid, FileGuid2),

    % Cleanup
    ok = space_file_events_test_sse_client:stop(Client1Pid),
    ok = space_file_events_test_sse_client:stop(Client2Pid).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    opt:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_single_provider_test_env(map()) -> test_env().
create_single_provider_test_env(Opts) ->
    create_test_env(Opts).


%% @private
-spec create_multi_provider_test_env(map()) -> test_env().
create_multi_provider_test_env(Opts) ->
    Defaults = #{
        setup_provider => krakow,
        events_provider => paris,
        space => space_krk_par_p,
        connecting_user => user2,
        observed_attrs => [?attr_name, ?attr_mode, ?attr_size]
    },
    create_test_env(maps:merge(Defaults, Opts)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates standard test environment for space file events tests.
%% Returns common test setup including space, user, observed directory, and client args.
%%
%% Options:
%%   - setup_provider: selector of a setup provider (default: krakow)
%%   - events_provider: selector of a provider clients connect to for events (default: <setup_provider>)
%%   - space: selector of target space (default: space_krk_p)
%%   - file_owner: selector of user creating files (default: user1)
%%   - file_tree_spec: description of initial file_tree to create in space root (default: single dir)
%%   - connecting_user: selector of user subscribing for events (default: user3)
%%   - observed_attrs: list of attributes to observe (default: [name, size])
%% @end
%%--------------------------------------------------------------------
-spec create_test_env(map()) -> test_env().
create_test_env(Opts) ->
    SetupProviderSelector = maps:get(setup_provider, Opts, krakow),
    EventsProviderSelector = maps:get(events_provider, Opts, SetupProviderSelector),
    EventsProviderNode = oct_background:get_random_provider_node(EventsProviderSelector),

    SpaceSelector = maps:get(space, Opts, space_krk_p),
    SpaceId = oct_background:get_space_id(SpaceSelector),
    SpaceGuid = space_dir:guid(SpaceId),

    FileOwner = maps:get(file_owner, Opts, user1),
    FileOwnerUserId = oct_background:get_user_id(FileOwner),
    FileOwnerSessionId = oct_background:get_user_session_id(FileOwner, SetupProviderSelector),

    FileTreeSpec = maps:get(file_tree_spec, Opts, #dir_spec{mode = ?FILE_MODE(8#777)}),
    FileTree = onenv_file_test_utils:create_and_sync_file_tree(
        FileOwnerUserId, SpaceGuid, FileTreeSpec, SetupProviderSelector
    ),
    ObservedDirGuid = case FileTree of
        #object{guid = G1} -> G1;
        [#object{guid = G2} | _] -> G2  % You may want more than one dir, but the first one will be observed
    end,

    ConnectingUser = maps:get(connecting_user, Opts, user3),
    ObservedAttrs = maps:get(observed_attrs, Opts, [?attr_name, ?attr_size]),

    ClientArgs = #{
        node => EventsProviderNode,
        space_id => SpaceId,
        token => oct_background:get_user_access_token(ConnectingUser),
        observed_dirs => [ObservedDirGuid],
        observed_attrs => ObservedAttrs
    },

    #{
        setup_provider => SetupProviderSelector,
        events_provider => EventsProviderSelector,

        space => SpaceSelector,
        space_id => SpaceId,
        space_guid => SpaceGuid,

        file_owner => FileOwner,
        file_owner_user_id => FileOwnerUserId,
        file_owner_session_id => FileOwnerSessionId,

        file_tree => FileTree,
        observed_dir_guid => ObservedDirGuid,
        work_dir_guid => ObservedDirGuid,

        connecting_user => ConnectingUser,
        observed_attrs => ObservedAttrs,
        client_args => ClientArgs
    }.


%% @private
-spec start_client(test_env()) -> pid().
start_client(TestEnv) ->
    start_client(TestEnv, undefined).


%% @private
-spec start_client(test_env(), undefined | non_neg_integer() | binary()) -> pid().
start_client(TestEnv, LastEventId) ->
    BasicClientArgs = maps:get(client_args, TestEnv),
    ClientArgsWithHeader = case LastEventId of
        undefined ->
            BasicClientArgs;
        _ ->
            BasicClientArgs#{
                headers => [{<<"Last-Event-Id">>, str_utils:to_binary(LastEventId)}]
            }
    end,
    {ok, Pid} = ?assertMatch({ok, _}, space_file_events_test_sse_client:start(ClientArgsWithHeader)),
    Pid.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates N events by creating files.
%% Uses control client to ensure all events are in the system.
%% Returns list of created file GUIDs.
%% @end
%%--------------------------------------------------------------------
generate_n_events(N, TestEnv, ControlClientPid) ->
    lists_utils:pmap(fun(_) ->
        create_file_and_await_sync(TestEnv, #file_spec{}, ControlClientPid)
    end, lists:seq(1, N)).


%% @private
create_file_and_await_sync(TestEnv, FileName, ControlClientPid) when is_binary(FileName) ->
    create_file_and_await_sync(TestEnv, #file_spec{name = FileName}, ControlClientPid);

create_file_and_await_sync(TestEnv, FileSpec, ControlClientPid) ->
    SetupProvider = maps:get(setup_provider, TestEnv),
    FileOwnerUserId = maps:get(file_owner_user_id, TestEnv),
    WorkDirGuid = maps:get(work_dir_guid, TestEnv),

    #object{guid = FileGuid} = onenv_file_test_utils:create_file_tree(
        FileOwnerUserId, WorkDirGuid, SetupProvider, FileSpec
    ),
    await_event_for_file(ControlClientPid, FileGuid),
    FileGuid.


%% @private
assert_all_client_events_sequential(EventsOrClientPid) ->
    Events = ensure_events(EventsOrClientPid),

    EventIds = [get_event_id(E) || E <- Events],
    % Check that EventIds are sorted (ascending order) AND no duplicates
    % Note: gaps are OK (other documents in space may create sequence gaps)
    ?assertEqual(EventIds, lists:usort(EventIds)).


%% @private
await_event_for_file(ClientPid, FileGuid) ->
    ?assert(length(get_events_for_file(ClientPid, FileGuid)) > 0, ?ATTEMPTS).


%% @private
assert_no_event_for_file(ClientPid, FileGuid) ->
    ?assertEqual([], get_events_for_file(ClientPid, FileGuid)).


%% @private
get_events_for_file(SSEClientPid, FileGuid) ->
    {ok, Events} = space_file_events_test_sse_client:get_events(SSEClientPid),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    lists:filter(fun(Event) ->
        case is_file_event(Event) of
            true ->
                #{data := [EventData]} = Event,
                maps:get(<<"fileId">>, EventData) =:= FileObjectId;
            false ->
                false
        end
    end, Events).


%% @private
get_last_event_id(EventsOrClientPid) ->
    case ensure_events(EventsOrClientPid) of
        [] -> undefined;
        Events -> get_event_id(lists:last(Events))
    end.


%% @private
get_event_id(Event) ->
    binary_to_integer(maps:get(last_event_id, Event)).


%% @private
count_heartbeat_events(EventsOrClientPid) ->
    length(get_heartbeat_events(EventsOrClientPid)).


%% @private
get_heartbeat_events(EventsOrClientPid) ->
    filter_events(EventsOrClientPid, fun is_heartbeat_event/1).


%% @private
count_file_events(EventsOrClientPid) ->
    length(filter_events(EventsOrClientPid, fun is_file_event/1)).


%% @private
count_file_deleted_events(Events) ->
    length(filter_events(Events, fun is_file_deleted_event/1)).


%% @private
filter_events(EventsOrClientPid, Predicate) ->
    [Event || Event <- ensure_events(EventsOrClientPid), Predicate(Event)].


%% @private
ensure_events(ClientPid) when is_pid(ClientPid) ->
    {ok, Events} = space_file_events_test_sse_client:get_events(ClientPid),
    Events;
ensure_events(Events) when is_list(Events) ->
    Events.


%% @private
is_heartbeat_event(#{event_type := <<"heartbeat">>}) -> true;
is_heartbeat_event(_) -> false.


%% @private
is_file_event(#{event_type := <<"changedOrCreated">>}) -> true;
is_file_event(#{event_type := <<"deleted">>}) -> true;
is_file_event(_) -> false.


%% @private
is_file_deleted_event(#{event_type := <<"deleted">>}) -> true;
is_file_deleted_event(_) -> false.


%% @private
get_data_attributes_from_changed_or_created_events(Events) ->
    lists:filtermap(fun
        (#{event_type := <<"changedOrCreated">>, data := [#{<<"attributes">> := ChangedAttrs}]}) ->
            {true, ChangedAttrs};
        (_) ->
            false
    end, Events).
