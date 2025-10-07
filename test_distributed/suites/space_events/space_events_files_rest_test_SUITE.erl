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

    unauthorized_client_test/1,
    token_caveats_test/1,

    reconnect_without_catching_test/1,
    reconnect_with_old_last_event_id_test/1
]).

groups() -> [
    {basic_tests, [sequential], [
        non_existing_space_test,
        invalid_args_test,
        deleted_events_test,
        changed_or_created_events_test
    ]},
    {auth_tests, [sequential], [
        unauthorized_client_test,
        token_caveats_test
    ]},
    {reconnect_tests, [sequential], [
        reconnect_without_catching_test,
        reconnect_with_old_last_event_id_test
    ]}
].

all() -> [
    {group, basic_tests},
    {group, auth_tests},
    {group, reconnect_tests}
].


-define(assert_deleted_events(__EXP_STATE, __SSE_CLIENT_PID, __FILE_GUID),
    ?assertEqual(
        __EXP_STATE,
        check_for_file_deleted_event(get_events_for_file(__SSE_CLIENT_PID, __FILE_GUID)),
        ?ATTEMPTS
    )
).

-define(assert_attr_changed_or_created_events(__EXP_ATTR_DATA, __SSE_CLIENT_PID, __FILE_GUID),
    ?assertEqual(
        lists:usort(__EXP_ATTR_DATA),
        lists:usort(get_data_attributes_from_changed_or_created_events(get_events_for_file(__SSE_CLIENT_PID, __FILE_GUID))),
        ?ATTEMPTS
    )
).

-define(ATTEMPTS, 30).


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
    SpaceKrkId = oct_background:get_space_id(space_krk_par_p),
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
        token => oct_background:get_user_access_token(user2),
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
    ChildFileName = ?RAND_STR(),
    TestEnv = create_test_env(krakow, #{
        dir_spec => #dir_spec{
            mode = ?FILE_MODE(8#777),
            children = [#dir_spec{}, #file_spec{name = ChildFileName}]
        },
        observed_attrs => [?attr_name, ?attr_mode, ?attr_size]
    }),

    #{
        file_owner_user_id := FileOwnerUserId,
        observed_dir_object := #object{children = [
            %% TODO VFS-12887 Test changes for child dir
            #object{guid = ChildDirGuid},
            #object{guid = ChildFileGuid}
        ]}
    } = TestEnv,

    SSEClientPid = start_client(TestEnv),

    % Removing file in observed dir should result in event
    % NOTE: rm will choose random provider for removal (not necessarily krakow)
    onenv_file_test_utils:rm_and_sync_file(FileOwnerUserId, ChildFileGuid),
    ?assert_deleted_events(true, SSEClientPid, ChildFileGuid),

    % while deleting dir, at least for now, does not produce events
    % NOTE: rm will choose random provider for removal (not necessarily krakow)
    onenv_file_test_utils:rm_and_sync_file(FileOwnerUserId, ChildDirGuid),
    ?assert_deleted_events(false, SSEClientPid, ChildDirGuid).


changed_or_created_events_test(_Config) ->
    ClientProvider = krakow,
    ModifyingProvider = ?RAND_ELEMENT([krakow, paris]),
    ct:pal("Provider with SSE client: ~ts~nProvider modifying data: ~ts", [ClientProvider, ModifyingProvider]),

    ChildFileName = ?RAND_STR(),
    TestEnv = create_test_env(ClientProvider, #{
        dir_spec => #dir_spec{
            mode = ?FILE_MODE(8#777),
            children = [#dir_spec{}, #file_spec{}]
        },
        observed_attrs => [?attr_name, ?attr_mode, ?attr_size]
    }),

    #{
        file_owner_user_id := UserId,
        observed_dir_guid := ObservedDirGuid
    } = TestEnv,

    SSEClientPid = start_client(TestEnv),

    % Creating new files in observed dir should result in its events for all observed documents
    #object{guid = ChildFileGuid} = onenv_file_test_utils:create_file_tree(
        UserId, ObservedDirGuid, ModifyingProvider, #file_spec{name = ChildFileName}
    ),

    ExpAttrsForAttrChangedEvents1 = [
        #{<<"name">> => ChildFileName, <<"posixPermissions">> => <<"664">>},
        #{<<"size">> => 0}
    ],
    ?assert_attr_changed_or_created_events(ExpAttrsForAttrChangedEvents1, SSEClientPid, ChildFileGuid),

    % mode change should result in event
    Node = oct_background:get_random_provider_node(ModifyingProvider),
    FileOwnerSessionId = oct_background:get_user_session_id(user1, ModifyingProvider),
    ?assertMatch(ok, lfm_proxy:set_perms(Node, FileOwnerSessionId, ?FILE_REF(ChildFileGuid), 8#740)),

    ExpAttrsForAttrChangedEvents2 = ExpAttrsForAttrChangedEvents1 ++ [
        #{<<"name">> => ChildFileName, <<"posixPermissions">> => <<"740">>}
    ],
    ?assert_attr_changed_or_created_events(ExpAttrsForAttrChangedEvents2, SSEClientPid, ChildFileGuid).


unauthorized_client_test(_Config) ->
    Space1Id = oct_background:get_space_id(space_krk_par_p),
    Space1Guid = space_dir:guid(Space1Id),

    ClientArgs = #{
        node => oct_background:get_random_provider_node(krakow),
        space_id => Space1Id,
        observed_dirs => [Space1Guid]
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
            token => oct_background:get_user_access_token(user3)
        })
    ).


token_caveats_test(_Config) ->
    SpaceKrkId = oct_background:get_space_id(space_krk_par_p),
    SpaceKrkGuid = space_dir:guid(SpaceKrkId),
    Token = oct_background:get_user_access_token(user2),

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


reconnect_without_catching_test(_Config) ->
    TestEnv = create_test_env(krakow),

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
            ct:pal("Testing reconnect WITH future Last-Event-Id: ~p (current was ~p)", [FutureEventId, LastEventId]),
            start_client_with_last_event_id(TestEnv, FutureEventId)
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
    TestEnv = create_test_env(krakow),

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
    Client2ReconnectedPid = start_client_with_last_event_id(TestEnv, LastEventId),

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


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates standard test environment for space file events tests.
%% Returns common test setup including space, user, observed directory, and client args.
%%
%% Options:
%%   - observed_attrs: list of attributes to observe (default: [name, size])
%%   - dir_spec: custom directory spec (default: empty dir with mode 777)
%% @end
%%--------------------------------------------------------------------
-spec create_test_env(atom()) -> #{
    provider => oct_background:entity_selector(),
    space_id => od_space:id(),
    space_guid => file_id:file_guid(),
    file_owner_user_id => od_user:id(),
    observed_dir_guid => file_id:file_guid(),
    client_args => map()
}.
create_test_env(Provider) ->
    create_test_env(Provider, #{}).


%% @private
create_test_env(Provider, Opts) ->
    SpaceKrkId = oct_background:get_space_id(space_krk_par_p),
    SpaceKrkGuid = space_dir:guid(SpaceKrkId),
    FileOwnerUserId = oct_background:get_user_id(user1),

    DirSpec = maps:get(dir_spec, Opts, #dir_spec{mode = ?FILE_MODE(8#777)}),
    ObservedDirObject = onenv_file_test_utils:create_and_sync_file_tree(
        FileOwnerUserId, SpaceKrkGuid, DirSpec, Provider
    ),
    ObservedDirGuid = ObservedDirObject#object.guid,

    ObservedAttrs = maps:get(observed_attrs, Opts, [?attr_name, ?attr_size]),
    ClientArgs = #{
        node => oct_background:get_random_provider_node(Provider),
        space_id => SpaceKrkId,
        token => oct_background:get_user_access_token(user2),
        observed_dirs => [ObservedDirGuid],
        observed_attrs => ObservedAttrs
    },

    #{
        provider => Provider,
        space_id => SpaceKrkId,
        space_guid => SpaceKrkGuid,
        file_owner_user_id => FileOwnerUserId,
        observed_dir_guid => ObservedDirGuid,
        observed_dir_object => ObservedDirObject,
        client_args => ClientArgs
    }.


%% @private
await_event_for_file(ClientPid, FileGuid) ->
    ?assert(length(get_events_for_file(ClientPid, FileGuid)) > 0, ?ATTEMPTS).


%% @private
assert_no_event_for_file(ClientPid, FileGuid) ->
    ?assertEqual([], get_events_for_file(ClientPid, FileGuid)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates file and waits for control client to confirm event arrival.
%% This ensures the event is in the system before proceeding.
%% Returns the created file GUID.
%% @end
%%--------------------------------------------------------------------
create_file_and_await_sync(TestEnv, FileName, ControlClientPid) when is_binary(FileName) ->
    create_file_and_await_sync(TestEnv, #file_spec{name = FileName}, ControlClientPid);

create_file_and_await_sync(TestEnv, FileSpec, ControlClientPid) ->
    #{
        provider := Provider,
        file_owner_user_id := UserId,
        observed_dir_guid := ObservedDirGuid
    } = TestEnv,
    #object{guid = FileGuid} = onenv_file_test_utils:create_file_tree(
        UserId, ObservedDirGuid, Provider, FileSpec
    ),
    await_event_for_file(ControlClientPid, FileGuid),
    FileGuid.


%% @private
start_client(TestEnv) ->
    #{client_args := ClientArgs} = TestEnv,
    {ok, Pid} = ?assertMatch({ok, _}, space_file_events_test_sse_client:start(ClientArgs)),
    Pid.


%% @private
get_last_event_id(ClientPid) ->
    {ok, Events} = space_file_events_test_sse_client:get_events(ClientPid),
    case Events of
        [] -> undefined;
        _ -> get_event_id(lists:last(Events))
    end.


%% @private
get_event_id(Event) ->
    maps:get(last_event_id, Event).


%% @private
start_client_with_last_event_id(TestEnv, LastEventId) ->
    #{client_args := ClientArgs} = TestEnv,
    ClientArgsWithHeader = ClientArgs#{
        headers => [{<<"Last-Event-Id">>, str_utils:to_binary(LastEventId)}]
    },
    TestEnvWithHeader = TestEnv#{client_args => ClientArgsWithHeader},
    start_client(TestEnvWithHeader).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates N events by creating files in observed directory.
%% Uses control client to ensure all events are in the system.
%% Returns list of created file GUIDs.
%% @end
%%--------------------------------------------------------------------
generate_n_events(N, TestEnv, ControlClientPid) ->
    lists:map(fun(I) ->
        FileName = <<"file_", (integer_to_binary(I))/binary, ".txt">>,
        create_file_and_await_sync(TestEnv, #file_spec{name = FileName}, ControlClientPid)
    end, lists:seq(1, N)).


%% @private
get_events_for_file(SSEClientPid, FileGuid) ->
    {ok, Events} = space_file_events_test_sse_client:get_events(SSEClientPid),

    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
    lists:filter(fun
        (#{event_type := <<"heartbeat">>}) ->
            false;
        (#{data := [EventData]}) ->
            maps:get(<<"fileId">>, EventData) =:= FileObjectId
    end, Events).


%% @private
check_for_file_deleted_event(Events) ->
    lists:any(fun
        (#{event_type := <<"deleted">>}) -> true;
        (_) -> false
    end, Events).


%% @private
get_data_attributes_from_changed_or_created_events(Events) ->
    lists:filtermap(fun
        (#{event_type := <<"changedOrCreated">>, data := [#{<<"attributes">> := ChangedAttrs}]}) ->
            {true, ChangedAttrs};
        (_) ->
            false
    end, Events).


%% @private
assert_all_client_events_sequential(ClientPid) ->
    {ok, Events} = space_file_events_test_sse_client:get_events(ClientPid),

    EventIds = [get_event_id(E) || E <- Events, maps:get(event, E, undefined) =/= <<"heartbeat">>],
    EventIdsInt = [binary_to_integer(Id) || Id <- EventIds],
    SortedEventIds = lists:sort(EventIdsInt),
    UniqueSortedEventIds = lists:usort(EventIdsInt),

    ?assertEqual(SortedEventIds, UniqueSortedEventIds).
