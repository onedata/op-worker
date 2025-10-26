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
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    non_existing_space_test/1,
    unauthorized_client_test/1,
    token_caveats_test/1,
    invalid_args_test/1,
    changed_or_created_events_test/1
]).

all() ->
    ?ALL([
        non_existing_space_test,
        unauthorized_client_test,
        token_caveats_test,
        invalid_args_test,
        changed_or_created_events_test
    ]).


-define(assert_attr_changed_or_created_events(__EXP_ATTR_DATA, __SSE_CLIENT_PID, __FILE_GUID),
    ?assertEqual(
        __EXP_ATTR_DATA,
        get_data_attributes_from_changed_or_created_events(get_events_for_file(__SSE_CLIENT_PID, __FILE_GUID)),
        ?ATTEMPTS
    )
).

-define(ATTEMPTS, 5).


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


unauthorized_client_test(_Config) ->
    Space1Id = oct_background:get_space_id(space1),
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
    SpaceKrkId = oct_background:get_space_id(space_krk),
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


invalid_args_test(_Config) ->
    SpaceKrkId = oct_background:get_space_id(space_krk),
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
        <<"name">>, <<"parentFileId">>, <<"index">>, <<"type">>, <<"activePermissionsType">>,
        <<"posixPermissions">>, <<"acl">>,
        <<"originProviderId">>, <<"directShareIds">>, <<"ownerUserId">>,
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
        }
    ])).


changed_or_created_events_test(_Config) ->
    SpaceKrkId = oct_background:get_space_id(space_krk),
    SpaceKrkGuid = space_dir:guid(SpaceKrkId),
    FileOwnerUserId = oct_background:get_user_id(user1),
    FileOwnerSessionId = oct_background:get_user_session_id(user1, krakow),

    ChildFileName = ?RAND_STR(),
    #object{
        guid = ObservedDirGuid,
        children = [
            %% TODO VFS-12699 Test changes for child dir
            #object{guid = _ChildDirGuid},
            #object{guid = ChildFileGuid}
        ]
    } = onenv_file_test_utils:create_file_tree(
        FileOwnerUserId, SpaceKrkGuid, krakow, #dir_spec{
            mode = ?FILE_MODE(8#777),
            children = [#dir_spec{}, #file_spec{name = ChildFileName}]
        }
    ),
    ObservedAttrs = [?attr_name, ?attr_mode, ?attr_size],

    ClientArgs = #{
        node => oct_background:get_random_provider_node(krakow),
        space_id => SpaceKrkId,
        token => oct_background:get_user_access_token(user2),
        observed_dirs => [ObservedDirGuid],
        observed_attrs => ObservedAttrs
    },

    {ok, SSEClientPid} = ?assertMatch({ok, _}, space_file_events_test_sse_client:start(ClientArgs)),

    % Creating new files in observed dir should result in its events for all observed documents
    onenv_file_test_utils:create_file_tree(
        FileOwnerUserId, ObservedDirGuid, krakow, #file_spec{}
    ),

    ExpAttrsForAttrChangedEvents1 = [
        #{<<"name">> => ChildFileName, <<"posixPermissions">> => <<"664">>},
        #{<<"size">> => 0}
    ],
    ?assert_attr_changed_or_created_events(ExpAttrsForAttrChangedEvents1, SSEClientPid, ChildFileGuid),

    % mode change should result in event
    Node = oct_background:get_random_provider_node(krakow),
    ?assertMatch(ok, lfm_proxy:set_perms(Node, FileOwnerSessionId, ?FILE_REF(ChildFileGuid), 8#740)),

    ExpAttrsForAttrChangedEvents2 = ExpAttrsForAttrChangedEvents1 ++ [
        #{<<"name">> => ChildFileName, <<"posixPermissions">> => <<"740">>}
    ],
    ?assert_attr_changed_or_created_events(ExpAttrsForAttrChangedEvents2, SSEClientPid, ChildFileGuid).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    opt:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
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
get_events_for_file(SSEClientPid, FileGuid) ->
    {ok, Events} = space_file_events_test_sse_client:get_events(SSEClientPid),

    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
    lists:filter(fun(#{data := [EventData]}) ->
        maps:get(<<"fileId">>, EventData) =:= FileObjectId
    end, Events).


%% @private
get_data_attributes_from_changed_or_created_events(Events) ->
    lists:filtermap(fun
        (#{event_type := <<"changedOrCreated">>, data := [#{<<"attributes">> := ChangedAttrs}]}) ->
            {true, ChangedAttrs};
        (_) ->
            false
    end, Events).
