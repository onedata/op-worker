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
    token_caveats_test/1,
    invalid_args_test/1
]).

all() ->
    ?ALL([
        token_caveats_test,
        invalid_args_test
    ]).


-define(ATTEMPTS, 5).


%%%===================================================================
%%% Test functions
%%%===================================================================


token_caveats_test(_Config) ->
    SpaceKrkId = oct_background:get_space_id(space_krk),
    SpaceKrkGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceKrkId),
    Token = oct_background:get_user_access_token(user2),

    ClientArgs = #{
        node => oct_background:get_random_provider_node(krakow),
        space_id => SpaceKrkId,
        token => Token,
        observed_dirs => [SpaceKrkGuid]
    },

    % Request containing data caveats should be rejected
    DataCaveat = #cv_data_path{whitelist = [<<"/", SpaceKrkId/binary>>]},
    TokenWithDataCaveat = tokens:confine(Token, DataCaveat),
    ?assertMatch(
        {error, {401, ?ERR_UNAUTHORIZED(?ERR_TOKEN_CAVEAT_UNVERIFIED(DataCaveat))}},
        space_file_events_test_sse_client:start(ClientArgs#{token => TokenWithDataCaveat})
    ),

    % Request containing invalid api caveat should be rejected
    InvalidApiCaveat = #cv_api{whitelist = [
        % valid caveat - operation check user perms and as such permission to get user record is required
        {all, all, ?GRI_PATTERN(od_user, <<"*">>, <<"instance">>, '*')},
        % invalid caveat
        {all, all, ?GRI_PATTERN(op_metrics, <<"ASD">>, <<"changes">>)}
    ]},
    TokenWithInvalidApiCaveat = tokens:confine(Token, InvalidApiCaveat),
    ?assertMatch(
        {error, {401, ?ERR_UNAUTHORIZED(?ERR_TOKEN_CAVEAT_UNVERIFIED(InvalidApiCaveat))}},
        space_file_events_test_sse_client:start(ClientArgs#{token => TokenWithInvalidApiCaveat})
    ),

    % Request containing valid api caveat should succeed
    ValidApiCaveat = #cv_api{whitelist = [
        {all, all, ?GRI_PATTERN(od_user, <<"*">>, <<"instance">>, '*')},
        {all, all, ?GRI_PATTERN(op_metrics, SpaceKrkId, <<"file_events">>)}
    ]},
    TokenWithValidApiCaveat = tokens:confine(Token, ValidApiCaveat),
    {ok, Client} = ?assertMatch(
        {ok, _},
        space_file_events_test_sse_client:start(ClientArgs#{token => TokenWithValidApiCaveat})
    ),
    ok = space_file_events_test_sse_client:stop(Client).


invalid_args_test(_Config) ->
    SpaceKrkId = oct_background:get_space_id(space_krk),
    SpaceKrkGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceKrkId),
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
        <<"index">>, <<"type">>, <<"activePermissionsType">>, <<"posixPermissions">>, <<"acl">>,
        <<"parentFileId">>, <<"originProviderId">>, <<"directShareIds">>, <<"ownerUserId">>,
        <<"hardlinkCount">>, <<"symlinkValue">>, <<"creationTime">>, <<"atime">>, <<"mtime">>,
        <<"ctime">>, <<"size">>, <<"isFullyReplicatedLocally">>, <<"localReplicationRate">>,
        %% TODO VFS-12699 should not be returned!
        <<"xattr.*">>
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
