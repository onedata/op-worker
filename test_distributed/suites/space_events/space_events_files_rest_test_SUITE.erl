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
    token_caveats_test/1
]).

all() ->
    ?ALL([
        token_caveats_test
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
