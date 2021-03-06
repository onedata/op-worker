%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests idp_access_token model.
%%% @end
%%%-------------------------------------------------------------------
-module(idp_access_token_test_SUITE).
-author("Jakub Kudzia").

-include("proto/common/credentials.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    get_idp_token_by_user_and_session_id/1,
    get_idp_token_by_onedata_token/1,
    token_should_be_acquired_from_onezone_due_to_too_short_ttl/1,
    token_should_be_acquired_from_onezone_due_to_exceeded_ttl/1,
    erroneous_response_should_not_be_cached/1]).

all() ->
    ?ALL([
        get_idp_token_by_user_and_session_id,
        get_idp_token_by_onedata_token,
        token_should_be_acquired_from_onezone_due_to_too_short_ttl,
        token_should_be_acquired_from_onezone_due_to_exceeded_ttl,
        erroneous_response_should_not_be_cached
    ]).

-define(ONEDATA_TOKEN, <<"ONEDATA_ACCESS_TOKEN">>).
-define(ADMIN_CREDENTIALS,
    auth_manager:build_token_credentials(
        ?ONEDATA_TOKEN, undefined,
        undefined, rest, disallow_data_access_caveats
    )
).

-define(IDP_ACCESS_TOKEN1, <<"IDP_ACCESS_TOKEN1">>).
-define(IDP_ACCESS_TOKEN2, <<"IDP_ACCESS_TOKEN2">>).
-define(IDP_ACCESS_TOKEN3, <<"IDP_ACCESS_TOKEN3">>).

-define(ADMIN, <<"admin">>).
-define(USER1, <<"user1">>).
-define(USER2, <<"user2">>).
-define(USER3, <<"user3">>).

-define(SESSION_ID1, <<"session1">>).
-define(SESSION_ID2, <<"session2">>).
-define(SESSION_ID3, <<"session3">>).

-define(IDP, <<"IDP">>).
-define(IDP2, <<"IDP2">>).

-define(assertFetchTokenCalls(Worker, Args, ExpNumCalls),
    test_utils:mock_assert_num_calls(Worker, user_logic, fetch_idp_access_token,
        Args, ExpNumCalls)).

%%%====================================================================
%%% Test function
%%%====================================================================

get_idp_token_by_user_and_session_id(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    mock_fetch_idp_access_token(Worker, fun
        (?SESSION_ID1, ?USER1, ?IDP) -> {ok, {?IDP_ACCESS_TOKEN1, 6000}};
        (?SESSION_ID2, ?USER2, ?IDP) -> {ok, {?IDP_ACCESS_TOKEN2, 6000}};
        (?SESSION_ID3, ?USER3, ?IDP2) -> {ok, {?IDP_ACCESS_TOKEN3, 6000}}
    end),

    % 1st get USER1 should be served from onezone
    ?assertMatch({ok, {?IDP_ACCESS_TOKEN1, _}},
        acquire_token(Worker, ?USER1, ?SESSION_ID1, ?IDP)),
    ?assertFetchTokenCalls(Worker, [?SESSION_ID1, ?USER1, ?IDP], 1),
    
    % 2nd get for USER1 should be served from cache
    ?assertMatch({ok, {?IDP_ACCESS_TOKEN1, _}},
        acquire_token(Worker, ?USER1, ?SESSION_ID1, ?IDP)),
    ?assertFetchTokenCalls(Worker, [?SESSION_ID1, ?USER1, ?IDP], 1),

    % 1st get for USER2 user should be served from onezone
    ?assertMatch({ok, {?IDP_ACCESS_TOKEN2, _}},
        acquire_token(Worker, ?USER2, ?SESSION_ID2, ?IDP)),
    ?assertFetchTokenCalls(Worker, [?SESSION_ID2, ?USER2, ?IDP], 1),

    % 2nd get for USER2 should be served from cache
    ?assertMatch({ok, {?IDP_ACCESS_TOKEN2, _}},
        acquire_token(Worker, ?USER2, ?SESSION_ID2, ?IDP)),
    ?assertFetchTokenCalls(Worker, [?SESSION_ID2, ?USER2, ?IDP], 1),

    ?assertMatch({ok, {?IDP_ACCESS_TOKEN3, _}},
        acquire_token(Worker, ?USER3, ?SESSION_ID3, ?IDP2)),
    % 1st get for USER3 should be served from onezone
    ?assertFetchTokenCalls(Worker, [?SESSION_ID3, ?USER3, ?IDP2], 1),

    ?assertMatch({ok, {?IDP_ACCESS_TOKEN3, _}},
        acquire_token(Worker, ?USER3, ?SESSION_ID3, ?IDP2)),
    % 2nd get for USER3 should be served from onezone
    ?assertFetchTokenCalls(Worker, [?SESSION_ID3, ?USER3, ?IDP2], 1),

    % get for USER1 with different SessionId should also be served from cache
    ?assertMatch({ok, {?IDP_ACCESS_TOKEN1, _}},
        acquire_token(Worker, ?USER1, ?SESSION_ID2, ?IDP)),
    ?assertFetchTokenCalls(Worker, [?SESSION_ID2, ?USER1, ?IDP], 0).


get_idp_token_by_onedata_token(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    mock_fetch_idp_access_token(Worker, fun(_, _, _) ->
        {ok, {?IDP_ACCESS_TOKEN1, 6000}}
    end),

    ?assertMatch({ok, {?IDP_ACCESS_TOKEN1, _}},
        acquire_token(Worker, ?ADMIN, ?ADMIN_CREDENTIALS, ?IDP)),
    % 1st get should be served from onezone
    ?assertFetchTokenCalls(Worker, [?ADMIN_CREDENTIALS, ?ADMIN, ?IDP], 1),

    ?assertMatch({ok, {?IDP_ACCESS_TOKEN1, _}},
        acquire_token(Worker, ?ADMIN, ?ADMIN_CREDENTIALS, ?IDP)),

    % 2nd get should be served from cache
    ?assertFetchTokenCalls(Worker, [?ADMIN_CREDENTIALS, ?ADMIN, ?IDP], 1).


token_should_be_acquired_from_onezone_due_to_too_short_ttl(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    %return TTL smaller than REFRESH_THRESHOLD=300
    %token will always be acquired from Onezone
    mock_fetch_idp_access_token(Worker, fun(_, _, _) ->
        {ok, {?IDP_ACCESS_TOKEN1, 299}}
    end),

    ?assertMatch({ok, {?IDP_ACCESS_TOKEN1, _}},
        acquire_token(Worker, ?USER1, ?SESSION_ID1, ?IDP)),
    % 1st get should be served from Onezone
    ?assertFetchTokenCalls(Worker, [?SESSION_ID1, ?USER1, ?IDP], 1),

    ?assertMatch({ok, {?IDP_ACCESS_TOKEN1, _}},
        acquire_token(Worker, ?USER1, ?SESSION_ID1, ?IDP)),

    % 2nd get should be served from Onezone too
    ?assertFetchTokenCalls(Worker, [?SESSION_ID1, ?USER1, ?IDP], 2).

token_should_be_acquired_from_onezone_due_to_exceeded_ttl(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    TTL = 5,
    %return TTL
    mock_fetch_idp_access_token(Worker, fun(_, _, _) ->
        {ok, {?IDP_ACCESS_TOKEN1, TTL}}
    end),

    ?assertMatch({ok, {?IDP_ACCESS_TOKEN1, _}},
        acquire_token(Worker, ?USER1, ?SESSION_ID1, ?IDP)),
    % 1st get should be served from Onezone
    ?assertFetchTokenCalls(Worker, [?SESSION_ID1, ?USER1, ?IDP], 1),

    timer:sleep(timer:seconds(TTL + 1)),

    ?assertMatch({ok, {?IDP_ACCESS_TOKEN1, _}},
        acquire_token(Worker, ?USER1, ?SESSION_ID1, ?IDP)),

    % 2nd get should be served from Onezone too
    ?assertFetchTokenCalls(Worker, [?SESSION_ID1, ?USER1, ?IDP], 2).

erroneous_response_should_not_be_cached(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    mock_fetch_idp_access_token(Worker, fun(_, _, _) ->
        {error, test_reason}
    end),

    % 1st get for USER1 should be served from Onezone
    ?assertMatch({error, test_reason},
        acquire_token(Worker, ?USER1, ?SESSION_ID1, ?IDP)),
    ?assertFetchTokenCalls(Worker, [?SESSION_ID1, ?USER1, ?IDP], 1),

    % 2nd get for USER1 should be served from Onezone
    ?assertMatch({error, test_reason},
        acquire_token(Worker, ?USER1, ?SESSION_ID1, ?IDP)),
    ?assertFetchTokenCalls(Worker, [?SESSION_ID1, ?USER1, ?IDP], 2),

    mock_fetch_idp_access_token(Worker, fun(_, _, _) ->
        {ok, {?IDP_ACCESS_TOKEN1, 10000}}
    end),

    % 3rd get for USER1 should be served from Onezone and cached
    ?assertMatch({ok, {?IDP_ACCESS_TOKEN1, _}},
        acquire_token(Worker, ?USER1, ?SESSION_ID1, ?IDP)),
    ?assertFetchTokenCalls(Worker, [?SESSION_ID1, ?USER1, ?IDP], 3),

    % 4tg get for USER1 should be served from cache
    ?assertMatch({ok, {?IDP_ACCESS_TOKEN1, _}},
        acquire_token(Worker, ?USER1, ?SESSION_ID1, ?IDP)),
    ?assertFetchTokenCalls(Worker, [?SESSION_ID1, ?USER1, ?IDP], 3).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Worker, user_logic, [passthrough]),
    Config.

end_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    delete_entry(Worker, ?USER1, ?IDP),
    delete_entry(Worker, ?USER2, ?IDP),
    delete_entry(Worker, ?USER3, ?IDP2),
    delete_entry(Worker, ?ADMIN, ?IDP),
    ok = test_utils:mock_unload(Worker, user_logic).


%%%===================================================================
%%% Internal functions
%%%===================================================================

acquire_token(Node, UserId, Client, IdP) ->
    rpc:call(Node, idp_access_token, acquire, [UserId, Client, IdP]).

mock_fetch_idp_access_token(Worker, Fun) ->
    ok = test_utils:mock_expect(Worker, user_logic, fetch_idp_access_token, Fun).

delete_entry(Node, UserId, IdP) ->
    rpc:call(Node, idp_access_token, delete, [UserId, IdP]).