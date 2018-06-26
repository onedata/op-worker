%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests space logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(space_logic_test_SUITE).
-author("Lukasz Opiola").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    get_test/1,
    get_protected_data_test/1,
    mixed_get_test/1,
    subscribe_test/1,
    convenience_functions_test/1,
    create_update_delete_test/1,
    update_privileges_test/1,
    create_tokens_test/1
]).

all() -> ?ALL([
    get_test,
    get_protected_data_test,
    mixed_get_test,
    subscribe_test,
    convenience_functions_test,
    create_update_delete_test,
    update_privileges_test,
    create_tokens_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % User 3 does not have access to the space
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Space private data should now be cached
    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider can access cached data
    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, space_logic, get, [User3Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider can access non-cached data
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, space_logic, get, [User3Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    ok.


get_protected_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % User 3 does not have access to the space
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Space protected data should now be cached
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider can access cached data
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Provider should also be able to fetch non-cached data
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, space_logic, get, [User3Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, space_logic, get, [User3Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph)),

    ok.


mixed_get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),
    UnsubCalls = logic_tests_common:count_reqs(Config, unsub),

    % Fetching rising scopes should cause an unsub and new fetch every time
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub)),

    % When private data is cached, any scope should always be fetched from cache

    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    % Simulate received updates on different scopes (in rising order)
    Space1ProtectedGRI = #gri{type = od_space, id = ?SPACE_1, aspect = instance, scope = protected},
    Space1ProtectedData = ?SPACE_PROTECTED_DATA_VALUE(?SPACE_1),
    Space1PrivateGRI = #gri{type = od_space, id = ?SPACE_1, aspect = instance, scope = private},
    Space1PrivateData = ?SPACE_PRIVATE_DATA_VALUE(?SPACE_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % protected scope
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ChangedData1 = Space1ProtectedData#{<<"name">> => <<"changedName">>},
    PushMessage1 = #gs_push_graph{gri = Space1ProtectedGRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(
        {ok, #document{key = ?SPACE_1, value = #od_space{
            name = <<"changedName">>
        }}},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % private scope
    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ChangedData2 = Space1PrivateData#{<<"name">> => <<"changedName2">>},
    PushMessage2 = #gs_push_graph{gri = Space1PrivateGRI, data = ChangedData2, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage2]),

    ?assertMatch(
        {ok, #document{key = ?SPACE_1, value = #od_space{
            name = <<"changedName2">>
        }}},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        {ok, #document{key = ?SPACE_1, value = #od_space{
            name = <<"changedName2">>
        }}},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Update of protected scope should not affect the cache
    ChangedData3 = Space1ProtectedData#{<<"name">> => <<"changedName3">>},
    PushMessage3 = #gs_push_graph{gri = Space1ProtectedGRI, data = ChangedData3, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage3]),

    ?assertMatch(
        {ok, #document{key = ?SPACE_1, value = #od_space{
            name = <<"changedName2">>
        }}},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        {ok, #document{key = ?SPACE_1, value = #od_space{
            name = <<"changedName2">>
        }}},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage4 = #gs_push_graph{gri = Space1PrivateGRI, change_type = deleted},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage4]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_space, get, [?SPACE_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),

    PushMessage5 = #gs_push_nosub{gri = Space1PrivateGRI, reason = forbidden},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage5]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_space, get, [?SPACE_1])
    ),

    ok.


convenience_functions_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Test convenience functions and if they fetch correct scopes

    % Name is within shared scope
    ?assertMatch(
        {ok, ?SPACE_NAME(?SPACE_1)},
        rpc:call(Node, space_logic, get_name, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Eff users are within private scope
    ?assertMatch(
        {ok, ?SPACE_EFF_USERS_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_eff_users, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Shares are within private scope
    ?assertMatch(
        {ok, ?SPACE_SHARES(?SPACE_1)},
        rpc:call(Node, space_logic, get_shares, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Providers are within private scope
    ExpProviderIds = maps:keys(?SPACE_PROVIDERS_VALUE(?SPACE_1)),
    ?assertMatch(
        {ok, ExpProviderIds},
        rpc:call(Node, space_logic, get_provider_ids, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        {ok, ?SPACE_PROVIDERS_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_providers_supports, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Eff users are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, space_logic, has_eff_user, [User1Sess, ?SPACE_1, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, has_eff_user, [User1Sess, ?SPACE_1, ?USER_3])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Eff groups are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, space_logic, has_eff_group, [User1Sess, ?SPACE_1, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, has_eff_group, [User1Sess, ?SPACE_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % User eff privileges are within private scope,
    % mocked users only have SPACE_VIEW privileges
    ?assertMatch(
        true,
        rpc:call(Node, space_logic, has_eff_privilege, [User1Sess, ?SPACE_1, ?USER_1, ?SPACE_VIEW])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, has_eff_privilege, [User1Sess, ?SPACE_1, ?USER_3, ?SPACE_VIEW])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, has_eff_privilege, [User1Sess, ?SPACE_1, ?USER_1, ?SPACE_INVITE_USER])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % can_view_user_through_space and can_view_group_through_space require private scope
    ?assertMatch(
        true,
        rpc:call(Node, space_logic, can_view_user_through_space, [User1Sess, ?SPACE_1, ?USER_1, ?USER_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, can_view_user_through_space, [User1Sess, ?SPACE_1, ?USER_1, ?USER_3])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, can_view_user_through_space, [User1Sess, ?SPACE_1, ?USER_3, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        true,
        rpc:call(Node, space_logic, can_view_group_through_space, [User1Sess, ?SPACE_1, ?USER_1, ?GROUP_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, can_view_group_through_space, [User1Sess, ?SPACE_1, ?USER_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, can_view_group_through_space, [User1Sess, ?SPACE_1, ?USER_3, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ok.


create_update_delete_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Create
    ?assertMatch(
        {ok, ?MOCK_CREATED_SPACE_ID},
        rpc:call(Node, space_logic, create, [
            User1Sess,
            ?SPACE_NAME(<<"newSpace">>)
        ])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_BAD_VALUE_BINARY(<<"name">>),
        rpc:call(Node, space_logic, create, [
            User1Sess,
            12345
        ])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Update
    ?assertMatch(
        ok,
        rpc:call(Node, space_logic, update_name, [User1Sess, ?SPACE_1, <<"newName">>])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_BAD_VALUE_BINARY(<<"name">>),
        rpc:call(Node, space_logic, update_name, [User1Sess, ?SPACE_1, 1234])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph)),

    % Delete
    ?assertMatch(
        ok,
        rpc:call(Node, space_logic, delete, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 5, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, space_logic, delete, [User1Sess, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 6, logic_tests_common:count_reqs(Config, graph)),

    ok.

update_privileges_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        ok,
        rpc:call(Node, space_logic, update_user_privileges, [
            User1Sess,
            ?SPACE_1,
            ?USER_1,
            [?SPACE_VIEW, ?SPACE_INVITE_USER]
        ])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, space_logic, update_user_privileges, [
            User1Sess,
            ?SPACE_1,
            ?USER_3,
            [?SPACE_VIEW, ?SPACE_INVITE_USER]
        ])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ok,
        rpc:call(Node, space_logic, update_group_privileges, [
            User1Sess,
            ?SPACE_1,
            ?GROUP_2,
            [?SPACE_VIEW, ?SPACE_INVITE_USER]
        ])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, space_logic, update_group_privileges, [
            User1Sess,
            ?SPACE_1,
            <<"wrongId">>,
            [?SPACE_VIEW, ?SPACE_INVITE_USER]
        ])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph)),

    ok.

create_tokens_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?MOCK_INVITE_USER_TOKEN},
        rpc:call(Node, space_logic, create_user_invite_token, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, space_logic, create_user_invite_token, [User1Sess, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        {ok, ?MOCK_INVITE_GROUP_TOKEN},
        rpc:call(Node, space_logic, create_group_invite_token, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, space_logic, create_group_invite_token, [User1Sess, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        {ok, ?MOCK_INVITE_PROVIDER_TOKEN},
        rpc:call(Node, space_logic, create_provider_invite_token, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 5, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, space_logic, create_provider_invite_token, [User1Sess, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 6, logic_tests_common:count_reqs(Config, graph)),

    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        logic_tests_common:mock_gs_client(NewConfig),
        NewConfig
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [logic_tests_common, initializer]} | Config].

init_per_testcase(_, Config) ->
    logic_tests_common:init_per_testcase(Config).

end_per_testcase(_, _Config) ->
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================