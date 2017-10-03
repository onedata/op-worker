%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests group logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(group_logic_test_SUITE).
-author("Lukasz Opiola").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    get_test/1,
    get_protected_data_test/1,
    get_shared_data_test/1,
    mixed_get_test/1,
    subscribe_test/1,
    convenience_functions_test/1,
    create_update_delete_test/1,
    update_privileges_test/1,
    create_tokens_test/1,
    join_or_leave_group_or_space_test/1
]).

all() -> ?ALL([
    get_test,
    get_protected_data_test,
    get_shared_data_test,
    mixed_get_test,
    subscribe_test,
    convenience_functions_test,
    create_update_delete_test,
    update_privileges_test,
    create_tokens_test,
    join_or_leave_group_or_space_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:create_user_session(Config, ?USER_1),
    % User 3 does not have access to the group
    User3Sess = logic_tests_common:create_user_session(Config, ?USER_3),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?GROUP_PRIVATE_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Group private data should now be cached
    ?assertMatch(
        {ok, ?GROUP_PRIVATE_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider and other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, group_logic, get, [?ROOT_SESS_ID, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, group_logic, get, [User3Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider and other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_group, ?GROUP_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, group_logic, get, [?ROOT_SESS_ID, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, group_logic, get, [User3Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    ok.

get_protected_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:create_user_session(Config, ?USER_1),
    % User 3 does not have access to the group
    User3Sess = logic_tests_common:create_user_session(Config, ?USER_3),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?GROUP_PROTECTED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_protected_data, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Group protected data should now be cached
    ?assertMatch(
        {ok, ?GROUP_PROTECTED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_protected_data, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Provider should also be able to fetch the data from cache without
    % additional requests
    ?assertMatch(
        {ok, ?GROUP_PROTECTED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_protected_data, [?ROOT_SESS_ID, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Provider should also be able to fetch non-cached data
    logic_tests_common:invalidate_cache(Config, od_group, ?GROUP_1),
    ?assertMatch(
        {ok, ?GROUP_PROTECTED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_protected_data, [?ROOT_SESS_ID, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, group_logic, get, [User3Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_group, ?GROUP_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, group_logic, get, [User3Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph)),

    ok.

get_shared_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:create_user_session(Config, ?USER_1),
    % User 3 does not have access to the group
    User3Sess = logic_tests_common:create_user_session(Config, ?USER_3),

    % Groups and spaces need to be fetched for provider to be able to assert
    % THROUGH_GROUP and THROUGH_SPACE authorization without making additional requests.
    rpc:call(Node, group_logic, get, [User1Sess, ?GROUP_2]),
    rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1]),
    rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_2]),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?GROUP_SHARED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_shared_data, [User1Sess, ?GROUP_1, ?THROUGH_GROUP(?GROUP_2)])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Group shared data should now be cached
    ?assertMatch(
        {ok, ?GROUP_SHARED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_shared_data, [User1Sess, ?GROUP_1, ?THROUGH_SPACE(?SPACE_1)])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Provider should also be able to fetch the data from cache without
    % additional requests
    ?assertMatch(
        {ok, ?GROUP_SHARED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_shared_data, [?ROOT_SESS_ID, ?GROUP_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Provider should also be able to fetch non-cached data
    logic_tests_common:invalidate_cache(Config, od_group, ?GROUP_1),
    ?assertMatch(
        {ok, ?GROUP_SHARED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_shared_data, [?ROOT_SESS_ID, ?GROUP_1, undefined])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, group_logic, get_shared_data, [User3Sess, ?GROUP_1, ?THROUGH_SPACE(?SPACE_2)])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_group, ?GROUP_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, group_logic, get_shared_data, [User3Sess, ?GROUP_1, ?THROUGH_GROUP(?GROUP_2)])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    ok.

mixed_get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:create_user_session(Config, ?USER_1),

    % Groups and spaces need to be fetched for provider to be able to assert
    % authorization without making additional requests.
    rpc:call(Node, group_logic, get, [User1Sess, ?GROUP_2]),
    rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1]),
    rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_2]),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),
    UnsubCalls = logic_tests_common:count_reqs(Config, unsub),

    % Fetching rising scopes should cause an unsub and new fetch every time
    ?assertMatch(
        {ok, ?GROUP_SHARED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_shared_data, [User1Sess, ?GROUP_1, ?THROUGH_SPACE(?SPACE_1)])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?GROUP_PROTECTED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_protected_data, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?GROUP_PRIVATE_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 2, logic_tests_common:count_reqs(Config, unsub)),

    % When private data is cached, any scope should always be fetched from cache

    ?assertMatch(
        {ok, ?GROUP_PRIVATE_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 2, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?GROUP_PROTECTED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_protected_data, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 2, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?GROUP_SHARED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_shared_data, [User1Sess, ?GROUP_1, ?THROUGH_SPACE(?SPACE_1)])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 2, logic_tests_common:count_reqs(Config, unsub)),

    ok.

subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:create_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Simulate received updates on different scopes (in rising order)
    Group1SharedGRI = #gri{type = od_group, id = ?GROUP_1, aspect = instance, scope = shared},
    Group1SharedData = ?GROUP_SHARED_DATA_VALUE(?GROUP_1),
    Group1ProtectedGRI = #gri{type = od_group, id = ?GROUP_1, aspect = instance, scope = protected},
    Group1ProtectedData = ?GROUP_PROTECTED_DATA_VALUE(?GROUP_1),
    Group1PrivateGRI = #gri{type = od_group, id = ?GROUP_1, aspect = instance, scope = private},
    Group1PrivateData = ?GROUP_PRIVATE_DATA_VALUE(?GROUP_1),

    % shared scope
    ?assertMatch(
        {ok, ?GROUP_SHARED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_shared_data, [User1Sess, ?GROUP_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ChangedData1 = Group1SharedData#{<<"name">> => <<"changedName">>},
    PushMessage1 = #gs_push_graph{gri = Group1SharedGRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(
        {ok, #document{key = ?GROUP_1, value = #od_group{
            name = <<"changedName">>
        }}},
        rpc:call(Node, group_logic, get_shared_data, [User1Sess, ?GROUP_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % protected scope
    ?assertMatch(
        {ok, ?GROUP_PROTECTED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_protected_data, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ChangedData2 = Group1ProtectedData#{<<"name">> => <<"changedName2">>},
    PushMessage2 = #gs_push_graph{gri = Group1ProtectedGRI, data = ChangedData2, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage2]),

    ?assertMatch(
        {ok, #document{key = ?GROUP_1, value = #od_group{
            name = <<"changedName2">>
        }}},
        rpc:call(Node, group_logic, get_protected_data, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Update of shared scope should not affect the cache
    ChangedData3 = Group1SharedData#{<<"name">> => <<"changedName3">>},
    PushMessage3 = #gs_push_graph{gri = Group1SharedGRI, data = ChangedData3, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage3]),

    ?assertMatch(
        {ok, #document{key = ?GROUP_1, value = #od_group{
            name = <<"changedName2">>
        }}},
        rpc:call(Node, group_logic, get_shared_data, [User1Sess, ?GROUP_1, undefined])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        {ok, #document{key = ?GROUP_1, value = #od_group{
            name = <<"changedName2">>
        }}},
        rpc:call(Node, group_logic, get_protected_data, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % private scope
    ?assertMatch(
        {ok, ?GROUP_PRIVATE_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ChangedData4 = Group1PrivateData#{<<"name">> => <<"changedName4">>},
    PushMessage4 = #gs_push_graph{gri = Group1PrivateGRI, data = ChangedData4, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage4]),

    ?assertMatch(
        {ok, #document{key = ?GROUP_1, value = #od_group{
            name = <<"changedName4">>
        }}},
        rpc:call(Node, group_logic, get, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    % Update of protected or shared scope should not affect the cache
    ChangedData5 = Group1SharedData#{<<"name">> => <<"changedName5">>},
    PushMessage5 = #gs_push_graph{gri = Group1SharedGRI, data = ChangedData5, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage5]),

    ChangedData6 = Group1ProtectedData#{<<"name">> => <<"changedName6">>},
    PushMessage6 = #gs_push_graph{gri = Group1ProtectedGRI, data = ChangedData6, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage6]),

    ?assertMatch(
        {ok, #document{key = ?GROUP_1, value = #od_group{
            name = <<"changedName4">>
        }}},
        rpc:call(Node, group_logic, get_shared_data, [User1Sess, ?GROUP_1, undefined])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        {ok, #document{key = ?GROUP_1, value = #od_group{
            name = <<"changedName4">>
        }}},
        rpc:call(Node, group_logic, get_protected_data, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        {ok, #document{key = ?GROUP_1, value = #od_group{
            name = <<"changedName4">>
        }}},
        rpc:call(Node, group_logic, get, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage7 = #gs_push_graph{gri = Group1PrivateGRI, change_type = deleted},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage7]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_group, get, [?GROUP_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    ?assertMatch(
        {ok, ?GROUP_PRIVATE_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get, [User1Sess, ?GROUP_1])
    ),

    PushMessage8 = #gs_push_nosub{gri = Group1PrivateGRI, reason = forbidden},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage8]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_group, get, [?GROUP_1])
    ),


    ok.

convenience_functions_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:create_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Test convenience functions and if they fetch correct scopes

    % Name is within shared scope
    ?assertMatch(
        {ok, ?GROUP_NAME(?GROUP_1)},
        rpc:call(Node, group_logic, get_name, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Eff children are within private scope
    ?assertMatch(
        {ok, ?GROUP_EFF_CHILDREN_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_eff_children, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Eff spaces are within private scope
    ?assertMatch(
        {ok, ?USER_EFF_SPACES(?GROUP_1)},
        rpc:call(Node, group_logic, get_eff_spaces, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Eff users are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, group_logic, has_eff_user, [User1Sess, ?GROUP_1, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        true,
        rpc:call(Node, group_logic, has_eff_user, [User1Sess, ?GROUP_1, ?USER_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, group_logic, has_eff_user, [User1Sess, ?GROUP_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Eff children are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, group_logic, has_eff_child, [User1Sess, ?GROUP_1, ?GROUP_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, group_logic, has_eff_child, [User1Sess, ?GROUP_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Eff spaces are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, group_logic, has_eff_space, [User1Sess, ?GROUP_1, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        true,
        rpc:call(Node, group_logic, has_eff_space, [User1Sess, ?GROUP_1, ?SPACE_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, group_logic, has_eff_space, [User1Sess, ?GROUP_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % User eff privileges are within private scope,
    % mocked users only have GROUP_VIEW privileges
    ?assertMatch(
        true,
        rpc:call(Node, group_logic, has_eff_privilege, [User1Sess, ?GROUP_1, ?USER_1, ?GROUP_VIEW])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, group_logic, has_eff_privilege, [User1Sess, ?GROUP_1, ?USER_3, ?GROUP_VIEW])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, group_logic, has_eff_privilege, [User1Sess, ?GROUP_1, ?USER_1, ?GROUP_INVITE_GROUP])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % can_view_user_through_group and can_view_child_through_group require private scope
    ?assertMatch(
        true,
        rpc:call(Node, group_logic, can_view_user_through_group, [User1Sess, ?GROUP_1, ?USER_1, ?USER_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, group_logic, can_view_user_through_group, [User1Sess, ?GROUP_1, ?USER_1, ?USER_3])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, group_logic, can_view_user_through_group, [User1Sess, ?GROUP_1, ?USER_3, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        true,
        rpc:call(Node, group_logic, can_view_child_through_group, [User1Sess, ?GROUP_1, ?USER_1, ?GROUP_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, group_logic, can_view_child_through_group, [User1Sess, ?GROUP_1, ?USER_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, group_logic, can_view_child_through_group, [User1Sess, ?GROUP_1, ?USER_3, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ok.

create_update_delete_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:create_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Create
    ?assertMatch(
        {ok, ?MOCK_CREATED_GROUP_ID},
        rpc:call(Node, group_logic, create, [
            User1Sess,
            ?GROUP_NAME(<<"newGroup">>)
        ])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_BAD_VALUE_BINARY(<<"name">>),
        rpc:call(Node, group_logic, create, [
            User1Sess,
            12345
        ])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Update
    ?assertMatch(
        ok,
        rpc:call(Node, group_logic, update_name, [User1Sess, ?GROUP_1, <<"newName">>])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_BAD_VALUE_BINARY(<<"name">>),
        rpc:call(Node, group_logic, update_name, [User1Sess, ?GROUP_1, 1234])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph)),

    % Delete
    ?assertMatch(
        ok,
        rpc:call(Node, group_logic, delete, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 5, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, group_logic, delete, [User1Sess, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 6, logic_tests_common:count_reqs(Config, graph)),

    ok.

update_privileges_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:create_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        ok,
        rpc:call(Node, group_logic, update_user_privileges, [
            User1Sess,
            ?GROUP_1,
            ?USER_1,
            [?GROUP_VIEW, ?GROUP_INVITE_GROUP]
        ])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, group_logic, update_user_privileges, [
            User1Sess,
            ?GROUP_1,
            ?USER_3,
            [?GROUP_VIEW, ?GROUP_INVITE_GROUP]
        ])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ok,
        rpc:call(Node, group_logic, update_child_privileges, [
            User1Sess,
            ?GROUP_1,
            ?GROUP_2,
            [?GROUP_VIEW, ?GROUP_INVITE_GROUP]
        ])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, group_logic, update_child_privileges, [
            User1Sess,
            ?GROUP_1,
            <<"wrongId">>,
            [?GROUP_VIEW, ?GROUP_INVITE_GROUP]
        ])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph)),

    ok.

create_tokens_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:create_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?MOCK_INVITE_USER_TOKEN},
        rpc:call(Node, group_logic, create_user_invite_token, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, group_logic, create_user_invite_token, [User1Sess, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        {ok, ?MOCK_INVITE_GROUP_TOKEN},
        rpc:call(Node, group_logic, create_group_invite_token, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, group_logic, create_group_invite_token, [User1Sess, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph)),

    ok.

join_or_leave_group_or_space_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:create_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?MOCK_JOINED_GROUP_ID},
        rpc:call(Node, group_logic, join_group, [User1Sess, ?GROUP_1, ?MOCK_JOIN_GROUP_TOKEN])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_BAD_VALUE_TOKEN(<<"token">>),
        rpc:call(Node, group_logic, join_group, [User1Sess, ?GROUP_1, <<"wrongToken">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        {ok, ?MOCK_JOINED_SPACE_ID},
        rpc:call(Node, group_logic, join_space, [User1Sess, ?GROUP_1, ?MOCK_JOIN_SPACE_TOKEN])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_BAD_VALUE_TOKEN(<<"token">>),
        rpc:call(Node, group_logic, join_space, [User1Sess, ?GROUP_1, <<"wrongToken">>])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ok,
        rpc:call(Node, group_logic, leave_group, [User1Sess, ?GROUP_1, ?GROUP_2])
    ),
    ?assertEqual(GraphCalls + 5, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, group_logic, leave_group, [User1Sess, ?GROUP_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 6, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ok,
        rpc:call(Node, group_logic, leave_space, [User1Sess, ?GROUP_1, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 7, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, group_logic, leave_space, [User1Sess, ?GROUP_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 8, logic_tests_common:count_reqs(Config, graph)),

    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        logic_tests_common:mock_gs_client(NewConfig),
        NewConfig
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [logic_tests_common]} | Config].

init_per_testcase(_, Config) ->
    logic_tests_common:wait_for_mocked_connection(Config),
    Config.

end_per_testcase(_, Config) ->
    logic_tests_common:invalidate_all_test_records(Config),
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================