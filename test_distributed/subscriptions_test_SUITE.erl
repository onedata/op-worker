%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Covers handling updates from subscription.
%%% Usually injects updates from OZ and later verifies datastore state.
%%% @end
%%%--------------------------------------------------------------------
-module(subscriptions_test_SUITE).
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([registers_for_updates/1, accounts_incoming_updates/1,
    saves_the_actual_data/1, updates_with_the_actual_data/1,
    resolves_conflicts/1, registers_for_updates_with_users/1,
    applies_deletion/1]).

-define(SUBSCRIPTIONS_STATE_KEY, <<"current_state">>).
-define(MESSAGES_WAIT_TIMEOUT, timer:seconds(3)).
-define(MESSAGES_RECEIVE_ATTEMPTS, 30).

%% appends function name to id (atom) and yields binary accepted by the db
-define(ID(Id), list_to_binary(
    atom_to_list(Id) ++ " # " ++
        atom_to_list(element(2, element(2, process_info(self(), current_function))))
)).

all() -> ?ALL([
    registers_for_updates,
    accounts_incoming_updates,
    saves_the_actual_data,
    updates_with_the_actual_data,
    applies_deletion,
    resolves_conflicts,
    registers_for_updates_with_users
]).


%%%===================================================================
%%% Test functions
%%%===================================================================

registers_for_updates(_) ->
    expect_message([], 0, []),
    ok.

registers_for_updates_with_users(Config) ->
    %% given
    [Node | _] = ?config(op_worker_nodes, Config),
    U1 = ?ID(u1),
    U2 = ?ID(u2),
    expect_message([], 0, []),

    %% when
    create_session(Node, U1),
    create_session(Node, U2),

    %% then
%%    expect_message([U1, U1], 0, []),
    ok.

accounts_incoming_updates(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    S1 = ?ID(s1),

    push_update(Node, [
        update(1, [<<"r1">>], S1, space(S1, S1))
    ]),
    expect_message([], 1, []),

    push_update(Node, [
        update(2, [<<"r1">>], S1, space(S1, S1)),
        update(3, [<<"r1">>], S1, space(S1, S1)),
        update(4, [<<"r1">>], S1, space(S1, S1))
    ]),
    expect_message([], 4, []),

    push_update(Node, [
        update(5, [<<"r1">>], S1, space(S1, S1)),
        update(7, [<<"r1">>], S1, space(S1, S1)),
        update(9, [<<"r1">>], S1, space(S1, S1))
    ]),
    expect_message([], 9, [6, 8]),

    push_update(Node, [
        update(10, [<<"r1">>], S1, space(S1, S1)),
        update(11, [<<"r1">>], S1, space(S1, S1)),
        update(12, [<<"r1">>], S1, space(S1, S1))
    ]),
    expect_message([], 12, [6, 8]),

    push_update(Node, [
        update(1, [<<"r1">>], S1, space(S1, S1)),
        update(2, [<<"r1">>], S1, space(S1, S1)),
        update(3, [<<"r1">>], S1, space(S1, S1))
    ]),
    expect_message([], 12, [6, 8]),

    push_update(Node, [
        update(6, [<<"r1">>], S1, space(S1, S1)),
        update(8, [<<"r1">>], S1, space(S1, S1)),
        update(15, [<<"r1">>], S1, space(S1, S1))
    ]),

    push_update(Node, [
        update(13, undefined, undefined, {<<"ignore">>, true}),
        update(14, undefined, undefined, {<<"ignore">>, true}),
        update(16, undefined, undefined, {<<"ignore">>, true})
    ]),
    expect_message([], 16, []),
    ok.


saves_the_actual_data(Config) ->
    %% given
    [Node | _] = ?config(op_worker_nodes, Config),
    {S1, U1, G1} = {?ID(s1), ?ID(u1), ?ID(g1)},

    %% when
    push_update(Node, [
        update(1, [<<"r1">>, <<"r2">>], S1, space(<<"space xp">>, S1)),
        update(2, [<<"r1">>, <<"r2">>], G1, group(<<"group lol">>)),
        update(3, [<<"r1">>, <<"r2">>], U1,
            user(<<"onedata ftw">>, [<<"A">>, <<"B">>], [<<"C">>, <<"D">>])
        )
    ]),
    expect_message([], 3, []),

    %% then
    ?assertMatch({ok, #document{key = S1, value = #space_info{
        id = S1,
        name = <<"space xp">>,
        revision_history = [<<"r1">>, <<"r2">>]}}
    }, fetch(Node, space_info, S1)),
    ?assertMatch({ok, #document{key = G1, value = #onedata_group{
        name = <<"group lol">>,
        revision_history = [<<"r1">>, <<"r2">>]}}
    }, fetch(Node, onedata_group, G1)),
    ?assertMatch({ok, #document{key = U1, value = #onedata_user{
        name = <<"onedata ftw">>,
        group_ids = [<<"A">>, <<"B">>], space_ids = [<<"C">>, <<"D">>],
        revision_history = [<<"r1">>, <<"r2">>]}}
    }, fetch(Node, onedata_user, U1)),
    ok.

updates_with_the_actual_data(Config) ->
    %% given
    [Node | _] = ?config(op_worker_nodes, Config),
    {S1, U1, G1} = {?ID(s1), ?ID(u1), ?ID(g1)},
    push_update(Node, [
        update(1, [<<"r1">>, <<"r2">>], S1, space(<<"space">>, S1)),
        update(2, [<<"r1">>, <<"r2">>], G1, group(<<"group">>)),
        update(3, [<<"r1">>, <<"r2">>], U1,
            user(<<"onedata">>, [], [])
        )
    ]),
    expect_message([], 3, []),

    %% when
    push_update(Node, [
        update(4, [<<"r3">>, <<"r1">>, <<"r2">>], S1, space(<<"space xp">>, S1)),
        update(5, [<<"r3">>, <<"r1">>, <<"r2">>], G1, group(<<"group lol">>)),
        update(6, [<<"r3">>, <<"r1">>, <<"r2">>], U1,
            user(<<"onedata ftw">>, [<<"A">>, <<"B">>], [<<"C">>, <<"D">>])
        )
    ]),
    expect_message([], 6, []),

    %% then
    ?assertMatch({ok, #document{key = S1, value = #space_info{
        id = S1,
        name = <<"space xp">>,
        revision_history = [<<"r3">>, <<"r1">>, <<"r2">>]}}
    }, fetch(Node, space_info, S1)),
    ?assertMatch({ok, #document{key = G1, value = #onedata_group{
        name = <<"group lol">>,
        revision_history = [<<"r3">>, <<"r1">>, <<"r2">>]}}
    }, fetch(Node, onedata_group, G1)),
    ?assertMatch({ok, #document{key = U1, value = #onedata_user{
        name = <<"onedata ftw">>,
        group_ids = [<<"A">>, <<"B">>], space_ids = [<<"C">>, <<"D">>],
        revision_history = [<<"r3">>, <<"r1">>, <<"r2">>]}}
    }, fetch(Node, onedata_user, U1)),
    ok.

applies_deletion(Config) ->
    %% given
    [Node | _] = ?config(op_worker_nodes, Config),
    {S1, U1, G1} = {?ID(s1), ?ID(u1), ?ID(g1)},
    push_update(Node, [
        update(1, [<<"r1">>, <<"r2">>], S1, space(<<"space">>, S1)),
        update(2, [<<"r1">>, <<"r2">>], G1, group(<<"group">>)),
        update(3, [<<"r1">>, <<"r2">>], U1, user(<<"onedata">>, [], []))
    ]),
    expect_message([], 3, []),

    %% when
    push_update(Node, [
        update(4, undefined, S1, {<<"space">>, <<"delete">>}),
        update(5, undefined, G1, {<<"group">>, <<"delete">>}),
        update(6, undefined, U1, {<<"user">>, <<"delete">>})
    ]),
    expect_message([], 6, []),

    %% then
    ?assertMatch({error, {not_found, space_info}},
        fetch(Node, space_info, S1)),
    ?assertMatch({error, {not_found, onedata_group}},
        fetch(Node, onedata_group, G1)),
    ?assertMatch({error, {not_found, onedata_user}},
        fetch(Node, onedata_user, U1)),
    ok.

%% details of conflict resolutions are covered by eunit tests
resolves_conflicts(Config) ->
    %% given
    [Node | _] = ?config(op_worker_nodes, Config),
    {S1, U1, G1} = {?ID(s1), ?ID(u1), ?ID(g1)},
    push_update(Node, [
        update(1, [<<"r3">>, <<"r1">>, <<"r2">>], S1, space(<<"space xp">>, S1)),
        update(2, [<<"r3">>, <<"r1">>, <<"r2">>], G1, group(<<"group lol">>)),
        update(3, [<<"r3">>, <<"r1">>, <<"r2">>], U1,
            user(<<"onedata ftw">>, [<<"A">>, <<"B">>], [<<"C">>, <<"D">>])
        )
    ]),
    expect_message([], 3, []),

    %% when
    push_update(Node, [
        update(4, [<<"r1">>, <<"r2">>], S1, space(<<"space">>, S1)),
        update(5, [<<"r3">>], G1, group(<<"group">>)),
        update(6, [<<"r3">>, <<"r1">>, <<"r2">>], U1,
            user(<<"onedata">>, [], [])
        )
    ]),
    expect_message([], 6, []),

    %% then
    ?assertMatch({ok, #document{key = S1, value = #space_info{
        id = S1,
        name = <<"space xp">>,
        revision_history = [<<"r3">>, <<"r1">>, <<"r2">>]}}
    }, fetch(Node, space_info, S1)),
    ?assertMatch({ok, #document{key = G1, value = #onedata_group{
        name = <<"group lol">>,
        revision_history = [<<"r3">>, <<"r1">>, <<"r2">>]}}
    }, fetch(Node, onedata_group, G1)),
    ?assertMatch({ok, #document{key = U1, value = #onedata_user{
        name = <<"onedata ftw">>,
        group_ids = [<<"A">>, <<"B">>], space_ids = [<<"C">>, <<"D">>],
        revision_history = [<<"r3">>, <<"r1">>, <<"r2">>]}}
    }, fetch(Node, onedata_user, U1)),
    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config1) ->
    ?TEST_INIT(Config1, ?TEST_FILE(Config1, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config),
    ok.

init_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    Self = self(),

    test_utils:mock_new(Nodes, subscription_wss),
    test_utils:mock_expect(Nodes, subscription_wss, healthcheck,
        fun() -> ok end),
    test_utils:mock_expect(Nodes, subscription_wss, push,
        fun(Message) -> Self ! Message end),
    test_utils:mock_expect(Nodes, subscription_wss, start_link, fun() ->
        {ok, Self}
    end),

    reset_state(Nodes),
    flush(),

    Config.

reset_state(Nodes) ->
    rpc:call(hd(Nodes), subscriptions_state, delete, [?SUBSCRIPTIONS_STATE_KEY]),
    rpc:call(hd(Nodes), subscriptions, ensure_initialised, []).

end_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Nodes, subscription_wss),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

push_update(Node, Updates) ->
    EncodedUpdates = json_utils:encode(Updates),
    Request = [{binary, EncodedUpdates}, undefined, undefined],
    Result = rpc:call(Node, subscription_wss, websocket_handle, Request),
    ?assertMatch({ok, _}, Result).

space(Name, ID) ->
    {space, [{id, ID}, {name, Name}]}.

group(Name) ->
    {group, [{name, Name}]}.

user(Name, GIDs, SIDs) ->
    {user, [{name, Name}, {group_ids, GIDs}, {space_ids, SIDs}]}.

update(Seq, Revs, ID, Core) ->
    [{seq, Seq}, {revs, Revs}, {id, ID}, Core].


fetch(Node, Model, ID) ->
    rpc:call(Node, Model, get, [ID]).

create_session(Node, UserID) ->
    ?assertMatch({ok, _}, rpc:call(Node, session_manager, reuse_or_create_rest_session, [
        #identity{user_id = UserID}, #auth{}
    ])).

expectation(Users, ResumeAt, Missing) ->
    json_utils:encode([
        {users, lists:usort(Users)},
        {resume_at, ResumeAt},
        {missing, Missing}
    ]).

expect_message(Users, ResumeAt, Missing) ->
    Match = expectation(Users, ResumeAt, Missing),
    expect_message(Match, ?MESSAGES_RECEIVE_ATTEMPTS).

expect_message(Match, 1) ->
    receive Rcv -> ?assertMatch(Match, Rcv)
    after 0 -> ?assertMatch(Match, <<"timeout">>) end;

expect_message(Match, Retries) ->
    receive
        Match -> ok;
        _Any ->
            ct:print("Miss ~p ~p", [_Any, Match]),
            expect_message(Match, Retries - 1)
    after
        ?MESSAGES_WAIT_TIMEOUT -> ?assertMatch(Match, <<"timeout">>)
    end.


flush() ->
    receive _ -> ok
    after 0 -> ok end.