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
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([registers_for_updates/1, accounts_incoming_updates/1,
    saves_the_actual_data/1, updates_with_the_actual_data/1,
    resolves_conflicts/1, registers_for_updates_with_users/1,
    applies_deletion/1,
    new_user_with_present_space_triggers_file_meta_creation/1,
    new_user_with_present_space_triggers_file_meta_creation2/1,
    new_user_with_new_space_triggers_file_meta_creation/1,
    updated_user_with_present_space_triggers_file_meta_creation/1,
    updated_user_with_present_space_triggers_file_meta_creation2/1,
    updated_user_with_present_space_triggers_file_meta_creation3/1,
    add_user_to_group_triggers_file_meta_creation/1,
    add_space_to_group_triggers_file_meta_creation/1,
    add_provider_to_space_triggers_file_meta_creation/1,
    space_without_support_test/1]).

-define(SUBSCRIPTIONS_STATE_KEY, <<"current_state">>).
-define(MESSAGES_WAIT_TIMEOUT, timer:seconds(3)).
-define(MESSAGES_RECEIVE_ATTEMPTS, 30).

%% appends function name to id (atom) and yields binary accepted by the db
-define(ID(Id),
    ?ID(Id, element(2, element(2, process_info(self(), current_function))))
).

-define(ID(Id, Ext), list_to_binary(
    atom_to_list(Id) ++ " # " ++ atom_to_list(Ext)
)).

all() -> ?ALL([
    new_user_with_present_space_triggers_file_meta_creation,
    new_user_with_present_space_triggers_file_meta_creation2,
    new_user_with_new_space_triggers_file_meta_creation,
    updated_user_with_present_space_triggers_file_meta_creation,
    updated_user_with_present_space_triggers_file_meta_creation2,
    updated_user_with_present_space_triggers_file_meta_creation3,
%%    add_user_to_group_triggers_file_meta_creation,
%%    add_space_to_group_triggers_file_meta_creation,
    add_provider_to_space_triggers_file_meta_creation,
%%    space_without_support_test,
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
    create_rest_session(Node, U1),
    create_rest_session(Node, U2),

    %% then
    expect_message([U1, U2], 0, []),
    ok.

accounts_incoming_updates(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    S1 = ?ID(s1),

    push_update(Node, [
        update(1, [<<"r1">>], S1, space(S1))
    ]),
    expect_message([], 1, []),

    push_update(Node, [
        update(2, [<<"r1">>], S1, space(S1)),
        update(3, [<<"r1">>], S1, space(S1)),
        update(4, [<<"r1">>], S1, space(S1))
    ]),
    expect_message([], 4, []),

    push_update(Node, [
        update(5, [<<"r1">>], S1, space(S1)),
        update(7, [<<"r1">>], S1, space(S1)),
        update(9, [<<"r1">>], S1, space(S1))
    ]),
    expect_message([], 9, [6, 8]),

    push_update(Node, [
        update(10, [<<"r1">>], S1, space(S1)),
        update(11, [<<"r1">>], S1, space(S1)),
        update(12, [<<"r1">>], S1, space(S1))
    ]),
    expect_message([], 12, [6, 8]),

    push_update(Node, [
        update(1, [<<"r1">>], S1, space(S1)),
        update(2, [<<"r1">>], S1, space(S1)),
        update(3, [<<"r1">>], S1, space(S1))
    ]),
    expect_message([], 12, [6, 8]),

    push_update(Node, [
        update(6, [<<"r1">>], S1, space(S1)),
        update(8, [<<"r1">>], S1, space(S1)),
        update(15, [<<"r1">>], S1, space(S1))
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
    {P1, Sp1, Sh1, U1, U2, G1} =
        {?ID(p1), ?ID(sp1), ?ID(sh1), ?ID(u1), ?ID(u2), ?ID(g1)},
    Priv1 = privileges:space_user(),
    Priv2 = privileges:space_admin(),

    %% when
    push_update(Node, [
        update(1, [<<"r2">>, <<"r1">>], Sp1, space(
            <<"space xp">>,
            [{<<"U1">>, Priv1}, {<<"U2">>, []}],
            [{<<"G1">>, Priv2}],
            [{<<"P1">>, 1000}]
        )),
        update(2, [<<"r2">>, <<"r1">>], G1, group(
            <<"group lol">>,
            [<<"S1">>, <<"S2">>],
            [{<<"U1">>, Priv1}, {<<"U2">>, []}],
            [{<<"U1">>, Priv1}, {<<"U2">>, Priv2}, {<<"U3">>, []}],
            [{<<"bastard">>, []}, {<<"sob">>, Priv2}],
            [<<"dad">>, <<"mom">>],
            <<"unit">>
        )),
        update(3, [<<"r2">>, <<"r1">>], P1, provider(
            <<"diginet rulz">>,
            [<<"url1">>, <<"url2">>],
            [Sp1],
            false
        )),
        update(4, [<<"r2">>, <<"r1">>], Sh1, share(
            <<"Share 1">>,
            Sp1,
            <<"root_file_id">>,
            <<"public_url">>
        ))
    ]),
    expect_message([], 4, []),

    push_update(Node, [
        update(5, [<<"r2">>, <<"r1">>], U1,
            user(<<"onedata ftw">>, [<<"A">>, <<"B">>],
                [{<<"C">>, <<"D">>}, {<<"E">>, <<"F">>}], <<"C">>,
                [<<"A">>, <<"B">>, <<"Z">>])
        ),
        update(6, [<<"r2">>, <<"r1">>], U2,
            public_only_user(<<"bombastic">>)
        )
    ]),
    expect_message([], 6, []),

    %% then
    ?assertMatch({ok, (#document{key = Sp1, value = #space_info{
        name = <<"space xp">>,
        users = [{<<"U1">>, Priv1}, {<<"U2">>, []}],
        groups = [{<<"G1">>, Priv2}],
        providers_supports = [{<<"P1">>, 1000}],
        revision_history = [<<"r2">>, <<"r1">>]}})
    }, fetch(Node, space_info, Sp1)),
    ?assertMatch({ok, (#document{key = Sh1, value = #share_info{
        name = <<"Share 1">>,
        parent_space = Sp1,
        root_file_id = <<"root_file_id">>,
        public_url = <<"public_url">>,
        revision_history = [<<"r2">>, <<"r1">>]}})
    }, fetch(Node, share_info, Sh1)),
    ?assertMatch({ok, #document{key = G1, value = #onedata_group{
        name = <<"group lol">>,
        type = unit,
        spaces = [<<"S1">>, <<"S2">>],
        users = [{<<"U1">>, Priv1}, {<<"U2">>, []}],
        effective_users = [{<<"U1">>, Priv1}, {<<"U2">>, Priv2}, {<<"U3">>, []}],
        nested_groups = [{<<"bastard">>, []}, {<<"sob">>, Priv2}],
        parent_groups = [<<"dad">>, <<"mom">>],
        revision_history = [<<"r2">>, <<"r1">>]}}
    }, fetch(Node, onedata_group, G1)),
    ?assertMatch({ok, #document{key = U1, value = #onedata_user{
        name = <<"onedata ftw">>,
        group_ids = [<<"A">>, <<"B">>],
        spaces = [{<<"C">>, <<"D">>}, {<<"E">>, <<"F">>}],
        default_space = <<"C">>,
        effective_group_ids = [<<"A">>, <<"B">>, <<"Z">>],
        revision_history = [<<"r2">>, <<"r1">>]}}
    }, fetch(Node, onedata_user, U1)),
    ?assertMatch({ok, #document{key = U2, value = #onedata_user{
        name = <<"bombastic">>,
        revision_history = []}}
    }, fetch(Node, onedata_user, U2)),
    ?assertMatch({ok, #document{key = P1, value = #provider_info{
        client_name = <<"diginet rulz">>,
        revision_history = [<<"r2">>, <<"r1">>],
        urls = [<<"url1">>, <<"url2">>],
        space_ids = [Sp1],
        public_only = false}}
    }, fetch(Node, provider_info, P1)),
    ok.

check_file_operations_test_base(Config, UpdateFun, IdExt) ->
    %% given
    [Node | _] = Nodes = ?config(op_worker_nodes, Config),
    {P1, S1, U1, G1} = {get_provider_id(Node), ?ID(s1, IdExt), ?ID(u1, IdExt), ?ID(g1, IdExt)},
    Priv1 = privileges:space_admin(),
    SessionID = <<"session">>,
    create_fuse_session(Node, SessionID, U1),
    oz_spaces_mock(Nodes, <<"space_name">>),

    %% when
    UpdateFun(Node, S1, U1, P1, Priv1, G1),

    %% then
    FilePath = <<"/space_name/", (generator:gen_name())/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:create(Node, SessionID, FilePath, 8#240)),
    OpenResult = lfm_proxy:open(Node, SessionID, {path, FilePath}, write),
    ?assertMatch({ok, _}, OpenResult),
    {ok, Handle} = OpenResult,
    ?assertMatch({ok, _}, lfm_proxy:write(Node, Handle, 0, <<"yolo">>)),
    ok.

new_user_with_present_space_triggers_file_meta_creation(Config) ->
    UpdateFun = fun(Node, S1, U1, P1, Priv1, _G1) ->
        push_update(Node, [
            update(1, [<<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [{U1, Priv1}], [], [{P1, 1000}]
            )),
            update(2, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet rulz">>))
        ]),
        expect_message([U1], 2, []),

        push_update(Node, [
            update(3, [<<"r2">>, <<"r1">>], U1,
                user(<<"onedata ftw">>, [], [{S1, <<"space_name">>}], S1)
            )
        ]),
        expect_message([U1], 3, [])
    end,

    check_file_operations_test_base(Config, UpdateFun, ?FUNCTION).

new_user_with_present_space_triggers_file_meta_creation2(Config) ->
    UpdateFun = fun(Node, S1, U1, P1, Priv1, _G1) ->
        push_update(Node, [
            update(1, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet rulz">>))
        ]),
        expect_message([U1], 1, []),

        push_update(Node, [
            update(2, [<<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [{U1, Priv1}], [], [{P1, 1000}]
            ))
        ]),
        expect_message([U1], 2, []),

        push_update(Node, [
            update(3, [<<"r2">>, <<"r1">>], U1,
                user(<<"onedata ftw">>, [], [{S1, <<"space_name">>}], S1)
            )
        ]),
        expect_message([U1], 3, [])
    end,

    check_file_operations_test_base(Config, UpdateFun, ?FUNCTION).

new_user_with_new_space_triggers_file_meta_creation(Config) ->
    UpdateFun = fun(Node, S1, U1, P1, Priv1, _G1) ->
        push_update(Node, [
            update(1, [<<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [{U1, Priv1}], [], [{P1, 1000}]
            )),
            update(2, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet rulz">>)),
            update(3, [<<"r2">>, <<"r1">>], U1,
                user(<<"onedata ftw">>, [], [{S1, <<"space_name">>}], S1)
            )
        ]),
        expect_message([U1], 3, [])
    end,

    check_file_operations_test_base(Config, UpdateFun, ?FUNCTION).

updated_user_with_present_space_triggers_file_meta_creation(Config) ->
    UpdateFun = fun(Node, S1, U1, P1, Priv1, _G1) ->
        push_update(Node, [
            update(1, [<<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [], [], [{P1, 1000}]
            )),
            update(2, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet rulz">>)),
            update(3, [<<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [], [], undefined))
        ]),
        expect_message([U1], 3, []),

        push_update(Node, [
            update(4, [<<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [{U1, Priv1}], [], [{P1, 1000}]
            )),
            update(5, [<<"r3">>, <<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [],
                [{S1, <<"space_name">>}], S1))
        ]),
        expect_message([U1], 5, [])
    end,

    check_file_operations_test_base(Config, UpdateFun, ?FUNCTION).

updated_user_with_present_space_triggers_file_meta_creation2(Config) ->
    UpdateFun = fun(Node, S1, U1, P1, Priv1, _G1) ->
        push_update(Node, [
            update(1, [<<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [], [], undefined))
        ]),
        expect_message([U1], 1, []),

        push_update(Node, [
            update(2, [<<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [], [], [{P1, 1000}]
            )),
            update(3, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet rulz">>))
        ]),
        expect_message([U1], 3, []),

        push_update(Node, [
            update(4, [<<"r3">>, <<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [],
                [{S1, <<"space_name">>}], S1)),
            update(5, [<<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [{U1, Priv1}], [], [{P1, 1000}]
            ))
        ]),
        expect_message([U1], 5, [])
    end,

    check_file_operations_test_base(Config, UpdateFun, ?FUNCTION).

updated_user_with_present_space_triggers_file_meta_creation3(Config) ->
    UpdateFun = fun(Node, S1, U1, P1, Priv1, _G1) ->
        push_update(Node, [
            update(1, [<<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [], [], undefined))
        ]),
        expect_message([U1], 1, []),

        push_update(Node, [
            update(2, [<<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [], [], [{P1, 1000}]
            )),
            update(3, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet rulz">>))
        ]),
        expect_message([U1], 3, []),

        push_update(Node, [
            update(4, [<<"r3">>, <<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [],
                [{S1, <<"space_name">>}], S1))
        ]),
        expect_message([U1], 4, []),

        push_update(Node, [
            update(5, [<<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [{U1, Priv1}], [], [{P1, 1000}]
            ))
        ]),
        expect_message([U1], 5, [])
    end,

    check_file_operations_test_base(Config, UpdateFun, ?FUNCTION).

add_user_to_group_triggers_file_meta_creation(Config) ->
    UpdateFun = fun(Node, S1, U1, P1, Priv1, G1) ->
        push_update(Node, [
            update(1, [<<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [], [], [{P1, 1000}]
            )),
            update(2, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet rulz">>)),
            update(3, [<<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [], [], undefined)),
            update(4, [<<"r2">>, <<"r1">>], G1, group(<<"onedata_gr">>, [S1], []))
        ]),
        expect_message([U1], 4, []),

        push_update(Node, [
            update(5, [<<"r3">>, <<"r2">>, <<"r1">>], G1, group(<<"onedata">>, [], [{U1, Priv1}]))
        ]),
        expect_message([U1], 5, [])
    end,

    check_file_operations_test_base(Config, UpdateFun, ?FUNCTION).

add_space_to_group_triggers_file_meta_creation(Config) ->
    UpdateFun = fun(Node, S1, U1, P1, Priv1, G1) ->
        push_update(Node, [
            update(1, [<<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [], [], [{P1, 1000}]
            )),
            update(2, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet rulz">>)),
            update(3, [<<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [], [], undefined)),
            update(4, [<<"r2">>, <<"r1">>], G1, group(<<"onedata_gr">>, [], [{U1, Priv1}]))
        ]),
        expect_message([U1], 4, []),

        push_update(Node, [
            update(5, [<<"r3">>, <<"r2">>, <<"r1">>], G1, group(<<"onedata">>, [S1], []))
        ]),
        expect_message([U1], 5, [])
    end,

    check_file_operations_test_base(Config, UpdateFun, ?FUNCTION).

add_provider_to_space_triggers_file_meta_creation(Config) ->
    UpdateFun = fun(Node, S1, U1, P1, Priv1, _G1) ->
        push_update(Node, [
            update(1, [<<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [{U1, Priv1}], [], []
            )),
            update(2, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet rulz">>)),
            update(3, [<<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [],
                [{S1, <<"space_name">>}], S1))
        ]),
        expect_message([U1], 3, []),

        push_update(Node, [
            update(4, [<<"r3">>, <<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [{U1, Priv1}], [], [{P1, 1000}]
            ))
        ]),
        expect_message([U1], 4, [])
    end,

    check_file_operations_test_base(Config, UpdateFun, ?FUNCTION).

space_without_support_test(Config) ->
    %% given
    [Node | _] = Nodes = ?config(op_worker_nodes, Config),
    {P1, S1, U1} = {get_provider_id(Node), ?ID(s1), ?ID(u1)},
    Priv1 = privileges:space_admin(),
    SessionID = <<"session">>,
    create_fuse_session(Node, SessionID, U1),
    oz_spaces_mock(Nodes, <<"space_name">>),

    %% when
    push_update(Node, [
        update(1, [<<"r2">>, <<"r1">>], S1, space(
            <<"space_name">>, [{U1, Priv1}], [], []
        )),
        update(2, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet rulz">>)),
        update(3, [<<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [],
            [{S1, <<"space_name">>}], S1))
    ]),
    expect_message([U1], 3, []),

    push_update(Node, [
        update(4, [<<"r3">>, <<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [], [S1], S1))
    ]),
    expect_message([U1], 4, []),

    %% then
    FilePath = <<"/space_name/", (generator:gen_name())/binary>>,
    ?assertMatch({error, _}, lfm_proxy:create(Node, SessionID, FilePath, 8#240)),
    ok.


updates_with_the_actual_data(Config) ->
    %% given
    [Node | _] = ?config(op_worker_nodes, Config),
    {P1, S1, U1, U2, G1} = {?ID(p1), ?ID(s1), ?ID(u1), ?ID(u2), ?ID(g1)},
    Priv1 = privileges:space_user(),
    Priv2 = privileges:space_admin(),

    push_update(Node, [
        update(1, [<<"r2">>, <<"r1">>], S1, space(<<"space">>)),
        update(2, [<<"r2">>, <<"r1">>], G1, group(<<"group">>)),
        update(3, [<<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [], [], S1, [<<"Z">>])),
        update(4, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet">>))
    ]),
    expect_message([], 4, []),

    %% when
    push_update(Node, [
        update(5, [<<"r3">>, <<"r2">>, <<"r1">>], S1, space(
            <<"space xp">>,
            [{<<"U1">>, Priv1}, {<<"U2">>, []}],
            [{<<"G1">>, Priv2}],
            [{<<"P1">>, 1000}]
        )),
        update(6, [<<"r3">>, <<"r2">>, <<"r1">>], G1, group(
            <<"group lol">>,
            [<<"S1">>, <<"S2">>],
            [{<<"U1">>, Priv1}, {<<"U2">>, []}],
            [{<<"U1">>, Priv1}, {<<"U2">>, Priv2}, {<<"U3">>, []}],
            [{<<"bastard">>, []}, {<<"sob">>, Priv2}],
            [<<"dad">>, <<"mom">>],
            <<"team">>
        )),
        update(7, [<<"r3">>, <<"r2">>, <<"r1">>], U1,
            user(<<"onedata ftw">>, [<<"A">>, <<"B">>],
                [{<<"C">>, <<"D">>}, {<<"E">>, <<"F">>}], <<"C">>,
                [<<"A">>, <<"B">>, <<"Y">>])
        ),
        update(8, [<<"r2">>, <<"r1">>], U2,
            public_only_user(<<"bombastic">>)
        ),
        update(9, [<<"r3">>, <<"r2">>, <<"r1">>], P1, provider(
            <<"diginet rulz">>,
            [<<"url1">>, <<"url2">>],
            [S1],
            true
        ))
    ]),
    expect_message([], 9, []),

    %% then
    ?assertMatch({ok, (#document{key = S1, value = #space_info{
        name = <<"space xp">>,
        users = [{<<"U1">>, Priv1}, {<<"U2">>, []}],
        groups = [{<<"G1">>, Priv2}],
        providers_supports = [{<<"P1">>, 1000}],
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}})
    }, fetch(Node, space_info, S1)),
    ?assertMatch({ok, #document{key = G1, value = #onedata_group{
        name = <<"group lol">>,
        type = team,
        spaces = [<<"S1">>, <<"S2">>],
        users = [{<<"U1">>, Priv1}, {<<"U2">>, []}],
        effective_users = [{<<"U1">>, Priv1}, {<<"U2">>, Priv2}, {<<"U3">>, []}],
        nested_groups = [{<<"bastard">>, []}, {<<"sob">>, Priv2}],
        parent_groups = [<<"dad">>, <<"mom">>],
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}}
    }, fetch(Node, onedata_group, G1)),
    ?assertMatch({ok, #document{key = U1, value = #onedata_user{
        name = <<"onedata ftw">>,
        group_ids = [<<"A">>, <<"B">>],
        effective_group_ids = [<<"A">>, <<"B">>, <<"Y">>],
        default_space = <<"C">>,
        spaces = [{<<"C">>, <<"D">>}, {<<"E">>, <<"F">>}],
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}}
    }, fetch(Node, onedata_user, U1)),
    ?assertMatch({ok, #document{key = U2, value = #onedata_user{
        name = <<"bombastic">>,
        revision_history = []}}
    }, fetch(Node, onedata_user, U2)),
    ?assertMatch({ok, #document{key = P1, value = #provider_info{
        client_name = <<"diginet rulz">>,
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>],
        urls = [<<"url1">>, <<"url2">>],
        space_ids = [S1],
        public_only = true}}
    }, fetch(Node, provider_info, P1)),
    ok.

applies_deletion(Config) ->
    %% given
    [Node | _] = ?config(op_worker_nodes, Config),
    {P1, Sp1, Sh1, U1, G1} = {?ID(p1), ?ID(sp1), ?ID(sh1), ?ID(u1), ?ID(g1)},
    push_update(Node, [
        update(1, [<<"r2">>, <<"r1">>], Sp1, space(<<"space">>)),
        update(2, [<<"r2">>, <<"r1">>], Sh1, share(<<"share">>, Sp1)),
        update(3, [<<"r2">>, <<"r1">>], G1, group(<<"group">>)),
        update(4, [<<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [], [], Sp1)),
        update(5, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet">>))
    ]),
    expect_message([], 5, []),

    %% when
    push_update(Node, [
        update(6, undefined, P1, {<<"provider">>, <<"delete">>}),
        update(8, undefined, Sp1, {<<"space">>, <<"delete">>}),
        update(9, undefined, G1, {<<"group">>, <<"delete">>}),
        update(7, undefined, Sh1, {<<"share">>, <<"delete">>}),
        update(10, undefined, U1, {<<"user">>, <<"delete">>})
    ]),
    expect_message([], 10, []),

    %% then
    ?assertMatch({error, {not_found, space_info}},
        fetch(Node, space_info, Sp1)),
    ?assertMatch({error, {not_found, share_info}},
        fetch(Node, share_info, Sh1)),
    ?assertMatch({error, {not_found, onedata_group}},
        fetch(Node, onedata_group, G1)),
    ?assertMatch({error, {not_found, onedata_user}},
        fetch(Node, onedata_user, U1)),
    ?assertMatch({error, {not_found, provider_info}},
        fetch(Node, provider_info, P1)),
    ok.

%% details of conflict resolutions are covered by eunit tests
resolves_conflicts(Config) ->
    %% given
    [Node | _] = ?config(op_worker_nodes, Config),
    {Sp1, Sh1, U1, G1} = {?ID(sp1), ?ID(sh1), ?ID(u1), ?ID(g1)},
    push_update(Node, [
        update(1, [<<"r3">>, <<"r2">>, <<"r1">>], Sp1, space(<<"space xp">>)),
        update(2, [<<"r3">>, <<"r2">>, <<"r1">>], Sh1, share(<<"share xp">>, Sp1)),
        update(3, [<<"r3">>, <<"r2">>, <<"r1">>], G1, group(<<"group lol">>)),
        update(4, [<<"r3">>, <<"r2">>, <<"r1">>], U1,
            user(<<"onedata ftw">>, [<<"A">>, <<"B">>],
                [{<<"C">>, <<"D">>}, {<<"E">>, <<"F">>}], <<"C">>)
        )
    ]),
    expect_message([], 4, []),

    %% when
    push_update(Node, [
        update(5, [<<"r2">>, <<"r1">>], Sp1, space(<<"space">>)),
        update(6, [<<"r2">>, <<"r1">>], Sh1, share(<<"share">>, Sp1)),
        update(7, [<<"r3">>], G1, group(<<"group">>)),
        update(8, [<<"r3">>, <<"r2">>, <<"r1">>], U1,
            user(<<"onedata">>, [], [], Sp1)
        )
    ]),
    expect_message([], 8, []),

    %% then
    ?assertMatch({ok, #document{key = Sp1, value = #space_info{
        name = <<"space xp">>,
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}}
    }, fetch(Node, space_info, Sp1)),
    ?assertMatch({ok, #document{key = Sh1, value = #share_info{
        name = <<"share xp">>,
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}}
    }, fetch(Node, share_info, Sh1)),
    ?assertMatch({ok, #document{key = G1, value = #onedata_group{
        name = <<"group lol">>,
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}}
    }, fetch(Node, onedata_group, G1)),
    ?assertMatch({ok, #document{key = U1, value = #onedata_user{
        name = <<"onedata ftw">>, default_space = <<"C">>,
        group_ids = [<<"A">>, <<"B">>], spaces = [{<<"C">>, <<"D">>}, {<<"E">>, <<"F">>}],
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}}
    }, fetch(Node, onedata_user, U1)),
    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    ?TEST_STOP(Config).

init_per_testcase(Case, Config) ->
    ?CASE_START(Case),
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

    initializer:communicator_mock(Nodes),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    FinalConfig = lfm_proxy:init(ConfigWithSessionInfo),

    reset_state(Nodes),
    FinalConfig.

reset_state(Nodes) ->
    clear_sessions(Nodes),
    rpc:call(hd(Nodes), subscriptions_state, delete, [?SUBSCRIPTIONS_STATE_KEY]),
    rpc:call(hd(Nodes), subscriptions, ensure_initialised, []),
    flush().

clear_sessions(Nodes) ->
    {ok, Docs} = rpc:call(hd(Nodes), session, list, []),
    FilteredDocs = lists:filter(fun(#document{key = Key}) ->
        Key =/= ?ROOT_SESS_ID
    end, Docs),

    lists:foreach(fun(#document{key = Key}) ->
        ok = rpc:call(hd(Nodes), session, delete, [Key])
    end, FilteredDocs).

end_per_testcase(Case, Config) ->
    ?CASE_STOP(Case),
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Nodes, subscription_wss),

    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_unload(Nodes, [communicator]),

    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

push_update(Node, Updates) ->
    EncodedUpdates = json_utils:encode(Updates),
    Request = [{binary, EncodedUpdates}, undefined, undefined],
    Result = rpc:call(Node, subscription_wss, websocket_handle, Request),
    ?assertMatch({ok, _}, Result).

provider(Name) ->
    provider(Name, [], [], false).
provider(Name, URLs, Spaces, PublicOnly) ->
    {provider, [{client_name, Name}, {urls, URLs}, {space_ids, Spaces},
        {public_only, PublicOnly}]}.

space(Name) ->
    space(Name, [], [], []).
space(Name, UsersWithPrivileges, GroupsWithPrivileges, Supports) ->
    {space, [{name, Name}, {users, UsersWithPrivileges},
        {groups, GroupsWithPrivileges}, {providers_supports, Supports}]}.

share(Name, ParentSpaceId) ->
    share(Name, ParentSpaceId, <<"">>, <<"">>).
share(Name, ParentSpaceId, RootFileId, PublicUrl) ->
    {share, [{name, Name}, {root_file_id, RootFileId},
        {parent_space, ParentSpaceId}, {public_url, PublicUrl}]}.

group(Name) ->
    group(Name, [], []).
group(Name, SIDs, Users) ->
    group(Name, SIDs, Users, Users, [], [], undefined).
group(Name, SIDs, Users, EffectiveUsers, NestedGroups, ParentGroups, Type) ->
    {group, [{name, Name}, {type, Type}, {spaces, SIDs}, {users, Users},
        {effective_users, EffectiveUsers}, {nested_groups, NestedGroups},
        {parent_groups, ParentGroups}]}.

public_only_user(Name) ->
    user(Name, [], [], undefined, [], true).
user(Name, GIDs, Spaces, DefaultSpace) ->
    user(Name, GIDs, Spaces, DefaultSpace, GIDs).
user(Name, GIDs, Spaces, DefaultSpace, EffectiveGroups) ->
    user(Name, GIDs, Spaces, DefaultSpace, EffectiveGroups, false).
user(Name, GIDs, Spaces, DefaultSpace, EffectiveGroups, PublicOnly) ->
    {user, [{name, Name}, {group_ids, GIDs}, {space_names, Spaces},
        {public_only, PublicOnly}, {default_space, DefaultSpace},
        {effective_group_ids, EffectiveGroups}]}.

update(Seq, Revs, ID, Core) ->
    [{seq, Seq}, {revs, Revs}, {id, ID}, Core].


fetch(Node, Model, ID) ->
    rpc:call(Node, Model, get, [ID]).

get_provider_id(Node) ->
    rpc:call(Node, oneprovider, get_provider_id, []).

create_rest_session(Node, UserID) ->
    ?assertMatch({ok, _}, rpc:call(Node, session_manager, reuse_or_create_rest_session, [
        #user_identity{user_id = UserID}, #token_auth{}
    ])).

create_fuse_session(Node, SessionID, UserID) ->
    ?assertMatch({ok, _}, rpc:call(Node, session_manager, reuse_or_create_fuse_session, [
        SessionID, #user_identity{user_id = UserID}, #token_auth{}, self()
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
            expect_message(Match, Retries - 1)
    after
        ?MESSAGES_WAIT_TIMEOUT -> ?assertMatch(Match, <<"timeout">>)
    end.


flush() ->
    receive _ -> ok
    after 0 -> ok end.

oz_spaces_mock(Nodes, SpaceName) ->
    test_utils:mock_expect(Nodes, oz_spaces, get_details,
        fun(_, _) -> {ok, #space_details{name = SpaceName}} end),
    test_utils:mock_expect(Nodes, oz_spaces, get_users,
        fun(_, _) -> {ok, []} end),
    test_utils:mock_expect(Nodes, oz_spaces, get_groups,
        fun(_, _) -> {ok, []} end).
