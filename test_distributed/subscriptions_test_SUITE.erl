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
    space_without_support_test/1,
    pushing_space_user_write_priv_unlocks_space_for_user/1,
    pushing_space_group_write_priv_unlocks_space_for_user/1,
    pushing_space_group_write_priv_locks_space_for_user/1,
    pushing_space_user_write_priv_locks_space_for_user/1,
    pushing_space_group_write_priv_locks_space_for_user_even_if_owner/1
]).

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
    registers_for_updates_with_users,
    pushing_space_user_write_priv_unlocks_space_for_user,
    pushing_space_group_write_priv_unlocks_space_for_user,
    pushing_space_group_write_priv_locks_space_for_user,
    pushing_space_user_write_priv_locks_space_for_user,
    pushing_space_group_write_priv_locks_space_for_user_even_if_owner
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
    {P1, Sp1, U1, U2, G1} = {?ID(p1), ?ID(sp1), ?ID(u1), ?ID(u2), ?ID(g1)},
    {Sh1, HS1, H1} = {?ID(sh1), ?ID(hs1), ?ID(h1)},
    Priv1 = privileges:space_user(),
    Priv2 = privileges:space_admin(),
    HSPrivs = privileges:handle_service_admin(),
    HPrivs = privileges:handle_admin(),

    %% when
    push_update(Node, [
        update(1, [<<"r2">>, <<"r1">>], Sp1, space(
            <<"space xp">>,
            [{<<"U1">>, Priv1}, {<<"U2">>, []}],
            [{<<"G1">>, Priv2}],
            [{<<"P1">>, 1000}],
            [<<"Share1">>]
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
            <<"public_url">>,
            <<"handle">>
        )),
        update(5, [<<"r2">>, <<"r1">>], HS1, handle_service(
            <<"Handle Service 1">>,
            [{<<"U1">>, HSPrivs}],
            [{<<"G1">>, HSPrivs}]
        )),
        update(6, [<<"r2">>, <<"r1">>], H1, handle(
            HS1,
            <<"share">>,
            [{<<"U2">>, HPrivs}],
            [{<<"G2">>, HPrivs}]
        ))
    ]),
    expect_message([], 6, []),

    push_update(Node, [
        update(7, [<<"r2">>, <<"r1">>], U1,
            user(<<"onedata ftw">>, [<<"A">>, <<"B">>, <<"Z">>],
                [{<<"C">>, <<"D">>}, {<<"E">>, <<"F">>}], <<"C">>)
        ),
        update(8, [<<"r2">>, <<"r1">>], U2,
            public_only_user(<<"bombastic">>)
        )
    ]),
    expect_message([], 8, []),

    %% then
    ?assertMatch({ok, (#document{key = Sp1, value = #od_space{
        name = <<"space xp">>,
        users = [{<<"U1">>, Priv1}, {<<"U2">>, []}],
        groups = [{<<"G1">>, Priv2}],
        providers_supports = [{<<"P1">>, 1000}],
        shares = [<<"Share1">>],
        revision_history = [<<"r2">>, <<"r1">>]}})
    }, fetch(Node, od_space, Sp1)),
    ?assertMatch({ok, (#document{key = Sh1, value = #od_share{
        name = <<"Share 1">>,
        space = Sp1,
        root_file = <<"root_file_id">>,
        public_url = <<"public_url">>,
        handle = <<"handle">>,
        revision_history = [<<"r2">>, <<"r1">>]}})
    }, fetch(Node, od_share, Sh1)),
    ?assertMatch({ok, #document{key = G1, value = #od_group{
        name = <<"group lol">>,
        type = unit,
        spaces = [<<"S1">>, <<"S2">>],
        users = [{<<"U1">>, Priv1}, {<<"U2">>, []}],
        eff_users = [{<<"U1">>, Priv1}, {<<"U2">>, Priv2}, {<<"U3">>, []}],
        children = [{<<"bastard">>, []}, {<<"sob">>, Priv2}],
        parents = [<<"dad">>, <<"mom">>],
        revision_history = [<<"r2">>, <<"r1">>]}}
    }, fetch(Node, od_group, G1)),
    ?assertMatch({ok, #document{key = U1, value = #od_user{
        name = <<"onedata ftw">>,
        space_aliases = [{<<"C">>, <<"D">>}, {<<"E">>, <<"F">>}],
        default_space = <<"C">>,
        eff_groups = [<<"A">>, <<"B">>, <<"Z">>],
        revision_history = [<<"r2">>, <<"r1">>]}}
    }, fetch(Node, od_user, U1)),
    ?assertMatch({ok, #document{key = U2, value = #od_user{
        name = <<"bombastic">>,
        revision_history = []}}
    }, fetch(Node, od_user, U2)),
    ?assertMatch({ok, #document{key = P1, value = #od_provider{
        client_name = <<"diginet rulz">>,
        revision_history = [<<"r2">>, <<"r1">>],
        urls = [<<"url1">>, <<"url2">>],
        spaces = [Sp1],
        public_only = false}}
    }, fetch(Node, od_provider, P1)),
    ?assertMatch({ok, #document{key = HS1, value = #od_handle_service{
        name = <<"Handle Service 1">>,
        proxy_endpoint = <<"">>,
        service_properties = [],
        users = [{<<"U1">>, HSPrivs}],
        groups = [{<<"G1">>, HSPrivs}],
        revision_history = [<<"r2">>, <<"r1">>]}}
    }, fetch(Node, od_handle_service, HS1)),
    ?assertMatch({ok, #document{key = H1, value = #od_handle{
        handle_service = HS1,
        public_handle = <<"">>,
        resource_type = <<"">>,
        resource_id = <<"share">>,
        metadata = <<"">>,
        users = [{<<"U2">>, HPrivs}],
        groups = [{<<"G2">>, HPrivs}],
        timestamp = {{0, 0, 0}, {0, 0, 0}},
        revision_history = [<<"r2">>, <<"r1">>]}}
    }, fetch(Node, od_handle, H1)),
    ok.

check_file_operations_test_base0(Config, IdExt) ->
    %% given
    [Node | _] = Nodes = ?config(op_worker_nodes, Config),
    {P1, S1, U1, G1} = {get_provider_id(Node), ?ID(s1, IdExt), ?ID(u1, IdExt), ?ID(g1, IdExt)},
    Priv1 = privileges:space_admin(),
    SessionID = <<"session">>,
    create_fuse_session(Node, SessionID, U1),
    oz_spaces_mock(Nodes, <<"space_name">>),

    {SessionID, Node, S1, U1, P1, Priv1, G1}.


check_file_operations_test_base(Config, UpdateFun, IdExt) ->
    {SessionID, Node, S1, U1, P1, Priv1, G1} = check_file_operations_test_base0(Config, IdExt),
    FilePath = <<"/space_name/", (generator:gen_name())/binary>>,

    %% when
    UpdateFun(Node, S1, U1, P1, Priv1, G1),

    %% then
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
                <<"space_name">>, [{U1, Priv1}], [], [{P1, 1000}], [<<"Share1">>]
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
                <<"space_name">>, [{U1, Priv1}], [], [{P1, 1000}], [<<"Share1">>]
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
                <<"space_name">>, [{U1, Priv1}], [], [{P1, 1000}], [<<"Share1">>]
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
                <<"space_name">>, [], [], [{P1, 1000}], [<<"Share1">>]
            )),
            update(2, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet rulz">>)),
            update(3, [<<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [], [], undefined))
        ]),
        expect_message([U1], 3, []),

        push_update(Node, [
            update(4, [<<"r3">>, <<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [{U1, Priv1}], [], [{P1, 1000}], [<<"Share1">>]
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
                <<"space_name">>, [], [], [{P1, 1000}], [<<"Share1">>]
            )),
            update(3, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet rulz">>))
        ]),
        expect_message([U1], 3, []),

        push_update(Node, [
            update(4, [<<"r3">>, <<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [],
                [{S1, <<"space_name">>}], S1)),
            update(5, [<<"r3">>, <<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [{U1, Priv1}], [], [{P1, 1000}], [<<"Share1">>]
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
                <<"space_name">>, [], [], [{P1, 1000}], [<<"Share1">>]
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
            update(5, [<<"r3">>, <<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [{U1, Priv1}], [], [{P1, 1000}], [<<"Share1">>]
            ))
        ]),
        expect_message([U1], 5, [])
    end,

    check_file_operations_test_base(Config, UpdateFun, ?FUNCTION).

add_user_to_group_triggers_file_meta_creation(Config) ->
    UpdateFun = fun(Node, S1, U1, P1, Priv1, G1) ->
        push_update(Node, [
            update(1, [<<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [], [], [{P1, 1000}], [<<"Share1">>]
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
                <<"space_name">>, [], [], [{P1, 1000}], [<<"Share1">>]
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
                <<"space_name">>, [{U1, Priv1}], [], [], []
            )),
            update(2, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet rulz">>)),
            update(3, [<<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [],
                [{S1, <<"space_name">>}], S1))
        ]),
        expect_message([U1], 3, []),

        push_update(Node, [
            update(4, [<<"r3">>, <<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [{U1, Priv1}], [], [{P1, 1000}], [<<"Share1">>]
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
            <<"space_name">>, [{U1, Priv1}], [], [], []
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
        update(3, [<<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [<<"Z">>], [], S1)),
        update(4, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet">>))
    ]),
    expect_message([], 4, []),

    %% when
    push_update(Node, [
        update(5, [<<"r3">>, <<"r2">>, <<"r1">>], S1, space(
            <<"space xp">>,
            [{<<"U1">>, Priv1}, {<<"U2">>, []}],
            [{<<"G1">>, Priv2}],
            [{<<"P1">>, 1000}],
            [<<"Share1">>]
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
            user(<<"onedata ftw">>, [<<"A">>, <<"B">>, <<"Y">>],
                [{<<"C">>, <<"D">>}, {<<"E">>, <<"F">>}], <<"C">>)
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
    ?assertMatch({ok, (#document{key = S1, value = #od_space{
        name = <<"space xp">>,
        users = [{<<"U1">>, Priv1}, {<<"U2">>, []}],
        groups = [{<<"G1">>, Priv2}],
        providers_supports = [{<<"P1">>, 1000}],
        shares = [<<"Share1">>],
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}})
    }, fetch(Node, od_space, S1)),
    ?assertMatch({ok, #document{key = G1, value = #od_group{
        name = <<"group lol">>,
        type = team,
        spaces = [<<"S1">>, <<"S2">>],
        users = [{<<"U1">>, Priv1}, {<<"U2">>, []}],
        eff_users = [{<<"U1">>, Priv1}, {<<"U2">>, Priv2}, {<<"U3">>, []}],
        children = [{<<"bastard">>, []}, {<<"sob">>, Priv2}],
        parents = [<<"dad">>, <<"mom">>],
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}}
    }, fetch(Node, od_group, G1)),
    ?assertMatch({ok, #document{key = U1, value = #od_user{
        name = <<"onedata ftw">>,
        eff_groups = [<<"A">>, <<"B">>, <<"Y">>],
        default_space = <<"C">>,
        space_aliases = [{<<"C">>, <<"D">>}, {<<"E">>, <<"F">>}],
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}}
    }, fetch(Node, od_user, U1)),
    ?assertMatch({ok, #document{key = U2, value = #od_user{
        name = <<"bombastic">>,
        revision_history = []}}
    }, fetch(Node, od_user, U2)),
    ?assertMatch({ok, #document{key = P1, value = #od_provider{
        client_name = <<"diginet rulz">>,
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>],
        urls = [<<"url1">>, <<"url2">>],
        spaces = [S1],
        public_only = true}}
    }, fetch(Node, od_provider, P1)),
    ok.

applies_deletion(Config) ->
    %% given
    [Node | _] = ?config(op_worker_nodes, Config),
    {P1, Sp1, Sh1, U1, G1, HS1, H1} =
        {?ID(p1), ?ID(sp1), ?ID(sh1), ?ID(u1), ?ID(g1), ?ID(hs1), ?ID(h1)},
    push_update(Node, [
        update(1, [<<"r2">>, <<"r1">>], Sp1, space(<<"space">>)),
        update(2, [<<"r2">>, <<"r1">>], Sh1, share(<<"share">>, Sp1)),
        update(3, [<<"r2">>, <<"r1">>], G1, group(<<"group">>)),
        update(4, [<<"r2">>, <<"r1">>], U1, user(<<"onedata">>, [], [], Sp1)),
        update(5, [<<"r2">>, <<"r1">>], P1, provider(<<"diginet">>)),
        update(6, [<<"r3">>, <<"r2">>, <<"r1">>], HS1,
            handle_service(<<"handle service first">>, [], [])),
        update(7, [<<"r3">>, <<"r2">>, <<"r1">>], H1,
            handle(HS1, <<"someId">>, [], []))
    ]),
    expect_message([], 7, []),

    %% when
    push_update(Node, [
        update(8, undefined, P1, {<<"od_provider">>, <<"delete">>}),
        update(9, undefined, Sp1, {<<"od_space">>, <<"delete">>}),
        update(10, undefined, G1, {<<"od_group">>, <<"delete">>}),
        update(11, undefined, Sh1, {<<"od_share">>, <<"delete">>}),
        update(12, undefined, U1, {<<"od_user">>, <<"delete">>}),
        update(13, undefined, HS1, {<<"od_handle_service">>, <<"delete">>}),
        update(14, undefined, H1, {<<"od_handle">>, <<"delete">>})
    ]),
    expect_message([], 14, []),

    %% then
    ?assertMatch({error, {not_found, od_space}},
        fetch(Node, od_space, Sp1)),
    ?assertMatch({error, {not_found, od_share}},
        fetch(Node, od_share, Sh1)),
    ?assertMatch({error, {not_found, od_group}},
        fetch(Node, od_group, G1)),
    ?assertMatch({error, {not_found, od_user}},
        fetch(Node, od_user, U1)),
    ?assertMatch({error, {not_found, od_provider}},
        fetch(Node, od_provider, P1)),
    ?assertMatch({error, {not_found, od_handle_service}},
        fetch(Node, od_handle_service, HS1)),
    ?assertMatch({error, {not_found, od_handle}},
        fetch(Node, od_handle, H1)),
    ok.

%% details of conflict resolutions are covered by eunit tests
resolves_conflicts(Config) ->
    %% given
    [Node | _] = ?config(op_worker_nodes, Config),
    {Sp1, Sh1, U1, G1, HS1, H1} =
        {?ID(sp1), ?ID(sh1), ?ID(u1), ?ID(g1), ?ID(hs1), ?ID(h1)},
    push_update(Node, [
        update(1, [<<"r3">>, <<"r2">>, <<"r1">>], Sp1, space(<<"space xp">>)),
        update(2, [<<"r3">>, <<"r2">>, <<"r1">>], Sh1, share(<<"share xp">>, Sp1)),
        update(3, [<<"r3">>, <<"r2">>, <<"r1">>], G1, group(<<"group lol">>)),
        update(4, [<<"r3">>, <<"r2">>, <<"r1">>], U1,
            user(<<"onedata ftw">>, [<<"A">>, <<"B">>],
                [{<<"C">>, <<"D">>}, {<<"E">>, <<"F">>}], <<"C">>)
        ),
        update(5, [<<"r3">>, <<"r2">>, <<"r1">>], HS1,
            handle_service(<<"handle service first">>, [], [])),
        update(6, [<<"r3">>, <<"r2">>, <<"r1">>], H1,
            handle(HS1, <<"someId">>, [], []))
    ]),
    expect_message([], 6, []),

    %% when
    push_update(Node, [
        update(7, [<<"r2">>, <<"r1">>], Sp1, space(<<"space">>)),
        update(8, [<<"r2">>, <<"r1">>], Sh1, share(<<"share">>, Sp1)),
        update(9, [<<"r3">>], G1, group(<<"group">>)),
        update(10, [<<"r3">>, <<"r2">>, <<"r1">>], U1,
            user(<<"onedata">>, [], [], Sp1)
        ),
        update(11, [<<"r2">>, <<"r1">>], HS1,
            handle_service(<<"handle service second">>, [], [])),
        update(12, [<<"r2">>, <<"r1">>], H1,
            handle(HS1, <<"someOtherId">>, [], []))
    ]),
    expect_message([], 12, []),

    %% then
    ?assertMatch({ok, #document{key = Sp1, value = #od_space{
        name = <<"space xp">>,
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}}
    }, fetch(Node, od_space, Sp1)),
    ?assertMatch({ok, #document{key = Sh1, value = #od_share{
        name = <<"share xp">>,
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}}
    }, fetch(Node, od_share, Sh1)),
    ?assertMatch({ok, #document{key = G1, value = #od_group{
        name = <<"group lol">>,
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}}
    }, fetch(Node, od_group, G1)),
    ?assertMatch({ok, #document{key = U1, value = #od_user{
        name = <<"onedata ftw">>, default_space = <<"C">>,
        eff_groups = [<<"A">>, <<"B">>],
        space_aliases = [{<<"C">>, <<"D">>}, {<<"E">>, <<"F">>}],
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}}
    }, fetch(Node, od_user, U1)),
    ?assertMatch({ok, #document{key = HS1, value = #od_handle_service{
        name = <<"handle service first">>,
        users = [],
        groups = [],
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}}
    }, fetch(Node, od_handle_service, HS1)),
    ?assertMatch({ok, #document{key = H1, value = #od_handle{
        handle_service = HS1,
        resource_id = <<"someId">>,
        users = [],
        groups = [],
        timestamp = {{0, 0, 0}, {0, 0, 0}},
        revision_history = [<<"r3">>, <<"r2">>, <<"r1">>]}}
    }, fetch(Node, od_handle, H1)),
    ok.


pushing_space_user_write_priv_unlocks_space_for_user(Config) ->
    G1 = <<"group1">>,
    G2 = <<"group2">>,

    UpdateFun1 = fun(Node, S1, U1, P1, Priv1, _G1) ->
        push_update(Node, [
            update(1, [<<"r1">>], S1, space(
                <<"space_name">>, [{U1, ordsets:from_list([space_write_files])}],
                [], [{P1, 1000}], [<<"Share1">>]
            )),
            update(2, [<<"r1">>], U1, user(<<"onedata">>, [G1, G2],
                [{S1, <<"space_name">>}], S1))
        ]),
        expect_message([U1], 2, [])
    end,

    check_file_operations_test_base(Config, UpdateFun1, ?FUNCTION).


pushing_space_group_write_priv_unlocks_space_for_user(Config) ->
    G1 = <<"group1">>,
    G2 = <<"group2">>,

    UpdateFun1 = fun(Node, S1, U1, P1, _Priv1, _G1) ->
        push_update(Node, [
            update(1, [<<"r1">>], S1, space(
                <<"space_name">>, [], [{G2, ordsets:from_list([space_write_files])}],
                [{P1, 1000}], [<<"Share1">>]
            )),
            update(2, [<<"r1">>], U1, user(<<"onedata">>, [G1, G2],
                [{S1, <<"space_name">>}], S1))
        ]),
        expect_message([U1], 2, [])
    end,

    check_file_operations_test_base(Config, UpdateFun1, ?FUNCTION).


pushing_space_group_write_priv_locks_space_for_user(Config) ->
    G1 = <<"group1">>,
    G2 = <<"group2">>,
    G3 = <<"group3">>,

    UpdateFun1 = fun(Node, S1, U1, P1, _Priv1, _G1) ->
        push_update(Node, [
            update(1, [<<"r1">>], S1, space(
                <<"space_name">>, [], [{G3, ordsets:from_list([space_write_files])}],
                [{P1, 1000}], [<<"Share1">>]
            )),
            update(2, [<<"r1">>], U1, user(<<"onedata">>, [G1, G2],
                [{S1, <<"space_name">>}], S1))
        ]),
        expect_message([U1], 2, [])
    end,

    FilePath = <<"/space_name/", (generator:gen_name())/binary>>,
    {SessionID, Node, S1, U1, P1, Priv1, G0} = check_file_operations_test_base0(Config, ?FUNCTION),
    UpdateFun1(Node, S1, U1, P1, Priv1, G0),

    ?assertMatch({error, eacces}, lfm_proxy:create(Node, SessionID, FilePath, 8#240)).

pushing_space_user_write_priv_locks_space_for_user(Config) ->
    G1 = <<"group1">>,
    G2 = <<"group2">>,

    UpdateFun1 = fun(Node, S1, U1, P1, _Priv1, _G1) ->
        push_update(Node, [
            update(1, [<<"r1">>], S1, space(
                <<"space_name">>, [{<<"other_user">>, ordsets:from_list([space_write_files])}],
                [], [{P1, 1000}], [<<"Share1">>]
            )),
            update(2, [<<"r1">>], U1, user(<<"onedata">>, [G1, G2],
                [{S1, <<"space_name">>}], S1))
        ]),
        expect_message([U1], 2, [])
    end,

    FilePath = <<"/space_name/", (generator:gen_name())/binary>>,
    {SessionID, Node, S1, U1, P1, Priv1, G0} = check_file_operations_test_base0(Config, ?FUNCTION),
    UpdateFun1(Node, S1, U1, P1, Priv1, G0),

    ?assertMatch({error, eacces}, lfm_proxy:create(Node, SessionID, FilePath, 8#240)).


pushing_space_group_write_priv_locks_space_for_user_even_if_owner(Config) ->
    UpdateFun1 = fun(Node, S1, U1, P1, _Priv1, _G1) ->
        push_update(Node, [
            update(1, [<<"r1">>], S1, space(
                <<"space_name">>, [{U1, ordsets:from_list([space_write_files])}],
                [], [{P1, 1000}], [<<"Share1">>]
            )),
            update(2, [<<"r1">>], U1, user(<<"onedata">>, [],
                [{S1, <<"space_name">>}], S1))
        ]),
        expect_message([U1], 2, [])
    end,


    FilePath = <<"/space_name/", (generator:gen_name())/binary>>,
    {SessionID, Node, S1, U1, P1, Priv1, G1} = check_file_operations_test_base0(Config, ?FUNCTION),
    UpdateFun1(Node, S1, U1, P1, Priv1, G1),

    ?assertMatch({ok, _}, lfm_proxy:create(Node, SessionID, FilePath, 8#660)),
    OpenResult = lfm_proxy:open(Node, SessionID, {path, FilePath}, write),
    ?assertMatch({ok, _}, OpenResult),
    {ok, Handle} = OpenResult,
    ?assertMatch({ok, _}, lfm_proxy:write(Node, Handle, 0, <<"yolo">>)),


    UpdateFun2 = fun(Node, S1, U1, P1, _Priv1, _G1) ->
        push_update(Node, [
            update(3, [<<"r2">>, <<"r1">>], S1, space(
                <<"space_name">>, [{U1, ordsets:subtract(
                    privileges:space_admin(), ordsets:from_list([space_write_files])
                )}], [], [{P1, 1000}], [<<"Share1">>]
            ))
        ]),
        expect_message([U1], 3, [])
    end,

    UpdateFun2(Node, S1, U1, P1, Priv1, G1),


    OpenResultW = lfm_proxy:open(Node, SessionID, {path, FilePath}, write),
    ?assertMatch({error, eacces}, OpenResultW),

    %% Read should be possible
    OpenResultR = lfm_proxy:open(Node, SessionID, {path, FilePath}, read),
    ?assertMatch({ok, _}, OpenResultR),
    {ok, HandleR} = OpenResultR,
    ?assertMatch({ok, <<"yolo">>}, lfm_proxy:read(Node, HandleR, 0, 4)),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(_Case, Config) ->
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

end_per_testcase(_Case, Config) ->
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

public_only_user(Name) ->
    user(Name, [], [], undefined, true).
user(Name, Groups, Spaces, DefaultSpace) ->
    user(Name, Groups, Spaces, DefaultSpace, [], [], false).
user(Name, Groups, Spaces, DefaultSpace, PublicOnly) ->
    user(Name, Groups, Spaces, DefaultSpace, [], [], PublicOnly).
user(Name, Groups, Spaces, DefaultSpace, HandleServices, Handles, PublicOnly) ->
    {od_user, [
        {name, Name},
        {alias, <<"">>}, % TODO currently always empty
        {email_list, []}, % TODO currently always empty
        {connected_accounts, []}, % TODO currently always empty
        {default_space, DefaultSpace},
        {space_aliases, Spaces},

        {groups, Groups},
        {spaces, []}, % TODO currently always empty
        {handle_services, HandleServices},
        {handles, Handles},

        {eff_groups, Groups},
        {eff_spaces, []}, % TODO currently always empty
        {eff_shares, []}, % TODO currently always empty
        {eff_providers, []}, % TODO currently always empty
        {eff_handle_services, []}, % TODO currently always empty
        {eff_handles, []}, % TODO currently always empty

        {public_only, PublicOnly}
    ]}.

group(Name) ->
    group(Name, [], []).
group(Name, SIDs, Users) ->
    group(Name, SIDs, Users, Users, [], [], undefined).
group(Name, SIDs, Users, EffUsers, Children, Parents, Type) ->
    group(Name, SIDs, Users, EffUsers, Children, Parents, [], [], Type).
group(Name, SIDs, Users, EffUsers, Children, Parents, HandleServices, Handles, Type) ->
    {od_group, [
        {name, Name},
        {type, Type},

        {parents, Parents},
        {children, Children},
        {eff_parents, []}, % TODO currently always empty
        {eff_children, []}, % TODO currently always empty

        {users, Users},
        {spaces, SIDs},
        {handle_services, HandleServices},
        {handles, Handles},

        {eff_users, EffUsers},
        {eff_spaces, []}, % TODO currently always empty
        {eff_shares, []}, % TODO currently always empty
        {eff_providers, []}, % TODO currently always empty
        {eff_handle_services, []}, % TODO currently always empty
        {eff_handles, []} % TODO currently always empty
    ]}.

space(Name) ->
    space(Name, [], [], [], []).
space(Name, UsersWithPrivileges, GroupsWithPrivileges, Supports, Shares) ->
    {od_space, [
        {name, Name},

        {providers_supports, Supports},
        {users, UsersWithPrivileges},
        {groups, GroupsWithPrivileges},
        {shares, Shares},

        {eff_users, []}, % TODO currently always empty
        {eff_groups, []} % TODO currently always empty
    ]}.

share(Name, ParentSpaceId) ->
    share(Name, ParentSpaceId, <<"">>, <<"">>, <<"">>).
share(Name, ParentSpaceId, RootFileId, PublicUrl, Handle) ->
    {od_share, [
        {name, Name},
        {public_url, PublicUrl},

        {space, ParentSpaceId},
        {handle, Handle},
        {root_file, RootFileId},

        {eff_users, []}, % TODO currently always empty
        {eff_groups, []} % TODO currently always empty
    ]}.

provider(Name) ->
    provider(Name, [], [], false).
provider(Name, URLs, Spaces, PublicOnly) ->
    {od_provider, [
        {client_name, Name},
        {urls, URLs},

        {spaces, Spaces},

        {public_only, PublicOnly}
    ]}.

handle_service(Name, UsersWithPrivs, GroupsWithPrivs) ->
    handle_service(Name, <<"">>, [], UsersWithPrivs, GroupsWithPrivs).
handle_service(Name, ProxyEndpoint, ServiceProperties, UsersWithPrivs, GroupsWithPrivs) ->
    {od_handle_service, [
        {name, Name},
        {proxy_endpoint, ProxyEndpoint},
        {service_properties, ServiceProperties},

        {users, UsersWithPrivs},
        {groups, GroupsWithPrivs},

        {eff_users, []}, % TODO currently always empty
        {eff_groups, []} % TODO currently always empty
    ]}.

handle(HandleServiceId, ResourceId, UsersWithPrivs, GroupsWithPrivs) ->
    handle(HandleServiceId, <<"">>, <<"">>, ResourceId, <<"">>, UsersWithPrivs,
        GroupsWithPrivs, timestamp_utils:datetime_to_datestamp({{0, 0, 0}, {0, 0, 0}})).
handle(HandleService, PublicHandle, ResourceType, ResourceId, Metadata, UsersWithPrivs,
    GroupsWithPrivs, Timestamp) ->
    {od_handle, [
        {public_handle, PublicHandle},
        {resource_type, ResourceType},
        {resource_id, ResourceId},
        {metadata, Metadata},
        {timestamp, Timestamp},

        {handle_service, HandleService},
        {users, UsersWithPrivs},
        {groups, GroupsWithPrivs},

        {eff_users, []}, % TODO currently always empty
        {eff_groups, []} % TODO currently always empty
    ]}.

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
