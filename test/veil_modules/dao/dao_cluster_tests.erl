%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao_cluster module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dao_cluster_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-endif.

-ifdef(TEST).

state_test_() ->
    {foreach, fun setup/0, fun teardown/1, [fun save_state/0, fun get_state/0, fun clear_state/0]}.

fuse_session_test_() ->
    {foreach, fun setup/0, fun teardown/1, [fun save_fuse_session/0, fun get_fuse_session/0, fun remove_fuse_session/0]}.

setup() ->
    case ets:info(dao_fuse_cache) of
        undefined   -> ets:new(dao_fuse_cache, [named_table, public, set, {read_concurrency, true}]);
        [_ | _]     -> ok
    end,
    ets:delete_all_objects(dao_fuse_cache),
    meck:new(dao).

teardown(_) ->
    ok = meck:unload(dao).

save_state() ->
    meck:expect(dao, save_record, fun(#veil_document{record = #some_record{}, uuid = cluster_state, force_update = true}) -> {ok, "cluster_state"} end),
    ?assertEqual({ok, "cluster_state"}, dao_cluster:save_state(#some_record{})),
    ?assert(meck:validate(dao)).

get_state() ->
    meck:expect(dao, get_record, fun(cluster_state) -> {ok, #veil_document{record = #some_record{}}} end),
    ?assertMatch({ok, #some_record{}}, dao_cluster:get_state()),
    ?assert(meck:validate(dao)).

clear_state() ->
    meck:expect(dao, remove_record, fun(cluster_state) -> ok end),
    ?assertEqual(ok, dao_cluster:clear_state()),
    ?assert(meck:validate(dao)).

save_fuse_session() ->
    meck:expect(dao, save_record, fun(#veil_document{record = #fuse_session{}, uuid = "UUID"}) -> {ok, "UUID"} end),
    ?assertEqual({ok, "UUID"}, dao_cluster:save_fuse_session(#veil_document{record = #fuse_session{}, uuid = "UUID"})),
    ?assert(meck:validate(dao)).

get_fuse_session() ->
    meck:expect(dao, get_record, fun("UUID") -> {ok, #veil_document{record = #fuse_session{uid = 123}}} end),
    ?assertMatch({ok, #veil_document{record = #fuse_session{uid = 123}}}, dao_cluster:get_fuse_session("UUID")),

    %% Cache is used
    meck:expect(dao, get_record, fun("UUID") -> {ok, #veil_document{record = #fuse_session{uid = 1234}}} end),
    ?assertMatch({ok, #veil_document{record = #fuse_session{uid = 123}}}, dao_cluster:get_fuse_session("UUID")),

    %% Update cache
    ?assertMatch({ok, #veil_document{record = #fuse_session{uid = 1234}}}, dao_cluster:get_fuse_session("UUID", {stale, update_before})),

    ?assert(meck:validate(dao)).

remove_fuse_session() ->
    meck:expect(dao, remove_record, fun("UUID") -> ok end),
    ?assertEqual(ok, dao_cluster:remove_fuse_session("UUID")),
    ?assert(meck:validate(dao)).


-endif.