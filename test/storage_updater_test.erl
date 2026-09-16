%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of the LUMA DB namespace bookkeeping done by the storage update
%%% saga - which namespace a storage reads through, and which ones are recorded
%%% for the garbage collector to clean up.
%%%
%%% The bookkeeping is split between the action (draw a namespace, mark the
%%% previous one stale) and the compensation (bring the previous one back, mark
%%% the one just abandoned stale), and the two must agree on one invariant: a
%%% storage must
%%% never list the namespace it reads through as stale, or the garbage collector
%%% would delete live mappings. These tests pin that down, as the saga is the
%%% kind of code that gets reshuffled later.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_updater_test).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("eunit/include/eunit.hrl").


%%%===================================================================
%%% Tests
%%%===================================================================


rotation_marks_the_previous_namespace_stale_test() ->
    Fresh = #storage_config{luma_db_namespace = undefined, stale_luma_db_namespaces = []},

    Rotated = storage_updater:rotate_luma_db_namespace(Fresh),

    % the original, unnamespaced keyspace goes on the list rather than being
    % forgotten - a
    % storage that predates the mechanism may well have entries in it
    ?assert(is_binary(Rotated#storage_config.luma_db_namespace)),
    ?assertEqual([undefined], Rotated#storage_config.stale_luma_db_namespaces).


rotation_never_repeats_a_namespace_test() ->
    Fresh = #storage_config{luma_db_namespace = undefined, stale_luma_db_namespaces = []},

    Rotated1 = storage_updater:rotate_luma_db_namespace(Fresh),
    Rotated2 = storage_updater:rotate_luma_db_namespace(Rotated1),
    Rotated3 = storage_updater:rotate_luma_db_namespace(Rotated2),

    AllNamespaces = [
        Rotated1#storage_config.luma_db_namespace,
        Rotated2#storage_config.luma_db_namespace,
        Rotated3#storage_config.luma_db_namespace
    ],
    ?assertEqual(3, length(lists:usort(AllNamespaces))),

    % every namespace ever superseded is still on the list, newest first
    ?assertEqual(
        [Rotated2#storage_config.luma_db_namespace, Rotated1#storage_config.luma_db_namespace, undefined],
        Rotated3#storage_config.stale_luma_db_namespaces
    ).


rollback_without_a_luma_change_moves_nothing_test() ->
    Prev = storage_config_with_history(),
    New = storage_updater:rotate_luma_db_namespace(Prev),

    ?assertEqual(Prev, storage_updater:build_rollback_storage_config(Prev, New, false)).


rollback_marks_the_abandoned_namespace_stale_test() ->
    Prev = storage_config_with_history(),
    New = storage_updater:rotate_luma_db_namespace(Prev),

    RolledBack = storage_updater:build_rollback_storage_config(Prev, New, true),

    % the storage reads through the previous namespace again...
    ?assertEqual(
        Prev#storage_config.luma_db_namespace,
        RolledBack#storage_config.luma_db_namespace
    ),
    % ...while the namespace the action drew is marked stale, as entries may have
    % been stored under it during the window in which it was live
    ?assert(lists:member(
        New#storage_config.luma_db_namespace,
        RolledBack#storage_config.stale_luma_db_namespaces
    )),
    % and nothing marked stale earlier is dropped on the way
    ?assert(lists:all(fun(Namespace) ->
        lists:member(Namespace, RolledBack#storage_config.stale_luma_db_namespaces)
    end, Prev#storage_config.stale_luma_db_namespaces)).


%% This is the one that matters: the rollback restores the previous namespace,
%% so it must also take it back off the stale list. Were it left there, the
%% garbage collector would delete the mappings the storage is reading through.
live_namespace_is_never_stale_test_() ->
    Prev = storage_config_with_history(),
    New = storage_updater:rotate_luma_db_namespace(Prev),
    RolledBack = storage_updater:build_rollback_storage_config(Prev, New, true),

    [
        {"after rotation", ?_assertNot(lists:member(
            New#storage_config.luma_db_namespace,
            New#storage_config.stale_luma_db_namespaces
        ))},
        {"after rollback", ?_assertNot(lists:member(
            RolledBack#storage_config.luma_db_namespace,
            RolledBack#storage_config.stale_luma_db_namespaces
        ))}
    ].


rollback_changes_nothing_but_the_stale_list_test() ->
    Prev = storage_config_with_history(),
    New = storage_updater:rotate_luma_db_namespace(Prev),

    RolledBack = storage_updater:build_rollback_storage_config(Prev, New, true),

    ?assertEqual(
        Prev#storage_config{stale_luma_db_namespaces = []},
        RolledBack#storage_config{stale_luma_db_namespaces = []}
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
%% A storage that has already changed its LUMA config a couple of times, one of
%% those namespaces still awaiting cleanup.
storage_config_with_history() ->
    Fresh = #storage_config{luma_db_namespace = undefined, stale_luma_db_namespaces = []},
    storage_updater:rotate_luma_db_namespace(
        storage_updater:rotate_luma_db_namespace(Fresh)
    ).
