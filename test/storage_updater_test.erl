%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of the storage update saga, covering the two things about it that
%%% are easy to break while reshuffling the code.
%%%
%%% The first is the LUMA DB namespace bookkeeping - which namespace a storage
%%% reads through, and which ones are recorded for the garbage collector to
%%% clean up. It is split between the action (draw a namespace, mark the
%%% previous one stale) and the compensation (bring the previous one back, mark
%%% the one just abandoned stale), and the two must agree on one invariant: a
%%% storage must never list the namespace it reads through as stale, or the
%%% garbage collector would delete live mappings.
%%%
%%% The second is what a saga step is allowed to report. A compensation is armed
%%% only once its action has returned 'ok', so a step that reports the failure
%%% of anything it did after the storage_config write would leave that write
%%% applied with nothing to take it back. The write is therefore the only thing
%%% the step reports on; that the propagation which follows it cannot fail the
%%% step is covered by rtransfer_config_test.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_updater_test).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(STORAGE_ID, <<"dummyStorageId">>).


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
%%% Saga step reporting tests
%%%===================================================================


helper_spec_change_propagates_to_clients_and_rtransfer_test_() ->
    {setup, fun mock_update_in_op_deps/0, fun unmock_update_in_op_deps/1, fun() ->
        ?assertEqual(ok, update_in_op(_HelperSpecChanged = true, _LumaChanged = false)),

        % neither of the two can fail the step - both are best effort, which is
        % covered by rtransfer_config_test and fslogic_event_emitter itself
        ?assertEqual(1, meck:num_calls(fslogic_event_emitter, emit_helper_params_changed, '_')),
        ?assertEqual(1, meck:num_calls(rtransfer_config, add_storage, '_'))
    end}.


failed_storage_config_write_fails_the_step_test_() ->
    {setup, fun mock_update_in_op_deps/0, fun unmock_update_in_op_deps/1, fun() ->
        % the write is the one thing the step can report on - there is nothing
        % applied yet for a missing compensation to leave behind
        meck:expect(storage_config, update, fun(_StorageId, _Diff) -> {error, not_found} end),

        ?assertEqual(
            {error, not_found},
            update_in_op(_HelperSpecChanged = true, _LumaChanged = false)
        ),
        ?assertEqual(0, meck:num_calls(rtransfer_config, add_storage, '_'))
    end}.


luma_only_change_does_not_touch_rtransfer_test_() ->
    {setup, fun mock_update_in_op_deps/0, fun unmock_update_in_op_deps/1, fun() ->
        ?assertEqual(ok, update_in_op(_HelperSpecChanged = false, _LumaChanged = true)),

        % rtransfer reaches storages with the helper spec's own credentials,
        % which LUMA has no say over
        ?assertEqual(0, meck:num_calls(rtransfer_config, add_storage, '_')),
        ?assertEqual(1, meck:num_calls(fslogic_event_emitter, emit_helper_params_changed, '_'))
    end}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
update_in_op(HelperSpecChanged, LumaChanged) ->
    storage_updater:update_in_op(
        ?STORAGE_ID, #storage_config{}, HelperSpecChanged, LumaChanged
    ).


%% @private
mock_update_in_op_deps() ->
    meck:new([storage_config, fslogic_event_emitter, rtransfer_config], [no_link]),
    meck:expect(storage_config, update, fun(StorageId, _Diff) ->
        {ok, #document{key = StorageId}}
    end),
    meck:expect(fslogic_event_emitter, emit_helper_params_changed, fun(_StorageId) -> ok end),
    meck:expect(rtransfer_config, add_storage, fun(_StorageId) -> ok end).


%% @private
unmock_update_in_op_deps(_) ->
    meck:unload().


%% @private
%% A storage that has already changed its LUMA config a couple of times, one of
%% those namespaces still awaiting cleanup.
storage_config_with_history() ->
    Fresh = #storage_config{luma_db_namespace = undefined, stale_luma_db_namespaces = []},
    storage_updater:rotate_luma_db_namespace(
        storage_updater:rotate_luma_db_namespace(Fresh)
    ).
