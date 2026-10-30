%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of storage_updater: the update saga with its LUMA DB namespace
%%% rotation and compensations, and what an update checks and sends to Onezone.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_updater_test).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/http.hrl").
-include_lib("opw_panel_contracts/include/storage/posix.hrl").
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
%%% Compensation reporting tests
%%%===================================================================


compensation_returning_error_is_logged_test_() ->
    {setup, fun mock_logger/0, fun unmock_logger/1, fun() ->
        storage_updater:run_compensations([{<<"step-a">>, fun() -> {error, not_found} end}]),

        ?assertEqual(1, count_failed_compensation_logs(<<"step-a">>))
    end}.


compensation_crashing_is_logged_test_() ->
    {setup, fun mock_logger/0, fun unmock_logger/1, fun() ->
        storage_updater:run_compensations([{<<"step-a">>, fun() -> error(crash) end}]),

        ?assertEqual(1, count_failed_compensation_logs(<<"step-a">>))
    end}.


successful_compensation_logs_no_failure_test_() ->
    {setup, fun mock_logger/0, fun unmock_logger/1, fun() ->
        storage_updater:run_compensations([{<<"step-a">>, fun() -> ok end}]),

        ?assertEqual(0, count_failed_compensation_logs(<<"step-a">>))
    end}.


failed_compensation_does_not_stop_the_others_test_() ->
    {setup, fun mock_logger/0, fun unmock_logger/1, fun() ->
        Self = self(),
        storage_updater:run_compensations([
            {<<"step-b">>, fun() -> {error, not_found} end},
            {<<"step-a">>, fun() -> Self ! step_a_compensated, ok end}
        ]),

        ?assert(receive step_a_compensated -> true after 0 -> false end)
    end}.


%%%===================================================================
%%% Failure handling tests
%%%===================================================================


%% A type that no helper spec module serves makes the print of the diff raise;
%% it must end as the rest of the update's failures do.
unprintable_diff_fails_the_update_with_an_error_test_() ->
    {setup, fun mock_logger/0, fun unmock_logger/1, fun() ->
        UpdateSpec = #storage_update_spec{
            type = <<"unknown">>,
            credentials = #posix_helper_credentials_diff{uid = 0}
        },

        ?assertMatch(?ERR_INTERNAL_SERVER_ERROR(_, _), storage_updater:update(?STORAGE_ID, UpdateSpec))
    end}.


%% A refusal is thrown, not returned, and must leave the same line in the log
%% as a returned error does.
refused_update_is_logged_test_() ->
    {setup, fun mock_missing_storage_config/0, fun unmock_missing_storage_config/1, fun() ->
        UpdateSpec = #storage_update_spec{type = <<"posix">>},

        ?assertEqual(?ERROR_NOT_FOUND, storage_updater:update(?STORAGE_ID, UpdateSpec)),
        ?assertEqual(1, count_error_logs_containing(<<"Failed to update storage '", ?STORAGE_ID/binary, "'">>))
    end}.


%%%===================================================================
%%% QoS parameters tests
%%%===================================================================


%% Onepanel passes the QoS parameters as the REST request had them, as strings.
update_sends_normalized_qos_parameters_to_onezone_test_() ->
    {setup, fun mock_update_up_to_onezone/0, fun unmock_update_up_to_onezone/1, fun() ->
        storage_updater:update(?STORAGE_ID, #storage_update_spec{
            type = <<"posix">>,
            qos_parameters = #{<<"priority">> => <<"2">>, <<"region">> => <<"eu">>}
        }),

        #storage_update_spec{qos_parameters = QosParameters} = meck:capture(
            first, storage_logic, update_in_zone, '_', 2
        ),
        ?assertEqual(#{<<"priority">> => 2, <<"region">> => <<"eu">>}, QosParameters)
    end}.


%%%===================================================================
%%% LUMA config tests
%%%===================================================================


%% A change of the LUMA config rotates the storage's LUMA DB namespace and runs
%% the diagnostics on every node, so an update that names no LUMA setting - here
%% the empty LUMA spec that comes with a rename - must do neither.
update_without_luma_change_keeps_the_storage_config_test_() ->
    {setup, fun mock_update_of_external_feed_storage/0, fun unmock_update_of_external_feed_storage/1, fun() ->
        ?assertEqual(ok, storage_updater:update(?STORAGE_ID, #storage_update_spec{
            type = <<"posix">>,
            name = <<"new-name">>,
            luma = #luma_spec_diff{}
        })),

        ?assertEqual(0, meck:num_calls(storage_config, update, '_')),
        ?assertEqual(0, meck:num_calls(storage_crud_utils, run_diagnostics, '_'))
    end}.


%%%===================================================================
%%% Credentials tests
%%%===================================================================


%% Every use of an HTTP storage requires the credentials unless its credentials
%% type is none, and the diagnostics skip HTTP, so nothing else stops a switch
%% to another type that doesn't bring them along.
credentials_are_refused_before_any_write_test_() ->
    {setup, fun mock_update_of_http_storage/0, fun unmock_update_of_http_storage/1, fun() ->
        ?assertMatch(
            ?ERR_MISSING_REQUIRED_VALUE(_, <<"credentials">>),
            storage_updater:update(?STORAGE_ID, #storage_update_spec{
                type = <<"http">>,
                credentials = #http_helper_credentials_diff{credentials_type = basic}
            })
        ),

        ?assertEqual(0, meck:num_calls(storage_config, update, '_')),
        ?assertEqual(0, meck:num_calls(storage_logic, update_in_zone, '_'))
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
mock_logger() ->
    meck:new(onedata_logger, [passthrough, no_link]),
    meck:expect(onedata_logger, should_log, fun(_Loglevel) -> true end),
    meck:expect(onedata_logger, log, fun(_Loglevel, _Metadata, _FormattedLog) -> ok end).


%% @private
unmock_logger(_) ->
    meck:unload(onedata_logger).


%% @private
%% Everything the update does before it writes to Onezone passes, with no change
%% to the helper or to LUMA, and the write to Onezone fails, which ends the
%% update there.
mock_update_up_to_onezone() ->
    mock_logger(),
    meck:new([storage_config, storage_logic], [no_link]),
    meck:new(helper_spec, [passthrough, no_link]),
    meck:expect(storage_config, get, fun(StorageId) ->
        {ok, #document{key = StorageId, value = #storage_config{helper_spec = helper_spec_not_inspected}}}
    end),
    meck:expect(helper_spec, get_name, fun(_HelperSpec) -> <<"posix">> end),
    meck:expect(helper_spec, update, fun(_HelperSpec, _UpdateSpec) -> {error, no_change} end),
    meck:expect(storage_logic, get, fun(_StorageId) -> {ok, #document{value = #od_storage{}}} end),
    meck:expect(storage_logic, update_in_zone, fun(_StorageId, _UpdateSpec) ->
        {error, onezone_write_not_reached_in_test}
    end).


%% @private
unmock_update_up_to_onezone(_) ->
    meck:unload().


%% @private
%% A storage on the external feed, whose update with no change to the helper
%% writes to Onezone and passes.
mock_update_of_external_feed_storage() ->
    mock_logger(),
    meck:new([storage_config, storage_logic, fslogic_event_emitter], [no_link]),
    meck:new([helper_spec, storage_crud_utils], [passthrough, no_link]),
    meck:expect(storage_config, get, fun(StorageId) ->
        {ok, #document{key = StorageId, value = #storage_config{
            helper_spec = helper_spec_not_inspected,
            luma_config = luma_config:new_with_external_feed(<<"https://luma.example.com">>, undefined)
        }}}
    end),
    meck:expect(storage_config, update, fun(StorageId, _Diff) -> {ok, #document{key = StorageId}} end),
    meck:expect(helper_spec, get_name, fun(_HelperSpec) -> <<"posix">> end),
    meck:expect(helper_spec, update, fun(_HelperSpec, _UpdateSpec) -> {error, no_change} end),
    meck:expect(storage_crud_utils, run_diagnostics, fun(_HelperSpec, _LumaFeed, _Mode) -> ok end),
    meck:expect(fslogic_event_emitter, emit_helper_params_changed, fun(_StorageId) -> ok end),
    meck:expect(storage_logic, get, fun(_StorageId) -> {ok, #document{value = #od_storage{}}} end),
    meck:expect(storage_logic, update_in_zone, fun(_StorageId, _UpdateSpec) -> ok end).


%% @private
unmock_update_of_external_feed_storage(_) ->
    meck:unload().


%% @private
%% An HTTP storage with no credentials, whose update passes everything else it
%% checks and every write it makes.
mock_update_of_http_storage() ->
    mock_logger(),
    meck:new([storage_config, storage_logic, fslogic_event_emitter, rtransfer_config], [no_link]),
    meck:new(storage_crud_utils, [passthrough, no_link]),
    HelperSpec = helper_spec:build(#storage_create_spec{
        type = <<"http">>,
        configuration = #http_helper_configuration{
            endpoint = <<"https://example.com">>,
            storage_path_type = canonical
        },
        credentials = #http_helper_credentials{credentials_type = none}
    }),
    meck:expect(storage_config, get, fun(StorageId) ->
        {ok, #document{key = StorageId, value = #storage_config{
            helper_spec = HelperSpec,
            luma_config = luma_config:new(auto)
        }}}
    end),
    meck:expect(storage_config, update, fun(StorageId, _Diff) -> {ok, #document{key = StorageId}} end),
    meck:expect(storage_crud_utils, verify_configuration, fun(_IdOrName, _Readonly, _Imported, _HelperSpec) -> ok end),
    meck:expect(storage_crud_utils, run_diagnostics, fun(_HelperSpec, _LumaFeed, _Mode) -> ok end),
    meck:expect(fslogic_event_emitter, emit_helper_params_changed, fun(_StorageId) -> ok end),
    meck:expect(rtransfer_config, add_storage, fun(_StorageId) -> ok end),
    meck:expect(storage_logic, get, fun(_StorageId) ->
        {ok, #document{value = #od_storage{readonly = true, imported = true}}}
    end),
    meck:expect(storage_logic, update_in_zone, fun(_StorageId, _UpdateSpec) -> ok end).


%% @private
unmock_update_of_http_storage(_) ->
    meck:unload().


%% @private
mock_missing_storage_config() ->
    mock_logger(),
    meck:new(storage_config, [no_link]),
    meck:expect(storage_config, get, fun(_StorageId) -> {error, not_found} end).


%% @private
unmock_missing_storage_config(_) ->
    meck:unload().


%% @private
count_failed_compensation_logs(StepName) ->
    count_error_logs_containing(<<"FAILED to compensate step '", StepName/binary, "'">>).


%% @private
count_error_logs_containing(Pattern) ->
    length([
        FormattedLog
        || {_Pid, {onedata_logger, log, [error, _Metadata, FormattedLog]}, _Result} <- meck:history(onedata_logger),
        string:find(FormattedLog, Pattern) /= nomatch
    ]).


%% @private
%% A storage that has already changed its LUMA config a couple of times, one of
%% those namespaces still awaiting cleanup.
storage_config_with_history() ->
    Fresh = #storage_config{luma_db_namespace = undefined, stale_luma_db_namespaces = []},
    storage_updater:rotate_luma_db_namespace(
        storage_updater:rotate_luma_db_namespace(Fresh)
    ).
