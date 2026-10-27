%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of the cleanup of a space's LUMA entries, which a space's
%%% unsupport runs last. By then the support is revoked, so nothing stops the
%%% storage's deletion, and the deletion clears the storage's LUMA DB itself.
%%% Also of what a clear of a whole namespace leaves in the log: it runs on a
%%% storage's deletion, in the collector and on an admin's request, possibly at
%%% once, so each of its lines must name the storage and the namespace.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_crud_api_test).
-author("Bartosz Walkowicz").

-include_lib("eunit/include/eunit.hrl").

-define(STORAGE_ID, <<"someStorageId">>).
-define(SPACE_ID, <<"someSpaceId">>).
-define(NAMESPACE, <<"someNamespace">>).


%%%===================================================================
%%% Tests
%%%===================================================================


space_entries_of_a_storage_are_cleared_test_() ->
    {setup, fun() -> mock_storage_get({ok, storage_data_not_inspected}) end, fun unmock/1, fun() ->
        ?assertEqual(ok, luma_crud_api:clear_space_entries(?STORAGE_ID, ?SPACE_ID)),

        ?assertEqual(1, meck:num_calls(
            luma_spaces_display_defaults, delete, [storage_data_not_inspected, ?SPACE_ID]
        )),
        ?assertEqual(1, meck:num_calls(
            luma_spaces_posix_storage_defaults, delete, [storage_data_not_inspected, ?SPACE_ID]
        ))
    end}.


deleted_storage_has_no_space_entries_to_clear_test_() ->
    {setup, fun() -> mock_storage_get({error, not_found}) end, fun unmock/1, fun() ->
        ?assertEqual(ok, luma_crud_api:clear_space_entries(?STORAGE_ID, ?SPACE_ID)),

        ?assertEqual(0, meck:num_calls(luma_spaces_display_defaults, delete, '_')),
        ?assertEqual(0, meck:num_calls(luma_spaces_posix_storage_defaults, delete, '_'))
    end}.


%% One line where the clear starts and one where it ends, each naming what is
%% cleared; the tables at debug.
namespace_clear_logs_its_start_and_end_with_the_storage_and_namespace_test_() ->
    {setup, fun mock_namespace_clear/0, fun unmock/1, fun() ->
        ok = luma_crud_api:clear_stale_db_namespace(storage_data_not_inspected, ?NAMESPACE),

        InfoLogs = logs_at(info),
        ?assertEqual(2, length(InfoLogs)),
        ?assert(lists:all(fun(Log) ->
            string:find(Log, ?STORAGE_ID) /= nomatch andalso string:find(Log, ?NAMESPACE) /= nomatch
        end, InfoLogs))
    end}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
mock_namespace_clear() ->
    Tables = [
        luma_storage_users, luma_spaces_display_defaults, luma_spaces_posix_storage_defaults,
        luma_onedata_users, luma_onedata_groups
    ],
    meck:new([storage, fslogic_event_emitter | Tables], [no_link]),
    meck:new(onedata_logger, [passthrough, no_link]),
    meck:expect(onedata_logger, should_log, fun(_Loglevel) -> true end),
    meck:expect(onedata_logger, log, fun(_Loglevel, _Metadata, _FormattedLog) -> ok end),
    meck:expect(storage, get, fun(StorageData) -> {ok, StorageData} end),
    meck:expect(storage, get_id, fun(_StorageData) -> ?STORAGE_ID end),
    meck:expect(storage, with_luma_db_namespace, fun(StorageData, _Namespace) -> StorageData end),
    meck:expect(fslogic_event_emitter, emit_helper_params_changed, fun(_StorageId) -> ok end),
    lists:foreach(fun(Table) ->
        meck:expect(Table, clear_all, fun(_StorageData) -> ok end)
    end, Tables).


%% @private
logs_at(Loglevel) ->
    [
        lists:flatten(io_lib:format("~ts", [FormattedLog]))
        || {_Pid, {onedata_logger, log, [Level, _Metadata, FormattedLog]}, _Result} <- meck:history(onedata_logger),
        Level == Loglevel
    ].


%% @private
mock_storage_get(Result) ->
    meck:new([storage, luma_spaces_display_defaults, luma_spaces_posix_storage_defaults], [no_link]),
    meck:expect(storage, get, fun(_StorageId) -> Result end),
    meck:expect(luma_spaces_display_defaults, delete, fun(_StorageData, _SpaceId) -> ok end),
    meck:expect(luma_spaces_posix_storage_defaults, delete, fun(_StorageData, _SpaceId) -> ok end).


%% @private
unmock(_) ->
    meck:unload().
