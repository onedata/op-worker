%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of what the collector clears. It works from a snapshot of the
%%% storage configs, and between the first step of an update's saga and its
%%% rollback the namespace the storage reads through is on the stale list.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_db_garbage_collector_test).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(STORAGE_ID, <<"someStorageId">>).
-define(NAMESPACE, <<"someNamespace">>).


%%%===================================================================
%%% Tests
%%%===================================================================


namespace_still_stale_is_cleared_and_forgotten_test_() ->
    {setup, fun() -> mock_stale_check(true) end, fun unmock/1, fun() ->
        clear_stale_namespace(),

        ?assertEqual(1, meck:num_calls(luma_crud_api, clear_stale_db_namespace, '_')),
        ?assertEqual(1, meck:num_calls(
            storage_config, forget_stale_luma_db_namespace, [?STORAGE_ID, ?NAMESPACE]
        ))
    end}.


%% The snapshot still lists the namespace, which a rollback has made live again.
namespace_no_longer_stale_is_left_alone_test_() ->
    {setup, fun() -> mock_stale_check(false) end, fun unmock/1, fun() ->
        clear_stale_namespace(),

        ?assertEqual(0, meck:num_calls(luma_crud_api, clear_stale_db_namespace, '_')),
        ?assertEqual(0, meck:num_calls(storage_config, forget_stale_luma_db_namespace, '_'))
    end}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
clear_stale_namespace() ->
    SnapshotDoc = #document{key = ?STORAGE_ID, value = #storage_config{
        stale_luma_db_namespaces = [?NAMESPACE]
    }},
    luma_db_garbage_collector:clear_stale_namespace(SnapshotDoc, ?STORAGE_ID, ?NAMESPACE).


%% @private
mock_stale_check(IsStale) ->
    meck:new([storage, luma_crud_api, storage_config], [no_link]),
    meck:expect(storage, is_luma_db_namespace_stale, fun(_StorageId, _Namespace) -> IsStale end),
    meck:expect(luma_crud_api, clear_stale_db_namespace, fun(_StorageConfigDoc, _Namespace) -> ok end),
    meck:expect(storage_config, forget_stale_luma_db_namespace, fun(_StorageId, _Namespace) -> ok end).


%% @private
unmock(_) ->
    meck:unload().
