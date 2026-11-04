%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of storage: the check whether a LUMA DB namespace is stale.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_test).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(STORAGE_ID, <<"someStorageId">>).
-define(NAMESPACE, <<"someNamespace">>).


%%%===================================================================
%%% Tests
%%%===================================================================


stale_list_is_read_under_the_storage_lock_test_() ->
    {setup, fun() -> mock_storage_config({ok, config_doc([?NAMESPACE])}) end, fun unmock/1, fun() ->
        ?assert(storage:is_luma_db_namespace_stale(?STORAGE_ID, ?NAMESPACE)),

        ?assertEqual(1, meck:num_calls(critical_section, run, [{storage_id, ?STORAGE_ID}, '_']))
    end}.


namespace_off_the_stale_list_is_not_stale_test_() ->
    {setup, fun() -> mock_storage_config({ok, config_doc([<<"otherNamespace">>])}) end, fun unmock/1, fun() ->
        ?assertNot(storage:is_luma_db_namespace_stale(?STORAGE_ID, ?NAMESPACE))
    end}.


deleted_storage_has_no_stale_namespace_test_() ->
    {setup, fun() -> mock_storage_config({error, not_found}) end, fun unmock/1, fun() ->
        ?assertNot(storage:is_luma_db_namespace_stale(?STORAGE_ID, ?NAMESPACE))
    end}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
config_doc(StaleNamespaces) ->
    #document{key = ?STORAGE_ID, value = #storage_config{stale_luma_db_namespaces = StaleNamespaces}}.


%% @private
mock_storage_config(GetResult) ->
    meck:new(critical_section, [no_link]),
    meck:expect(critical_section, run, fun(_Key, Fun) -> Fun() end),
    meck:new(storage_config, [passthrough, no_link]),
    meck:expect(storage_config, get, fun(_StorageId) -> GetResult end).


%% @private
unmock(_) ->
    meck:unload().
