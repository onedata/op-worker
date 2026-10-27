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
%%% @end
%%%-------------------------------------------------------------------
-module(luma_crud_api_test).
-author("Bartosz Walkowicz").

-include_lib("eunit/include/eunit.hrl").

-define(STORAGE_ID, <<"someStorageId">>).
-define(SPACE_ID, <<"someSpaceId">>).


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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
mock_storage_get(Result) ->
    meck:new([storage, luma_spaces_display_defaults, luma_spaces_posix_storage_defaults], [no_link]),
    meck:expect(storage, get, fun(_StorageId) -> Result end),
    meck:expect(luma_spaces_display_defaults, delete, fun(_StorageData, _SpaceId) -> ok end),
    meck:expect(luma_spaces_posix_storage_defaults, delete, fun(_StorageData, _SpaceId) -> ok end).


%% @private
unmock(_) ->
    meck:unload().
