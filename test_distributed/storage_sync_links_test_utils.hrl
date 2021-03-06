%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------

-ifndef(STORAGE_SYNC_LINKS_TEST_UTILS_HRL).
-define(STORAGE_SYNC_LINKS_TEST_UTILS_HRL, 1).

-include_lib("ctool/include/test/test_utils.hrl").


-define(TIMEOUT, 30).

-define(assertList(ExpectedList, Worker, RootStorageFileId, StorageId),
    ?assertList(ExpectedList, Worker, RootStorageFileId, StorageId, ?TIMEOUT)).

-define(assertList(ExpectedList, Worker, RootStorageFileId, StorageId, Timeout),
    ?assertEqual(lists:sort(ExpectedList), try
        {ok, Result} = storage_sync_links_test_utils:list_recursive(Worker, RootStorageFileId, StorageId),
        lists:sort(Result)
    catch
        _:_ ->
            error
    end, Timeout)).

-endif.