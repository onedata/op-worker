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
-module(storage_sync_links_test_utils).
-author("Jakub Kudzia").

-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% API
-export([
    add_link/5, add_link/6,
    get_link/3, get_link/5,
    list/5, list/6,
    list_recursive/4, list_recursive/7,
    delete_link/5, delete_recursive/4
]).

%%%===================================================================
%%% API functions
%%%===================================================================

add_link(Worker, RootStorageFileId, SpaceId, StorageId, ChildStorageFileId) ->
    add_link(Worker, RootStorageFileId, SpaceId, StorageId, ChildStorageFileId, false).

add_link(Worker, RootStorageFileId, SpaceId, StorageId, ChildStorageFileId, MarkLeaves) ->
    rpc:call(Worker, storage_sync_links, add_link_recursive, [RootStorageFileId, SpaceId, StorageId, ChildStorageFileId, MarkLeaves]).

get_link(Worker, RootId, ChildName) ->
    rpc:call(Worker, storage_sync_links, get_link, [RootId, ChildName]).

get_link(Worker, RootStorageFileId, SpaceId, StorageId, ChildName) ->
    rpc:call(Worker, storage_sync_links, get_link, [RootStorageFileId, SpaceId, StorageId, ChildName]).

list(Worker, RootStorageFileId, SpaceId, StorageId, Limit) ->
    rpc:call(Worker, storage_sync_links, list, [RootStorageFileId, SpaceId, StorageId, Limit]).

list(Worker, RootStorageFileId, SpaceId, StorageId, Token, Limit) ->
    rpc:call(Worker, storage_sync_links, list, [RootStorageFileId, SpaceId, StorageId, Token, Limit]).

list_recursive(Worker, RootStorageFileId, SpaceId, StorageId) ->
    list_recursive(Worker, RootStorageFileId, SpaceId, StorageId, undefined, 1000, []).

list_recursive(Worker, RootStorageFileId, SpaceId, StorageId, Token, Limit, Result) ->
    case list(Worker, RootStorageFileId, SpaceId, StorageId, Token, Limit) of
        {{ok, Children}, Token2} ->
            Result2 = lists:foldl(fun
                ({ChildName, _ChildRootId}, AccIn) ->
                    ChildStorageFileId = filename:join([RootStorageFileId, ChildName]),
                    {ok, ChildResult} = list_recursive(Worker, ChildStorageFileId, SpaceId, StorageId),
                    [ChildStorageFileId] ++ ChildResult ++ AccIn
            end, Result, Children),
            case Token2#link_token.is_last of
                true ->
                    {ok, Result2};
                false ->
                    list_recursive(Worker, RootStorageFileId, SpaceId, StorageId, Token2, Limit, Result2)
            end;
        {error, not_found} ->
            {ok, []}
    end.


delete_link(Worker, RootStorageFileId, SpaceId, StorageId, ChildName) ->
    rpc:call(Worker, storage_sync_links, delete_link, [RootStorageFileId, SpaceId, StorageId, ChildName]).

delete_recursive(Worker, RootStorageFileId, SpaceId, StorageId) ->
    rpc:call(Worker, storage_sync_links, delete_recursive, [RootStorageFileId, SpaceId, StorageId]).