%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Thin rpc facade over the storage_sync_links datastore structure - the per
%%% directory link trees that storage import keeps to compare the files found on
%%% the storage with the ones already known to the system (see
%%% storage_sync_links.erl). Lets tests read, extend and prune those trees
%%% directly, without waiting for a scan to do it.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_links_test_utils).
-author("Jakub Kudzia").

-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% API
-export([
    add_link/4, add_link/5,
    get_link/3, get_link/4,
    list/4, list/5,
    list_recursive/3, list_recursive/6,
    delete_link/4, delete_recursive/3
]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec add_link(
    node(), RootStorageFileId :: helpers:file_id(), storage:id(),
    ChildStorageFileId :: helpers:file_id()
) ->
    ok.
add_link(Worker, RootStorageFileId, StorageId, ChildStorageFileId) ->
    add_link(Worker, RootStorageFileId, StorageId, ChildStorageFileId, false).

-spec add_link(
    node(), RootStorageFileId :: helpers:file_id(), storage:id(),
    ChildStorageFileId :: helpers:file_id(), MarkLeaves :: boolean()
) ->
    ok.
add_link(Worker, RootStorageFileId, StorageId, ChildStorageFileId, MarkLeaves) ->
    rpc:call(Worker, storage_sync_links, add_link_recursive, [RootStorageFileId, StorageId, ChildStorageFileId, MarkLeaves]).

-spec get_link(node(), RootId :: binary(), ChildName :: helpers:file_id()) ->
    {ok, undefined | binary()} | {error, term()}.
get_link(Worker, RootId, ChildName) ->
    rpc:call(Worker, storage_sync_links, get_link, [RootId, ChildName]).

-spec get_link(
    node(), RootStorageFileId :: helpers:file_id(), storage:id(), ChildName :: helpers:file_id()
) ->
    {ok, undefined | binary()} | {error, term()}.
get_link(Worker, RootStorageFileId, StorageId, ChildName) ->
    rpc:call(Worker, storage_sync_links, get_link, [RootStorageFileId, StorageId, ChildName]).

-spec list(node(), RootStorageFileId :: helpers:file_id(), storage:id(), Limit :: non_neg_integer()) ->
    {{ok, [storage_sync_links:link()]}, datastore_links_iter:token()} | {error, term()}.
list(Worker, RootStorageFileId, StorageId, Limit) ->
    rpc:call(Worker, storage_sync_links, list, [RootStorageFileId, StorageId, Limit]).

-spec list(
    node(), RootStorageFileId :: helpers:file_id(), storage:id(),
    datastore_links_iter:token(), Limit :: non_neg_integer()
) ->
    {{ok, [storage_sync_links:link()]}, datastore_links_iter:token()} | {error, term()}.
list(Worker, RootStorageFileId, StorageId, Token, Limit) ->
    rpc:call(Worker, storage_sync_links, list, [RootStorageFileId, StorageId, Token, Limit]).

%% @doc Storage file ids of all the descendants of the given root, in no particular order.
-spec list_recursive(node(), RootStorageFileId :: helpers:file_id(), storage:id()) ->
    {ok, [helpers:file_id()]}.
list_recursive(Worker, RootStorageFileId, StorageId) ->
    list_recursive(Worker, RootStorageFileId, StorageId, undefined, 1000, []).

%% @private
-spec list_recursive(
    node(), RootStorageFileId :: helpers:file_id(), storage:id(),
    undefined | datastore_links_iter:token(), Limit :: non_neg_integer(),
    Result :: [helpers:file_id()]
) ->
    {ok, [helpers:file_id()]}.
list_recursive(Worker, RootStorageFileId, StorageId, Token, Limit, Result) ->
    case list(Worker, RootStorageFileId, StorageId, Token, Limit) of
        {{ok, Children}, Token2} ->
            Result2 = lists:foldl(fun
                ({ChildName, _ChildRootId}, AccIn) ->
                    ChildStorageFileId = filename:join([RootStorageFileId, ChildName]),
                    {ok, ChildResult} = list_recursive(Worker, ChildStorageFileId, StorageId),
                    [ChildStorageFileId] ++ ChildResult ++ AccIn
            end, Result, Children),
            case Token2#link_token.is_last of
                true ->
                    {ok, Result2};
                false ->
                    list_recursive(Worker, RootStorageFileId, StorageId, Token2, Limit, Result2)
            end;
        {error, not_found} ->
            {ok, []}
    end.


-spec delete_link(
    node(), RootStorageFileId :: helpers:file_id(), storage:id(), ChildName :: helpers:file_id()
) ->
    ok.
delete_link(Worker, RootStorageFileId, StorageId, ChildName) ->
    rpc:call(Worker, storage_sync_links, delete_link, [RootStorageFileId, StorageId, ChildName]).

-spec delete_recursive(node(), RootStorageFileId :: helpers:file_id(), storage:id()) -> ok.
delete_recursive(Worker, RootStorageFileId, StorageId) ->
    rpc:call(Worker, storage_sync_links, delete_recursive, [RootStorageFileId, StorageId]).