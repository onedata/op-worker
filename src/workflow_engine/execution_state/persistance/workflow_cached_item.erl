%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Cache of items returned by iterator. Single item can be processed
%%% by multiple tasks so each item is cached when it is returned by
%%% iterator (before usage by first task) and is deleted from cache
%%% after usage by last task.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_cached_item).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([put/2, get_item/1, get_iterator/1, delete/1]).

-type id() :: binary().

-export_type([id/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec put(iterator:item(), iterator:iterator()) -> id().
put(Item, Iterator) ->
    Doc = #document{value = #workflow_cached_item{item = Item, iterator = Iterator}},
    {ok, #document{key = ItemId}} = datastore_model:save(?CTX, Doc),
    ItemId.

-spec get_item(id() | undefined) -> iterator:item() | undefined.
get_item(undefined) ->
    undefined;
get_item(ItemId) ->
    {ok, #document{value = #workflow_cached_item{item = Item}}} = datastore_model:get(?CTX, ItemId),
    Item.

-spec get_iterator(id()) -> iterator:iterator().
get_iterator(ItemId) ->
    {ok, #document{value = #workflow_cached_item{iterator = Iterator}}} = datastore_model:get(?CTX, ItemId),
    Iterator.

-spec delete(id()) -> ok.
delete(ItemId) ->
    ok = datastore_model:delete(?CTX, ItemId).