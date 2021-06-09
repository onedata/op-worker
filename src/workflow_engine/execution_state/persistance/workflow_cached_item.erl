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
%%% TODO VFS-7551 - delete not used items from cache
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_cached_item).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([put/1, get/1]).

-type id() :: binary().

-export_type([id/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec put(iterator:item()) -> id().
put(Item) ->
    Doc = #document{value = #workflow_cached_item{item = Item}},
    {ok, #document{key = ItemId}} = datastore_model:save(?CTX, Doc),
    ItemId.

-spec get(id()) -> iterator:item().
get(ItemId) ->
    {ok, #document{value = #workflow_cached_item{item = Item}}} = datastore_model:get(?CTX, ItemId),
    Item.