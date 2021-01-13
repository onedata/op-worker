%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility module for creation of links' trees with sequence number keys
%%% and binary values (values can have different meaning depending on the
%%% module that uses links). The trees are used to find next key/value pair
%%% starting from given key using ascending or descending order.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_seqs_tree).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

% API
-export([add_new/5, add_new/6, overwrite/5, overwrite/6,
    get_next/5, get_next/6]).

-type sorting_order() :: ascending | descending.
-type link_update_fun() :: fun((link_target() | undefined) -> link_target()).
-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type tree_id() :: datastore:tree_id().
-type link_name() :: datastore_doc:seq().
-type link_target() :: binary().

-define(DESCENDING_KEY(Key), <<Key/binary, "_descending">>).
-define(DESCENDING_LINK_NAME(Id), -Id).

%%%===================================================================
%%% API
%%%===================================================================

-spec add_new(ctx(), key(), tree_id(),
    link_name(), link_target()) -> ok.
add_new(Ctx, Key, TreeId, LinkName, Value) ->
    add_new(ascending, Ctx, Key, TreeId, LinkName, Value).

-spec add_new(sorting_order(), ctx(), key(), tree_id(),
    link_name(), link_target()) -> ok.
add_new(ascending, Ctx, Key, TreeId, LinkName, Value) ->
    add_new_internal(Ctx, Key, TreeId, LinkName, Value);
add_new(descending, Ctx, Key, TreeId, LinkName, Value) ->
    add_new_internal(Ctx, ?DESCENDING_KEY(Key), TreeId, ?DESCENDING_LINK_NAME(LinkName), Value).

-spec overwrite(ctx(), key(), tree_id(),
    link_name(), link_update_fun()) -> ok.
overwrite(Ctx, Key, TreeId, LinkName, UpdateFun) ->
    overwrite(ascending, Ctx, Key, TreeId, LinkName, UpdateFun).

%%--------------------------------------------------------------------
%% @doc
%% Overwrite = delete + add
%% @end
%%--------------------------------------------------------------------
-spec overwrite(sorting_order(), ctx(), key(), tree_id(),
    link_name(), link_update_fun() | link_target()) -> ok.
overwrite(ascending, Ctx, Key, TreeId, LinkName, UpdateFun) ->
    overwrite_internal(Ctx, Key, TreeId, LinkName, UpdateFun);
overwrite(descending, Ctx, Key, TreeId, LinkName, UpdateFun) ->
    overwrite_internal(Ctx, ?DESCENDING_KEY(Key), TreeId, ?DESCENDING_LINK_NAME(LinkName), UpdateFun).

-spec get_next(ctx(), key(), tree_id(),
    link_name(), link_target()) -> link_target().
get_next(Ctx, Key, TreeId, LinkName, DefaultValue) ->
    get_next(ascending, Ctx, Key, TreeId, LinkName, DefaultValue).

%%--------------------------------------------------------------------
%% @doc
%% Returns link's target of key or successor of the key
%% (if key does not exists) according to sorting order.
%% @end
%%--------------------------------------------------------------------
-spec get_next(sorting_order(), ctx(), key(), tree_id(),
    link_name(), link_target()) -> link_target().
get_next(ascending, Ctx, Key, TreeId, LinkName, DefaultValue) ->
    get_next_internal(Ctx, Key, TreeId, LinkName, DefaultValue);
get_next(descending, Ctx, Key, TreeId, LinkName, DefaultValue) ->
    get_next_internal(Ctx, ?DESCENDING_KEY(Key), TreeId, ?DESCENDING_LINK_NAME(LinkName), DefaultValue).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec add_new_internal(ctx(), key(), tree_id(),
    link_name(), link_target()) -> ok.
add_new_internal(Ctx, Key, TreeId, LinkName, Value) ->
    case datastore_model:add_links(Ctx, Key, TreeId, {LinkName, Value}) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end.

-spec overwrite_internal(ctx(), key(), tree_id(),
    link_name(), link_update_fun() | link_target()) -> ok.
overwrite_internal(Ctx, Key, TreeId, LinkName, UpdateFun) when is_function(UpdateFun) ->
    OldValue = case datastore_model:get_links(Ctx, Key, TreeId, LinkName) of
        {ok, [#link{target = LinkTarget}]} ->
            ok = datastore_model:delete_links(Ctx, Key, TreeId, LinkName),
            LinkTarget;
        {error, not_found} ->
            undefined
    end,
    add_new_internal(Ctx, Key, TreeId, LinkName, UpdateFun(OldValue));
overwrite_internal(Ctx, Key, TreeId, LinkName, Value) ->
    ok = datastore_model:delete_links(Ctx, Key, TreeId, LinkName),
    add_new_internal(Ctx, Key, TreeId, LinkName, Value).

-spec get_next_internal(ctx(), key(), tree_id(),
    link_name(), link_target()) -> link_target().
get_next_internal(Ctx, Key, TreeId, LinkName, DefaultValue) ->
    {ok, FirstValueOrDefault} = datastore_model:fold_links(Ctx, Key, TreeId,
        fun(#link{target = Target}, _Acc) -> {ok, Target} end,
        DefaultValue, #{prev_link_name => LinkName, size => 1}),
    FirstValueOrDefault.
