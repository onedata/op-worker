%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Util functions for operating on index links trees.
%%% @end
%%%-------------------------------------------------------------------
-module(index_links).
-author("Bartosz Walkowicz").
-author("Jakub Kudzia").

-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% API
-export([add_link/2, list/4, delete_links/2]).

-define(CTX, (index:get_ctx())).
-define(LINK_PREFIX, <<"INDEXES">>).

add_link(IndexName, SpaceId) ->
    TreeId = oneprovider:get_id(),
    Ctx = ?CTX#{scope => SpaceId},
    LinkRoot = link_root(SpaceId),
    case datastore_model:add_links(Ctx, LinkRoot, TreeId, {IndexName, IndexName}) of
        {ok, _} ->
            ok;
        {error, already_exists} ->
            ok
    end.

-spec list(SpaceId :: od_space:id(),
    index:name() | undefined, non_neg_integer(), non_neg_integer() | all) -> [transfer:id()].
list(SpaceId, StartId, Offset, Limit) ->
    Opts = #{offset => Offset},

    Opts2 = case StartId of
        undefined -> Opts;
        _ -> Opts#{prev_link_name => StartId}
    end,

    Opts3 = case Limit of
        all -> Opts2;
        _ -> Opts2#{size => Limit}
    end,

    {ok, Indexes} = for_each_link(fun(_LinkName, IndexName, Acc) ->
        [IndexName | Acc]
    end, [], SpaceId, Opts3),
    lists:reverse(Indexes).

-spec delete_links(index:name(), od_space:id()) -> ok.
delete_links(IndexName, SpaceId) ->
    LinkRoot = link_root(SpaceId),
    case datastore_model:get_links(?CTX, LinkRoot, all, IndexName) of
        {error, not_found} ->
            ok;
        {ok, Links} ->
            lists:foreach(fun(#link{tree_id = ProviderId, name = LinkName}) ->
                case oneprovider:is_self(ProviderId) of
                    true ->
                        ok = datastore_model:delete_links(
                            ?CTX#{scope => SpaceId}, LinkRoot,
                            ProviderId, LinkName
                        );
                    false ->
                        ok = datastore_model:mark_links_deleted(
                            ?CTX#{scope => SpaceId}, LinkRoot,
                            ProviderId, LinkName
                        )
                end
            end, Links)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec link_root(od_space:id()) -> binary().
link_root(SpaceId) ->
    <<?LINK_PREFIX/binary, "_", SpaceId/binary>>.

-spec for_each_link(fun((index:name(), index:name(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term(), od_space:id(), datastore_model:fold_opts()) ->
    {ok, Acc :: term()} | {error, term()}.
for_each_link(Callback, Acc0, SpaceId, Options) ->
    datastore_model:fold_links(?CTX, link_root(SpaceId), all, fun
        (#link{name = Name, target = Target}, Acc) ->
            {ok, Callback(Name, Target, Acc)}
    end, Acc0, Options).