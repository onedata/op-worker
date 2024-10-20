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
-module(view_links).
-author("Bartosz Walkowicz").
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([add_link/3, list/4, delete_links/2, get_view_id/2]).

-define(CTX, (index:get_ctx())).
-define(LINK_PREFIX, <<"INDEXES">>).
-define(VIEW_ID_TREE_ID_SEPARATOR, <<"@">>).


%%%===================================================================
%%% API
%%%===================================================================


-spec add_link(index:name(), index:id(), od_space:id()) -> ok | {error, term()}.
add_link(ViewName, ViewId, SpaceId) ->
    TreeId = oneprovider:get_id(),
    Ctx = ?CTX#{scope => SpaceId},
    LinkRoot = link_root(SpaceId),
    ?extract_ok(datastore_model:add_links(Ctx, LinkRoot, TreeId, {ViewName, ViewId})).


-spec get_view_id(index:name(), od_space:id()) -> {ok, index:id()} | {error, term()}.
get_view_id(ViewName, SpaceId) ->
    Tokens = binary:split(ViewName, ?VIEW_ID_TREE_ID_SEPARATOR, [global]),
    Ctx = ?CTX#{scope => SpaceId},
    LinkRoot = link_root(SpaceId),
    case lists:reverse(Tokens) of
        [TreeIdPrefix | Tokens2] when TreeIdPrefix =/= <<>>, Tokens2 =/= [], Tokens2 =/= [<<>>] ->
            PrefixSize = erlang:size(TreeIdPrefix),
            ViewName2 = binary:part(ViewName, 0, size(ViewName) - PrefixSize - size(?VIEW_ID_TREE_ID_SEPARATOR)),
            {ok, TreeIds} = datastore_model:get_links_trees(Ctx, LinkRoot),
            TreeIds2 = lists:filter(fun(TreeId) ->
                case TreeId of
                    <<TreeIdPrefix:PrefixSize/binary, _/binary>> -> true;
                    _ -> false
                end
            end, TreeIds),
            case TreeIds2 of
                [TreeId] ->
                    case get_view_id(ViewName2, TreeId, SpaceId) of
                        {ok, ViewId} ->
                            {ok, ViewId};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                [] ->
                    get_view_id(ViewName, all, SpaceId)
            end;
        _ ->
            case get_view_id(ViewName, oneprovider:get_id(), SpaceId) of
                {ok, ViewId} -> {ok, ViewId};
                {error, not_found} -> get_view_id(ViewName, all, SpaceId);
                {error, Reason} -> {error, Reason}
            end
    end.


-spec get_view_id(index:name(), datastore_model:tree_ids(),
    od_space:id()) -> {ok, index:id()} | {error, term()}.
get_view_id(ViewName, TreeIds, SpaceId) ->
    Ctx = ?CTX#{scope => SpaceId},
    LinkRoot = link_root(SpaceId),
    case datastore_model:get_links(Ctx, LinkRoot, TreeIds, ViewName) of
        {ok, [#link{target = ViewId}]} ->
            {ok, ViewId};
        {ok, [#link{} | _]} ->
            ?error("More than one link associated with view name ~tp ", [ViewName]),
            {error, ?EINVAL};
        {error, Reason} ->
            {error, Reason}
    end.


-spec list(SpaceId :: od_space:id(), undefined | index:name(), non_neg_integer(),
    non_neg_integer() | all) -> {ok, [index:name()]} | {error, term()}.
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

    Result = datastore_model:fold_links(?CTX, link_root(SpaceId), all,
        fun(Link, Acc) -> {ok, [Link | Acc]} end, [], Opts3),

    case Result of
        {ok, Indices} ->
            {ok, tag_indices(lists:reverse(Indices))};
        Error ->
            Error
    end.


-spec delete_links(index:name(), od_space:id()) -> ok.
delete_links(ViewName, SpaceId) ->
    LinkRoot = link_root(SpaceId),
    case datastore_model:get_links(?CTX, LinkRoot, all, ViewName) of
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


%%--------------------------------------------------------------------
%% @doc
%% Adds links tree ID suffix to indices with ambiguous names.
%% @end
%%--------------------------------------------------------------------
-spec tag_indices([datastore_links:link()]) -> [index:name()].
tag_indices([]) ->
    [];
tag_indices(Links) ->
    {Group2, Groups2} = lists:foldl(fun
        (Link = #link{}, {[], Groups}) ->
            {[Link], Groups};
        (Link = #link{name = N}, {Group = [#link{name = N} | _], Groups}) ->
            {[Link | Group], Groups};
        (Link = #link{}, {Group, Groups}) ->
            {[Link], [Group | Groups]}
    end, {[], []}, Links),
    lists:foldl(fun
        ([#link{name = Name}], Children) ->
            [Name | Children];
        (Group, Children) ->
            LocalTreeId = oneprovider:get_id(),
            {LocalLinks, RemoteLinks} = lists:partition(fun
                (#link{tree_id = TreeId}) -> TreeId == LocalTreeId
            end, Group),
            RemoteTreeIds = [Link#link.tree_id || Link <- RemoteLinks],
            RemoteTreeIdsLen = [size(TreeId) || TreeId <- RemoteTreeIds],
            Len = binary:longest_common_prefix(RemoteTreeIds),
            Len2 = min(max(4, Len + 1), lists:min(RemoteTreeIdsLen)),
            lists:foldl(fun
                (#link{
                    tree_id = TreeId, name = Name
                }, Children2) when TreeId == LocalTreeId ->
                    [Name | Children2];
                (#link{
                    tree_id = TreeId, name = Name
                }, Children2) ->
                    [<<Name/binary, ?VIEW_ID_TREE_ID_SEPARATOR/binary, TreeId:Len2/binary>> | Children2]
            end, Children, LocalLinks ++ RemoteLinks)
    end, [], [Group2 | Groups2]).