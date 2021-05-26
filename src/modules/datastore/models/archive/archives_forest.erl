%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements forest structure which is used to store
%%% ids of archives nested in another archives.
%%% @end
%%%-------------------------------------------------------------------
-module(archives_forest).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([add/3, list/3, delete/3]).

-define(CTX, (archive:get_ctx())).
-define(CTX(Scope), ?CTX#{scope => Scope}).
-define(FOREST(ParentArchiveId), <<"ARCHIVES_TREE_", ParentArchiveId/binary>>).
-define(LOCAL_TREE_ID, oneprovider:get_id()).
-define(LINK(LinkName), {LinkName, <<>>}).


-type link_name() :: archive:id().
-type tree_id() :: od_provider:id().
-type forest() :: binary(). % ?FOREST(ParentArchiveId)
-type limit() :: pos_integer().
-type token() :: datastore_links_iter:token().

-export_type([token/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec add(archive:id(), od_space:id(), archive:id()) -> ok.
add(ParentArchiveId, SpaceId, ArchiveId) ->
    ?extract_ok(datastore_model:add_links(?CTX(SpaceId), ?FOREST(ParentArchiveId), ?LOCAL_TREE_ID, ?LINK(ArchiveId))).


-spec delete(archive:id(), od_space:id(), archive:id()) -> ok.
delete(ParentArchiveId, SpaceId, ArchiveId) ->
    Forest = ?FOREST(ParentArchiveId),
    case datastore_model:get_links(?CTX, Forest, all, ArchiveId) of
        {error, not_found} ->
            ok;
        {ok, Links} ->
            lists:foreach(fun(#link{tree_id = ProviderId, name = LinkName}) ->
                case oneprovider:is_self(ProviderId) of
                    true -> delete_local(Forest, SpaceId, LinkName);
                    false -> delete_remote(Forest, SpaceId, LinkName, ProviderId)
                end
            end, Links)
    end.


-spec list(archive:id(), token(), limit()) -> {ok, [link_name()], token()}.
list(ArchiveId, Token, Limit) ->
    {{ok, Archives}, Token2} = fold_links(ArchiveId, fun(ArchiveId, Acc) ->
        [ArchiveId | Acc]
    end, [], #{token => Token, size => Limit}),
    {ok, Archives, Token2}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec fold_links(
    archive:id(),
    fun((link_name(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term(), datastore_model:fold_opts()) ->
    {{ok, Acc :: term()}, token()} | {error, term()}.
fold_links(ParentArchiveId, Callback, Acc0, Opts) ->
    datastore_model:fold_links(?CTX, ?FOREST(ParentArchiveId), all,
        fun(#link{name = Name}, Acc) -> {ok, Callback(Name, Acc)} end,
        Acc0, Opts
    ).


-spec delete_local(forest(), od_space:id(), link_name()) -> ok.
delete_local(Forest, SpaceId, LinkName) ->
    ok = datastore_model:delete_links(?CTX(SpaceId), Forest, ?LOCAL_TREE_ID, LinkName).


-spec delete_remote(forest(), od_space:id(), link_name(), tree_id()) -> ok.
delete_remote(Forest, SpaceId, LinkName, ProviderId) ->
    ok = datastore_model:mark_links_deleted(?CTX(SpaceId), Forest, ProviderId, LinkName).