%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Session links management model. Use dedicated model
%%% as session routing is global and links routing is local.
%%% @end
%%%-------------------------------------------------------------------
-module(session_local_links).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

% API - local links
-export([add_links/4, get_link/3, fold_links/3, delete_links/3]).
% API - protected local links
-export([add_protected_links/4, fold_protected_links/4, delete_protected_links/3]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined,
    routing => local
}).

-define(PROTECTED_LINK_KEY(SessID), <<"PROTECTED_LINK_", SessID/binary>>).

%%%===================================================================
%%% API - local links
%%%===================================================================

-spec add_links(session:id(), datastore:tree_id(), datastore:link_name(),
    datastore:link_target()) -> ok | {error, term()}.
add_links(SessId, TreeID, LinkName, LinkValue) ->
    ?extract_ok(datastore_model:add_links(?CTX, SessId, TreeID, {LinkName, LinkValue})).

-spec get_link(session:id(), datastore:tree_id(), datastore:link_name()) ->
    {ok, [datastore:link()]} | {error, term()}.
get_link(SessId, TreeID, LinkName) ->
    datastore_model:get_links(?CTX, SessId, TreeID, LinkName).


-spec fold_links(session:id(), datastore:tree_id(), datastore:fold_fun(datastore:link())) ->
    {ok, datastore:fold_acc()} | {error, term()}.
fold_links(SessId, TreeID, Fun) ->
    datastore_model:fold_links(?CTX, SessId, TreeID, Fun, [], #{}).

-spec delete_links
    (session:id(), datastore:tree_id(), datastore:link_name()) -> ok | {error, term()};
    (session:id(), datastore:tree_id(), [datastore:link_name()]) -> [ok | {error, term()}].
delete_links(SessId, TreeID, LinkName) ->
    datastore_model:delete_links(?CTX, SessId, TreeID, LinkName).

%%%===================================================================
%%% API - protected local links
%%%===================================================================

-spec add_protected_links(session:id(), datastore:tree_id(), datastore:link_name(),
    datastore:link_target()) -> ok | {error, term()}.
add_protected_links(SessId, TreeID, LinkName, LinkValue) ->
    ?extract_ok(datastore_model:add_links(?CTX#{ha_disabled => false},
        ?PROTECTED_LINK_KEY(SessId), TreeID, {LinkName, LinkValue})).

-spec fold_protected_links(session:id(), datastore:tree_id(), datastore:fold_fun(datastore:link()), node()) ->
    {ok, datastore:fold_acc()} | {error, term()}.
fold_protected_links(SessId, TreeID, Fun, FoldNode) ->
    datastore_model:fold_links(?CTX#{fold_node => FoldNode}, ?PROTECTED_LINK_KEY(SessId), TreeID, Fun, [], #{}).

-spec delete_protected_links
    (session:id(), datastore:tree_id(), datastore:link_name()) -> ok | {error, term()};
    (session:id(), datastore:tree_id(), [datastore:link_name()]) -> [ok | {error, term()}].
delete_protected_links(SessId, TreeID, LinkName) ->
    datastore_model:delete_links(?CTX#{ha_disabled => false}, ?PROTECTED_LINK_KEY(SessId), TreeID, LinkName).
