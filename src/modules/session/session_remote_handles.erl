%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module for storing, per session, the providers that hold file handles opened
%%% on this one's behalf. A request handed over to another provider is the only
%%% way a handle can come into being elsewhere (see fslogic_request), and every
%%% later request carrying that handle has to reach the very provider that
%%% registered it - this provider knows nothing about it.
%%% Handles opened locally are not kept here; they live in session_handles.
%%% NOTE: the links are node local and volatile, exactly like the ones in
%%% session_handles - losing them merely brings back the routing that would have
%%% been used had the handle never been noted.
%%% @end
%%%-------------------------------------------------------------------
-module(session_remote_handles).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% API
-export([add/3, get/2, remove/2, remove_all/1]).
%% For RPC
-export([remove_all_locally/1]).

-define(REMOTE_HANDLES_TREE_ID, <<"remote_file_handles">>).

%%%===================================================================
%%% API
%%%===================================================================

-spec add(session:id(), storage_driver:handle_id(), od_provider:id()) -> ok | {error, term()}.
add(SessId, HandleId, ProviderId) ->
    session_local_links:add_links(SessId, ?REMOTE_HANDLES_TREE_ID, HandleId, ProviderId).


-spec get(session:id(), storage_driver:handle_id()) -> {ok, od_provider:id()} | {error, term()}.
get(SessId, HandleId) ->
    case session_local_links:get_link(SessId, ?REMOTE_HANDLES_TREE_ID, HandleId) of
        {ok, [#link{target = ProviderId}]} -> {ok, ProviderId};
        {error, _} = Error -> Error
    end.


-spec remove(session:id(), storage_driver:handle_id()) -> ok | {error, term()}.
remove(SessId, HandleId) ->
    session_local_links:delete_links(SessId, ?REMOTE_HANDLES_TREE_ID, HandleId).


-spec remove_all(session:id()) -> ok.
remove_all(SessId) ->
    {AnsList, _} = utils:rpc_multicall(
        consistent_hashing:get_all_nodes(), ?MODULE, remove_all_locally, [SessId]
    ),
    lists:foreach(fun(Ans) -> ok = Ans end, AnsList).


-spec remove_all_locally(session:id()) -> ok.
remove_all_locally(SessId) ->
    {ok, HandleIds} = session_local_links:fold_links(SessId, ?REMOTE_HANDLES_TREE_ID,
        fun(#link{name = HandleId}, Acc) -> {ok, [HandleId | Acc]} end
    ),
    session_local_links:delete_links(SessId, ?REMOTE_HANDLES_TREE_ID, HandleIds),
    ok.
