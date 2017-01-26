%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for mapping objects to their nodes on the basis of hashes.
%%% @end
%%%--------------------------------------------------------------------
-module(consistent_hasing).
-author("Tomasz Lichon").

-include("global_definitions.hrl").

%% API
-export([init/1, deinit/0, get_node/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initialize chash ring
%% @end
%%--------------------------------------------------------------------
-spec init([node()]) -> ok.
init(Nodes) ->
    case application:get_env(?APP_NAME, chash) of
        undefined ->
            [Node0 | _] = Nodes,
            InitialCHash = chash:fresh(length(Nodes), Node0),
            CHash = lists:foldl(fun({I, Node}, CHashAcc) ->
                chash:update(get_nth_index(I, CHashAcc), Node, CHashAcc)
            end, InitialCHash, lists:zip(lists:seq(1, length(Nodes)), Nodes)),
            application:set_env(?APP_NAME, chash, CHash);
        {ok, _Val} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes chash ring.
%% @end
%%--------------------------------------------------------------------
-spec deinit() -> ok.
deinit() ->
    application:unset_env(?APP_NAME, chash).

%%--------------------------------------------------------------------
%% @doc
%% Get node that is responsible for the data labeled with given term.
%% @end
%%--------------------------------------------------------------------
-spec get_node(term()) -> node().
get_node(Label) ->
    {ok, CHash} = application:get_env(?APP_NAME, chash),
    Index = chash:key_of(Label),
    [{_, BestNode}] = chash:successors(Index, CHash, 1),
    BestNode.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get hash index of nth node in ring.
%% @end
%%--------------------------------------------------------------------
-spec get_nth_index(non_neg_integer(), chash:chash()) -> non_neg_integer().
get_nth_index(N, CHash) ->
    {Index, _} = lists:nth(N, chash:nodes(CHash)),
    Index.