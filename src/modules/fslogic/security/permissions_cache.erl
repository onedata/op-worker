%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model used for caching permissions to files (used by data_access_control and data_constraints modules).
%%% @end
%%%-------------------------------------------------------------------
-module(permissions_cache).
-author("Michal Wrzeszcz").


-include_lib("ctool/include/logging.hrl").

%% API
-export([init/0, terminate/0, check_permission/1, cache_permission/3, invalidate/0, invalidate_on_node/0]).


-define(CACHE, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

init() ->
    bounded_cache:init_cache(?CACHE, #{
        check_frequency => timer:seconds(30),
        size => 0, % Clear cache every 30 seconds
        worker => true
    }).


terminate() ->
    bounded_cache:terminate_cache(?CACHE).


%%--------------------------------------------------------------------
%% @doc
%% Checks permission in cache.
%% @end
%%--------------------------------------------------------------------
-spec check_permission(Rule :: term()) -> {ok, term()} | {error, not_found}.
check_permission(Rule) ->
    bounded_cache:get(?CACHE, Rule).

%%--------------------------------------------------------------------
%% @doc
%% Saves permission in cache.
%% @end
%%--------------------------------------------------------------------
-spec cache_permission(Rule :: term(), Value :: term(), bounded_cache:timestamp()) -> ok.
cache_permission(Rule, Value, Timestamp) ->
    bounded_cache:cache(?CACHE, Rule, Value, Timestamp, override_if_exists).

%%--------------------------------------------------------------------
%% @doc
%% Clears all permissions from cache on all nodes.
%% @end
%%--------------------------------------------------------------------
-spec invalidate() -> ok.
invalidate() ->
    Nodes = consistent_hashing:get_all_nodes(),
    {Res, BadNodes} = utils:rpc_multicall(Nodes, ?MODULE, invalidate_on_node, []),

    case BadNodes of
        [] -> ok;
        _ -> ?error("Invalidation of ~p failed on nodes: ~p (RPC error)", [?MODULE, BadNodes])
    end,

    lists:foreach(fun
        (ok) -> ok;
        ({badrpc, _} = Error) -> ?error("Invalidation of ~p failed.~nReason: ~p", [?MODULE, Error])
    end, Res).

%%--------------------------------------------------------------------
%% @doc
%% Clears all permissions from cache on node.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_on_node() -> ok.
invalidate_on_node() ->
    bounded_cache:invalidate(?CACHE).
