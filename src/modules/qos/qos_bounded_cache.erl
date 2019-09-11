%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions responsible for QoS bounded cache management.
%%% Qos bounded cache is used for calculating effective QoS for
%%% files and directories.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_bounded_cache).
-author("Michal Cwiertnia").

-include("modules/datastore/qos.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    init_group/0, init/1,
    ensure_exists_on_all_nodes/1, ensure_exists/1,
    invalidate_on_all_nodes/1
]).


-define(DEFAULT_CHECK_FREQUENCY, 300000). % 5 min
-define(DEFAULT_CACHE_SIZE, 15000).


%%%===================================================================
%%% API
%%%===================================================================

-spec init_group() -> ok | {error, term()}.
init_group() ->
    CheckFrequency = application:get_env(
        ?APP_NAME, qos_bounded_cache_check_frequency, ?DEFAULT_CHECK_FREQUENCY
    ),
    Size = application:get_env(?APP_NAME, qos_bounded_cache_size, ?DEFAULT_CACHE_SIZE),

    bounded_cache:init_group(?QOS_BOUNDED_CACHE_GROUP, #{
        check_frequency => CheckFrequency,
        size => Size,
        worker => true
    }).


-spec init(od_space:id()) -> ok | {error, term()}.
init(SpaceId) ->
    bounded_cache:init_cache(?CACHE_TABLE_NAME(SpaceId), #{group => ?QOS_BOUNDED_CACHE_GROUP}).


%%--------------------------------------------------------------------
%% @doc
%% Sends request to perform ensure_exists/1 on all nodes.
%% @end
%%--------------------------------------------------------------------
-spec ensure_exists_on_all_nodes(od_space:id()) -> ok.
ensure_exists_on_all_nodes(SpaceId) ->
    Nodes = consistent_hashing:get_all_nodes(),
    {Res, BadNodes} = rpc:multicall(Nodes, ?MODULE, ensure_exists, [SpaceId]),

    case length(BadNodes) > 0 of
        true ->
            ?error(
                "Error when ensuring QoS bounded cache on following nodes,
                as they are not exists: ~p ~n", [BadNodes]
            );
        false ->
            ok
    end,

    lists:foreach(fun
        (ok) -> ok;
        ({badrpc, _} = Error) ->
            ?critical(
                "Failed to ensure QoS bounded cache exists for space: ~p.
                Error: ~p~n", [SpaceId, Error]
            )
    end, Res).


%%--------------------------------------------------------------------
%% @doc
%% Checks whether cache for given space exists on current node. If cache does
%% not exist schedules initialization of cache in fslogic_worker.
%% @end
%%--------------------------------------------------------------------
-spec ensure_exists(od_space:id()) -> ok.
ensure_exists(SpaceId) ->
    CacheTableName = ?CACHE_TABLE_NAME(SpaceId),
    CacheTableInfo = ets:info(CacheTableName),
    case CacheTableInfo of
        undefined ->
            fslogic_worker:init_qos_cache_for_space(SpaceId);
        _ ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sends request to invalidate cache to all nodes.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_on_all_nodes(od_space:id()) -> ok.
invalidate_on_all_nodes(SpaceId) ->
    Nodes = consistent_hashing:get_all_nodes(),
    {Res, BadNodes} = rpc:multicall(Nodes, effective_value, invalidate, [?CACHE_TABLE_NAME(SpaceId)]),

    case length(BadNodes) > 0 of
        true ->
            ?error(
                "Error when invalidating QoS bounded cache on following nodes,
                as they are not exists: ~p ~n", [BadNodes]
            );
        false ->
            ok
    end,

    lists:foreach(fun
        (ok) -> ok;
        ({badrpc, _} = Error) ->
            ?error(
                "Failed to invalidate QoS bounded cache for space: ~p.
                Error: ~p~n", [SpaceId, Error]
            )
    end, Res).