%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions responsible for qos bounded cache management.
%%% Qos bounded cache is used for calculating effective qos for
%%% files and directories.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_bounded_cache).
-author("Michal Cwiertnia").

-include("modules/datastore/qos.hrl").
-include("global_definitions.hrl").

%% API
-export([
    init_group/0, init/1,
    ensure_exists_on_all_nodes/1, ensure_exists/1,
    invalidate_on_all_nodes/1, invalidate/1
]).

-define(CACHE_TABLE_NAME(SpaceId), binary_to_atom(SpaceId, utf8)).


%%%===================================================================
%%% API
%%%===================================================================

-spec init_group() -> ok | {error, term()}.
init_group() ->
    CheckFrequency = application:get_env(?APP_NAME, qos_bounded_cache_check_frequency),
    Size = application:get_env(?APP_NAME, qos_bounded_cache_size),

    bounded_cache:init_group(?QOS_BOUNDED_CACHE_GROUP, #{
        check_frequency => CheckFrequency,
        size => Size
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
    rpc:eval_everywhere(Nodes, ?MODULE, ensure_exists, [SpaceId]),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Checks whether cache for given space exists on current node. If cache does
%% not exist schedules initialization of cache in fslogic_worker.
%% @end
%%--------------------------------------------------------------------
-spec ensure_exists(od_space:id()) -> ok.
ensure_exists(SpaceId) ->
    CacheTableName = ?CACHE_TABLE_NAME(SpaceId),
    CacheTable = ets:info(CacheTableName),
    case CacheTable of
        undefined ->
            fslogic_worker:schedule_init_qos_cache_for_space(SpaceId);
        _ ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sends request to perform invalidate/1 on all nodes.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_on_all_nodes(od_space:id()) -> ok.
invalidate_on_all_nodes(SpaceId) ->
    Nodes = consistent_hashing:get_all_nodes(),
    rpc:eval_everywhere(Nodes, ?MODULE, invalidate, [SpaceId]),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Invalidates cache for given space.
%% @end
%%--------------------------------------------------------------------
-spec invalidate(od_space:id()) -> ok.
invalidate(SpaceId) ->
    effective_value:invalidate(binary_to_atom(SpaceId, utf8)).
