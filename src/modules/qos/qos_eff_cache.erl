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
%%% TODO VFS-7412 refactor this module (duplicated code in other effective caches modules)
%%% @end
%%%-------------------------------------------------------------------
-module(qos_eff_cache).
-author("Michal Cwiertnia").

-include("global_definitions.hrl").
-include("modules/datastore/qos.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    init_group/0,
    invalidate_on_all_nodes/1,
    init/1
]).


-define(DEFAULT_CHECK_FREQUENCY, 300000). % 5 min
-define(DEFAULT_CACHE_SIZE, 15000).
-define(QOS_EFF_CACHE_CHECK_FREQ, qos_eff_cache_check_frequency).
-define(QOS_EFF_CACHE_CACHE_SIZE, qos_eff_cache_size).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes bounded_cache group.
%% @end
%%--------------------------------------------------------------------
-spec init_group() -> ok | {error, term()}.
init_group() ->
    CheckFrequency = get_param(?QOS_EFF_CACHE_CHECK_FREQ, ?DEFAULT_CHECK_FREQUENCY),
    Size = get_param(?QOS_EFF_CACHE_CACHE_SIZE, ?DEFAULT_CACHE_SIZE),
    
    bounded_cache:init_group(?QOS_EFF_CACHE_GROUP, #{
        check_frequency => CheckFrequency,
        size => Size
    }).


%%--------------------------------------------------------------------
%% @doc
%% Initializes cache for given space. This function should be called by
%% some long living process as it will hold ets table.
%% @end
%%--------------------------------------------------------------------
-spec init(od_space:id() | all) -> ok.
init(all) ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            lists:foreach(fun init/1, SpaceIds);
        ?ERROR_NO_CONNECTION_TO_ONEZONE ->
            ?debug("Unable to initialize QoS effective cache.~nError: ~p", [?ERROR_NO_CONNECTION_TO_ONEZONE]);
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            ?debug("Unable to initialize QoS effective cache.~nError: ~p", [?ERROR_UNREGISTERED_ONEPROVIDER]);
        Error = {error, _} ->
            ?critical("Unable to initialize QoS effective cache.~nError: ~p", [Error])
    catch
        Class:Reason:Stacktrace ->
            ?critical_exception("Unable to initialize QoS effective cache", Class, Reason, Stacktrace)
    end;
init(SpaceId) ->
    CacheName = ?CACHE_TABLE_NAME(SpaceId),
    try
        case effective_value:cache_exists(CacheName) of
            true ->
                ok;
            _ ->
                case effective_value:init_cache(CacheName, #{group => ?QOS_EFF_CACHE_GROUP}) of
                    ok ->
                        ok;
                    Error = {error, _} ->
                        ?critical("Unable to initialize QoS effective cache for space ~p.~nError ~p",
                            [SpaceId, Error])
                end
        end
    catch
        Class:Reason:Stacktrace ->
            ?critical_exception(
                "Unable to initialize QoS effective cache for space ~p", [SpaceId],
                Class, Reason, Stacktrace
            )
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sends request to invalidate cache to all nodes.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_on_all_nodes(od_space:id()) -> ok.
invalidate_on_all_nodes(SpaceId) ->
    Nodes = consistent_hashing:get_all_nodes(),
    {Res, BadNodes} = utils:rpc_multicall(Nodes, effective_value, invalidate, [?CACHE_TABLE_NAME(SpaceId)]),

    case BadNodes of
        [] ->
            ok;
        _ ->
            ?error(
                "Invalidation of QoS bounded cache for space ~p failed on nodes: ~p (RPC error)",
                [BadNodes]
            )
    end,

    lists:foreach(fun
        (ok) -> ok;
        ({badrpc, _} = Error) ->
            ?error(
                "Invalidation of QoS bounded cache for space ~p failed.~n"
                "Reason: ~p", [SpaceId, Error]
            )
    end, Res).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_param(atom(), non_neg_integer()) -> non_neg_integer().
get_param(ParamName, DefaultVal) ->
    Value = op_worker:get_env(ParamName, DefaultVal),
    ensure_non_neg_integer(Value, ParamName, DefaultVal).


%% @private
-spec ensure_non_neg_integer(non_neg_integer(), atom(), non_neg_integer()) -> non_neg_integer().
ensure_non_neg_integer(Value, _, _) when is_integer(Value) andalso Value >= 0 ->
    Value;

ensure_non_neg_integer(Value, ParamName, DefaultVal) ->
    ?warning(
        "Got ~p value for ~p parameter. ~p should be a non-negative integer. "
        "Using default value instead (~p)", [Value, ParamName, ParamName, DefaultVal]
    ),
    DefaultVal.
