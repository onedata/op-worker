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

-include("global_definitions.hrl").
-include("modules/datastore/qos.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    init_group/0,
    ensure_exists_for_all_spaces/0,
    ensure_exists_on_all_nodes/1,
    ensure_exists/1, invalidate_on_all_nodes/1,
    init_qos_cache_for_space/1, init_qos_cache_for_all_spaces/0
]).


-define(DEFAULT_CHECK_FREQUENCY, 300000). % 5 min
-define(DEFAULT_CACHE_SIZE, 15000).
-define(QOS_BOUNDED_CACHE_CHECK_FREQ, qos_bounded_cache_check_frequency).
-define(QOS_BOUNDED_CACHE_CACHE_SIZE, qos_bounded_cache_size).


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
    CheckFrequency = get_param(?QOS_BOUNDED_CACHE_CHECK_FREQ, ?DEFAULT_CHECK_FREQUENCY),
    Size = get_param(?QOS_BOUNDED_CACHE_CACHE_SIZE, ?DEFAULT_CACHE_SIZE),
    
    bounded_cache:init_group(?QOS_BOUNDED_CACHE_GROUP, #{
        check_frequency => CheckFrequency,
        size => Size,
        worker => true
    }).


-spec ensure_exists_for_all_spaces() -> ok.
ensure_exists_for_all_spaces() ->
    % TODO: VFS-5744 potential race condition:
    % user may perform operations associated with QoS before cache initialization
    case get_space_list() of
        ok ->
            ok;
        SpaceIds ->
            lists:foreach(fun(SpaceId) -> ensure_exists_on_all_nodes(SpaceId) end, SpaceIds)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sends request to perform ensure_exists/1 on all nodes.
%% @end
%%--------------------------------------------------------------------
-spec ensure_exists_on_all_nodes(od_space:id()) -> ok.
ensure_exists_on_all_nodes(SpaceId) ->
    Nodes = consistent_hashing:get_all_nodes(),
    {Res, BadNodes} = rpc:multicall(Nodes, ?MODULE, ensure_exists, [SpaceId]),

    case BadNodes of
        [] ->
            ok;
        _ ->
            ?error(
                "Could not ensure that QoS bounded cache for space ~p exists on
                nodes: ~w (RPC error)", [SpaceId, BadNodes]
            )
    end,

    lists:foreach(fun
        (ok) -> ok;
        ({badrpc, _} = Error) ->
            ?error(
                "Could not ensure that QoS bounded cache for space: ~p exists.~n"
                "Reason: ~p", [SpaceId, Error]
            )
    end, Res).


%%--------------------------------------------------------------------
%% @doc
%% Checks whether cache for given space exists on current node. If cache does
%% not exist it will be initialized.
%% @end
%%--------------------------------------------------------------------
-spec ensure_exists(od_space:id()) -> ok.
ensure_exists(SpaceId) ->
    CacheTableName = ?CACHE_TABLE_NAME(SpaceId),
    CacheTableInfo = ets:info(CacheTableName),
    case CacheTableInfo of
        undefined ->
            % call to worker as process that will hold ets is needed
            qos_worker:init_qos_cache_for_space(SpaceId);
        _ ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Initializes caches for all spaces. This function should be called be
%% some long living process as it will hold ets tables.
%% @end
%%--------------------------------------------------------------------
-spec init_qos_cache_for_all_spaces() -> ok.
init_qos_cache_for_all_spaces() ->
    case get_space_list() of
        ok ->
            ok;
        SpaceIds ->
            lists:foreach(fun(SpaceId) -> init_qos_cache_for_space(SpaceId) end, SpaceIds)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Initializes cache for given space. This function should be called be
%% some long living process as it will hold ets table.
%% @end
%%--------------------------------------------------------------------
-spec init_qos_cache_for_space(od_space:id()) -> ok.
init_qos_cache_for_space(SpaceId) ->
    try bounded_cache:init_cache(?CACHE_TABLE_NAME(SpaceId), #{group => ?QOS_BOUNDED_CACHE_GROUP}) of
        ok ->
            ok;
        Error = {error, _} ->
            ?error("Unable to initialize QoS bounded cache due to: ~p", [Error])
    catch
        Error2:Reason ->
            ?error_stacktrace("Unable to initialize qos bounded cache due to: ~p", [{Error2, Reason}])
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
-spec get_space_list() -> [od_space:id()] | ok.
get_space_list() ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            SpaceIds;
        ?ERROR_NO_CONNECTION_TO_ONEZONE ->
            ?debug("Unable to initialize QoS bounded cache due to: ~p", [?ERROR_NO_CONNECTION_TO_ONEZONE]);
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            ?debug("Unable to initialize QoS bounded cache due to: ~p", [?ERROR_UNREGISTERED_ONEPROVIDER]);
        Error = {error, _} ->
            ?error("Unable to initialize QoS bounded cache due to: ~p", [Error])
    catch
        Error2:Reason ->
            ?error_stacktrace("Unable to initialize qos bounded cache due to: ~p", [{Error2, Reason}])
    end.


%% @private
-spec get_param(atom(), non_neg_integer()) -> non_neg_integer().
get_param(ParamName, DefaultVal) ->
    Value = application:get_env(?APP_NAME, ParamName, DefaultVal),
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