%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc Worker responsible for initialization of qos_bounded_cache.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_worker).
-behaviour(worker_plugin_behaviour).

-author("Michal Cwiertnia").

-include("modules/datastore/qos.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_qos_cache_for_space/1]).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).


-define(INIT_QOS_CACHE_FOR_SPACE, init_qos_cache_for_space).
-define(CHECK_QOS_CACHE, bounded_cache_timer).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State :: worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    qos_traverse:init_pool(),
    qos_bounded_cache:init_group(),
    qos_bounded_cache:ensure_exists_for_all_spaces(),
    {ok, #{}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck | term(),
    Result :: cluster_status:status() | ok | {ok, Response} |
    {error, Reason} | pong,
    Response :: term(),
    Reason :: term().
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;
handle({?CHECK_QOS_CACHE, Msg}) ->
    ?debug("Cleaning QoS bounded cache if needed"),
    bounded_cache:check_cache_size(Msg);
handle({?INIT_QOS_CACHE_FOR_SPACE, SpaceId}) ->
    ?debug("Initializing qos bounded cache for space: ~p", [SpaceId]),
    init_qos_cache_for_space_internal(SpaceId);
handle(_Request) ->
    ?log_bad_request(_Request),
    {error, wrong_request}.


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    ok.


%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Schedule initialization of QoS bounded cache for given space.
%% @end
%%--------------------------------------------------------------------
-spec init_qos_cache_for_space(od_space:id()) -> ok.
init_qos_cache_for_space(SpaceId) ->
    erlang:send_after(0, ?MODULE, {sync_timer, {?INIT_QOS_CACHE_FOR_SPACE, SpaceId}}),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init_qos_cache_for_space_internal(od_space:id()) -> ok.
init_qos_cache_for_space_internal(SpaceId) ->
    try bounded_cache:init_cache(?CACHE_TABLE_NAME(SpaceId), #{group => ?QOS_BOUNDED_CACHE_GROUP}) of
        ok ->
            ok;
        Error = {error, _} ->
            ?error("Unable to initialize QoS bounded cache due to: ~p", [Error])
    catch
        Error2:Reason ->
            ?error_stacktrace("Unable to initialize qos bounded cache due to: ~p", [{Error2, Reason}])
    end.
