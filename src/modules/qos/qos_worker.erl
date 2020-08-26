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

-include("global_definitions.hrl").
-include("modules/datastore/qos.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_qos_cache_for_space/1, init_retry_failed_files/0]).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).


-define(INIT_QOS_CACHE_FOR_SPACE, init_qos_cache_for_space).
-define(CHECK_QOS_CACHE, bounded_cache_timer).
-define(RETRY_FAILED_FILES, retry_failed_files).

-define(RETRY_FAILED_FILES_INTERVAL_SECONDS,
    application:get_env(?APP_NAME, qos_retry_failed_files_interval_seconds, 300)). % 5 minutes

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
    try
        qos_traverse:init_pool()
    catch
        throw:{error, already_exists} -> ok
    end,
    qos_bounded_cache:init_group(),
    qos_bounded_cache:init_qos_cache_for_all_spaces(),
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
    qos_bounded_cache:init_qos_cache_for_space(SpaceId);
handle(?RETRY_FAILED_FILES) ->
    case provider_logic:get_spaces() of
        {ok, Spaces} ->
            lists:foreach(fun(SpaceId) ->
                ok = qos_hooks:retry_failed_files(SpaceId)
            end, Spaces);
        Error -> 
            ?warning("QoS failed files retry failed to fetch provider spaces due to: ~p", [Error])
    end,
    erlang:send_after(timer:seconds(?RETRY_FAILED_FILES_INTERVAL_SECONDS),
        ?MODULE, {sync_timer, ?RETRY_FAILED_FILES});
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
    qos_traverse:stop_pool(),
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

-spec init_retry_failed_files() -> ok.
init_retry_failed_files() ->
    erlang:send_after(timer:seconds(?RETRY_FAILED_FILES_INTERVAL_SECONDS),
        ?MODULE, {sync_timer, ?RETRY_FAILED_FILES}),
    ok.
