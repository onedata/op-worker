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
-export([
    init_retry_failed_files/0,
    init_traverse_pools/0,
    init_traverse_pool/0
]).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).


-define(RETRY_FAILED_FILES, retry_failed_files).

-define(RETRY_FAILED_FILES_INTERVAL_SECONDS,
    op_worker:get_env(qos_retry_failed_files_interval_seconds, 300)). % 5 minutes

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
    % file_links_reconciliation_traverse is started here, as it uses QoS traverse pool.
    % Although it is sufficient to be started just once for every space, the procedure
    % is idempotent and it's safe to attempt its start at every application init.
    file_links_reconciliation_traverse:start(),
    % QoS traverse pools are initialized after successful zone connection
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
handle(?RETRY_FAILED_FILES) ->
    case provider_logic:get_spaces() of
        {ok, Spaces} ->
            lists:foreach(fun(SpaceId) ->
                ok = qos_logic:retry_failed_files(SpaceId)
            end, Spaces);
        Error -> 
            ?warning("QoS failed files retry failed to fetch provider spaces due to: ~tp", [Error])
    end,
    erlang:send_after(timer:seconds(?RETRY_FAILED_FILES_INTERVAL_SECONDS),
        ?MODULE, {sync_timer, ?RETRY_FAILED_FILES});
handle(Request) ->
    ?log_bad_request(Request),
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

-spec init_retry_failed_files() -> ok.
init_retry_failed_files() ->
    erlang:send_after(timer:seconds(?RETRY_FAILED_FILES_INTERVAL_SECONDS),
        ?MODULE, {sync_timer, ?RETRY_FAILED_FILES}),
    ok.


-spec init_traverse_pools() -> ok.
init_traverse_pools() ->
    {_, []} = utils:rpc_multicall(consistent_hashing:get_all_nodes(), ?MODULE, init_traverse_pool, []),
    ok.


-spec init_traverse_pool() -> ok.
init_traverse_pool() ->
    try
        qos_traverse:init_pool()
    catch
        throw:{error, already_exists} -> ok
    end.
