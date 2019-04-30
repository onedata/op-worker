%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% worker_plugin used for adding harvest supervision tree to the
%%% main tree.
%%% @end
%%%-------------------------------------------------------------------
-module(harvest_worker).
-author("Jakub Kudzia").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("modules/harvest/harvest.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").


%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([supervisor_flags/0, supervisor_children_spec/0]).


%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    {ok, #{}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(ping | healthcheck | monitor_streams) -> pong | ok.
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;
handle(Request) ->
    ?log_bad_request(Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> ok.
cleanup() ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns supervisor flags for harvest_worker.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_flags() -> supervisor:sup_flags().
supervisor_flags() ->
    #{strategy => one_for_all, intensity => 1000, period => 3600}.

%%--------------------------------------------------------------------
%% @doc
%% Returns supervisor flags for harvest_worker_sup.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_children_spec() -> [supervisor:child_spec()].
supervisor_children_spec() ->
    [harvest_manager_spec(), harvest_stream_sup_spec()].

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec harvest_stream_sup_spec() -> supervisor:child_spec().
harvest_stream_sup_spec() -> #{
    id => ?HARVEST_STREAM_SUP,
    start => {harvest_stream_sup, start_link, []},
    restart => transient,
    shutdown => infinity,
    type => supervisor
}.

-spec harvest_manager_spec() -> supervisor:child_spec().
harvest_manager_spec() -> #{
    id => ?HARVEST_MANAGER,
    start => {harvest_manager, start_link, []},
    restart => transient,
    shutdown => timer:seconds(10),
    type => worker
}.
