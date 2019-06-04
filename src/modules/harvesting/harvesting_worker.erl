%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% worker_plugin used for adding harvesting supervision tree to the
%%% main tree.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_worker).
-author("Jakub Kudzia").

-behaviour(worker_plugin_behaviour).

-include("modules/harvesting/harvesting.hrl").
-include_lib("ctool/include/logging.hrl").

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
%% Returns supervisor flags for harvesting_worker.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_flags() -> supervisor:sup_flags().
supervisor_flags() ->
    #{strategy => one_for_all, intensity => 1000, period => 3600}.

%%--------------------------------------------------------------------
%% @doc
%% Returns supervisor flags for harvesting_worker_sup.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_children_spec() -> [supervisor:child_spec()].
supervisor_children_spec() ->
    [harvesting_stream_sup_spec()].

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec harvesting_stream_sup_spec() -> supervisor:child_spec().
harvesting_stream_sup_spec() -> #{
    id => ?HARVESTING_STREAM_SUP,
    start => {harvesting_stream_sup, start_link, []},
    restart => transient,
    shutdown => infinity,
    type => supervisor
}.