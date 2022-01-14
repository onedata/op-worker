%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Worker responsible for initialization and termination of
%%% files_tree_gatherer_pes_executor.
%%% @end
%%%-------------------------------------------------------------------
-module(files_tree_gatherer_worker).
-author("Michal Wrzeszcz").


-behaviour(worker_plugin_behaviour).


-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").


%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).


%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    pes:start_link(files_tree_gatherer_pes_executor),
    {ok, #{}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(ping | healthcheck) -> pong | ok.
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
-spec cleanup() -> ok | {error, timeout | term()}.
cleanup() ->
    pes:stop(files_tree_gatherer_pes_executor),
    ok.