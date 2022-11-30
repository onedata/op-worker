%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handle graceful atm workflow executions shutdown when stopping
%%% Oneprovider.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_worker).
-author("Bartosz Walkowicz").

-behaviour(worker_plugin_behaviour).

-include_lib("ctool/include/logging.hrl").

%% API
-export([supervisor_flags/0, supervisor_children_spec/0]).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec supervisor_flags() -> supervisor:sup_flags().
supervisor_flags() ->
    #{strategy => one_for_one, intensity => 1000, period => 3600}.


-spec supervisor_children_spec() -> [supervisor:child_spec()].
supervisor_children_spec() ->
    [].


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
-spec handle(ping | healthcheck | monitor_streams) ->
    pong | ok | {ok, term()} | errors:error().
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
    % @TODO VFS-9846 Implement graceful atm workflow execution shutdown when stopping op
    ok.
