%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Worker_plugin used only for adding rtransfer_server to supervision
%%% tree.
%%% @end
%%%-------------------------------------------------------------------
-module(rtransfer_worker).
-author("Jakub Kudzia").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

-define(REFRESH_DISABLED_SPACES_INTERVAL, timer:seconds(1)).

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
    schedule_refresh_disabled_spaces(),
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
handle(refresh_disabled_spaces) ->
    case space_quota:get_disabled_spaces() of
        {ok, BlockedSpaces} ->
            {_, BadNodes} = rpc:multicall(consistent_hashing:get_all_nodes(),
                rtransfer_link_quota_manager,
                update_disabled_spaces,
                [BlockedSpaces]),
            BadNodes =/= [] andalso
                ?error("Failed to update disabled spaces on nodes ~p", [BadNodes]);
        {error, _} = Error ->
            ?debug("Cannot update disabled spaces due to ~p", [Error])
    end,
    schedule_refresh_disabled_spaces();
handle(Request) ->
    ?log_bad_request(Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> ok | {error, timeout | term()}.
cleanup() ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a rtransfer_worker supervisor flags.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_flags() -> supervisor:sup_flags().
supervisor_flags() ->
    #{strategy => one_for_all, intensity => 1000, period => 3600}.

%%--------------------------------------------------------------------
%% @doc
%% Returns a rtransfer_worker supervisor children_spec()
%% @end
%%--------------------------------------------------------------------
-spec supervisor_children_spec() -> [supervisor:child_spec()].
supervisor_children_spec() ->
    [#{
        id => rtransfer,
        start => {rtransfer_config, start_rtransfer, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor
    }].

%%%===================================================================
%%% Internal functions
%%%===================================================================

schedule_refresh_disabled_spaces() ->
    erlang:send_after(?REFRESH_DISABLED_SPACES_INTERVAL, self(),
                      {sync_timer, refresh_disabled_spaces}).
