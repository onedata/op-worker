%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements supervisor behaviour and is responsible
%%% for supervising and restarting event streams.
%%% @end
%%%-------------------------------------------------------------------
-module(event_stream_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

%% API
-export([start_link/0, start_event_stream/4, stop_event_stream/2]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    supervisor:start_link(?MODULE, []).

%%--------------------------------------------------------------------
%% @doc
%% Starts event stream supervised by event stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_event_stream(EvtStmSup :: pid(), EvtMan :: pid(),
    SubId :: event_manager:subscription_id(),
    EvtStmSpec :: event_stream:event_stream()) -> ok | {error, Reason :: term()}.
start_event_stream(EvtStmSup, EvtMan, SubId, EvtStmSpec) ->
    supervisor:start_child(EvtStmSup, [EvtMan, SubId, EvtStmSpec]).

%%--------------------------------------------------------------------
%% @doc
%% Stops event stream supervised by event stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec stop_event_stream(EvtStmSup :: pid(), EvtStm :: pid()) ->
    ok | {error, Reason :: term()}.
stop_event_stream(EvtStmSup, EvtStm) ->
    supervisor:terminate_child(EvtStmSup, EvtStm).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }} |
    ignore.
init([]) ->
    RestartStrategy = simple_one_for_one,
    MaxRestarts = 3,
    RestartTimeWindowSecs = 1,
    {ok, {{RestartStrategy, MaxRestarts, RestartTimeWindowSecs}, [
        event_stream_spec()
    ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a supervisor child_spec for a event stream.
%% @end
%%--------------------------------------------------------------------
-spec event_stream_spec() -> supervisor:child_spec().
event_stream_spec() ->
    Id = Module = event_stream,
    Restart = transient,
    Shutdown = timer:seconds(10),
    Type = worker,
    {Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}.