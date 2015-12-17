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
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1, start_event_stream/4]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the event stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SessType :: session:type()) ->
    {ok, EvtStmSup :: pid()} | ignore | {error, Reason :: term()}.
start_link(SessType) ->
    supervisor:start_link(?MODULE, [SessType]).

%%--------------------------------------------------------------------
%% @doc
%% Starts the event stream supervised by event stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_event_stream(EvtStmSup :: pid(), EvtMan :: pid(),
    Sub :: event:subscription(), SessId :: session:id()) ->
    supervisor:startchild_ret().
start_event_stream(EvtStmSup, EvtMan, Sub, SessId) ->
    supervisor:start_child(EvtStmSup, [EvtMan, Sub, SessId]).

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
    {ok, {SupFlags :: {
        RestartStrategy :: supervisor:strategy(),
        Intensity :: non_neg_integer(),
        Period :: non_neg_integer()
    }, [ChildSpec :: supervisor:child_spec()]}} | ignore.
init([SessType]) ->
    RestartStrategy = simple_one_for_one,
    Intensity = 3,
    Period = 1,
    {ok, {{RestartStrategy, Intensity, Period}, [
        event_stream_spec(SessType)
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
-spec event_stream_spec(SessType :: session:type()) -> supervisor:child_spec().
event_stream_spec(SessType) ->
    Id = Module = event_stream,
    Restart = transient,
    Shutdown = timer:seconds(10),
    Type = worker,
    {Id, {Module, start_link, [SessType]}, Restart, Shutdown, Type, [Module]}.
