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
    {ok, {SupFlags :: supervisor:sup_flags(), [ChildSpec :: supervisor:child_spec()]}}.
init([SessType]) ->
    {ok, {#{strategy => simple_one_for_one, intensity => 3, period => 1}, [
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
    #{
        id => event_stream,
        start => {event_stream, start_link, [SessType]},
        restart => transient,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [event_stream]
    }.
