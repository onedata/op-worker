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
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1, start_stream/4]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the event stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, StmsSup :: pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    supervisor:start_link(?MODULE, []).

%%--------------------------------------------------------------------
%% @doc
%% Starts the event stream supervised by event stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_stream(StmsSup :: pid(), Mgr :: pid(), Sub :: event:subscription(),
    SessId :: session:id()) -> supervisor:startchild_ret().
start_stream(StmsSup, Mgr, Sub, SessId) ->
    supervisor:start_child(StmsSup, [Mgr, Sub, SessId]).

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
init([]) ->
    {ok, {#{strategy => simple_one_for_one, intensity => 3, period => 1}, [
        stream_spec()
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
-spec stream_spec() -> supervisor:child_spec().
stream_spec() ->
    #{
        id => event_stream,
        start => {event_stream, start_link, []},
        restart => transient,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [event_stream]
    }.
