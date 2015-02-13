%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements supervisor behaviour and is responsible
%%% for supervising and restarting event managers.
%%% @end
%%%-------------------------------------------------------------------
-module(event_dispatcher_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

%% API
-export([start_link/0, start_event_stream_sup/1, start_event_dispatcher/3]).

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
%% Starts event stream supervisor supervised by event manager
%% supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_event_stream_sup(EvtDispSup :: pid()) ->
    supervisor:startchild_ret().
start_event_stream_sup(EvtDispSup) ->
    ChildSpec = event_stream_sup_spec(),
    supervisor:start_child(EvtDispSup, ChildSpec).

%%--------------------------------------------------------------------
%% @doc
%% Starts event dispatcher supervised by event dispatcher
%% supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_event_dispatcher(EvtDispSup :: pid(), EvtStmSup :: pid(),
    SessionId :: session:session_id()) -> supervisor:startchild_ret().
start_event_dispatcher(EvtDispSup, EvtStmSup, SessionId) ->
    ChildSpec = event_dispatcher_spec(EvtDispSup, EvtStmSup, SessionId),
    supervisor:start_child(EvtDispSup, ChildSpec).

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
    RestartStrategy = one_for_all,
    MaxR = 3,
    MaxT = timer:minutes(1),
    {ok, {{RestartStrategy, MaxR, MaxT}, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a supervisor child_spec for a event stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec event_stream_sup_spec() -> supervisor:child_spec().
event_stream_sup_spec() ->
    Id = Module = event_stream_sup,
    Restart = permanent,
    Shutdown = timer:seconds(10),
    Type = supervisor,
    {Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a supervisor child_spec for a event dispatcher.
%% @end
%%--------------------------------------------------------------------
-spec event_dispatcher_spec(EvtDispSup :: pid(), EvtStmSup :: pid(),
    SessionId :: session:session_id()) -> supervisor:child_spec().
event_dispatcher_spec(EvtDispSup, EvtStmSup, SessionId) ->
    Id = Module = event_dispatcher,
    Restart = permanent,
    Shutdown = timer:seconds(10),
    Type = worker,
    {Id, {Module, start_link, [EvtDispSup, EvtStmSup, SessionId]},
        Restart, Shutdown, Type, [Module]}.