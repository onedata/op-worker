%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(session_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SessId :: session:id(), SessType :: session:type()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SessId, SessType) ->
    supervisor:start_link(?MODULE, [SessId, SessType]).

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
init([SessId, SessType]) ->
    RestartStrategy = one_for_all,
    MaxRestarts = 0,
    RestartTimeWindowSecs = 1,

    {ok, SessId} = session:update(SessId, #{supervisor => self(), node => node()}),

    {ok, {{RestartStrategy, MaxRestarts, RestartTimeWindowSecs}, [
        session_watcher_spec(SessId, SessType),
        sequencer_manager_sup_spec(SessId),
        event_manager_sup_spec(SessId)
    ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a worker child_spec for a session watcher child.
%% @end
%%--------------------------------------------------------------------
-spec session_watcher_spec(SessId :: session:id(), SessType :: session:type()) ->
    supervisor:child_spec().
session_watcher_spec(SessId, SessType) ->
    Id = Module = session_watcher,
    Restart = transient,
    Shutdown = timer:seconds(10),
    Type = worker,
    {Id, {Module, start_link, [SessId, SessType]}, Restart, Shutdown, Type, [Module]}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a supervisor child_spec for a sequencer manager child.
%% @end
%%--------------------------------------------------------------------
-spec sequencer_manager_sup_spec(SessId :: session:id()) ->
    supervisor:child_spec().
sequencer_manager_sup_spec(SessId) ->
    Id = Module = sequencer_manager_sup,
    Restart = transient,
    Shutdown = infinity,
    Type = supervisor,
    {Id, {Module, start_link, [SessId]}, Restart, Shutdown, Type, [Module]}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a supervisor child_spec for a event manager child.
%% @end
%%--------------------------------------------------------------------
-spec event_manager_sup_spec(SessId :: session:id()) ->
    supervisor:child_spec().
event_manager_sup_spec(SessId) ->
    Id = Module = event_manager_sup,
    Restart = transient,
    Shutdown = infinity,
    Type = supervisor,
    {Id, {Module, start_link, [SessId]}, Restart, Shutdown, Type, [Module]}.