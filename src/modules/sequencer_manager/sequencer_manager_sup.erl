%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements supervisor behaviour and is responsible
%%% for supervising and restarting sequencer managers.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_manager_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

%% API
-export([start_link/1]).

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
-spec start_link(SessId :: session:id()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SessId) ->
    supervisor:start_link(?MODULE, [SessId]).

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
init([SessId]) ->
    RestartStrategy = one_for_all,
    MaxRestarts = 3,
    RestartTimeWindowSecs = 1,
    {ok, {{RestartStrategy, MaxRestarts, RestartTimeWindowSecs}, [
        sequencer_stream_sup_spec(),
        sequencer_manager_spec(self(), SessId)
    ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a supervisor child_spec for a sequencer stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec sequencer_stream_sup_spec() -> supervisor:child_spec().
sequencer_stream_sup_spec() ->
    Id = Module = sequencer_stream_sup,
    Restart = permanent,
    Shutdown = infinity,
    Type = supervisor,
    {Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a worker child_spec for a sequencer manager.
%% @end
%%--------------------------------------------------------------------
-spec sequencer_manager_spec(SeqManSup :: pid(), SessId :: session:id()) ->
    supervisor:child_spec().
sequencer_manager_spec(SeqManSup, SessId) ->
    Id = Module = sequencer_manager,
    Restart = permanent,
    Shutdown = timer:seconds(10),
    Type = worker,
    {Id, {Module, start_link, [SeqManSup, SessId]}, Restart, Shutdown, Type, [Module]}.