%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements supervisor behaviour and is responsible
%%% for supervising and restarting sequencer streams.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_stream_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

%% API
-export([start_link/0, start_sequencer_stream/4]).

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
%% Starts sequencer stream supervised by sequencer stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_sequencer_stream(SeqStmSup :: pid(), SeqMan :: pid(),
    SessId :: session:id(), StmId :: non_neg_integer()) ->
    supervisor:startchild_ret().
start_sequencer_stream(SeqStmSup, SeqMan, SessId, StmId) ->
    supervisor:start_child(SeqStmSup, [SeqMan, SessId, StmId]).

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
        sequencer_stream_spec()
    ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a supervisor child_spec for a sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec sequencer_stream_spec() -> supervisor:child_spec().
sequencer_stream_spec() ->
    Id = Module = sequencer_stream,
    Restart = transient,
    Shutdown = timer:seconds(10),
    Type = worker,
    {Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}.