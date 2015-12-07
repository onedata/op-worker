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
-export([start_link/1, start_sequencer_stream/4]).

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
-spec start_link(Child :: atom()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(Child) ->
    supervisor:start_link(?MODULE, [Child]).

%%--------------------------------------------------------------------
%% @doc
%% Starts sequencer stream supervised by sequencer stream supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_sequencer_stream(SeqStmSup :: pid(), SeqMan :: pid(),
    StmId :: sequencer:stream_id(), SessId :: session:id()) ->
    supervisor:startchild_ret().
start_sequencer_stream(SeqStmSup, SeqMan, StmId, SessId) ->
    supervisor:start_child(SeqStmSup, [SeqMan, StmId, SessId]).

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
init([Child]) ->
    RestartStrategy = simple_one_for_one,
    Intensity = 3,
    Period = 1,
    {ok, {{RestartStrategy, Intensity, Period}, [
        sequencer_stream_spec(Child)
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
-spec sequencer_stream_spec(Module :: atom()) -> supervisor:child_spec().
sequencer_stream_spec(Module) ->
    Id = Module,
    Restart = case Module of
                  sequencer_in_stream -> temporary;
                  sequencer_out_stream -> transient
              end,
    Shutdown = timer:seconds(10),
    Type = worker,
    {Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}.