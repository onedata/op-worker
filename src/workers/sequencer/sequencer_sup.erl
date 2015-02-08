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
-module(sequencer_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

-include("registered_names.hrl").

%% API
-export([start_link/0, start_sequencer/3]).

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
%% Starts sequencer manager supervisor supervised by sequencer dispatcher
%% supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_sequencer(SeqSup :: supervisor:sup_ref(),
    SeqMan :: pid(), MsgId :: integer()) ->
    supervisor:startchild_ret().
start_sequencer(SeqSup, SeqMan, MsgId) ->
    supervisor:start_child(SeqSup, [SeqMan, MsgId]).

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
    ignore |
    {error, Reason :: term()}.
init([]) ->
    RestartStrategy = simple_one_for_one,
    MaxR = 3,
    MaxT = timer:minutes(1),
    {ok, {{RestartStrategy, MaxR, MaxT}, [sequencer_spec()]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a supervisor child_spec for a sequencer manager supervisor child.
%% @end
%%--------------------------------------------------------------------
-spec sequencer_spec() -> supervisor:child_spec().
sequencer_spec() ->
    ChildId = Module = sequencer,
    Restart = transient,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ChildId, {Module, start_link, []}, Restart, ExitTimeout, Type, [Module]}.