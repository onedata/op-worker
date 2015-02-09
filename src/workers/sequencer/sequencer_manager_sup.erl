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
-module(sequencer_manager_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

-include("registered_names.hrl").

%% API
-export([start_link/0, start_sequencer_sup/1, start_sequencer_manager/3]).

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
%% Starts sequencer supervisor supervised by sequencer manager
%% supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_sequencer_sup(SeqManSup :: supervisor:sup_ref()) ->
    supervisor:startchild_ret().
start_sequencer_sup(SeqencerManagerSup) ->
    ChildSpec = sequencer_sup_spec(),
    supervisor:start_child(SeqencerManagerSup, ChildSpec).

%%--------------------------------------------------------------------
%% @doc
%% Starts sequencer manager supervised by sequencer manager
%% supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_sequencer_manager(SeqManSup :: supervisor:sup_ref(),
    SeqSup :: supervisor:sup_ref(), Connection :: pid()) ->
    supervisor:startchild_ret().
start_sequencer_manager(SeqencerManagerSup, SeqSup, Connection) ->
    ChildSpec = sequencer_manager_spec(SeqSup, Connection),
    supervisor:start_child(SeqencerManagerSup, ChildSpec).


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
%% Creates a supervisor child_spec for a sequencer supervisor child.
%% @end
%%--------------------------------------------------------------------
-spec sequencer_sup_spec() -> supervisor:child_spec().
sequencer_sup_spec() ->
    ChildId = Module = sequencer_sup,
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = supervisor,
    {ChildId, {Module, start_link, []}, Restart, ExitTimeout, Type, [Module]}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a supervisor child_spec for a sequencer manager child.
%% @end
%%--------------------------------------------------------------------
-spec sequencer_manager_spec(SeqSup :: supervisor:sup_ref(),
    Connection :: pid()) -> supervisor:child_spec().
sequencer_manager_spec(SeqSup, Connection) ->
    ChildId = Module = sequencer_manager,
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = worker,
    {ChildId, {Module, start_link, [SeqSup, Connection]},
        Restart, ExitTimeout, Type, [Module]}.