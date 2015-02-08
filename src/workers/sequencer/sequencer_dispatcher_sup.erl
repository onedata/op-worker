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
-module(sequencer_dispatcher_sup).
-author("Krzysztof Trzepla").

-behaviour(supervisor).

-include("registered_names.hrl").

%% API
-export([start_link/0, start_sequencer_manager_sup/0, stop_sequencer_manager_sup/2]).

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
    supervisor:start_link(
        {local, ?SEQUENCER_DISPATCHER_SUPERVISOR_NAME}, ?MODULE, []).

%%--------------------------------------------------------------------
%% @doc
%% Starts sequencer manager supervisor supervised by sequencer dispatcher
%% supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_sequencer_manager_sup() -> supervisor:startchild_ret().
start_sequencer_manager_sup() ->
    supervisor:start_child(?SEQUENCER_DISPATCHER_SUPERVISOR_NAME, []).

%%--------------------------------------------------------------------
%% @doc
%% Stops sequencer manager supervisor and its children.
%% @end
%%--------------------------------------------------------------------
-spec stop_sequencer_manager_sup(Node :: node(), Pid :: pid()) ->
    ok | {error, Reason :: term()}.
stop_sequencer_manager_sup(Node, Pid) ->
    supervisor:terminate_child({?SEQUENCER_DISPATCHER_SUPERVISOR_NAME, Node}, Pid).

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
    {ok, {{RestartStrategy, MaxR, MaxT}, [sequencer_manager_sup_spec()]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a supervisor child_spec for a sequencer manager supervisor child.
%% @end
%%--------------------------------------------------------------------
-spec sequencer_manager_sup_spec() -> supervisor:child_spec().
sequencer_manager_sup_spec() ->
    ChildId = Module = sequencer_manager_sup,
    Restart = permanent,
    ExitTimeout = timer:seconds(10),
    Type = supervisor,
    {ChildId, {Module, start_link, []}, Restart, ExitTimeout, Type, [Module]}.