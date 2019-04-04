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

-include("modules/datastore/datastore_models.hrl").

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
    {ok, {SupFlags :: supervisor:sup_flags(), [ChildSpec :: supervisor:child_spec()]}}.
init([SessId, SessType]) ->
    Self = self(),
    Node = node(),
    {ok, _} = session:update(SessId, fun(Session = #session{}) ->
        {ok, Session#session{supervisor = Self, node = Node}}
    end),

    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    {ok, {SupFlags, child_specs(SessId, SessType)}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a list of all child_spec necessary for given session type.
%% @end
%%--------------------------------------------------------------------
-spec child_specs(session:id(), session:type()) -> [supervisor:child_spec()].
child_specs(SessId, root) ->
    [event_manager_sup_spec(SessId)];
child_specs(SessId, guest) ->
    [event_manager_sup_spec(SessId)];
child_specs(SessId, provider_incoming) ->
    [
        session_watcher_spec(SessId, provider_incoming),
        async_request_manager_spec(SessId),
        event_manager_sup_spec(SessId)
    ];
child_specs(SessId, provider_outgoing) ->
    [
        sequencer_manager_sup_spec(SessId),
        event_manager_sup_spec(SessId),
        connection_manager_spec(SessId)
    ];
child_specs(SessId, fuse) ->
    [
        session_watcher_spec(SessId, fuse),
        async_request_manager_spec(SessId),
        sequencer_manager_sup_spec(SessId),
        event_manager_sup_spec(SessId)
    ];
child_specs(SessId, SessType) ->
    [
        session_watcher_spec(SessId, SessType),
        event_manager_sup_spec(SessId)
    ].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a worker child_spec for a session watcher child.
%% @end
%%--------------------------------------------------------------------
-spec session_watcher_spec(SessId :: session:id(), SessType :: session:type()) ->
    supervisor:child_spec().
session_watcher_spec(SessId, SessType) ->
    #{
        id => session_watcher,
        start => {session_watcher, start_link, [SessId, SessType]},
        restart => transient,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [session_watcher]
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a supervisor child_spec for a async_request_manager child.
%% @end
%%--------------------------------------------------------------------
-spec async_request_manager_spec(session:id()) -> supervisor:child_spec().
async_request_manager_spec(SessId) ->
    #{
        id => async_request_manager,
        start => {async_request_manager, start_link, [SessId]},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [async_request_manager]
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a supervisor child_spec for a connection_manager child.
%% @end
%%--------------------------------------------------------------------
-spec connection_manager_spec(session:id()) -> supervisor:child_spec().
connection_manager_spec(SessId) ->
    #{
        id => connection_manager,
        start => {connection_manager, start_link, [SessId]},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [connection_manager]
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a supervisor child_spec for a sequencer manager child.
%% @end
%%--------------------------------------------------------------------
-spec sequencer_manager_sup_spec(SessId :: session:id()) ->
    supervisor:child_spec().
sequencer_manager_sup_spec(SessId) ->
    #{
        id => sequencer_manager_sup,
        start => {sequencer_manager_sup, start_link, [SessId]},
        restart => transient,
        shutdown => infinity,
        type => supervisor,
        modules => [sequencer_manager_sup]
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a supervisor child_spec for a event manager child.
%% @end
%%--------------------------------------------------------------------
-spec event_manager_sup_spec(SessId :: session:id()) ->
    supervisor:child_spec().
event_manager_sup_spec(SessId) ->
    #{
        id => event_manager_sup,
        start => {event_manager_sup, start_link, [SessId]},
        restart => transient,
        shutdown => infinity,
        type => supervisor,
        modules => [event_manager_sup]
    }.
