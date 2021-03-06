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
-export([spec/0, start_link/2]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns child spec for ?SERVER to attach it to supervision.
%% @end
%%-------------------------------------------------------------------
-spec spec() -> supervisor:child_spec().
spec() -> #{
    id => ?MODULE,
    start => {?MODULE, start_link, []},
    restart => temporary,
    shutdown => infinity,
    type => supervisor,
    modules => [?MODULE]
}.

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_link(Sess :: session:id() | session:doc(), SessType :: session:type()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(Sess, SessType) ->
    supervisor:start_link(?MODULE, [Sess, SessType]).

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
% Creation of new session
init([#document{key = SessId, value = Record} = Doc, SessType]) ->
    Self = self(),
    Node = node(),
    case session:create(Doc#document{value = Record#session{supervisor = Self, node = Node}}) of
        {ok, _} ->
            get_flags_and_child_spec(SessId, SessType);
        {error, already_exists} ->
            ignore
    end;
% Recreation of session which supervisor is dead. It is possible after node failure.
% In such a case session document and some connections can exist.
% As a result session elements (supervisor and its children) are recreated on slave node.
init([SessId, SessType]) ->
    Self = self(),
    Node = node(),

    case session_manager:restore_session_on_slave_node(SessId, Self, Node) of
        {ok, _} ->
            get_flags_and_child_spec(SessId, SessType);
        {error, supervisor_alive} ->
            ignore
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_flags_and_child_spec(session:id(), session:type()) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
get_flags_and_child_spec(SessId, SessType) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    {ok, {SupFlags, child_specs(SessId, SessType)}}.

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
        async_request_manager_spec(SessId),
        event_manager_sup_spec(SessId),
        incoming_session_watcher_spec(SessId, provider_incoming)
    ];
child_specs(SessId, provider_outgoing) ->
    [
        sequencer_manager_sup_spec(SessId),
        event_manager_sup_spec(SessId),
        outgoing_connection_manager_spec(SessId)
    ];
child_specs(SessId, fuse) ->
    [
        async_request_manager_spec(SessId),
        sequencer_manager_sup_spec(SessId),
        event_manager_sup_spec(SessId),
        incoming_session_watcher_spec(SessId, fuse)
    ];
child_specs(SessId, SessType) ->
    [
        event_manager_sup_spec(SessId),
        incoming_session_watcher_spec(SessId, SessType)
    ].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a worker child_spec for a incoming session watcher child.
%% @end
%%--------------------------------------------------------------------
-spec incoming_session_watcher_spec(session:id(), session:type()) ->
    supervisor:child_spec().
incoming_session_watcher_spec(SessId, SessType) ->
    #{
        id => incoming_session_watcher,
        start => {incoming_session_watcher, start_link, [SessId, SessType]},
        restart => transient,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [incoming_session_watcher]
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
-spec outgoing_connection_manager_spec(session:id()) ->
    supervisor:child_spec().
outgoing_connection_manager_spec(SessId) ->
    #{
        id => outgoing_connection_manager,
        start => {outgoing_connection_manager, start_link, [SessId]},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [outgoing_connection_manager]
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
