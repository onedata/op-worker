%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements {@link worker_plugin_behaviour} and is responsible
%%% for creating and removing event dispatchers for client sessions.
%%% @end
%%%-------------------------------------------------------------------
-module(event_manager_worker).
-author("Krzysztof Trzepla").

-behaviour(worker_plugin_behaviour).

-include("workers/event_manager/events.hrl").
-include("workers/datastore/datastore_models.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/2, cleanup/0]).

%% API
-export([supervisor_spec/0, supervisor_child_spec/0]).

-define(EVENT_MANAGER_WORKER_SUP, event_manager_worker_sup).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State :: term()} | {error, Reason :: term()}.
init(_Args) ->
    {ok, []}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request, State :: term()) -> Result when
    Request :: ping | healthcheck |
    {emit, Evt :: event_manager:event(), SessionId :: session:session_id()} |
    {subscribe, Sub :: event_manager:subscription()} |
    {unsubscribe, SubId :: event_manager:subscription_id()} |
    {get_or_create_event_dispatcher, SessionId :: session:session_id()} |
    {remove_event_dispatcher, SessionId :: session:session_id()},
    Result :: nagios_handler:healthcheck_reponse() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: term(),
    Reason :: term().
handle(ping, _) ->
    pong;

handle(healthcheck, _) ->
    ok;

handle({emit, Evt, SessionId}, _) ->
    emit(Evt, SessionId);

handle({subscribe, Sub}, _) ->
    subscribe(Sub);

handle({unsubscribe, SubId}, _) ->
    unsubscribe(SubId);

handle({get_or_create_event_dispatcher, SessionId}, _) ->
    get_or_create_event_dispatcher(SessionId);

handle({remove_event_dispatcher, SessionId}, _) ->
    remove_event_dispatcher(SessionId);

handle(_Request, _) ->
    ?log_bad_request(_Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a supervisor spec for a event manager worker supervisor.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_spec() ->
    {RestartStrategy :: supervisor:strategy(), MaxR :: integer(), MaxT :: integer()}.
supervisor_spec() ->
    RestartStrategy = simple_one_for_one,
    MaxR = 3,
    MaxT = timer:minutes(1),
    {RestartStrategy, MaxR, MaxT}.

%%--------------------------------------------------------------------
%% @doc
%% Returns a supervisor child_spec for a event dispatcher supervisor.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_child_spec() -> [supervisor:child_spec()].
supervisor_child_spec() ->
    Id = Module = event_dispatcher_sup,
    Restart = permanent,
    Shutdown = timer:seconds(10),
    Type = supervisor,
    [{Id, {Module, start_link, []}, Restart, Shutdown, Type, [Module]}].

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Emits an event to event manager associated with given session.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: event_manager:event(), SessionId :: session:session_id()) ->
    ok | {error, Reason :: term()}.
emit(Evt, SessionId) ->
    case get_or_create_event_dispatcher(SessionId) of
        {ok, EvtDisp} ->
            gen_server:cast(EvtDisp, #client_message{client_message = Evt});
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates subscription for events.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Sub :: event_manager:subscription()) ->
    {ok, SubId :: event_manager:subscription_id()} | {error, Reason :: term()}.
subscribe(Sub) ->
    SubId = crypto:rand_uniform(0, 16#FFFFFFFFFFFFFFFF),
    NewSub = set_id(SubId, Sub),
    {ok, SubId} = subscription:create(
        #document{key = SubId, value = #subscription{value = NewSub}}
    ),
    {ok, Docs} = event_dispatcher_data:list(),
    lists:foreach(fun(#document{value = #event_dispatcher_data{pid = EvtDisp}}) ->
        ok = gen_server:call(EvtDisp, {add_subscription, NewSub})
    end, Docs),
    {ok, SubId}.

%%--------------------------------------------------------------------
%% @doc
%% Sets ID for event subscription.
%% @end
%%--------------------------------------------------------------------
-spec set_id(SubId :: event_manager:subscription_id(), Sub :: event_manager:subscription()) ->
    NewSub :: event_manager:subscription().
set_id(SubId, #read_event_subscription{} = Sub) ->
    Sub#read_event_subscription{id = SubId};
set_id(SubId, #write_event_subscription{} = Sub) ->
    Sub#write_event_subscription{id = SubId}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes subscription for events.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(SubId :: event_manager:subscription_id()) ->
    ok | {error, Reason :: term()}.
unsubscribe(SubId) ->
    {ok, Docs} = event_dispatcher_data:list(),
    lists:foreach(fun(#document{value = #event_dispatcher_data{pid = EvtDisp}}) ->
        ok = gen_server:call(EvtDisp, {remove_subscription, SubId})
    end, Docs),
    ok = subscription:delete(SubId).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns pid of event dispatcher for client session. If event
%% dispatcher does not exist it is instantiated.
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_event_dispatcher(SessionId :: session:session_id()) ->
    {ok, Pid :: pid()} | {error, Reason :: term()}.
get_or_create_event_dispatcher(SessionId) ->
    case get_event_dispatcher_data(SessionId) of
        {ok, #event_dispatcher_data{pid = EvtDisp}} ->
            {ok, EvtDisp};
        {error, {not_found, _}} ->
            create_event_dispatcher(SessionId);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns model of existing event dispatcher for client session.
%% @end
%%--------------------------------------------------------------------
-spec get_event_dispatcher_data(SessionId :: session:session_id()) ->
    {ok, #event_dispatcher_data{}} | {error, Reason :: term()}.
get_event_dispatcher_data(SessionId) ->
    case event_dispatcher_data:get(SessionId) of
        {ok, #document{value = EvtDispData}} ->
            {ok, EvtDispData};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates event dispatcher for client session.
%% @end
%%--------------------------------------------------------------------
-spec create_event_dispatcher(SessionId :: session:session_id()) ->
    {ok, Pid :: pid()} | {error, Reason :: term()}.
create_event_dispatcher(SessionId) ->
    {ok, EvtDispSup} = start_event_dispatcher_sup(),
    {ok, EvtStmSup} = event_dispatcher_sup:start_event_stream_sup(EvtDispSup),
    case event_dispatcher_sup:start_event_dispatcher(EvtDispSup, EvtStmSup, SessionId) of
        {ok, EvtDisp} ->
            {ok, EvtDisp};
        {error, {already_exists, _}} ->
            ok = stop_event_dispatcher_sup(node(), EvtDispSup),
            get_or_create_event_dispatcher(SessionId);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes event dispatcher for client session.
%% @end
%%--------------------------------------------------------------------
-spec remove_event_dispatcher(SessionId :: session:session_id()) ->
    ok | {error, Reason :: term()}.
remove_event_dispatcher(SessionId) ->
    case get_event_dispatcher_data(SessionId) of
        {ok, #event_dispatcher_data{node = Node, sup = EvtDispSup}} ->
            stop_event_dispatcher_sup(Node, EvtDispSup);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts event dispatcher supervisor supervised by event manager
%% worker supervisor.
%% @end
%%--------------------------------------------------------------------
-spec start_event_dispatcher_sup() -> supervisor:startchild_ret().
start_event_dispatcher_sup() ->
    supervisor:start_child(?EVENT_MANAGER_WORKER_SUP, []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops event dispatcher supervisor and its children.
%% @end
%%--------------------------------------------------------------------
-spec stop_event_dispatcher_sup(Node :: node(), EvtDispSup :: pid()) ->
    ok | {error, Reason :: term()}.
stop_event_dispatcher_sup(Node, EvtDispSup) ->
    supervisor:terminate_child({?EVENT_MANAGER_WORKER_SUP, Node}, EvtDispSup).