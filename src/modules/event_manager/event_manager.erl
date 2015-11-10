%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for dispatching events to event streams.
%%% @end
%%%-------------------------------------------------------------------
-module(event_manager).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("cluster/worker/modules/datastore/datastore.hrl").
-include("modules/event_manager/events.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2, emit/2, subscribe/1, subscribe/2, unsubscribe/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export_type([event/0, subscription/0, subscription_id/0, producer/0]).

-type event() :: #read_event{} | #write_event{}.
-type subscription() :: #read_event_subscription{} |
                        #write_event_subscription{}.
-type subscription_id() :: non_neg_integer().
-type producer() :: gui | all.
-type event_stream_status() :: {running, EvtStm :: pid(),
                                AdmRule :: event_stream:admission_rule()} | {terminated, EvtStmState :: term(),
                                                                             AddRule :: event_stream:admission_rule(),
                                                                             PendingMsgs :: [{event, event()} | terminate]}.

%% event manager state:
%% session_id       - ID of session associated with event manager
%% event_stream_sup - pid of event stream supervisor
%% event_streams    - mapping from subscription ID to event stream status
-record(state, {
          session_id :: session:id(),
          event_stream_sup :: pid(),
          event_streams = [] :: [{SubId :: subscription_id(), event_stream_status()}]
         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(EvtManSup :: pid(), SessId :: session:id()) ->
                        {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(EvtManSup, SessId) ->
    gen_server:start_link(?MODULE, [EvtManSup, SessId], []).

%%--------------------------------------------------------------------
%% @doc
%% Emits an event on behalf of client's session.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: event(), SessId :: session:id()) ->
                  ok | {error, Reason :: event_manager_not_found | term()}.
emit(Evt, SessId) ->
    case session:get_event_manager(SessId) of
        {ok, EvtMan} ->
            gen_server:cast(EvtMan, #client_message{message_body = Evt});
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates subscription for events.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Sub :: subscription()) -> {ok, SubId :: subscription_id()}.
subscribe(Sub) ->
    SubId = crypto:rand_uniform(0, 16#FFFFFFFFFFFFFFFF),
    case subscribe(SubId, Sub) of
        ok -> {ok, SubId};
        {error, already_exists} -> subscribe(Sub);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Cancels subscription for events.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(Sub :: subscription_id()) -> ok.
unsubscribe(SubId) ->
    case subscription:delete(SubId) of
        ok ->
            case session:list() of
                {ok, Docs} ->
                    lists:foreach(fun
                                      (#document{value = #session{event_manager = EvtMan}}) ->
                                         gen_server:cast(EvtMan, {unsubscribe, SubId})
                                 end, Docs);
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} -> {error, Reason}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
                  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
                  {stop, Reason :: term()} | ignore.
init([EvtManSup, SessId]) ->
    process_flag(trap_exit, true),
    {ok, SessId} = session:update(SessId, #{event_manager => self()}),
    gen_server:cast(self(), {initialize, EvtManSup}),
    {ok, #state{session_id = SessId}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
                  State :: #state{}) ->
                         {reply, Reply :: term(), NewState :: #state{}} |
                         {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
                         {noreply, NewState :: #state{}} |
                         {noreply, NewState :: #state{}, timeout() | hibernate} |
                         {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
                         {stop, Reason :: term(), NewState :: #state{}}.
handle_call({event_stream_initialized, SubId}, {EvtStm, _}, #state{
                                                               event_streams = EvtStms} = State) ->
    case lists:keyfind(SubId, 1, EvtStms) of
        {SubId, {terminated, EvtStmState, AdmRule, PendingMsgs}} ->
            lists:foreach(fun(Msg) ->
                                  gen_server:cast(EvtStm, Msg)
                          end, lists:reverse(PendingMsgs)),
            {reply, {ok, EvtStmState}, State#state{event_streams =
                                                       lists:keyreplace(SubId, 1, EvtStms, {SubId, {running, EvtStm, AdmRule}})
                                                  }};
        _ ->
            {reply, undefined, State}
    end;

handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #state{}) ->
                         {noreply, NewState :: #state{}} |
                         {noreply, NewState :: #state{}, timeout() | hibernate} |
                         {stop, Reason :: term(), NewState :: #state{}}.
handle_cast({initialize, EvtManSup}, #state{session_id = SessId} = State) ->
    {ok, EvtStmSup} = get_event_stream_sup(EvtManSup),
    {ok, EvtStms} = create_event_streams(EvtStmSup, SessId),
    {noreply, State#state{event_stream_sup = EvtStmSup, event_streams = EvtStms}};

handle_cast({event_stream_terminated, SubId, shutdown, _},
            #state{event_streams = EvtStms} = State) ->
    {noreply, State#state{event_streams = lists:keydelete(SubId, 1, EvtStms)}};

handle_cast({event_stream_terminated, SubId, _, EvtStmState},
            #state{event_streams = EvtStms} = State) ->
    {SubId, {running, _, AdmRule}} = lists:keyfind(SubId, 1, EvtStms),
    {noreply, State#state{event_streams = lists:keyreplace(SubId, 1, EvtStms,
                                                           {SubId, {terminated, EvtStmState, AdmRule, []}})
                         }};

handle_cast({subscribe, Sub}, #state{event_stream_sup = EvtStmSup,
                                     session_id = SessId, event_streams = EvtStms} = State) ->
    ?debug("handle_cast({subscribe, ~p} for session ~p", [Sub, SessId]),
    {ok, EvtStm} = create_event_stream(EvtStmSup, SessId, Sub),
    {noreply, State#state{event_streams = [EvtStm | EvtStms]}};

handle_cast({unsubscribe, SubId}, #state{event_stream_sup = EvtStmSup,
                                         event_streams = EvtStms, session_id = SessId} = State) ->
    {ok, NewEvtStms} = remove_event_stream(EvtStmSup, SessId, SubId, EvtStms),
    {noreply, State#state{event_streams = NewEvtStms}};

handle_cast(#client_message{message_body = #end_of_message_stream{}}, State) ->
    {stop, shutdown, State};

handle_cast(#client_message{message_body = Evt}, #state{
                                                    event_streams = EvtStms, session_id = SessId} = State) ->
    EnrichedEvt = source_enricher({session, SessId}, Evt),
    NewEvtStms = lists:map(fun
                               ({_, {running, Pid, AdmRule}} = EvtStm) ->
                                  ?debug("cast event ~p, AdmRule: ~p", [EnrichedEvt, AdmRule(EnrichedEvt)]),
                                  case AdmRule(EnrichedEvt) of
                                      true -> gen_server:cast(Pid, {event, EnrichedEvt}), EvtStm;
                                      false -> EvtStm
                                  end;
                               ({_, {terminated, EvtStmState, AdmRule, PendingMsgs}} = EvtStm) ->
                                  case AdmRule(EnrichedEvt) of
                                      true ->
                                          {terminated, EvtStmState, AdmRule, [{event, EnrichedEvt} | PendingMsgs]};
                                      false ->
                                          EvtStm
                                  end
                          end, EvtStms),
    {noreply, State#state{event_streams = NewEvtStms}};

handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: #state{}) ->
                         {noreply, NewState :: #state{}} |
                         {noreply, NewState :: #state{}, timeout() | hibernate} |
                         {stop, Reason :: term(), NewState :: #state{}}.
handle_info(_Info, State) ->
    ?log_bad_request(_Info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
                State :: #state{}) -> term().
terminate(Reason, #state{session_id = SessId} = State) ->
    ?log_terminate(Reason, State),
    session:update(SessId, #{event_manager => undefined}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
                  Extra :: term()) -> {ok, NewState :: #state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns event stream supervisor associated with event manager.
%% @end
%%--------------------------------------------------------------------
-spec get_event_stream_sup(EvtManSup :: pid()) ->
                                  {ok, EvtStmSup :: pid()} | {error, not_found}.
get_event_stream_sup(EvtManSup) ->
    Id = event_stream_sup,
    Children = supervisor:which_children(EvtManSup),
    case lists:keyfind(Id, 1, Children) of
        {Id, EvtStmSup, _, _} when is_pid(EvtStmSup) -> {ok, EvtStmSup};
        _ -> {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates event subscription with given ID.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(SubId :: subscription_id(), Sub :: subscription()) ->
                       ok | {error, Reason :: term()}.
subscribe(SubId, Sub) ->
    NewSub = set_id(SubId, Sub),
    case subscription:create(#document{key = SubId,
                                       value = #subscription{value = NewSub}}) of
        {ok, SubId} ->
            case session:list() of
                {ok, Docs} ->
                    lists:foreach(fun
                                      (#document{value = #session{event_manager = EvtMan}}) ->
                                         gen_server:cast(EvtMan, {subscribe, NewSub})
                                 end, Docs);
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Set subscription's ID.
%% @end
%%--------------------------------------------------------------------
-spec set_id(SubId :: subscription_id(), Sub :: subscription()) ->
                    NewSub :: subscription().
set_id(SubId, #read_event_subscription{} = Sub) ->
    Sub#read_event_subscription{id = SubId};
set_id(SubId, #write_event_subscription{} = Sub) ->
    Sub#write_event_subscription{id = SubId}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates event streams for all current subscriptions.
%% @end
%%--------------------------------------------------------------------
-spec create_event_streams(EvtStmSup :: pid(), SessId :: session:id()) ->
                                  {ok, [{SubId :: subscription_id(), {running, EvtStm :: pid(),
                                                                      AddRule :: event_stream:admission_rule()}}]}.
create_event_streams(EvtStmSup, SessId) ->
    {ok, Docs} = subscription:list(),
    EvtStms = lists:map(fun(#document{value = #subscription{value = Sub}}) ->
                                {ok, EvtStm} = create_event_stream(EvtStmSup, SessId, Sub),
                                EvtStm
                        end, Docs),
    {ok, EvtStms}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates event stream for subscription.
%% @see create_event_stream/7
%% @end
%%--------------------------------------------------------------------
-spec create_event_stream(EvtStmSup :: pid(),
                          SessId :: session:id(), Sub :: subscription()) ->
                                 {ok, {SubId :: subscription_id(), event_stream_status()}}.
create_event_stream(EvtStmSup, SessId, #read_event_subscription{id = SubId,
                                                                producer = Prod, event_stream = #event_stream{admission_rule =
                                                                                                                  AdmRule} = EvtStmSpec} = Sub) ->
    create_event_stream(EvtStmSup, SessId, SubId, Prod, AdmRule, EvtStmSpec, Sub);
create_event_stream(EvtStmSup, SessId, #write_event_subscription{id = SubId,
                                                                 producer = Prod, event_stream = #event_stream{admission_rule =
                                                                                                                   AdmRule} = EvtStmSpec} = Sub) ->
    create_event_stream(EvtStmSup, SessId, SubId, Prod, AdmRule, EvtStmSpec, Sub).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates event stream for subscription.
%% @end
%%--------------------------------------------------------------------
-spec create_event_stream(EvtStmSup :: pid(), SessId :: session:id(),
                          SubId :: subscription_id(), Prod :: producer(),
                          AdmRule :: event_stream:admission_rule(),
                          EvtStmSpec :: event_stream:event_stream(), Sub :: subscription()) ->
                                 {ok, {SubId :: subscription_id(), event_stream_status()}}.
create_event_stream(EvtStmSup, SessId, SubId, Prod, AdmRule, EvtStmSpec, Sub) ->
    case Prod of
        gui -> ok;
        _ -> ok = communicator:send(Sub, SessId)
    end,
    {ok, EvtStm} = event_stream_sup:start_event_stream(EvtStmSup, self(), SubId,
                                                       EvtStmSpec),
    {ok, {SubId, {running, EvtStm, AdmRule}}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes event stream associated with subscription.
%% @end
%%--------------------------------------------------------------------
-spec remove_event_stream(EvtStmSup :: pid(), SessId :: session:id(),
                          SubId :: subscription_id(), EvtStms :: [event_stream_status()]) ->
                                 {ok, NewEvtStms :: [event_stream_status()]}.
remove_event_stream(EvtStmSup, SessId, SubId, EvtStms) ->
    case lists:keyfind(SubId, 1, EvtStms) of
        {SubId, {running, EvtStm, _}} ->
            event_stream_sup:stop_event_stream(EvtStmSup, EvtStm),
            ok = communicator:send(#event_subscription_cancellation{id = SubId}, SessId),
            {ok, lists:keydelete(SubId, 1, EvtStms)};
        {SubId, {terminated, EvtStmState, AdmRule, PendingMsgs}} ->
            ok = communicator:send(#event_subscription_cancellation{id = SubId}, SessId),
            {ok, lists:keyreplace(SubId, 1, EvtStms, {SubId, {terminated,
                                                              EvtStmState, AdmRule, [terminate | PendingMsgs]}})};
        _ ->
            {ok, EvtStms}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Attaches source to given event if supported.
%% @end
%%--------------------------------------------------------------------
-spec source_enricher({session, session:id()}, term()) -> term().
source_enricher(Source, #write_event{} = Evt) ->
    Evt#write_event{source = Source};
source_enricher(_, Evt) ->
    Evt.