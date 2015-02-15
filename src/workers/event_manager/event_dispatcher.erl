%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for dispatching events associated with given session to event streams.
%%% @end
%%%-------------------------------------------------------------------
-module(event_dispatcher).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("workers/event_manager/events.hrl").
-include("workers/datastore/datastore_models.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% event dispatcher state:
%% session_id  - ID of session associated with event dispatcher
%% evt_stm_sup - pid of event stream supervisor
%% evt_stms    - mapping from subscription ID to event stream
-record(state, {
    session_id :: session:session_id(),
    evt_stm_sup :: pid(),
    evt_stms = [] :: [{
        SubId :: event_manager:subscription_id(),
        {pid, Pid :: pid(), EvtStmSpec :: event_stream:event_stream()}
    } | {
        SubId :: event_manager:subscription_id(),
        {state, EvtStmState :: term(), EvtStmSpec :: event_stream:event_stream(),
            PendingEvt :: [event_manager:event()]}
    }]
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(EvtDispSup :: pid(), EvtStmSup :: pid(), SessionId :: session:session_id()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(EvtDispSup, EvtStmSup, SessionId) ->
    gen_server:start_link(?MODULE, [EvtDispSup, EvtStmSup, SessionId], []).

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
init([EvtDispSup, EvtStmSup, SessionId]) ->
    process_flag(trap_exit, true),
    case event_dispatcher_data:create(#document{key = SessionId,
        value = #event_dispatcher_data{node = node(), pid = self(), sup = EvtDispSup}
    }) of
        {ok, SessionId} ->
            {ok, Docs} = subscription:list(),
            EvtStms = lists:map(fun(#document{value = #subscription{value = Sub}}) ->
                create_event_stream(EvtStmSup, Sub)
            end, Docs),
            {ok, #state{evt_stms = EvtStms, evt_stm_sup = EvtStmSup, session_id = SessionId}};
        {error, Reason} ->
            {stop, Reason}
    end.

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
handle_call({event_stream_initialized, SubId}, {EvtStm, _}, #state{evt_stms = EvtStms} = State) ->
    case lists:keyfind(SubId, 1, EvtStms) of
        {SubId, {state, EvtStmState, EvtStmSpec, PendingEvts}} ->
            lists:foreach(fun(Evt) ->
                gen_server:cast(EvtStm, {event, Evt})
            end, lists:reverse(PendingEvts)),
            {reply, {ok, EvtStmState}, State#state{
                evt_stms = lists:keyreplace(SubId, 1, EvtStms, {SubId, {pid, EvtStm, EvtStmSpec}})
            }};
        _ ->
            {reply, undefined, State}
    end;

handle_call({add_subscription, Sub}, _From,
    #state{evt_stm_sup = EvtStmSup, evt_stms = EvtStms} = State) ->
    {reply, ok, State#state{evt_stms = [
        create_event_stream(EvtStmSup, Sub) | EvtStms
    ]}};

handle_call({remove_subscription, SubId}, _From,
    #state{evt_stm_sup = EvtStmSup, evt_stms = EvtStms} = State) ->
    case lists:keyfind(SubId, 1, EvtStms) of
        {SubId, {pid, EvtStm, _}} ->
            event_stream_sup:stop_event_stream(EvtStmSup, EvtStm),
            {reply, ok, State#state{evt_stms = lists:keydelete(SubId, 1, EvtStms)}};
        _ ->
            {reply, ok, State}
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
handle_cast({event_stream_terminated, SubId, shutdown, _}, #state{evt_stms = EvtStms} = State) ->
    {noreply, State#state{evt_stms = lists:keydelete(SubId, 1, EvtStms)}};

handle_cast({event_stream_terminated, SubId, _, EvtStmState}, #state{evt_stms = EvtStms} = State) ->
    {SubId, {pid, _, EvtStmSpec}} = lists:keyfind(SubId, 1, EvtStms),
    {noreply, State#state{
        evt_stms = lists:keyreplace(SubId, 1, EvtStms, {SubId, {state, EvtStmState, EvtStmSpec, []}})
    }};

handle_cast(#client_message{client_message = Evt}, #state{evt_stms = EvtStms} = State) ->
    NewEvtStms = lists:map(fun
        ({_, {pid, Pid, EvtStmSpec}} = EvtStm) ->
            case apply_admission_rule(Evt, EvtStmSpec) of
                true -> gen_server:cast(Pid, {event, Evt}), EvtStm;
                false -> EvtStm
            end;
        ({_, {state, EvtStmState, EvtStmSpec, PendingEvts}} = EvtStm) ->
            case apply_admission_rule(Evt, EvtStmSpec) of
                true -> {state, EvtStmState, EvtStmSpec, [Evt | PendingEvts]};
                false -> EvtStm
            end
    end, EvtStms),
    {noreply, State#state{evt_stms = NewEvtStms}};

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
terminate(Reason, #state{session_id = SessionId} = State) ->
    ?warning("Event dispatcher terminated in state ~p due to: ~p", [State, Reason]),
    event_dispatcher_data:delete(SessionId).

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
%% Applies event stream admission rule on an event.
%% @end
%%--------------------------------------------------------------------
-spec apply_admission_rule(Evt :: event_manager:event(),
    EvtStm :: event_stream:event_stream()) -> true | false.
apply_admission_rule(Evt, #event_stream{admission_rule = AdmRule}) ->
    AdmRule(Evt).

%%--------------------------------------------------------------------
%% @doc
%% Creates event stream for given subscription.
%% @end
%%--------------------------------------------------------------------
-spec create_event_stream(EvtStmSup :: pid(), Sub :: event_manager:subscription()) ->
    {SubId :: event_manager:subscription_id(),
        {pid, EvtStm :: pid(), EvtStmSpec :: event_stream:event_stream()}}.
create_event_stream(EvtStmSup, #read_event_subscription{id = SubId,
    event_stream_spec = EvtStmSpec} = Sub) ->
    ok = send_subscription(Sub),
    {ok, EvtStm} = event_stream_sup:start_event_stream(EvtStmSup, self(), SubId, EvtStmSpec),
    {SubId, {pid, EvtStm, EvtStmSpec}};
create_event_stream(EvtStmSup, #write_event_subscription{id = SubId,
    event_stream_spec = EvtStmSpec} = Sub) ->
    ok = send_subscription(Sub),
    {ok, EvtStm} = event_stream_sup:start_event_stream(EvtStmSup, self(), SubId, EvtStmSpec),
    {SubId, {pid, EvtStm, EvtStmSpec}}.

%%--------------------------------------------------------------------
%% @doc
%% Sends subscription to the client.
%% @end
%%--------------------------------------------------------------------
-spec send_subscription(Sub :: event_manager:subscription()) -> ok.
send_subscription(#read_event_subscription{producer = fuse}) ->
    ok;
send_subscription(#write_event_subscription{producer = fuse}) ->
    ok;
send_subscription(_) ->
    ok.
