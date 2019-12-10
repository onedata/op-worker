%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is a gen_server that allows to define generic
%%% "countdown counters" used in tests.
%%% One server should be started per tested node.
%%% One server can contain many counters.
%%% For each counter, server saves additional list which can be used
%%% to store counted objects (i. e. GUIDs of created files).
%%% @end
%%%-------------------------------------------------------------------
-module(countdown_server).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("countdown_server.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2, init_counter/2, decrease/2, decrease/3, decrease_by_value/3, await/3, await_many/3, stop/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(counter, {
    value = 0 :: non_neg_integer(),
    data = [] :: [term()]
}).

-record(state, {
    node :: node(),
    parent :: pid(),
    counters = #{} :: #{reference() => #counter{}}
}).

-type counter() :: #counter{}.
-type data() :: term().

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec(start_link(pid(), node()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Parent, Node) ->
    gen_server:start_link({local, ?COUNTDOWN_SERVER(Node)}, ?MODULE, [Parent, Node], []).

-spec stop(node()) -> ok.
stop(Node) ->
    gen_server:stop(?COUNTDOWN_SERVER(Node)).

%%-------------------------------------------------------------------
%% @doc
%% Initializes new counter with given InitialValue.
%% Returns counter reference.
%% @end
%%-------------------------------------------------------------------
-spec init_counter(node(), non_neg_integer()) -> reference().
init_counter(Node, InitialValue) ->
    gen_server:call(?COUNTDOWN_SERVER(Node), {init, InitialValue}).

%%-------------------------------------------------------------------
%% @doc
%% Decreases counter associated with given Ref.
%% @end
%%-------------------------------------------------------------------
-spec decrease(node() | pid(), reference()) -> ok.
decrease(NodeOrPid, Ref) ->
    decrease_by_value(NodeOrPid, Ref, 1).

-spec decrease_by_value(node() | pid(), reference(), non_neg_integer()) -> ok.
decrease_by_value(Node, Ref, Value) when is_atom(Node) ->
    gen_server:cast(?COUNTDOWN_SERVER(Node), {decrease_by_value, Ref, Value});
decrease_by_value(Pid, Ref, Value) when is_pid(Pid) ->
    gen_server:cast(Pid, {decrease_by_value, Ref, Value}).

%%-------------------------------------------------------------------
%% @doc
%% Decreases counter associated with given Ref. Saves Data
%% @end
%%-------------------------------------------------------------------
-spec decrease(node() | pid(), reference(), [data()] | data()) -> ok.
decrease(Node, Ref, Data) when is_atom(Node) ->
    gen_server:cast(?COUNTDOWN_SERVER(Node), {decrease, Ref, Data});
decrease(Pid, Ref, Data) when is_pid(Pid) ->
    gen_server:cast(Pid, {decrease, Ref, Data}).

%%-------------------------------------------------------------------
%% @doc
%% Awaits finish of counting down by counter associated with given Ref
%% and returns saved Data.
%% @end
%%-------------------------------------------------------------------
-spec await(node(), reference(), non_neg_integer()) -> term().
await(Node, Ref, Timeout) ->
    receive
        {?COUNTDOWN_FINISHED, Ref, Node, Data} ->
            Data
    after
        Timeout ->
            ct:print("Countdown server timeout"),
            ct:fail("Countdown server timeout")
    end.

%%-------------------------------------------------------------------
%% @doc
%% Awaits finish of counting down by counter associated with given Ref
%% and returns saved Data.
%% @end
%%-------------------------------------------------------------------
-spec await_many(node(), reference(), non_neg_integer()) -> #{reference() => term()}.
await_many(Node, Refs, Timeout) ->
    await_many(Node, Refs, Timeout, #{}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([Parent, Node]) ->
    {ok, #state{
        parent = Parent,
        node = Node
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call({init, 0}, _From, State = #state{
    parent = Parent,
    node = Node
}) ->
    Ref = make_ref(),
    notify_parent(Parent, Node, Ref, []),
    {reply, Ref, State};
handle_call({init, ToVerify}, _From, State = #state{counters = Tasks}) ->
    Ref = make_ref(),
    {reply, Ref, State#state{counters = Tasks#{Ref => #counter{value = ToVerify}}}};
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast({decrease_by_value, Ref, Value}, State = #state{
    parent = Parent,
    node = Node,
    counters = Counters
}) ->
    Counter = maps:get(Ref, Counters),
    Counter2 = decrease_by_value(Counter, Value),
    case Counter2#counter.value of
        0 ->
            notify_parent(Parent, Node, Ref, Counter2#counter.data),
            {noreply, State#state{counters = maps:remove(Ref, Counters)}};
        _ ->
            {noreply, State#state{counters = maps:update(Ref, Counter2, Counters)}}
    end;
handle_cast({decrease, Ref, Data}, State = #state{
    parent = Parent,
    node = Node,
    counters = Counters
}) ->
    Counter = maps:get(Ref, Counters),
    Counter2 = decrease_and_save_data(Counter, Data),
    case Counter2#counter.value of
        0 ->
            notify_parent(Parent, Node, Ref, Counter2#counter.data),
            {noreply, State#state{counters = maps:remove(Ref, Counters)}};
        _ ->
            {noreply, State#state{counters = maps:update(Ref, Counter2, Counters)}}
    end;
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
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
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Notifies parent about finished countdown.
%% @end
%%-------------------------------------------------------------------
-spec notify_parent(reference(), pid(), node(), term()) -> term().
notify_parent(Parent, Node, Ref, Data) ->
    Parent ! {?COUNTDOWN_FINISHED, Ref, Node, Data}.

-spec decrease_by_value(counter(), non_neg_integer()) -> counter().
decrease_by_value(Counter = #counter{value = Value0}, Value) ->
    Counter#counter{value = Value0 - Value}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Decreases given Counter and saves Data.
%% @end
%%-------------------------------------------------------------------
-spec decrease_and_save_data(counter(), data() | [data()]) -> counter().
decrease_and_save_data(Counter = #counter{value = Value, data = Data0}, Data) when is_list(Data) ->
    Counter#counter{
        value = Value - length(Data),
        data = Data ++ Data0
    };
decrease_and_save_data(Counter = #counter{value = Value, data = Data0}, Data) ->
    Counter#counter{
        value = Value - 1,
        data = [Data | Data0]
    }.

-spec await_many(node(), reference(), non_neg_integer(),
    #{reference() => term()}) -> #{reference() => term()}.
await_many(_Node, [], _Timeout, RefsToData) ->
    RefsToData;
await_many(Node, Refs, Timeout, RefsToData) ->
    receive
        {?COUNTDOWN_FINISHED, Ref, Node, Data} ->
            await_many(Node, Refs -- [Ref], Timeout, RefsToData#{Ref => Data})
    after
        Timeout ->
            ct:print("Countdown server timeout"),
            ct:fail("Countdown server timeout")
    end.