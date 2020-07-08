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

-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2, stop/1,
    init_counter/2, init_counter/3,
    decrease/2, decrease/3, decrease_by_value/3,
    await/3, await_all/2, await_all/3,
    await_many/3, await_many/4,
    not_received_any/2,not_received_any/3
]).

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
    counters = #{} :: #{counter_id() => #counter{}}
}).

-type counter() :: #counter{}.
-type data() :: [term()].
-type counter_id() :: reference() | term().
-type node_counters() :: #{node() => counter_id() | [counter_id()]}.
-type node_counters_data() :: #{node() => #{counter_id()  => data()}}.

-define(COUNTDOWN_SERVER(Node),
    binary_to_atom(<<"countdown_server_", (atom_to_binary(Node, latin1))/binary>>, latin1)
).

-define(INIT(InitialValue, CounterId), {init, InitialValue, CounterId}).
-define(DECREASE_BY_VALUE(Value, CounterId), {decrease_by_value, Value, CounterId}).
-define(DECREASE(Data, CounterId), {decrease, Data, CounterId}).
-define(COUNTDOWN_FINISHED(CounterId, Node, Data), {countdown_finished, CounterId, Node, Data}).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link(pid(), node()) -> {ok, Pid :: pid()} | {error, Reason :: term()}).
start_link(Parent, Node) ->
    gen_server:start_link({local, ?COUNTDOWN_SERVER(Node)}, ?MODULE, [Parent, Node], []).

-spec stop(node()) -> ok.
stop(Node) ->
    gen_server:stop(?COUNTDOWN_SERVER(Node)).

%%-------------------------------------------------------------------
%% @doc
%% Initializes new counter with given InitialValue.
%% Returns counter id.
%% @end
%%-------------------------------------------------------------------
-spec init_counter(node(), non_neg_integer()) -> counter_id().
init_counter(Node, InitialValue) ->
    init_counter(Node, InitialValue, undefined).

%%-------------------------------------------------------------------
%% @doc
%% Initializes new counter with given InitialValue and predefined CounterId.
%% If CounterId is undefined, it will be set and returned.
%% @end
%%-------------------------------------------------------------------
-spec init_counter(node(), non_neg_integer(), undefined | counter_id()) -> counter_id().
init_counter(Node, InitialValue, CounterId) ->
    gen_server:call(?COUNTDOWN_SERVER(Node), ?INIT(InitialValue, CounterId)).

%%-------------------------------------------------------------------
%% @doc
%% Decreases counter associated with given CounterId.
%% @end
%%-------------------------------------------------------------------
-spec decrease(node() | pid(), counter_id()) -> ok.
decrease(NodeOrPid, CounterId) ->
    decrease_by_value(NodeOrPid, CounterId, 1).

-spec decrease_by_value(node() | pid(), counter_id(), non_neg_integer()) -> ok.
decrease_by_value(Node, CounterId, Value) when is_atom(Node) ->
    gen_server:cast(?COUNTDOWN_SERVER(Node), ?DECREASE_BY_VALUE(Value, CounterId));
decrease_by_value(Pid, CounterId, Value) when is_pid(Pid) ->
    gen_server:cast(Pid, ?DECREASE_BY_VALUE(Value, CounterId)).

%%-------------------------------------------------------------------
%% @doc
%% Decreases counter associated with given CounterId. Saves Data.
%% Counter is decreased by length of Data (if it's a list).
%% @end
%%-------------------------------------------------------------------
-spec decrease(node() | pid(), counter_id(), [data()] | data()) -> ok.
decrease(Node, CounterId, Data) when is_atom(Node) ->
    gen_server:cast(?COUNTDOWN_SERVER(Node), ?DECREASE(Data, CounterId));
decrease(Pid, CounterId, Data) when is_pid(Pid) ->
    gen_server:cast(Pid, ?DECREASE(Data, CounterId)).

%%-------------------------------------------------------------------
%% @doc
%% Awaits for finish of counting down by counter associated with given
%% CounterId and returns saved Data.
%% @end
%%-------------------------------------------------------------------
-spec await(node(), counter_id(), non_neg_integer()) -> data().
await(Node, CounterId, Timeout) ->
    #{Node := #{CounterId := Data}} = await_all(Node, CounterId, Timeout),
    Data.

%%-------------------------------------------------------------------
%% @doc
%% Awaits for finish of counting down by all counters in NodesToCounters
%% map. Returns map of counters' data.
%% @end
%%-------------------------------------------------------------------
-spec await_all(node_counters(), non_neg_integer()) -> node_counters_data().
await_all(NodesToCounters, Timeout) ->
    NumberOfCounters = count_counters(NodesToCounters),
    {CountersData, #{}} = await_many(NodesToCounters, Timeout, NumberOfCounters),
    CountersData.

-spec await_all(node(), counter_id() | [counter_id()], non_neg_integer()) -> node_counters_data().
await_all(Node, CounterIds, Timeout) when is_list(CounterIds) ->
    await_all(#{Node => CounterIds}, Timeout);
await_all(Node, CounterId, Timeout) ->
    await_all(Node, [CounterId], Timeout).


%%-------------------------------------------------------------------
%% @doc
%% Awaits for finish of counting down by up to Count counters in
%% NodesToCounters map. Returns map of counters' data and node_counters()
%% map of not finished counters.
%% @end
%%-------------------------------------------------------------------
-spec await_many(node_counters(), non_neg_integer(), non_neg_integer()) ->
    {node_counters_data(), node_counters()}.
await_many(NodesToCounters, Timeout, Count) ->
    await_many_internal(NodesToCounters, Timeout, Count, #{}, []).

-spec await_many(node(), counter_id() | [counter_id()], non_neg_integer(), non_neg_integer()) ->
    {node_counters_data(), node_counters()}.
await_many(Node, CounterIds, Timeout, Count) when is_list(CounterIds) ->
    await_many(#{Node => CounterIds}, Timeout, Count);
await_many(Node, CounterId, Timeout, Count) ->
    await_many(Node, [CounterId], Timeout, Count).


%%-------------------------------------------------------------------
%% @doc
%% Ensures that none of counters present in NodesToCounters are finished.
%% @end
%%-------------------------------------------------------------------
-spec not_received_any(node_counters(), non_neg_integer()) -> ok.
not_received_any(NodesToCounters, Timeout) ->
    not_received_any_internal(NodesToCounters, Timeout, []).

-spec not_received_any(node(), counter_id() | [counter_id()], non_neg_integer()) -> ok.
not_received_any(Node, CounterIds, Timeout) when is_list(CounterIds) ->
    not_received_any(#{Node => CounterIds}, Timeout);
not_received_any(Node, CounterId, Timeout) ->
    not_received_any(Node, [CounterId], Timeout).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init(Args :: term()) -> {ok, State :: #state{}}.
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
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}}.
handle_call(?INIT(0, IdOrUndefined), _From, State = #state{
    parent = Parent,
    node = Node
}) ->
    Id = ensure_id(IdOrUndefined),
    notify_parent(Parent, Node, Id, []),
    {reply, Id, State};
handle_call(?INIT(ToVerify, IdOrUndefined), _From, State = #state{counters = Tasks}) ->
    Id = ensure_id(IdOrUndefined),
    {reply, Id, State#state{counters = Tasks#{Id => #counter{value = ToVerify}}}};
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, {error, wrong_request}, State}.


-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}}.
handle_cast(?DECREASE_BY_VALUE(Value, CounterId), State = #state{
    parent = Parent,
    node = Node,
    counters = Counters
}) ->
    Counter = maps:get(CounterId, Counters),
    Counter2 = decrease_by_value(Counter, Value),
    ct:pal("Decreased value: ~p", [Counter2]),
    ?alert("Decreased value: ~p", [Counter2]),
    case Counter2#counter.value of
        0 ->
            notify_parent(Parent, Node, CounterId, Counter2#counter.data),
            {noreply, State#state{counters = maps:remove(CounterId, Counters)}};
        _ ->
            {noreply, State#state{counters = maps:update(CounterId, Counter2, Counters)}}
    end;
handle_cast(?DECREASE(Data, CounterId), State = #state{
    parent = Parent,
    node = Node,
    counters = Counters
}) ->
    Counter = maps:get(CounterId, Counters),
    Counter2 = decrease_and_save_data(Counter, Data),
    ct:pal("Decreased value2: ~p", [Counter2]),
    ?alert("Decreased value2: ~p", [Counter2]),
    case Counter2#counter.value of
        0 ->
            notify_parent(Parent, Node, CounterId, Counter2#counter.data),
            {noreply, State#state{counters = maps:remove(CounterId, Counters)}};
        _ ->
            {noreply, State#state{counters = maps:update(CounterId, Counter2, Counters)}}
    end;
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.

-spec handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}}.
handle_info(_Info, State) ->
    ?log_bad_request(_Info),
    {noreply, State}.

-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

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
notify_parent(Parent, Node, CounterId, Data) ->
    Parent ! ?COUNTDOWN_FINISHED(CounterId, Node, Data).

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


-spec await_many_internal(node_counters(), non_neg_integer(), non_neg_integer(), node_counters_data(), [term()]) ->
    {node_counters_data(), node_counters()}.
await_many_internal(NodesToCounterIds, _Timeout, 0, DataAcc, NotMatchedMessages) when map_size(NodesToCounterIds) =:= 0 ->
    resend_messages_to_self(NotMatchedMessages),
    {DataAcc, NodesToCounterIds};
await_many_internal(NodesToCounterIds, _Timeout, _Count, DataAcc, NotMatchedMessages) when map_size(NodesToCounterIds) =:= 0 ->
    resend_messages_to_self(NotMatchedMessages),
    {DataAcc, NodesToCounterIds};
await_many_internal(NodesToCounterIds, Timeout, Count, DataAcc, NotMatchedMessages) ->
    receive
        Msg = ?COUNTDOWN_FINISHED(CounterId, Node, Data) ->
            case is_expected_counter(Node, CounterId, NodesToCounterIds) of
                true ->
                    NodesToCounterIds2 = remove_counter(Node, CounterId, NodesToCounterIds),
                    await_many_internal(NodesToCounterIds2, Timeout, Count - 1, maps:update_with(Node,
                        fun(NodeCounterIdsToData) -> NodeCounterIdsToData#{CounterId => Data} end,
                        #{CounterId => Data}, DataAcc), NotMatchedMessages);
                false ->
                    await_many_internal(NodesToCounterIds, Timeout, Count, DataAcc, [Msg | NotMatchedMessages])
            end
    after
        Timeout ->
            resend_messages_to_self(NotMatchedMessages),
            {DataAcc, NodesToCounterIds}
    end.

-spec not_received_any_internal(node_counters(), non_neg_integer(), [term()]) -> ok.
not_received_any_internal(NodesToCounterIds, Timeout, NotMatchedMessages) ->
    receive
        Msg = ?COUNTDOWN_FINISHED(CounterId, Node, _Data) ->
            case is_expected_counter(Node, CounterId, NodesToCounterIds) of
                true ->
                    ct:print("Countdown server unexpectedly received ~p", [Msg]),
                    ct:fail("Countdown server unexpectedly received ~p", [Msg]);
                false ->
                    not_received_any_internal(NodesToCounterIds, Timeout, [Msg | NotMatchedMessages])
            end
    after
        Timeout ->
            resend_messages_to_self(NotMatchedMessages),
            ok
    end.


-spec ensure_id(counter_id() | undefined) -> counter_id().
ensure_id(undefined) -> make_ref();
ensure_id(Id) -> Id.

-spec is_expected_counter(node(), counter_id(), node_counters()) -> node_counters().
is_expected_counter(Node, ExpectedCounterId, NodesToCounterIds) ->
    case maps:is_key(Node, NodesToCounterIds) of
        true ->
            case maps:get(Node, NodesToCounterIds) of
                CounterIds when is_list(CounterIds) ->
                    lists:member(ExpectedCounterId, maps:get(Node, NodesToCounterIds));
                CounterId ->
                    CounterId =:= ExpectedCounterId
            end;
        false ->
            false
    end.

-spec remove_counter(node(), counter_id(), node_counters()) -> node_counters().
remove_counter(Node, ExpectedCounterId, NodesToCounterIds) ->
    NodesToCounterIds2 = maps:update_with(Node, fun
        (CounterIds) when is_list(CounterIds) -> CounterIds -- [ExpectedCounterId];
        (CounterId) when CounterId =:= ExpectedCounterId -> undefined
    end, NodesToCounterIds),
    case maps:get(Node, NodesToCounterIds2) of
        [] -> maps:remove(Node, NodesToCounterIds2);
        undefined -> maps:remove(Node, NodesToCounterIds2);
        _ -> NodesToCounterIds2
    end.

-spec resend_messages_to_self([term()]) -> ok.
resend_messages_to_self([]) ->
    ok;
resend_messages_to_self([Msg | Rest]) ->
    self() ! Msg,
    resend_messages_to_self(Rest).

-spec count_counters(node_counters()) -> non_neg_integer().
count_counters(NodesToCounters) ->
    maps:fold(fun
        (_Node, Counters, SumAcc) when is_list(Counters) ->
            SumAcc + length(Counters);
        (_Node, _Counter, SumAcc) ->
            SumAcc + 1
    end, 0, NodesToCounters).
