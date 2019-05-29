-module(couchbase_changes_stream_mock).
-author("").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

%% Mocked API
-export([mocked_start_link/3, mocked_stop/1]).

%% API
-export([stop/1, stream_changes/2, generate_changes/5]).

%% RPC API
-export([]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(STOP, stop).
-define(STREAM_CHANGES(Changes), {stream_changes, Changes}).

-record(state, {
    callback :: couchbase_changes:callback(),
    since :: couchbase_changes:since(),
    until :: couchbase_changes:until(),
    stream_pid :: pid()
}).

%%%===================================================================
%%% Mocked API
%%%===================================================================

mocked_start_link(CallbackFun, Since, Until) ->
    gen_server:start_link(?MODULE, [CallbackFun, Since, Until, self()], []).

mocked_stop(Pid) ->
    gen_server:cast(Pid, ?STOP).

%%%===================================================================
%%% API
%%%===================================================================

% todo needed???
stop(Pid) ->
    gen_server:stop(Pid, normal, infinity).

stream_changes(Pid, Changes) ->
    gen_server:call(Pid, ?STREAM_CHANGES(Changes)).

generate_changes(Since, Until, Count, CustomMetadataProb, DeletedProb) when (Until - Since) >= Count ->
    Seqs = generate_sequences(Since, Until, Count),
    [generate_change(Seq, CustomMetadataProb, DeletedProb) || Seq <- Seqs].

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) -> {ok, State :: #state{}}).
init([Callback, Since, Until, StreamPid]) ->
    couchbase_changes_stream_mock_registry:register(StreamPid),
    {ok, #state{
        callback = Callback,
        since = Since,
        until = Until,
        stream_pid = StreamPid
    }}.

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) -> {reply, Reply :: term(), NewState :: #state{}}).
handle_call(?STREAM_CHANGES(Changes), From, State) ->
    gen_server:reply(From, ok),
    stream_changes_to_harvesting_stream(Changes, State).


-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(?STOP, State) ->
    {stop, normal, State}.

-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}}).
handle_info(_Info, State) ->
    {noreply, State}.


-spec(terminate(Reason :: normal, State :: #state{}) -> term()).
terminate(normal, #state{callback = Callback, stream_pid = StreamPid}) ->
    couchbase_changes_stream_mock_registry:deregister(StreamPid),
    Callback({ok, end_of_stream}),
    ok.

-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

stream_changes_to_harvesting_stream(Changes, State) ->
    State2 = #state{since = Since, until = Until} = lists:foldl(fun(Change, State) ->
        stream_change_to_harvesting_stream(Change, State)
    end, State, Changes),
    case Until =/= infinity andalso Since >= Until of
        true -> {stop, normal, State2};
        false -> {noreply, State2}
    end.

stream_change_to_harvesting_stream(#document{seq = Seq},
    State = #state{since = Since}
) when Seq < Since ->
    State;
stream_change_to_harvesting_stream(Doc = #document{seq = Seq},
    State = #state{since = Since, until = Until, callback = Callback}
) when (Until =:= infinity)
    orelse (Until =/= infinity andalso Since < Until andalso Seq < Until) ->
    Callback({ok, Doc}),
    State#state{since = Seq};
stream_change_to_harvesting_stream(#document{seq = Seq},
    State = #state{since = Since, until = Until}
) when Since < Until andalso Seq >= Until ->
    State#state{since = Seq};
stream_change_to_harvesting_stream(_Doc, State) ->
    State.

generate_change(Seq, CustomMetadataProb, DeletedProb) ->
    #document{
        seq = Seq,
        value = case rand:uniform() < CustomMetadataProb of
            true -> #custom_metadata{
                file_objectid = crypto:strong_rand_bytes(10)
            };
            false -> undefined
        end,
        deleted = rand:uniform() < DeletedProb
    }.

generate_sequences(Since, Until, Count) ->
    Seqs = lists:seq(Since, Until - 1),
    throw_out_random_elements(Seqs, Count).

throw_out_random_elements(Seqs, Count) when length(Seqs) =< Count ->
    Seqs;
throw_out_random_elements(Seqs, Count) ->
    E = lists:nth(rand:uniform(length(Seqs)), Seqs),
    Seqs2 = [X || X <- Seqs, X /= E],
    throw_out_random_elements(Seqs2, Count).
