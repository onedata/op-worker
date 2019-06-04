-module(couchbase_changes_stream_mock).
-author("").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

%% Mocked API
-export([mocked_start_link/3, mocked_stop/1]).

%% API
-export([stream_changes/2, generate_changes/7, generate_changes/8]).

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
-define(RESTART(CallbackFun, Since, Until, StreamPid), {restart, CallbackFun, Since, Until, StreamPid}).
-define(STREAM_CHANGES(Changes), {stream_changes, Changes}).
-define(STREAM_CHANGE, stream_change).

-record(state, {
    callback :: couchbase_changes:callback(),
    since :: couchbase_changes:since(),
    until :: couchbase_changes:until(),
    stream_pid :: pid(),
    changes = [] :: [datastore:doc()],
    stopped = false :: boolean()
}).

%%%===================================================================
%%% Mocked API
%%%===================================================================

mocked_start_link(CallbackFun, Since, Until) ->
    HarvestingStreamPid = self(),
    case couchbase_changes_stream_mock_registry:get(HarvestingStreamPid) of
        undefined ->
            gen_server:start_link(?MODULE, [CallbackFun, Since, Until, self()], []);
        Pid when is_pid(Pid) ->
            restart(Pid, CallbackFun, Since, Until, self()),
            {ok, Pid}
    end.

mocked_stop(Pid) ->
    gen_server:cast(Pid, ?STOP).

%%%===================================================================
%%% API
%%%===================================================================

stream_changes(Pid, Changes) ->
    gen_server:call(Pid, ?STREAM_CHANGES(Changes)).

generate_changes(Since, Until, Count, CustomMetadataProb, EmptyValueProb, DeletedProb, ProviderIds) ->
    generate_changes(Since, Until, Count, CustomMetadataProb, DeletedProb, EmptyValueProb, ProviderIds, undefined).

generate_changes(Since, Until, Count, CustomMetadataProb, DeletedProb, EmptyValueProb, ProviderIds, FileId) when (Until - Since) >= Count ->
    Seqs = generate_sequences(Since, Until, Count),
    ProviderIds2 = utils:ensure_list(ProviderIds),
    [generate_change(Seq, CustomMetadataProb, DeletedProb, EmptyValueProb, ProviderIds2, FileId) || Seq <- Seqs].

%%%===================================================================
%%% Internal API
%%%===================================================================

restart(Pid, CallbackFun, Since, Until, StreamPid) ->
    gen_server:cast(Pid, ?RESTART(CallbackFun, Since, Until, StreamPid)).

stream_next_change() ->
    gen_server:cast(self(), ?STREAM_CHANGE).

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

-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) -> {reply, Reply :: term(), NewState :: #state{}}.
handle_call(?STREAM_CHANGES(NewChanges), _From, State = #state{changes = Changes, stopped = Stopped}) ->
    case Stopped of
        true -> ok;
        false -> stream_next_change()
    end,
    {reply, ok, State#state{changes = Changes ++ NewChanges}}.


-spec handle_cast(Request :: term(), State :: #state{}) -> {noreply, #state{}}.
handle_cast(?STREAM_CHANGE, State = #state{changes = []}) ->
    {noreply, State};
handle_cast(?STREAM_CHANGE, State = #state{stopped = true}) ->
    {noreply, State};
handle_cast(?STREAM_CHANGE, State = #state{stopped = false}) ->
    State2 = #state{since = Since, until = Until} =
        stream_change_to_harvesting_stream(State),
    case Until =/= infinity andalso Since >= Until of
        true -> ok;
        false -> stream_next_change()
    end,
    {noreply, State2};
handle_cast(?RESTART(CallbackFun, Since, Until, StreamPid), State = #state{}) ->
    stream_next_change(),
    {noreply, State#state{
        since = Since,
        until = Until,
        stream_pid = StreamPid,
        callback = CallbackFun,
        stopped = false
    }};
handle_cast(?STOP, State = #state{callback = Callback}) ->
    Callback({ok, end_of_stream}),
    {noreply, State#state{stopped = true}}.


-spec handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}}.
handle_info(_Info, State) ->
    {noreply, State}.


-spec terminate(Reason :: normal, State :: #state{}) -> term().
terminate(normal, #state{stream_pid = StreamPid}) ->
    couchbase_changes_stream_mock_registry:deregister(StreamPid).

-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

stream_change_to_harvesting_stream(State = #state{
    since = Since,
    changes = [#document{seq = Seq} | Changes]
}) when Seq < Since ->
    State#state{changes = Changes};
stream_change_to_harvesting_stream(State = #state{
        since = Since,
        until = Until,
        callback = Callback,
        stream_pid = StreamPid,
        changes = [Doc = #document{seq = Seq} | Changes]
}) when (Until =:= infinity)
    orelse (Until =/= infinity andalso Since < Until andalso Seq < Until) ->
    Callback({ok, Doc}),
    State#state{since = Seq, changes = Changes};
stream_change_to_harvesting_stream(State = #state{
    callback = Callback,
    since = Since,
    until = Until,
    changes = Changes = [#document{seq = Seq} | _]
}) when Since < Until andalso Until =< Seq ->
    Callback({ok, end_of_stream}),
    State#state{
        since = Seq,
        changes = Changes,
        stopped = true
    };
stream_change_to_harvesting_stream(State) ->
    State.


generate_change(Seq, CustomMetadataProb, DeletedProb, EmptyValueProb, ProviderIds, FileId) ->
    #document{
        seq = Seq,
        value = doc_value(CustomMetadataProb, EmptyValueProb, FileId),
        deleted = rand:uniform() < DeletedProb,
        mutators = [utils:random_element(ProviderIds)]
    }.

generate_sequences(Since, Until, Count) ->
    Seqs = lists:seq(Since, Until - 1),
    throw_out_random_elements(Seqs, Count).

throw_out_random_elements(Seqs, TargetLength) when length(Seqs) =:= TargetLength ->
    Seqs;
throw_out_random_elements(Seqs, TargetLength) ->
    Seqs2 = Seqs -- [utils:random_element(Seqs)],
    throw_out_random_elements(Seqs2, TargetLength).

doc_value(CustomMetadataProb, EmptyValueProb, undefined) ->
    doc_value(CustomMetadataProb, EmptyValueProb, crypto:strong_rand_bytes(10));
doc_value(CustomMetadataProb, EmptyValueProb, FileId) ->
    case rand:uniform() < CustomMetadataProb of
        true -> #custom_metadata{
            file_objectid = FileId,
            value = metadata_value(EmptyValueProb)
        };
        false -> undefined
    end.

metadata_value(EmptyValueProb) ->
    case rand:uniform() < EmptyValueProb of
        true -> #{};
        false -> #{<<"key">> => <<"value">>}
    end.