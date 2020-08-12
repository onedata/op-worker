%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Mock of couchbase_changes_stream used in tests of harvesting. It
%%% allows to simulate behaviour of couchbase_changes_stream.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_changes_stream_mock).
-author("Jakub Kudzia").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

%% Mocked API
-export([mocked_start_link/3, mocked_stop/1]).

%% API
-export([stream_changes/2, generate_changes/9, generate_changes/10]).

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

generate_changes(Since, Until, Count, CustomMetadataProb, FileMetaProb, DeletedProb, EmptyValueProb, ProviderIds, SpaceId) ->
    generate_changes(Since, Until, Count, CustomMetadataProb, FileMetaProb,
        DeletedProb, EmptyValueProb, ProviderIds, SpaceId, undefined).

generate_changes(Since, Until, Count, CustomMetadataProb, FileMetaProb, DeletedProb, EmptyValueProb, ProviderIds,
    SpaceId, DocKey
) when (Until - Since) >= Count ->
    Seqs = generate_sequences(Since, Until, Count),
    ProviderIds2 = utils:ensure_list(ProviderIds),
    lists:map(fun(Seq) ->
        generate_change(Seq, CustomMetadataProb, FileMetaProb, DeletedProb, EmptyValueProb, ProviderIds2, SpaceId, DocKey)
    end, Seqs).

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


generate_change(Seq, CustomMetadataProb, FileMetaProb, DeletedProb, EmptyValueProb, ProviderIds, SpaceId, DocKey) ->
    DocKey2 = case DocKey =:= undefined of
        true -> generate_file_uuid();
        false -> DocKey
    end,
    #document{
        key = DocKey2,
        seq = Seq,
        value = doc_value(DocKey2, SpaceId, CustomMetadataProb, FileMetaProb, EmptyValueProb),
        deleted = rand:uniform() < DeletedProb,
        mutators = [lists_utils:random_element(ProviderIds)],
        scope = SpaceId
    }.

generate_sequences(Since, Until, Count) ->
    Seqs = lists:seq(Since, Until - 1),
    throw_out_random_elements(Seqs, Count).

throw_out_random_elements(Seqs, TargetLength) when length(Seqs) =:= TargetLength ->
    Seqs;
throw_out_random_elements(Seqs, TargetLength) ->
    Seqs2 = Seqs -- [lists_utils:random_element(Seqs)],
    throw_out_random_elements(Seqs2, TargetLength).

doc_value(DocKey, SpaceId, CustomMetadataProb, FileMetaProb, EmptyValueProb) ->
    case rand:uniform() of
        Rand when Rand < CustomMetadataProb ->
            custom_metadata_record(DocKey, SpaceId, EmptyValueProb);
        Rand when Rand >= CustomMetadataProb andalso (Rand < (CustomMetadataProb + FileMetaProb)) ->
            file_meta_record();
        _ -> undefined
    end.

custom_metadata_record(FileUuid, SpaceId, EmptyValueProb) ->
    {ok, FileId} = file_id:guid_to_objectid(file_id:pack_guid(FileUuid, SpaceId)),
    #custom_metadata{
        file_objectid = FileId,
        value = metadata_value(EmptyValueProb),
        space_id = SpaceId
    }.

file_meta_record() ->
    #file_meta{
        name = get_random_string()
    }.

generate_file_uuid() ->
    get_random_string().

metadata_value(EmptyValueProb) ->
    case rand:uniform() < EmptyValueProb of
        true -> #{};
        false -> #{<<"key">> => <<"value">>}
    end.

get_random_string() ->
    get_random_string(10, "abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ").

get_random_string(Length, AllowedChars) ->
    list_to_binary(lists:foldl(fun(_, Acc) ->
        [lists:nth(rand:uniform(length(AllowedChars)),
            AllowedChars)]
        ++ Acc
    end, [], lists:seq(1, Length))).