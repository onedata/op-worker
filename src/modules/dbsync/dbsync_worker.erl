%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc datastore worker's implementation
%%% @end
%%%--------------------------------------------------------------------
-module(dbsync_worker).
-author("Rafal Slota").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("modules/dbsync/common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).
-export([state_get/1, state_put/2]).
-export([apply_batch_changes/3, init_stream/3]).
-export([bcast_status/0, on_status_received/3]).

-define(MODELS_TO_SYNC, [file_meta, file_location]).
-define(BROADCAST_STATUS_INTERVAL, timer:seconds(15)).
-define(FLUSH_QUEUE_INTERVAL, timer:seconds(2)).
-define(GLOBAL_STREAM_RESTART_INTERVAL, 500).

-type queue_id() :: binary().
-type queue() :: global | {provider, oneprovider:id(), queue_id()}.
-type change() :: #change{}.
-type batch() :: #batch{}.

-export_type([change/0, batch/0, queue/0]).


%% Record for storing current state of dbsync stream queue.
-record(queue, {
    key :: queue(),
    since = 0 :: non_neg_integer(),
    batch_map = #{} :: #{},
    removed = false :: boolean()
}).

%% Defines maximum size of changes buffer per space in bytes.
-define(CHANGES_STASH_MAX_SIZE_BYTES, 10 * 1024 * 1024).


%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    ?info("[ DBSync ]: Starting dbsync..."),
    Since = 0,
    timer:send_after(?BROADCAST_STATUS_INTERVAL, whereis(dbsync_worker), {timer, bcast_status}),
    timer:send_after(timer:seconds(5), whereis(dbsync_worker), {timer, {async_init_stream, Since, infinity, global}}),
    timer:send_after(?FLUSH_QUEUE_INTERVAL, whereis(dbsync_worker), {timer, {flush_queue, global}}),
    {ok, #{changes_stream => undefined}}.

%%--------------------------------------------------------------------
%% @doc
%% Starts changes stream and pushes all changes from the stream to the changes queue.
%% Global queue will be restarted after crash.
%% @end
%%--------------------------------------------------------------------
-spec init_stream(Since :: non_neg_integer(), Until :: non_neg_integer() | infinity, Queue :: queue()) ->
    {ok, pid()} | {error, Reason :: term()}.
init_stream(Since, Until, Queue) ->
    ?info("[ DBSync ]: Starting stream ~p ~p ~p", [Since, Until, Queue]),
    case Queue of
        {provider, _, _} ->
            BatchMap = maps:from_list(lists:map(
                fun(SpaceId) ->
                    {SpaceId, #batch{since = Since, until = Since}}
                end, dbsync_utils:get_spaces_for_provider())),
            timer:send_after(?FLUSH_QUEUE_INTERVAL, whereis(dbsync_worker), {timer, {flush_queue, Queue}}),
            state_put({queue, Queue}, #queue{batch_map = BatchMap, since = Since});
        _ -> ok
    end,

    couchdb_datastore_driver:changes_start_link(
        fun
            (_, stream_ended, _) ->
                worker_proxy:call(dbsync_worker, {Queue, {cleanup, Until}});
            (Seq, Doc, Model) ->
                worker_proxy:call(dbsync_worker, {Queue, #change{seq = Seq, doc = Doc, model = Model}})
        end, Since, Until).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck | term(),
    Result :: nagios_handler:healthcheck_response() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: term(),
    Reason :: term().
handle(ping) ->
    pong;

handle(healthcheck) ->
    ensure_global_stream_active(),
    case state_get(changes_stream) of
        Stream when is_pid(Stream) ->
            ok;
        Other ->
            ?error("DBSync global stream not ready (~p)", [Other]),
            {error, global_stream_down}
    end;

handle({reemit, Msg}) ->
    dbsync_proto:reemit(Msg);

handle(bcast_status) ->
    timer:send_after(?BROADCAST_STATUS_INTERVAL, whereis(dbsync_worker), {timer, bcast_status}),
        catch bcast_status();

handle(requested_bcast_status) ->
    bcast_status();

%% Remove given changes queue
handle({QueueKey, {cleanup, Until}}) ->
    queue_remove(QueueKey, Until);

%% Append change to given queue
handle({QueueKey, #change{seq = Seq, doc = #document{key = Key} = Doc} = Change}) ->
    ?debug("[ DBSync ] Received change on queue ~p with seq ~p: ~p", [QueueKey, Seq, Doc]),
    case has_sync_context(Doc) of
        true ->
            case get_space_id(Doc) of
                {ok, <<"spaces">>} ->
                    ?debug("Skipping doc ~p", [Doc]),
                    skip;
                {ok, <<"">>} ->
                    ?debug("Skipping doc ~p", [Doc]),
                    skip;
                {ok, SpaceId} ->
                    queue_push(QueueKey, Change, SpaceId);
                {error, not_a_space} ->
                    skip;
                {error, Reason} ->
                    ?error("Unable to find space id for document ~p due to: ~p", [Doc, Reason]),
                    {error, Reason}
            end;
        false ->
            ?debug("Skipping doc ~p", [Doc]),
            ok
    end;

%% Push changes from queue to providers
handle({flush_queue, QueueKey}) ->
    ?debug("[ DBSync ] Flush queue ~p", [QueueKey]),
    ensure_global_stream_active(),
    state_update({queue, QueueKey},
        fun(#queue{batch_map = BatchMap, removed = IsRemoved} = Queue) ->
            NewBatchMap = maps:map(
                fun(SpaceId, #batch{until = Until} = B) ->
                    spawn(fun() -> dbsync_proto:send_batch(QueueKey, SpaceId, B) end),
                    set_current_seq(oneprovider:get_provider_id(), SpaceId, Until),
                    case QueueKey of
                        global -> state_put(global_resume_seq, Until);
                        _ -> ok
                    end,
                    #batch{since = Until, until = Until}
                end,
                BatchMap),

            case IsRemoved of
                true ->
                    ?info("[ DBSync ] Queue ~p removed!", [QueueKey]),
                    undefined;
                false ->
                    timer:send_after(?FLUSH_QUEUE_INTERVAL, whereis(dbsync_worker), {timer, {flush_queue, QueueKey}}),
                    Queue#queue{batch_map = NewBatchMap}
            end;
            (undefined) when QueueKey =:= global ->
                timer:send_after(?FLUSH_QUEUE_INTERVAL, whereis(dbsync_worker), {timer, {flush_queue, QueueKey}}),
                undefined;
            (undefined) ->
                ?warning("Unknown operation on empty queue ~p", [QueueKey]),
                undefined
        end);

%% Handle external dbsync requests
handle({dbsync_request, SessId, DBSyncRequest}) ->
    dbsync_proto:handle(SessId, DBSyncRequest);

%% Handle stream crashes
%% todo: ensure VFS-1877 is resolved (otherwise it probably isn't working)
handle({'EXIT', Stream, Reason}) ->
    case state_get(changes_stream) of
        Stream ->
            Since = state_get(global_resume_seq),
            state_update(changes_stream,
                fun(_) ->
                    {ok, NewStream} = init_stream(Since, infinity, global),
                    NewStream
                end);
        _ ->
            ?warning("Unknown stream crash ~p: ~p", [Stream, Reason])
    end;
handle({async_init_stream, undefined, Until, Queue}) ->
    handle({async_init_stream, 0, Until, Queue});
handle({async_init_stream, Since, Until, Queue}) ->
    state_update(changes_stream, fun(OldStream) ->
        case catch init_stream(Since, infinity, Queue) of
            {ok, Pid} ->
                    catch exit(OldStream, shutdown),
                Pid;
            Reason ->
                ?warning("Unable to start stream ~p (since ~p until ~p) due to: ~p", [Queue, Since, Until, Reason]),
                timer:send_after(?GLOBAL_STREAM_RESTART_INTERVAL, whereis(dbsync_worker), {timer, {async_init_stream, Since, Until, Queue}}),
                undefined
        end
    end);
%% Unknown request
handle(_Request) ->
    ?log_bad_request(_Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok.
cleanup() ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Removes queue from dbsync state. If batch in queue was empty 'until' marker is set to given value.
%% @end
%%--------------------------------------------------------------------
-spec queue_remove(queue(), Until :: non_neg_integer()) -> ok.
queue_remove(QueueKey, Until) ->
    state_update({queue, QueueKey},
        fun(Queue) ->
            case Queue of
                undefined ->
                    undefined;
                #queue{batch_map = BatchMap} = Q ->
                    NewBatchMap = maps:map(
                        fun(_SpaceId, #batch{} = B) ->
                            B#batch{until = Until}
                        end,
                        BatchMap),
                    ?debug("Removing queue with Batch ~p", [NewBatchMap]),
                    Q#queue{removed = true, batch_map = NewBatchMap}
            end
        end).

%%--------------------------------------------------------------------
%% @doc
%% Push given change to given queue.
%% @end
%%--------------------------------------------------------------------
-spec queue_push(queue(), change(), SpaceId :: binary()) -> ok.
queue_push(QueueKey, #change{seq = Until} = Change, SpaceId) ->
    state_update({queue, QueueKey},
        fun(Queue) ->
            Queue1 =
                case Queue of
                    undefined ->
                        #queue{batch_map = #{}};
                    #queue{} = Q -> Q
                end,
            BatchMap = Queue1#queue.batch_map,
            Since = Queue1#queue.since,
            Batch0 = maps:get(SpaceId, BatchMap, #batch{since = Since, until = Until}),
            Batch = Batch0#batch{changes = [Change | Batch0#batch.changes], until = Until},
            Queue1#queue{batch_map = maps:put(SpaceId, Batch, BatchMap)}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Apply whole batch of changes from remote provider and try to consume batches from stash.
%% @end
%%--------------------------------------------------------------------
-spec apply_batch_changes(FromProvider :: oneprovider:id(), SpaceId :: binary(), batch()) ->
    ok | no_return().
apply_batch_changes(FromProvider, SpaceId, #batch{changes = Changes, since = Since, until = Until} = Batch) ->
    ?debug("Pre-Apply changes from ~p ~p: ~p", [FromProvider, SpaceId, Batch]),
        catch consume_batches(FromProvider, SpaceId),
    NewChanges = lists:sort(lists:flatten(Changes)),
    do_apply_batch_changes(FromProvider, SpaceId, Batch#batch{changes = NewChanges}, true).

%%--------------------------------------------------------------------
%% @doc
%% Apply whole batch of changes from remote provider.
%% @end
%%--------------------------------------------------------------------
-spec do_apply_batch_changes(FromProvider :: oneprovider:id(), SpaceId :: binary(), batch(), ShouldRequest :: boolean()) ->
    ok | no_return().
do_apply_batch_changes(FromProvider, SpaceId, #batch{changes = Changes, since = Since, until = Until} = Batch, ShouldRequest) ->
    ?debug("Apply changes from ~p ~p: ~p", [FromProvider, SpaceId, Batch]),
    CurrentUntil = get_current_seq(FromProvider, SpaceId),
    case CurrentUntil < Since of
        true ->
            ?error("Unable to apply changes from provider ~p (space id ~p). Current 'until': ~p, batch 'since': ~p", [FromProvider, SpaceId, CurrentUntil, Since]),
            stash_batch(FromProvider, SpaceId, Batch),
            case ShouldRequest of
                true ->
                    request_missing_changes(FromProvider, SpaceId, CurrentUntil, Since);
                false ->
                    ok
            end;
        false when Until < CurrentUntil ->
            ?info("Dropping changes {~p, ~p} since current sequence is ~p", [Since, Until, CurrentUntil]),
            ok;
        false ->
            ok = apply_changes(SpaceId, Changes),
            ?debug("Changes applied ~p ~p ~p", [FromProvider, SpaceId, Until]),
            update_current_seq(FromProvider, SpaceId, Until)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Apply list of changes from remote provider.
%% @end
%%--------------------------------------------------------------------
-spec apply_changes(SpaceId :: binary(), [change()]) ->
    ok | {error, any()}.
apply_changes(SpaceId, [#change{doc = #document{key = Key, value = Value} = Doc, model = ModelName} = Change | T]) ->
    try
        ModelConfig = ModelName:model_init(),

        %todo add cache manipulation functions and activate GLOBALLY_CACHED levels of datastore for file_meta and file_location
        MainDocKey = case Value of
            #links{} ->
                Value#links.doc_key;
            _ -> Key
        end,
        datastore:run_synchronized(ModelName, MainDocKey, fun() ->
            {ok, _} = couchdb_datastore_driver:force_save(ModelConfig, Doc)
        end),



        spawn(
            fun() ->
                dbsync_events:change_replicated(SpaceId, Change),
                ok
            end),
        apply_changes(SpaceId, T)
    catch
        _:Reason ->
            ?error_stacktrace("Unable to apply change ~p due to: ~p", [Change, Reason]),
            {error, Reason}
    end;
apply_changes(_, []) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Puts given Value in datastore worker's state
%% @end
%%--------------------------------------------------------------------
-spec state_put(Key :: term(), Value :: term()) -> ok.
state_put(Key, Value) ->
    {ok, _} = dbsync_state:save(#document{key = Key, value = #dbsync_state{entry = Value}}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Puts Value from datastore worker's state
%% @end
%%--------------------------------------------------------------------
-spec state_get(Key :: term()) -> Value :: term().
state_get(Key) ->
    case dbsync_state:get(Key) of
        {ok, #document{value = #dbsync_state{entry = Value}}} ->
            Value;
        {error, {not_found, _}} ->
            undefined
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates Value for given Key in DBSync KV state using given function. The function gets as argument old Value and
%% shall return new Value. The function runs in worker_host's process.
%% @end
%%--------------------------------------------------------------------
-spec state_update(Key :: term(), UpdateFun :: fun((OldValue :: term()) -> NewValue :: term())) ->
    ok | no_return().
state_update(Key, UpdateFun) when is_function(UpdateFun) ->
    DoUpdate = fun() ->
        OldValue = state_get(Key),
        NewValue = UpdateFun(OldValue),
        case OldValue of
            NewValue ->
                ok;
            _ ->
                state_put(Key, NewValue)
        end
    end,

    DBSyncPid = whereis(?MODULE),
    case self() of
        DBSyncPid ->
            DoUpdate();
        _ ->
            datastore:run_synchronized(dbsync_state, term_to_binary(Key), DoUpdate)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks if given document should be synced across providers.
%% @end
%%--------------------------------------------------------------------
-spec has_sync_context(datastore:document()) ->
    boolean().
has_sync_context(#document{value = #links{model = ModelName}}) ->
    lists:member(ModelName, ?MODELS_TO_SYNC);
has_sync_context(#document{value = Value}) when is_tuple(Value) ->
    ModelName = element(1, Value),
    lists:member(ModelName, ?MODELS_TO_SYNC).


%%--------------------------------------------------------------------
%% @doc
%% For given document returns file's UUID that is associated with this document in some way.
%% @end
%%--------------------------------------------------------------------
-spec get_sync_context(datastore:document()) ->
    datastore:key().
get_sync_context(#document{key = Key, value = #file_meta{}}) ->
    Key;
get_sync_context(#document{value = #links{doc_key = DocKey, model = file_meta}}) ->
    DocKey;
get_sync_context(#document{value = #links{doc_key = DocKey, model = file_location}}) ->
    #model_config{store_level = StoreLevel} = file_location:model_init(),
    {ok, #document{value = #file_location{}} = Doc} = datastore:get(StoreLevel, file_location, DocKey),
    get_sync_context(Doc);
get_sync_context(#document{value = #file_location{uuid = FileUUID}}) ->
    FileUUID.


%%--------------------------------------------------------------------
%% @doc
%% Returns cached SpaceId for given document. If there is no cached value
%% calculates new one and puts into the cache.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id(datastore:document()) ->
    {ok, SpaceId :: binary()} | {error, Reason :: term()}.
get_space_id(#document{key = Key} = Doc) ->
    try state_get({sid, Key}) of
        undefined ->
            get_space_id_not_cached(Key, Doc);
        SpaceId ->
            {ok, SpaceId}
    catch
        _:Reason ->
            ?warning_stacktrace("Unable to fetch cached space_id for document ~p due to: ~p", [Key, Reason]),
            get_space_id_not_cached(Key, Doc)
    end.


%%--------------------------------------------------------------------
%% @doc
%% For given document returns SpaceId and puts this value to cache using given key.
%% This cache may be used later on by get_space_id/1.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id_not_cached(KeyToCache :: term(), datastore:document()) ->
    {ok, SpaceId :: binary()} | {error, Reason :: term()}.
get_space_id_not_cached(KeyToCache, #document{} = Doc) ->
    FileUUID = get_sync_context(Doc),
    case file_meta:get_scope({uuid, FileUUID}) of
        {ok, #document{key = <<"">> = Root}} ->
            state_put({sid, KeyToCache}, Root),
            {ok, Root};
        {ok, #document{key = ScopeUUID}} ->
            try
                SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(ScopeUUID),
                state_put({sid, KeyToCache}, SpaceId),
                {ok, SpaceId}
            catch
                _:_ -> {error, not_a_space}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Updates sequence number for given Provider and Space.
%% @end
%%--------------------------------------------------------------------
-spec update_current_seq(oneprovider:id(), SpaceId :: binary(), SeqNum :: non_neg_integer()) ->
    ok.
update_current_seq(ProviderId, SpaceId, SeqNum) ->
    Key = {current_seq, ProviderId, SpaceId},
    OP =
        fun(undefined) ->
            SeqNum;
            (CurrentSeq) ->
                max(SeqNum, CurrentSeq)
        end,

    state_update(Key, OP).


%%--------------------------------------------------------------------
%% @doc
%% Sets sequence number for given Provider and Space.
%% @end
%%--------------------------------------------------------------------
-spec set_current_seq(oneprovider:id(), SpaceId :: binary(), SeqNum :: non_neg_integer()) ->
    ok.
set_current_seq(ProviderId, SpaceId, SeqNum) ->
    Key = {current_seq, ProviderId, SpaceId},
    OldValue = state_get(Key),
    OP =
        fun(undefined) ->
            SeqNum;
            (CurrentSeq) ->
                max(SeqNum, CurrentSeq)
        end,

    state_put(Key, OP(OldValue)).


%%--------------------------------------------------------------------
%% @doc
%% Gets sequence number for local provider and given Space.
%% @end
%%--------------------------------------------------------------------
-spec get_current_seq(SpaceId :: binary()) ->
    non_neg_integer().
get_current_seq(SpaceId) ->
    get_current_seq(oneprovider:get_provider_id(), SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Gets sequence number for given Provider and Space.
%% @end
%%--------------------------------------------------------------------
-spec get_current_seq(oneprovider:id(), SpaceId :: binary()) ->
    non_neg_integer().
get_current_seq(ProviderId, SpaceId) ->
    case state_get({current_seq, ProviderId, SpaceId}) of
        undefined -> 0;
        Other -> Other
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sends status to all providers that support at least one common space with local provider.
%% @end
%%--------------------------------------------------------------------
-spec bcast_status() ->
    ok.
bcast_status() ->
    SpaceIds = dbsync_utils:get_spaces_for_provider(),
    lists:foreach(
        fun(SpaceId) ->
            CurrentSeq = get_current_seq(SpaceId),
            ?debug("DBSync broadcast for space ~p: ~p", [SpaceId, CurrentSeq]),
            {ok, Providers} = oz_spaces:get_providers(provider, SpaceId),
            dbsync_proto:status_report(SpaceId, Providers -- [oneprovider:get_provider_id()], CurrentSeq)
        end, SpaceIds).


%%--------------------------------------------------------------------
%% @doc
%% Handle received status broadcast. If received status and last updated status
%% doesn't match, missing changes will be requested.
%% @end
%%--------------------------------------------------------------------
-spec on_status_received(oneprovider:id(), SpaceId :: binary(), SeqNum :: non_neg_integer()) ->
    ok | no_return().
on_status_received(ProviderId, SpaceId, SeqNum) ->
    CurrentSeq = get_current_seq(ProviderId, SpaceId),
    ?info("Received status ~p ~p: ~p vs current ~p", [ProviderId, SpaceId, SeqNum, CurrentSeq]),
    case SeqNum > CurrentSeq of
        true ->
            do_request_changes(ProviderId, CurrentSeq, SeqNum);
        false ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Request changes for specific provider with specific range.
%% @end
%%--------------------------------------------------------------------
-spec do_request_changes(oneprovider:id(), Since :: non_neg_integer(), Until :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
do_request_changes(ProviderId, Since, Until) ->
    Now = erlang:system_time(seconds),
    MaxWaitTime = timer:seconds(5) + 10 * (Until - Since), %% Wait for 5sec + 10ms per one document
    case state_get({do_request_changes, ProviderId, Until}) of
        undefined ->
            state_put({do_request_changes, ProviderId, Until}, erlang:system_time(seconds)),
            dbsync_proto:changes_request(ProviderId, Since, Until);
        OldTime when OldTime + MaxWaitTime < Now ->
            state_put({do_request_changes, ProviderId, Until}, erlang:system_time(seconds)),
            dbsync_proto:changes_request(ProviderId, Since, Until);
        _ ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Requests changes since given sequence up to fist found cached (stashed) change or up to given 'Until' if
%% there is no stashed changes.
%% @end
%%--------------------------------------------------------------------
-spec request_missing_changes(oneprovider:id(), SpaceId :: binary(), Since :: non_neg_integer(), Until :: non_neg_integer()) ->
    ok | {error, Reason :: term()}.
request_missing_changes(ProviderId, SpaceId, Since, Until) ->
    case state_get({stash, ProviderId, SpaceId}) of
        undefined ->
            do_request_changes(ProviderId, Since, Until);
        Batches ->
            [#batch{since = FirstSince} | _] = lists:sort(Batches),
            do_request_changes(ProviderId, Since, FirstSince)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Stash changes batch in memory for further use.
%% @end
%%--------------------------------------------------------------------
-spec stash_batch(oneprovider:id(), SpaceId :: binary(), Batch :: batch()) ->
    ok | no_return().
stash_batch(ProviderId, SpaceId, Batch = #batch{since = NewSince, until = NewUntil, changes = NewChanges}) ->
    state_update({stash, ProviderId, SpaceId},
        fun
            (undefined) ->
                [Batch];
            (Batches) ->
                case size(term_to_binary(Batches)) > ?CHANGES_STASH_MAX_SIZE_BYTES of
                    true -> Batches;
                    false ->
                        [#batch{until = Until, since = Since, changes = Changes} | Tail] = Batches,
                        case NewSince > Until + 1 orelse NewSince < Since of
                            true ->
                                [Batch | Batches];
                            false ->
                                [#batch{since = Since, until = NewUntil, changes = lists:flatten([Changes, NewChanges])} | Tail]
                        end
                end
        end).


%%--------------------------------------------------------------------
%% @doc
%% Consumes batches from batch-stash.
%% @end
%%--------------------------------------------------------------------
-spec consume_batches(oneprovider:id(), SpaceId :: binary()) ->
    ok | no_return().
consume_batches(ProviderId, SpaceId) ->
    Batches = lists:sort(state_get({stash, ProviderId, SpaceId})),
    state_put({stash, ProviderId, SpaceId}, undefined),
    lists:foreach(fun(Batch) ->
        do_apply_batch_changes(ProviderId, SpaceId, Batch, false)
    end, Batches).


%%--------------------------------------------------------------------
%% @doc
%% Checks whether given term is valid stream reference.
%% @end
%%--------------------------------------------------------------------
-spec is_valid_stream(term()) -> boolean().
is_valid_stream(Stream) when is_pid(Stream) ->
    try erlang:process_info(Stream) =/= undefined
    catch _:_ -> node(Stream) =/= node() end;
is_valid_stream(_) ->
    false.


%%--------------------------------------------------------------------
%% @doc
%% Restarts global stream if necessary.
%% @end
%%--------------------------------------------------------------------
-spec ensure_global_stream_active() -> ok.
ensure_global_stream_active() ->
    case is_valid_stream(state_get(changes_stream)) of
        true -> ok;
        false ->
            Since = state_get(global_resume_seq),
            timer:send_after(0, whereis(dbsync_worker), {timer, {async_init_stream, Since, infinity, global}}),
            ok
    end.
