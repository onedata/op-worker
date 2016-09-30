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
-export([has_sync_context/1, get_space_id/1]).

-define(MODELS_TO_SYNC, [file_meta, file_location, monitoring_state, custom_metadata, change_propagation_controller]).
-define(BROADCAST_STATUS_INTERVAL, timer:seconds(15)).
-define(FLUSH_QUEUE_INTERVAL, timer:seconds(3)).
-define(DIRECT_REQUEST_PER_DOCUMENT_TIMEOUT, 10).
-define(DIRECT_REQUEST_BASE_TIMEOUT, timer:seconds(5)).
-define(GLOBAL_STREAM_RESTART_INTERVAL, 500).
-define(ETS_CACHE_NAME, doc_cache).

-type queue_id() :: binary().
-type queue() :: global | {provider, oneprovider:id(), queue_id()}.
-type change() :: #change{}.
-type batch() :: #batch{}.

-export_type([change/0, batch/0, queue/0]).


%% Record for storing current state of dbsync stream queue.
-record(queue, {
    key :: undefined | queue(),
    since = 0 :: non_neg_integer(),
    until = 0 :: non_neg_integer(),
    batch_map = #{} :: maps:map(),
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
    Since = state_get(global_resume_seq),
    timer:send_after(?BROADCAST_STATUS_INTERVAL, whereis(dbsync_worker), {timer, bcast_status}),
    timer:send_after(timer:seconds(5), whereis(dbsync_worker), {sync_timer, {async_init_stream, Since, infinity, global}}),
    timer:send_after(?FLUSH_QUEUE_INTERVAL, whereis(dbsync_worker), {timer, {flush_queue, global}}),
        catch ets:new(?ETS_CACHE_NAME, [named_table, set, public]),
    {ok, #{changes_stream => undefined}}.

%%--------------------------------------------------------------------
%% @doc
%% Starts changes stream and pushes all changes from the stream to the changes queue.
%% Global queue will be restarted after crash.
%% @end
%%--------------------------------------------------------------------
-spec init_stream(Since :: non_neg_integer(), Until :: non_neg_integer() | infinity, Queue :: queue()) ->
    {ok, pid() | already_started} | {error, Reason :: term()}.
init_stream(undefined, Until, Queue) ->
    init_stream(0, Until, Queue);
init_stream(Since, Until, Queue) ->
    ?info("[ DBSync ]: Starting stream ~p ~p ~p", [Since, Until, Queue]),
    Start = case Queue of
        {provider, _, _} ->
            case state_get({queue, Queue}) of
                #queue{} ->
                    already_started;
                _ ->
                    BatchMap = maps:from_list(lists:map(
                        fun(SpaceId) ->
                            {SpaceId, #batch{since = Since, until = Since}}
                        end, dbsync_utils:get_spaces_for_provider())),
                    timer:send_after(?FLUSH_QUEUE_INTERVAL, whereis(dbsync_worker), {timer, {flush_queue, Queue}}),
                    state_put({queue, Queue}, #queue{batch_map = BatchMap, since = Since, until = Since}),
                    ok
            end;
        _ ->
            CTime = erlang:monotonic_time(milli_seconds),
            dbsync_utils:temp_put(last_change, CTime, 0),
            ok
    end,

    NewUntil = case Until of
        infinity ->
            infinity;
        _ ->
            min(Until, Since + 1000)
    end,

    case Start of
        ok ->
            couchdb_datastore_driver:changes_start_link(
                fun
                    (_, stream_ended, _) ->
                        worker_proxy:call(dbsync_worker, {Queue, {cleanup, NewUntil}});
                    (Seq, Doc, Model) ->
                        worker_proxy:call(dbsync_worker, {Queue,
                            #change{seq = couchdb_datastore_driver:normalize_seq(Seq), doc = Doc, model = Model}})
                end, Since, NewUntil, couchdb_datastore_driver:sync_enabled_bucket());
        _ ->
            {ok, already_started}
    end.

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

handle({clear_temp, Key}) ->
    dbsync_utils:temp_clear(Key);

%% Append change to given queue
handle({QueueKey, #change{seq = Seq, doc = #document{key = Key, rev = Rev} = Doc, model = Model} = Change}) ->
    ?debug("[ DBSync ] Received change on queue ~p with seq ~p: ~p", [QueueKey, Seq, Doc]),
    dbsync_utils:temp_put(last_change, erlang:monotonic_time(milli_seconds), 0),
    Rereplication = QueueKey =:= global andalso dbsync_utils:temp_get({replicated, Key, Rev}) =:= true,
    case {has_sync_context(Doc), Rereplication} of
        {true, false} ->
            Ans = case get_space_id(Doc) of
                {ok, <<"spaces">>} ->
                    ?debug("Skipping doc ~p", [Doc]),
                    skip;
                {ok, <<"">>} ->
                    ?debug("Skipping doc ~p", [Doc]),
                    skip;
                {ok, {space_doc, SpaceId}} ->
                    queue_push(QueueKey, {init_batch, Seq}, SpaceId);
                {ok, SpaceId} ->
                    case dbsync_utils:validate_space_access(oneprovider:get_provider_id(), SpaceId) of
                        ok ->
                            queue_push(QueueKey, Change, SpaceId);
                        _ -> skip
                    end;
                {error, not_a_space} ->
                    skip;
                {error, Reason} ->
                    ?error("Unable to find space id for document ~p due to: ~p", [Doc, Reason]),
                    {error, Reason}
            end,
            case Model of
                dbsync_state ->
                    ok;
                _ ->
                    queue_update_until(QueueKey, Seq)
            end,
            Ans;
        {true, true} ->
            ?debug("Rereplication detected, skipping ~p", [Doc]),
            queue_update_until(QueueKey, Seq),
            ok;
        {false, _} ->
            ?debug("Skipping doc ~p", [Doc]),
            ok
    end;

%% Push changes from queue to providers
handle({flush_queue, QueueKey}) ->
    ?debug("[ DBSync ] Flush queue ~p", [QueueKey]),

    FlushInterval = ?FLUSH_QUEUE_INTERVAL,
    FlushAgainAfter = FlushInterval + crypto:rand_uniform(0, round(?FLUSH_QUEUE_INTERVAL / 2)),

    case QueueKey of
        global ->
            CTime = erlang:monotonic_time(milli_seconds),
            case dbsync_utils:temp_get(last_global_flush) of
                FTime when is_integer(FTime),
                    FTime + FlushInterval / 2 > CTime ->
                    ?info("[ DBSync ] Flush loop is too fast, breaking this one."),
                    throw({too_many_flush_loops, {flush_queue, QueueKey}});
                _ ->
                    dbsync_utils:temp_put(last_global_flush, CTime, 0),
                    ensure_global_stream_active()
            end;
        _ -> ok
    end,

    state_update({queue, QueueKey},
        fun(#queue{batch_map = BatchMap, removed = IsRemoved, until = Until} = Queue) ->
            NewBatchMap = maps:map(
                fun(SpaceId, B0) ->
                    B = B0#batch{until = Until},
                    spawn(fun() -> dbsync_proto:send_batch(QueueKey, SpaceId, B) end),

                    case QueueKey of
                        global ->
                            set_current_seq(oneprovider:get_provider_id(), SpaceId, Until);
                        _ -> ok
                    end,

                    #batch{since = Until, until = Until}
                end,
                BatchMap),

            case QueueKey of
                global ->
                    state_put(global_resume_seq, Until);
                _ -> ok
            end,

            case IsRemoved of
                true ->
                    ?info("[ DBSync ] Queue ~p removed!", [QueueKey]),
                    undefined;
                false ->
                    timer:send_after(FlushAgainAfter, whereis(dbsync_worker), {timer, {flush_queue, QueueKey}}),
                    Queue#queue{batch_map = NewBatchMap}
            end;
            (undefined) when QueueKey =:= global ->
                timer:send_after(FlushAgainAfter, whereis(dbsync_worker), {timer, {flush_queue, QueueKey}}),
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
                fun(OldStream) ->
                    case is_valid_stream(OldStream) of
                        true ->
                            OldStream;
                        _ ->
                            {ok, NewStream} = init_stream(Since, infinity, global),
                            NewStream
                    end
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
                timer:send_after(?GLOBAL_STREAM_RESTART_INTERVAL, whereis(dbsync_worker), {sync_timer, {async_init_stream, Since, Until, Queue}}),
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
-spec queue_push(queue(), change() | {init_batch, non_neg_integer()}, SpaceId :: binary()) -> ok.
queue_push(QueueKey, {init_batch, Seq}, SpaceId) ->
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
            Batch0 = maps:get(SpaceId, BatchMap, #batch{since = Since, until = Seq}),
            Batch = Batch0#batch{until = max(Seq, Since)},
            UntilToSet = max(Batch0#batch.until, Seq),
            Queue1#queue{batch_map = maps:put(SpaceId, Batch, BatchMap),
                until = queue_calculate_until(UntilToSet, Queue1)}
        end);
queue_push(QueueKey, #change{seq = Until, doc = #document{key = ChangeKey}} = Change, SpaceId) ->
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
            FilteredChanges = lists:filter(
                fun(#change{doc = #document{key = Key}}) ->
                    ChangeKey /= Key
                end, Batch0#batch.changes),
            ?debug("Changes stream aggregation level: ~p", [length(FilteredChanges) / length(Batch0#batch.changes)]),
            Batch = Batch0#batch{changes = [Change | FilteredChanges], until = max(Until, Since)},
            UntilToSet = max(Batch0#batch.until, Until),
            Queue1#queue{batch_map = maps:put(SpaceId, Batch, BatchMap),
                until = queue_calculate_until(UntilToSet, Queue1)}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Updates queue until field.
%% @end
%%--------------------------------------------------------------------
-spec queue_update_until(queue(), Until :: non_neg_integer()) -> ok.
queue_update_until(QueueKey, Until) ->
    state_update({queue, QueueKey},
        fun(Queue) ->
            Queue1 =
                case Queue of
                    undefined ->
                        #queue{batch_map = #{}};
                    #queue{} = Q -> Q
                end,
            Queue1#queue{until = queue_calculate_until(Until, Queue1)}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Calculates queue until field's value.
%% @end
%%--------------------------------------------------------------------
-spec queue_calculate_until(NewUntil :: non_neg_integer(), #queue{}) -> non_neg_integer().
queue_calculate_until(NewUntil, Queue) ->
    max(NewUntil, Queue#queue.until).


%%--------------------------------------------------------------------
%% @doc
%% Apply whole batch of changes from remote provider and try to consume batches from stash.
%% @end
%%--------------------------------------------------------------------
-spec apply_batch_changes(FromProvider :: oneprovider:id(), SpaceId :: binary(), batch()) ->
    ok | no_return().
apply_batch_changes(FromProvider, SpaceId, #batch{since = Since, until = Until, changes = Changes} = Batch0) ->
    ?debug("Pre-Apply changes from ~p ~p: ~p", [FromProvider, SpaceId, Batch0]),
    ?info("Pre-Apply changes from ~p ~p: ~p:~p", [FromProvider, SpaceId, Since, Until]),

    %% Both providers have to support this space
    ok = dbsync_utils:validate_space_access(oneprovider:get_provider_id(), SpaceId),
    ok = dbsync_utils:validate_space_access(FromProvider, SpaceId),

    NewChanges = lists:sort(lists:flatten(Changes)),
    Batch = Batch0#batch{changes = NewChanges},
    CurrentUntil = get_current_seq(FromProvider, SpaceId),
    stash_batch(FromProvider, SpaceId, Batch, CurrentUntil), % Other batches will not ask for missing changes if long batch is processed

    consume_batches(FromProvider, SpaceId, CurrentUntil, Since, Until).

%%--------------------------------------------------------------------
%% @doc
%% Apply whole batch of changes from remote provider.
%% @end
%%--------------------------------------------------------------------
-spec do_apply_batch_changes(FromProvider :: oneprovider:id(), SpaceId :: binary(), batch()) ->
    ok | no_return().
do_apply_batch_changes(FromProvider, SpaceId, #batch{changes = Changes, since = Since, until = Until} = Batch) ->
    ?debug("Apply changes from ~p ~p: ~p", [FromProvider, SpaceId, Batch]),
    CurrentUntil = get_current_seq(FromProvider, SpaceId),
    case CurrentUntil + 1 < Since of
        true ->
            ?error("Unable to apply changes from provider ~p (space id ~p). Current 'until': ~p, batch 'since': ~p",
                [FromProvider, SpaceId, CurrentUntil, Since]),
            ok;
        false when Until =< CurrentUntil ->
            ?info("Dropping changes {~p, ~p} since current sequence is ~p", [Since, Until, CurrentUntil]),
            retrieve_stashed_batch(FromProvider, SpaceId, Batch),
            ok;
        false ->
            ok = apply_changes(SpaceId, Changes),
            ?info("Changes applied ~p ~p ~p", [FromProvider, SpaceId, Until]),
            update_current_seq(FromProvider, SpaceId, Until),
            retrieve_stashed_batch(FromProvider, SpaceId, Batch)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Gets current version of links' document.
%% @end
%%--------------------------------------------------------------------
-spec forign_links_get(model_behaviour:model_config(), datastore:ext_key()) ->
    {ok, datastore:document()} | {error, Reason :: any()}.
forign_links_get(ModelConfig, Key) ->
    case ets:lookup(?ETS_CACHE_NAME, Key) of
        [] ->
            case couchdb_datastore_driver:get(ModelConfig, couchdb_datastore_driver:default_bucket(), Key) of
                {ok, Doc = #document{key = Key}} ->
                    ets:insert_new(?ETS_CACHE_NAME, {Key, Doc}),
                    {ok, Doc};
                Error ->
                    Error
            end;
        [{_, Doc}] ->
            {ok, Doc}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Saves links document received from other provider and returns current version of given document.
%% @end
%%--------------------------------------------------------------------
-spec forign_links_save(model_behaviour:model_config(), OldRevNum :: non_neg_integer(), datastore:document()) ->
    {ok, datastore:document()} | {error, Reason :: any()}.
forign_links_save(ModelConfig, OldRevNum, Doc = #document{key = Key, rev = {NewRevNum, _}}) ->
    MaxCacheSize = ?CHANGES_STASH_MAX_SIZE_BYTES,
    case ets:info(?ETS_CACHE_NAME, memory) of
        V when V > MaxCacheSize ->
            ets:delete_all_objects(?ETS_CACHE_NAME);
        _ -> ok
    end,


    case couchdb_datastore_driver:force_save(ModelConfig, couchdb_datastore_driver:default_bucket(), Doc) of
        {ok, _} ->
            case OldRevNum of
                _ when OldRevNum > NewRevNum ->
                    ok;
                _ when OldRevNum < NewRevNum ->
                    ets:insert(?ETS_CACHE_NAME, {Key, Doc});
                _ ->
                    ets:delete(?ETS_CACHE_NAME, Key)

            end,
            forign_links_get(ModelConfig, Key);
        Error ->
            ?error("Unable to save forign links document (key ~p) due to ~p", [Key, Error]),
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Normalizes revision number to simple integer format.
%% @end
%%--------------------------------------------------------------------
-spec rev_to_num(binary() | {non_neg_integer(), term()}) -> non_neg_integer().
rev_to_num({Num, _}) ->
    Num;
rev_to_num(Rev) ->
    [Num, _] = binary:split(Rev, <<"-">>),
    binary_to_integer(Num).

%%--------------------------------------------------------------------
%% @doc
%% Apply list of changes from remote provider.
%% @end
%%--------------------------------------------------------------------
-spec apply_changes(SpaceId :: binary(), [change()]) ->
    ok | {error, any()}.
apply_changes(SpaceId, Changes) ->
    apply_changes(SpaceId, Changes, []).
apply_changes(SpaceId,
    [#change{doc = #document{key = Key, value = Value, rev = Rev} = Doc, model = ModelName} = Change | T], Done) ->
    try
        ModelConfig = ModelName:model_init(),

        MainDocKey = case Value of
            #links{} ->
                MDK = Value#links.doc_key,
                file_meta:set_link_context_for_space(SpaceId),

                OldLinksMap = case forign_links_get(ModelConfig, Key) of
                    {ok, #document{value = OldLinks0}} ->
                        OldLinks0;
                    {error, _} ->
                        #links{link_map = #{}, model = ModelName}
                end,
                {AddedMap0, DeletedMap0} = links_utils:diff(OldLinksMap, Value),
                HKs = maps:keys(maps:merge(AddedMap0, DeletedMap0)),

                lists:foreach(fun(LName) ->
                    ok = caches_controller:flush(?GLOBAL_ONLY_LEVEL, ModelName, MDK, LName)
                end, HKs),
                MDK;
            _ ->
                ok = caches_controller:flush(?GLOBAL_ONLY_LEVEL, ModelName, Key),
                Key
        end,
        MyProvId = oneprovider:get_provider_id(),
        ChangedLinks = datastore:run_transaction(ModelName, couchdb_datastore_driver:synchronization_link_key(ModelConfig, MainDocKey), fun() ->
            case Value of
                #links{origin = MyProvId} ->
                    ?warning("Received private, local links change from other provider ~p", [Change]),
                    [];
                #links{origin = Origin} ->
                    {OldLinks, OldRev} = case forign_links_get(ModelConfig, Key) of
                        {ok, #document{value = OldLinks1, rev = RRev}} ->
                            {OldLinks1, rev_to_num(RRev)};
                        {error, _Reason0} ->
                            {#links{link_map = #{}, model = ModelName}, 0}
                    end,
                    {ok, #document{value = CurrentLinks = #links{origin = Origin}}} = forign_links_save(ModelConfig, OldRev, Doc),
                    {AddedMap, DeletedMap} = links_utils:diff(OldLinks, CurrentLinks),
                    ok = dbsync_events:links_changed(Origin, ModelName, MainDocKey, AddedMap, DeletedMap),
                    maps:keys(AddedMap) ++ maps:keys(DeletedMap);
                _ ->
                    {ok, _} = couchdb_datastore_driver:force_save(ModelConfig, Doc),
                    []
            end
        end),

        case Value of
            #links{} ->
                lists:foreach(fun(LName) ->
                    caches_controller:clear(?GLOBAL_ONLY_LEVEL, ModelName, MainDocKey, LName),
                    ok
                end, ChangedLinks);
            _ ->
                ok = caches_controller:clear(?GLOBAL_ONLY_LEVEL, ModelName, Key)
        end,

        dbsync_utils:temp_put({replicated, Key, Rev}, true, timer:minutes(15)),

        apply_changes(SpaceId, T, [Change | Done])
    catch
        _:Reason ->
            ?error_stacktrace("Unable to apply change ~p due to: ~p", [Change, Reason]),
            {error, Reason}
    end;
apply_changes(SpaceId, [], Done) ->
    run_posthooks(SpaceId, lists:reverse(Done)).

%%--------------------------------------------------------------------
%% @doc
%% Apply posthooks for list of changes from remote provider.
%% @end
%%--------------------------------------------------------------------
-spec run_posthooks(SpaceId :: binary(), [change()]) -> ok.
run_posthooks(_, []) ->
    ok;
run_posthooks(SpaceId, [Change | Done]) ->
    spawn(
        fun() ->
            try
                dbsync_events:change_replicated(SpaceId, Change)
            catch
                E1:E2 ->
                    ?error_stacktrace("Change ~p post-processing failed: ~p:~p", [Change, E1, E2])
            end,
            ok
        end),

    run_posthooks(SpaceId, Done).

%%--------------------------------------------------------------------
%% @doc
%% Puts given Value in datastore worker's state
%% @end
%%--------------------------------------------------------------------
-spec state_put(Key :: term(), Value :: term()) -> ok.
state_put(Key, Value) ->
    case dbsync_state:get(Key) of
        {ok, #document{value = #dbsync_state{entry = Value}}} ->
            ok;
        _ ->
            {ok, _} = dbsync_state:save(#document{key = Key, value = #dbsync_state{entry = Value}})
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Gets Value from datastore worker's state
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
                dbsync_state:save(#document{key = Key, value = #dbsync_state{entry = NewValue}})
        end
    end,

    critical_section:run([dbsync_state, term_to_binary(Key)], DoUpdate).


%%--------------------------------------------------------------------
%% @doc
%% Checks if given document should be synced across providers.
%% @end
%%--------------------------------------------------------------------
-spec has_sync_context(datastore:document()) ->
    boolean().
has_sync_context(#document{value = #links{model = ModelName}}) ->
    lists:member(ModelName, ?MODELS_TO_SYNC);
has_sync_context(#document{value = #monitoring_state{monitoring_id = MonitoringId}}) ->
    case MonitoringId of
        #monitoring_id{main_subject_type = space} ->
            true;
        _ ->
            false
    end;
has_sync_context(#document{value = Value}) when is_tuple(Value) ->
    ModelName = element(1, Value),
    lists:member(ModelName, ?MODELS_TO_SYNC).


%%--------------------------------------------------------------------
%% @doc
%% For given document returns file's UUID that is associated with this document in some way.
%% @end
%%--------------------------------------------------------------------
-spec get_sync_context(datastore:document()) ->
    datastore:key() | datastore:document().
get_sync_context(#document{value = #file_meta{}} = Doc) ->
    Doc;
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
    {ok, SpaceId :: binary() | {space_doc, SpaceId :: binary()}} | {error, Reason :: term()}.
get_space_id(#document{value = #monitoring_state{monitoring_id = MonitoringId}}) ->
    #monitoring_id{main_subject_type = space, main_subject_id = SpaceId} = MonitoringId,
    {ok, SpaceId};
get_space_id(#document{value = #change_propagation_controller{space_id = SpaceId}}) ->
    {ok, SpaceId};
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
    {ok, SpaceId :: binary() | {space_doc, SpaceId :: binary()}} | {error, Reason :: term()}.
get_space_id_not_cached(KeyToCache, #document{key = Key} = Doc) ->
    Context = get_sync_context(Doc),
    case file_meta:get_scope(Context) of
        {ok, #document{key = <<"">> = Root}} ->
            try
                SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(Key),
                state_put({sid, KeyToCache}, {space_doc, SpaceId}),
                {ok, {space_doc, SpaceId}}
            catch
                _:_ ->
                    state_put({sid, KeyToCache}, Root),
                    {ok, Root}
            end;
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
    ensure_global_stream_active(),
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
    ensure_global_stream_active(),
    CurrentSeq = get_current_seq(ProviderId, SpaceId),
    ?info("Received status ~p ~p: ~p vs current ~p", [ProviderId, SpaceId, SeqNum, CurrentSeq]),
    case SeqNum > CurrentSeq of
        true ->
            case dbsync_utils:validate_space_access(oneprovider:get_provider_id(), SpaceId) of
                ok ->
                    timer:sleep(timer:seconds(2)),
                    Batches = get_stashed_batches(ProviderId, SpaceId),
                    SortedKeys = lists:sort(maps:keys(Batches)),
                    Stashed = lists:foldl(fun(S, Acc) ->
                        case Acc + 1 >= S of
                            true ->
                                #batch{until = U} = maps:get(S, Batches),
                                max(U, Acc);
                            _ -> Acc
                        end
                    end, get_current_seq(ProviderId, SpaceId), SortedKeys),
                    case Stashed < SeqNum of
                        true ->
                            do_request_changes(ProviderId, Stashed, SeqNum);
                        _ ->
                            ok
                    end;
                {error, space_not_supported_locally} ->
                    ?info("Ignoring space ~p status since it's not supported locally."),
                    ok
            end;
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
    Now = erlang:monotonic_time(milli_seconds),
    %% Wait for {X}ms + {Y}ms per one document
    MaxWaitTime = ?DIRECT_REQUEST_BASE_TIMEOUT + ?DIRECT_REQUEST_PER_DOCUMENT_TIMEOUT * (Until - Since),
    case state_get({do_request_changes, ProviderId, Since}) of
        undefined ->
            state_put({do_request_changes, ProviderId, Since}, {erlang:monotonic_time(milli_seconds), Until}),
            dbsync_proto:changes_request(ProviderId, Since, Until);
        {OldTime, _} when OldTime + MaxWaitTime < Now ->
            state_put({do_request_changes, ProviderId, Since}, {erlang:monotonic_time(milli_seconds), Until}),
            dbsync_proto:changes_request(ProviderId, Since, Until);
        {_, OldUntil} when OldUntil < Until ->
            state_put({do_request_changes, ProviderId, OldUntil}, {erlang:monotonic_time(milli_seconds), Until}),
            dbsync_proto:changes_request(ProviderId, OldUntil, Until);
        _ ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Gets stashed batches from datastore
%% @end
%%--------------------------------------------------------------------
-spec get_stashed_batches(ProviderId :: oneprovider:id(), SpaceId :: binary()) -> Value :: maps:map().
get_stashed_batches(ProviderId, SpaceId) ->
    case dbsync_batches:get({ProviderId, SpaceId}) of
        {ok, #document{value = #dbsync_batches{batches = Value}}} ->
            Value;
        {error, {not_found, _}} ->
            #{}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Stash changes batch in memory for further use.
%% @end
%%--------------------------------------------------------------------
-spec stash_batch(oneprovider:id(), SpaceId :: binary(), Batch :: batch(), CurrentUntil :: non_neg_integer()) ->
    ok | no_return().
stash_batch(ProviderId, SpaceId, Batch = #batch{since = NewSince, until = NewUntil}, CurrentUntil) ->
    {ok, _} = dbsync_batches:create_or_update(
        #document{key = {ProviderId, SpaceId}, value = #dbsync_batches{batches = maps:put(NewSince, Batch, #{})}},
        fun(#dbsync_batches{batches = Batches} = BatchesRecord) ->
            case (CurrentUntil + 1 < NewSince) andalso (size(term_to_binary(Batches)) > ?CHANGES_STASH_MAX_SIZE_BYTES) of
                true -> {ok, BatchesRecord};
                false ->
                    #batch{until = Until} = maps:get(NewSince, Batches, #batch{until = 0}),
                    NewBatches = case NewUntil > Until of
                        true ->
                            maps:put(NewSince, Batch, Batches);
                        false ->
                            Batches
                    end,
                    {ok, BatchesRecord#dbsync_batches{batches = NewBatches}}
            end
        end),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Retrieve changes batch from memory.
%% @end
%%--------------------------------------------------------------------
-spec retrieve_stashed_batch(oneprovider:id(), SpaceId :: binary(), Batch :: batch()) ->
    ok | no_return().
retrieve_stashed_batch(ProviderId, SpaceId, #batch{since = NewSince, until = NewUntil}) ->
    {ok, _} = dbsync_batches:create_or_update(
        #document{key = {ProviderId, SpaceId}, value = #dbsync_batches{batches = #{}}},
        fun(#dbsync_batches{batches = Batches} = BatchesRecord) ->
            #batch{until = Until} = maps:get(NewSince, Batches, #batch{until = 0}),
            NewBatches = case NewUntil >= Until of
                true ->
                    maps:remove(NewSince, Batches);
                false ->
                    Batches
            end,
            {ok, BatchesRecord#dbsync_batches{batches = NewBatches}}
        end),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Consumes batches from batch-stash.
%% @end
%%--------------------------------------------------------------------
-spec consume_batches(oneprovider:id(), SpaceId :: binary(), CurrentUntil :: non_neg_integer(),
    NewBranchSince :: non_neg_integer(), NewBranchUntil :: non_neg_integer()) ->
    ok | no_return().
consume_batches(_, _, CurrentUntil, _, NewBranchUntil) when CurrentUntil >= NewBranchUntil ->
    ok;
consume_batches(ProviderId, SpaceId, CurrentUntil, NewBranchSince, NewBranchUntil) ->
    Batches = get_stashed_batches(ProviderId, SpaceId),
    SortedKeys = lists:sort(maps:keys(Batches)),

    % check if we have all needed changes to start batches application
    Stashed = lists:foldl(fun(S, Acc) ->
        case Acc + 1 >= S of
            true ->
                #batch{until = U} = maps:get(S, Batches),
                max(U, Acc);
            _ -> Acc
        end
    end, CurrentUntil, SortedKeys),
    case Stashed + 1 < NewBranchSince of
        true ->
            % missing batches - check once more (requests may be processed in parallel) and request missing changes
            timer:sleep(timer:seconds(2)),
            NewCurrentUntil = get_current_seq(ProviderId, SpaceId),
            case NewCurrentUntil > CurrentUntil of
                true ->
                    consume_batches(ProviderId, SpaceId, NewCurrentUntil, NewBranchSince, NewBranchUntil);
                _ ->
                    ?info("Missing changes ~p:~p for provider ~p, space ~p", [Stashed, NewBranchSince, ProviderId, SpaceId]),
                    do_request_changes(ProviderId, Stashed, NewBranchSince)
            end;
        _ ->
            % set this pid as batches consumer (only one proc can consume batches)
            MyPid = self(),
            state_update({current_consumer, ProviderId, SpaceId},
                fun
                    ({Pid, To}) ->
                        case is_process_alive(Pid) of
                            true -> {Pid, To};
                            _ -> {MyPid, Stashed}
                        end;
                    (_) ->
                        {MyPid, Stashed}
                end),

            case state_get({current_consumer, ProviderId, SpaceId}) of
                {MyPid, _} ->
                    lists:foreach(fun(Key) ->
                        Batch = maps:get(Key, Batches),
                        do_apply_batch_changes(ProviderId, SpaceId, Batch)
                    end, SortedKeys);
                {_, To} when To < Stashed ->
                    % other proc is working - wait
                    timer:sleep(timer:seconds(5)),
                    NewCurrentUntil = get_current_seq(ProviderId, SpaceId),
                    consume_batches(ProviderId, SpaceId, NewCurrentUntil, NewBranchSince, NewBranchUntil);
                _ ->
                    ok
            end
    end.


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
    CTime = erlang:monotonic_time(milli_seconds),

    %% Check if flush loop works
    MaxFlushDelay = ?FLUSH_QUEUE_INTERVAL * 4,
    case dbsync_utils:temp_get(last_global_flush) of
        LastFlushTime when is_integer(LastFlushTime),
            LastFlushTime + MaxFlushDelay > CTime ->
            ok;
        LastFlushTime ->
            %% Initialize new flush loop
            ?info("[ DBSync ] Flush loop is too slow (last timestamp: ~p vs current: ~p), starting new one.", [LastFlushTime, CTime]),
            timer:send_after(0, whereis(dbsync_worker), {timer, {flush_queue, global}})
    end,

    case is_valid_stream(state_get(changes_stream)) of
        true ->
            MaxIdleTime = timer:minutes(1),
            case dbsync_utils:temp_get(last_change) of
                undefined ->
                    dbsync_utils:temp_put(last_change, CTime, 0);
                Time when Time + MaxIdleTime < CTime ->
                    erlang:exit(state_get(changes_stream), force_restart);
                _ ->
                    ok
            end;
        false ->
            Since = state_get(global_resume_seq),
            timer:send_after(0, whereis(dbsync_worker), {sync_timer, {async_init_stream, Since, infinity, global}}),
            ok
    end.
