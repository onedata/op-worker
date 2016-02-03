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
-compile([export_all]).

-type queue_id() :: binary().
-type queue() :: global | {provider, oneprovider:id(), queue_id()}.
-type change() :: #change{}.
-type batch() :: #batch{}.

-export_type([change/0, batch/0, queue/0]).


%% Record for storing current state of dbsync stream queue.
-record(queue, {
    key,
    since = 0 :: non_neg_integer(),
    batch_map = #{} :: #{},
    last_send,
    removed = false :: boolean()
}).


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
    timer:sleep(5000),
    ?info("[ DBSync ]: Starting dbsync..."),
    timer:send_after(timer:seconds(5), dbsync_worker, {timer, bcast_status}),
    Since = 0,
    state_put(global_resume_seq, Since),
    {ok, ChangesStream} = init_stream(Since, infinity, global),
    {ok, #{changes_stream => ChangesStream}}.

%%--------------------------------------------------------------------
%% @doc
%% Starts changes stream and pushes all changes from the stream to the changes queue.
%% Global queue will be restarted after crash.
%% @end
%%--------------------------------------------------------------------
-spec init_stream(Since :: non_neg_integer(), Until :: non_neg_integer(), Queue :: queue()) ->
    {ok, pid()} | {error, Reason :: term()}.
init_stream(Since, Until, Queue) ->
    ?info("[ DBSync ]: Starting stream ~p ~p ~p", [Since, Until, Queue]),
    case Queue of
        {provider, _, _} ->
            BatchMap = maps:from_list(lists:map(
                fun(SpaceId) ->
                    {SpaceId, #batch{since = Since, until = Since}}
                end, dbsync_utils:get_spaces_for_provider())),
            state_put({queue, Queue}, #queue{batch_map = BatchMap, last_send = now(), since = Since});
        _ -> ok
    end,

    timer:send_after(100, dbsync_worker, {timer, {flush_queue, Queue}}),
    couchdb_datastore_driver:changes_start_link(fun
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
    Request :: ping | healthcheck |
    {driver_call, Module :: atom(), Method :: atom(), Args :: [term()]},
    Result :: nagios_handler:healthcheck_response() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: term(),
    Reason :: term().
handle(ping) ->
    pong;

handle(healthcheck) ->
    ok;

handle({reemit, Msg}) ->
    dbsync_proto:reemit(Msg);

handle(bcast_status) ->
    timer:send_after(timer:seconds(15), dbsync_worker, {timer, bcast_status}),
        catch bcast_status();

handle(requested_bcast_status) ->
    bcast_status();

%% Remove given changes queue
handle({QueueKey, {cleanup, Until}}) ->
    queue_remove(QueueKey, Until);

%% Append change to given queue
handle({QueueKey, #change{seq = Seq, doc = #document{key = Key} = Doc} = Change}) ->
    ?debug("[ DBSync ] Received change on queue ~p with seq ~p: ~p", [QueueKey, Seq, Doc]),
    case is_file_scope(Doc) of
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
    worker_host:state_update(dbsync_worker, {queue, QueueKey},
        fun(#queue{batch_map = BatchMap, removed = IsRemoved} = Queue) ->
            NewBatchMap = maps:map(
                fun(SpaceId, #batch{until = Until} = B) ->
                    dbsync_proto:send_batch(QueueKey, SpaceId, B),
                    update_current_seq(oneprovider:get_provider_id(), SpaceId, Until),
                    #batch{since = Until, until = Until}
                end,
                BatchMap),

            case IsRemoved of
                true ->
                    ?info("[ DBSync ] Queue ~p removed!", [QueueKey]),
                    undefined;
                false ->
                    timer:send_after(200, dbsync_worker, {timer, {flush_queue, QueueKey}}),
                    Queue#queue{batch_map = NewBatchMap}
            end;
            (undefined) when QueueKey =:= global ->
                timer:send_after(200, dbsync_worker, {timer, {flush_queue, QueueKey}}),
                undefined;
            (undefined) ->
                ?warning("Unknown operation on empty queue ~p", [QueueKey]),
                undefined
        end);

%% Handle external dbsync requests
handle({dbsync_request, SessId, DBSyncRequest}) ->
    dbsync_proto:handle(SessId, DBSyncRequest);

%% Handle stream crashes
handle({'EXIT', Stream, Reason}) ->
    case state_get(changes_stream) of
        Stream ->
            Since = state_get(global_resume_seq),
            worker_host:state_update(dbsync_worker, changes_stream,
                fun(_) ->
                    {ok, NewStream} = init_stream(Since, infinity, global),
                    NewStream
                end);
        _ ->
            ?warning("Unknown stream crash ~p: ~p", [Stream, Reason])
    end;
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
    worker_host:state_update(dbsync_worker, {queue, QueueKey},
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
    worker_host:state_update(dbsync_worker, {queue, QueueKey},
        fun(Queue) ->
            Queue1 =
                case Queue of
                    undefined ->
                        #queue{batch_map = #{}, last_send = now()};
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
%% Apply whole batch of changes from remote provider.
%% @end
%%--------------------------------------------------------------------
-spec apply_batch_changes(FromProvider :: oneprovider:id(), SpaceId :: binary(), batch()) ->
    ok | no_return().
apply_batch_changes(FromProvider, SpaceId, #batch{changes = Changes, since = Since, until = Until} = Batch) ->
    ?debug("Apply changes from ~p ~p: ~p", [FromProvider, SpaceId, Batch]),
    CurrentUntil = get_current_seq(FromProvider, SpaceId),
    case CurrentUntil < Since of
        true ->
            ?error("Unable to apply changes from provider ~p (space id ~p). Current 'until': ~p, batch 'since': ~p", [FromProvider, SpaceId, CurrentUntil, Since]),
            stash_batch(FromProvider, SpaceId, Batch),
            do_request_changes(FromProvider, CurrentUntil, Since);
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
apply_changes(SpaceId, [#change{seq = Seq, doc = #document{key = Key} = Doc, model = ModelName} = Change | T]) ->
    try
        ModelConfig = ModelName:model_init(),
        {ok, _} = couchdb_datastore_driver:force_save(ModelConfig, Doc),
            catch caches_controller:flush_document(global_only, ModelName, Key),
            catch caches_controller:flush_document(local_only, ModelName, Key),
        spawn(
            fun() ->
                dbsync_events:change_replicated(SpaceId, Change)
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
    worker_host:state_put(?MODULE, Key, Value).

%%--------------------------------------------------------------------
%% @doc
%% Puts Value from datastore worker's state
%% @end
%%--------------------------------------------------------------------
-spec state_get(Key :: term()) -> Value :: term().
state_get(Key) ->
    worker_host:state_get(?MODULE, Key).


%%--------------------------------------------------------------------
%% @doc
%% Checks if given document should be synced across providers.
%% @end
%%--------------------------------------------------------------------
-spec is_file_scope(datastore:document()) ->
    boolean().
is_file_scope(#document{value = #links{model = ModelName}}) ->
    lists:member(ModelName, ?MODELS_TO_SYNC);
is_file_scope(#document{value = Value}) when is_tuple(Value) ->
    ModelName = element(1, Value),
    lists:member(ModelName, ?MODELS_TO_SYNC).


%%--------------------------------------------------------------------
%% @doc
%% For given document returns file's UUID that is associated with this document in some way.
%% @end
%%--------------------------------------------------------------------
-spec get_file_scope(datastore:document()) ->
    datastore:key().
get_file_scope(#document{key = Key, value = #file_meta{}}) ->
    Key;
get_file_scope(#document{value = #links{key = DocKey, model = file_meta}}) ->
    DocKey;
get_file_scope(#document{value = #links{key = DocKey, model = file_location}}) ->
    #model_config{store_level = StoreLevel} = file_location:model_init(),
    {ok, #document{value = #file_location{}} = Doc} = datastore:get(StoreLevel, file_location, DocKey),
    get_file_scope(Doc);
get_file_scope(#document{value = #file_location{uuid = FileUUID}}) ->
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
    case state_get({space_id, Key}) of
        undefined ->
            get_space_id_not_cached(Key, Doc);
        SpaceId ->
            {ok, SpaceId}
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
    FileUUID = get_file_scope(Doc),
    case file_meta:get_scope({uuid, FileUUID}) of
        {ok, #document{key = <<"">> = Root}} ->
            state_put({space_id, KeyToCache}, Root),
            {ok, Root};
        {ok, #document{key = ScopeUUID}} ->
            try
                SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(ScopeUUID),
                state_put({space_id, KeyToCache}, SpaceId),
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
    case self() =:= whereis(dbsync_worker) of
        true ->
            state_put(Key, OP(state_get(Key)));
        false ->
            worker_host:state_update(dbsync_worker, Key, OP)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Gets sequence number for local provider and given Space.
%% @end
%%--------------------------------------------------------------------
-spec get_current_seq(SpaceId :: binary()) ->
    non_neg_integer().
get_current_seq(SpaceId) ->
    case get_current_seq(oneprovider:get_provider_id(), SpaceId) of
        undefined -> 0;
        O -> O
    end.

%%--------------------------------------------------------------------
%% @doc
%% Gets sequence number for given Provider and Space.
%% @end
%%--------------------------------------------------------------------
-spec get_current_seq(oneprovider:id(), SpaceId :: binary()) ->
    non_neg_integer().
get_current_seq(ProviderId, SpaceId) ->
    case worker_host:state_get(dbsync_worker, {current_seq, ProviderId, SpaceId}) of
        undefined -> 0;
        O -> O
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
            ?info("DBSync broadcast for space ~p: ~p", [SpaceId, CurrentSeq]),
            {ok, Providers} = gr_spaces:get_providers(provider, SpaceId),
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
%% Request changes for specific provider.
%% @end
%%--------------------------------------------------------------------
-spec do_request_changes(oneprovider:id(), Since :: non_neg_integer(), Until :: non_neg_integer()) ->
    ok | no_return().
do_request_changes(ProviderId, Since, Until) ->
    dbsync_proto:changes_request(ProviderId, Since, Until).


%%--------------------------------------------------------------------
%% @doc
%% Stash changes batch in memory for further use.
%% @end
%%--------------------------------------------------------------------
-spec stash_batch(oneprovider:id(), SpaceId :: binary(), Batch :: batch()) ->
    ok | no_return().
stash_batch(ProviderId, SpaceId, Batch) ->
    worker_host:state_update(dbsync_worker, {stash, ProviderId, SpaceId},
        fun(undefined) ->
            [Batch];
            (Batches) ->
                [Batch | Batches]
        end).


