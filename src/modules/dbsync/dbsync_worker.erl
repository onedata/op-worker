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
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).
-export([state_get/1, state_put/2]).
-export([apply_batch_changes/2, init_stream/3]).
-export([bcast_status/0, on_status_received/2]).

-define(MODELS_TO_SYNC, [file_meta, file_location]).

-record(change, {
    seq,
    doc,
    model
}).

-record(seq_range, {
    since,
    until
}).

-record(batch, {
    changes = #{},
    since,
    until
}).


-record(queue, {
    key,
    current_batch,
    last_send,
    removed = false
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
    timer:send_after(100, dbsync_worker, {timer, bcast_status}),
    Since = 0,
    {ok, ChangesStream} = init_stream(Since, infinity, global),
    {ok, #{changes_stream => ChangesStream, {queue, global} => #queue{last_send = now(), current_batch = #batch{since = Since}}}}.

init_stream(Since, Until, Queue) ->
    ?info("[ DBSync ]: Starting stream ~p ~p ~p", [Since, Until, Queue]),
    timer:send_after(100, dbsync_worker, {timer, {flush_queue, Queue}}),
    couchdb_datastore_driver:changes_start_link(fun
        (_, stream_ended, _) ->
            worker_proxy:call(dbsync_worker, {Queue, cleanup});
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
    ok = bcast_status(),
    timer:send_after(timer:seconds(5), dbsync_worker, {timer, bcast_status});

handle(requested_bcast_status) ->
    ok = bcast_status();


handle({QueueKey, cleanup}) ->
    queue_remove(QueueKey);
handle({QueueKey, #change{seq = Seq, doc = #document{key = Key} = Doc} = Change}) ->
%%    ?info("[ DBSync ] Received change on queue ~p with seq ~p: ~p", [QueueKey, Seq, Doc]),
    case is_file_scope(Doc) of
        true ->
            case get_space_id(Doc) of
                {ok, <<"spaces">>} ->
                    ?info("Skipping1 doc ~p", [Doc]),
                    skip;
                {ok, <<"">>} ->
                    ?info("Skipping2 doc ~p", [Doc]),
                    skip;
                {ok, SpaceId} ->
%%                    ?info("Document ~p assigned to space ~p", [Key, SpaceId]),
                    queue_push(QueueKey, Change, SpaceId);
                {error, Reason} ->
                    ?error("Unable to find space id for document ~p due to: ~p", [Doc, Reason]),
                    {error, Reason}
            end ;
        false ->
            ?info("Skipping3 doc ~p", [Doc]),
            ok
    end;

handle({flush_queue, QueueKey}) ->
    ?info("[ DBSync ] Flush queue ~p", [QueueKey]),
    worker_host:state_update(dbsync_worker, {queue, QueueKey},
        fun(#queue{current_batch = #batch{until = Until} = BatchToSend, removed = IsRemoved} = Queue) ->
%%            ?info("[ DBSync ] Flush queue ~p ~p", [QueueKey, BatchToSend]),
            dbsync_proto:send_batch(QueueKey, BatchToSend),
            case IsRemoved of
                true ->
                    ?info("[ DBSync ] Queue ~p removed!", [QueueKey]),
                    undefined;
                false ->
                    timer:send_after(200, dbsync_worker, {timer, {flush_queue, QueueKey}}),
                    Queue#queue{current_batch = #batch{since = Until, until = Until}}
            end
        end);
handle({dbsync_request, SessId, DBSyncRequest}) ->
    dbsync_proto:handle(SessId, DBSyncRequest);
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

queue_remove(QueueKey) ->
    worker_host:state_update(dbsync_worker, {queue, QueueKey},
        fun(Queue) ->
            case Queue of
                undefined ->
                    undefined;
                #queue{} = Q -> Q#queue{removed = true}
            end
        end).

queue_push(QueueKey, #change{seq = Since} = Change, SpaceId) ->
    worker_host:state_update(dbsync_worker, {queue, QueueKey},
        fun(Queue) ->
            Queue1 =
                case Queue of
                    undefined ->
                        #queue{last_send = now(), current_batch = #batch{since = Since, until = Since}};
                    #queue{} = Q -> Q
                end,
            CurrentBatch = Queue1#queue.current_batch,
            ChangesList = maps:get(SpaceId, CurrentBatch#batch.changes, []),
            Queue1#queue{current_batch = CurrentBatch#batch{until = Since, changes = maps:put(SpaceId, [Change | ChangesList], CurrentBatch#batch.changes)}}
        end).


apply_batch_changes(FromProvider, #batch{changes = Changes, since = Since, until = Until} = Batch) ->
    CurrentUntil = get_current_seq(FromProvider),
    case CurrentUntil < Since of
        true ->
            ?error("Unable to apply changes from provider ~p. Current 'until': ~p, batch 'since': ~p", [FromProvider, CurrentUntil, Since]),
            stash_batch(FromProvider, Batch),
            do_request_changes(FromProvider, CurrentUntil, Since);
        false ->
            lists:foreach(fun({SpaceId, ChangeList}) ->
                ok = apply_changes(SpaceId, ChangeList)
                          end, maps:to_list(Changes)),
            update_current_seq(oneprovider:get_provider_id(), Until)
    end.

apply_changes(SpaceId, [#change{doc = #document{key = Key} = Doc, model = ModelName} = Change | T]) ->
%%    ?info("Apply in ~p change ~p", [SpaceId, Change]),
    try
        ModelConfig = ModelName:model_init(),
        {ok, _} = couchdb_datastore_driver:force_save(ModelConfig, Doc),
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


is_file_scope(#document{value = #links{model = ModelName}}) ->
    lists:member(ModelName, ?MODELS_TO_SYNC);
is_file_scope(#document{value = Value}) when is_tuple(Value) ->
    ModelName = element(1, Value),
    lists:member(ModelName, ?MODELS_TO_SYNC).


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


get_space_id(#document{key = Key} = Doc) ->
    case state_get({space_id, Key}) of
        undefined ->
            get_space_id_not_cached(Key, Doc);
        SpaceId ->
            {ok, SpaceId}
    end.

get_space_id_not_cached(KeyToCache, #document{} = Doc) ->
    FileUUID = get_file_scope(Doc),
    case file_meta:get_scope({uuid, FileUUID}) of
        {ok, #document{key = SpaceId}} ->
            state_put({space_id, KeyToCache}, SpaceId),
            {ok, SpaceId};
        {error, Reason} ->
            {error, Reason}
    end.


update_current_seq(ProviderId, SeqNum) ->
    worker_host:state_update(dbsync_worker, {current_seq, ProviderId},
        fun (undefined) ->
            SeqNum;
            (CurrentSeq) ->
                max(SeqNum, CurrentSeq)
        end).

get_current_seq() ->
    get_current_seq(oneprovider:get_provider_id()).

get_current_seq(ProviderId) ->
    worker_host:state_get(dbsync_worker, {current_seq, ProviderId}).



bcast_status() ->
    CurrentSeq = get_current_seq(),
    dbsync_proto:status_report(CurrentSeq).


on_status_received(ProviderId, SeqNum) ->
    CurrentSeq = get_current_seq(ProviderId),
    case SeqNum > CurrentSeq of
        true ->
            do_request_changes(ProviderId, CurrentSeq, SeqNum);
        false ->
            ok
    end.

do_request_changes(ProviderId, Since, Until) ->
    dbsync_proto:changes_request(ProviderId, Since, Until).


stash_batch(ProviderId, Batch) ->
    worker_host:state_update(dbsync_worker, {stash, ProviderId},
        fun (undefined) ->
            [Batch];
            (Batches) ->
                [Batch | Batches]
        end).
