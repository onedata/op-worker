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

init_stream(Since, Until, Queue) ->
    ?info("[ DBSync ]: Starting stream ~p ~p ~p", [Since, Until, Queue]),
    case Queue of
        {provider, _, _} ->
            BatchMap = maps:from_list(lists:map(
                fun(SpaceId) ->
                    {SpaceId, #batch{since = Since, until = Since}}
                end, get_spaces_for_provider())),
            state_put({queue, Queue}, #queue{batch_map = BatchMap, last_send = now(), since = Since});
        _ -> ok
    end,
%%
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
    ?info("ZOMFG"),
    timer:send_after(timer:seconds(15), dbsync_worker, {timer, bcast_status}),
    catch bcast_status();

handle(requested_bcast_status) ->
    bcast_status();


handle({QueueKey, {cleanup, Until}}) ->
    queue_remove(QueueKey, Until);
handle({QueueKey, #change{seq = Seq, doc = #document{key = Key} = Doc} = Change}) ->
    ?info("[ DBSync ] Received change on queue ~p with seq ~p: ~p", [QueueKey, Seq, Doc]),
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
                {error, not_a_space} ->
                    skip;
                {error, Reason} ->
                    ?error("Unable to find space id for document ~p due to: ~p", [Doc, Reason]),
                    {error, Reason}
            end ;
        false ->
            ?info("Skipping3 doc ~p", [Doc]),
            ok
    end;

handle({flush_queue, QueueKey}) ->
    ?debug("[ DBSync ] Flush queue ~p", [QueueKey]),
    worker_host:state_update(dbsync_worker, {queue, QueueKey},
        fun(#queue{batch_map = BatchMap, removed = IsRemoved} = Queue) ->
%%            ?info("[ DBSync ] Flush queue ~p ~p", [QueueKey, BatchToSend]),
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
handle({dbsync_request, SessId, DBSyncRequest}) ->
    dbsync_proto:handle(SessId, DBSyncRequest);
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
                    ?info("Removing queue wuth Batch ~p", [NewBatchMap]),
                    Q#queue{removed = true, batch_map = NewBatchMap}
            end
        end).

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
            Batch  = Batch0#batch{changes = [Change | Batch0#batch.changes], until = Until},
            Queue1#queue{batch_map = maps:put(SpaceId, Batch, BatchMap)}
        end).


apply_batch_changes(FromProvider, SpaceId, #batch{changes = Changes, since = Since, until = Until} = Batch) ->
    ?info("Apply changes from ~p ~p: ~p", [FromProvider, SpaceId, Batch]),
    CurrentUntil = get_current_seq(FromProvider, SpaceId),
    case CurrentUntil < Since of
        true ->
            ?error("Unable to apply changes from provider ~p (space id ~p). Current 'until': ~p, batch 'since': ~p", [FromProvider, SpaceId, CurrentUntil, Since]),
            stash_batch(FromProvider, SpaceId, Batch),
            do_request_changes(FromProvider, CurrentUntil, Since);
        false ->
            ok = apply_changes(SpaceId, Changes),
            ?info("Changes applied ~p ~p ~p", [FromProvider, SpaceId, Until]),
            update_current_seq(FromProvider, SpaceId, Until)
    end.

apply_changes(SpaceId, [#change{seq = Seq, doc = #document{key = Key} = Doc, model = ModelName} = Change | T]) ->
%%    ?info("Apply in ~p change ~p", [SpaceId, Change]),
    try
        ModelConfig = ModelName:model_init(),
        {ok, _} = couchdb_datastore_driver:force_save(ModelConfig, Doc),
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


update_current_seq(ProviderId, SpaceId, SeqNum) ->
    Key = {current_seq, ProviderId, SpaceId},
    OP =
        fun (undefined) ->
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

get_current_seq(SpaceId) ->
    case get_current_seq(oneprovider:get_provider_id(), SpaceId) of
        undefined -> 0;
        O -> O
    end.

get_current_seq(ProviderId, SpaceId) ->
    case worker_host:state_get(dbsync_worker, {current_seq, ProviderId, SpaceId}) of
        undefined -> 0;
        O -> O
    end.



bcast_status() ->
    SpaceIds = get_spaces_for_provider(),
    lists:foreach(
        fun(SpaceId) ->
            CurrentSeq = get_current_seq(SpaceId),
            ?info("BCast ~p ~p", [CurrentSeq, SpaceId]),
            {ok, Providers} = gr_spaces:get_providers(provider, SpaceId),
            dbsync_proto:status_report(SpaceId, Providers -- [oneprovider:get_provider_id()], CurrentSeq)
        end, SpaceIds).


on_status_received(ProviderId, SpaceId, SeqNum) ->
    CurrentSeq = get_current_seq(ProviderId, SpaceId),
    ?info("Received status ~p ~p: ~p vs current ~p", [ProviderId, SpaceId, SeqNum, CurrentSeq]),
    case SeqNum > CurrentSeq of
        true ->
            do_request_changes(ProviderId, CurrentSeq, SeqNum);
        false ->
            ok
    end.

do_request_changes(ProviderId, Since, Until) ->
    dbsync_proto:changes_request(ProviderId, Since, Until).


stash_batch(ProviderId, SpaceId, Batch) ->
    worker_host:state_update(dbsync_worker, {stash, ProviderId, SpaceId},
        fun (undefined) ->
            [Batch];
            (Batches) ->
                [Batch | Batches]
        end).


get_spaces_for_provider() ->
    get_spaces_for_provider(oneprovider:get_provider_id()).

get_spaces_for_provider(ProviderId) ->
    {ok, SpaceIds} = gr_providers:get_spaces(provider),
    lists:foldl(
        fun(SpaceId, Acc) ->
            {ok, Providers} = gr_spaces:get_providers(provider, SpaceId),
            case lists:member(ProviderId, Providers) of
                true -> [SpaceId | Acc];
                false -> Acc
            end
        end, [], SpaceIds).