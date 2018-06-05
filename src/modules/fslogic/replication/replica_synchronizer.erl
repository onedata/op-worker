%%%--------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Creates a process per FileGuid/SessionId that handles remote file
%%% synchronization by *this user* (more specifically: this session),
%%% for *this file*.
%%% @end
%%%--------------------------------------------------------------------
-module(replica_synchronizer).
-author("Konrad Zemek").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").

-behaviour(gen_server).

-define(MINIMAL_SYNC_REQUEST, application:get_env(?APP_NAME, minimal_sync_request, 4194304)).
-define(TRIGGER_BYTE, application:get_env(?APP_NAME, trigger_byte, 52428800)).
-define(PREFETCH_SIZE, application:get_env(?APP_NAME, prefetch_size, 104857600)).

%% The process is supposed to die after ?DIE_AFTER time of idling (no requests in flight)
-define(DIE_AFTER, 60000).

%% How long transfer stats are aggregated before updating transfer document
-define(STATS_AGGREGATION_TIME, application:get_env(
    ?APP_NAME, rtransfer_stats_aggregation_time, 1000)
).
-define(FLUSH_STATS, flush_stats).

-type fetch_ref() :: reference().
-type block() :: fslogic_blocks:block().
-type from() :: {pid(), any()}. %% `From` argument to gen_server:call callback

-record(state, {
    file_ctx :: file_ctx:ctx(),
    file_guid :: fslogic_worker:file_guid(),
    space_id :: od_space:id(),
    dest_storage_id :: storage:id(),
    dest_file_id :: helpers:file_id(),
    last_transfer :: undefined | block(),
    in_progress :: ordsets:ordset({block(), fetch_ref()}),
    in_sequence_hits = 0 :: non_neg_integer(),
    from_to_refs = #{} :: #{from() => [fetch_ref()]},
    ref_to_froms = #{} :: #{fetch_ref() => [from()]},
    from_sessions = #{} :: #{from() => session:id()},
    from_to_transfer_id = #{} :: #{from() => transfer:id() | undefined},
    transfer_id_to_from = #{} :: #{transfer:id() | undefined => from()},
    cached_stats = #{} :: #{transfer:id() => #{od_provider:id() => [block()]}},
    caching_timer :: undefined | reference()
}).

-export([synchronize/5, update_replica/4, cancel/1, init/1,
    handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2,
    init_or_return_existing/2, flush_location/1, apply/2, apply/3]).
-export([synchronize_internal/5, flush_location_internal/1, apply_internal/2,
    apply_internal/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv synchronize_internal(UserCtx, FileCtx, Block, Prefetch, TransferId)
%% on chosen node.
%% @end
%%--------------------------------------------------------------------
-spec synchronize(user_ctx:ctx(), file_ctx:ctx(), block(),
    Prefetch :: boolean(), transfer:id() | undefined) ->
    {ok, #file_location_changed{}} | {error, Reason :: any()}.
synchronize(UserCtx, FileCtx, Block, Prefetch, TransferId) ->
    Uuid = file_ctx:get_uuid_const(FileCtx),
    Node = consistent_hasing:get_node(Uuid),
    rpc:call(Node, ?MODULE, synchronize_internal,
        [UserCtx, FileCtx, Block, Prefetch, TransferId]).

%%--------------------------------------------------------------------
%% @doc
%% Blocks until a block is synchronized from a remote provider with
%% a newer version of data.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_internal(user_ctx:ctx(), file_ctx:ctx(), block(),
    Prefetch :: boolean(), transfer:id() | undefined) ->
    {ok, #file_location_changed{}} | {error, Reason :: any()}.
synchronize_internal(UserCtx, FileCtx, Block, Prefetch, TransferId) ->
    EnlargedBlock = enlarge_block(Block, Prefetch),
    try
        {ok, Process} = get_process(UserCtx, FileCtx),
        SessionId = user_ctx:get_session_id(UserCtx),
        gen_server2:call(Process, {synchronize, FileCtx, EnlargedBlock,
            Prefetch, TransferId, SessionId}, infinity)
    catch
        %% The process we called was already terminating because of idle timeout,
        %% there's nothing to worry about.
        exit:{{shutdown, timeout}, _} ->
            ?debug("Process stopped because of a timeout, retrying with a new one"),
            synchronize_internal(UserCtx, FileCtx, Block, Prefetch, TransferId)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv update_replica_internal(FileCtx, Blocks, FileSize, BumpVersion)
%% on chosen node.
%% @end
%%--------------------------------------------------------------------
-spec update_replica(file_ctx:ctx(), fslogic_blocks:blocks(),
    FileSize :: non_neg_integer() | undefined, BumpVersion :: boolean()) ->
    {ok, size_changed} | {ok, size_not_changed} | {error, Reason :: term()}.
update_replica(FileCtx, Blocks, FileSize, BumpVersion) ->
    ?MODULE:apply(FileCtx, fun() ->
        replica_updater:update(FileCtx, Blocks, FileSize, BumpVersion)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronically cancels a transfer in a best-effort manner.
%% @end
%%--------------------------------------------------------------------
-spec cancel(transfer:id()) -> ok.
cancel(TransferId) ->
    lists:foreach(
        fun(Pid) -> gen_server2:cast(Pid, {cancel, TransferId}) end,
        gproc:lookup_pids({c, l, TransferId})).

%%--------------------------------------------------------------------
%% @doc
%% @equiv flush_location_internal(Uuid) on chosen node.
%% @end
%%--------------------------------------------------------------------
-spec flush_location(file_meta:uuid()) -> ok.
flush_location(Uuid) ->
    Node = consistent_hasing:get_node(Uuid),
    rpc:call(Node, ?MODULE, flush_location_internal, [Uuid]).

%%--------------------------------------------------------------------
%% @doc
%% Forces location cache flush.
%% @end
%%--------------------------------------------------------------------
-spec flush_location_internal(file_meta:uuid()) -> ok.
flush_location_internal(Uuid) ->
%%  ?info("aaaaa ~p", [erlang:process_info(self(), current_stacktrace)]),
    case gproc:lookup_local_name({Uuid, undefined}) of
        undefined ->
            ok;
        Process ->
            gen_server2:call(Process, flush_location, infinity)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv apply_internal(Uuid, Fun1, Fun2) on chosen node.
%% @end
%%--------------------------------------------------------------------
-spec apply(file_meta:uuid(), fun(() -> term()), fun(() -> term())) ->
    term().
apply(Uuid, Fun1, Fun2) ->
    Node = consistent_hasing:get_node(Uuid),
    rpc:call(Node, ?MODULE, apply_internal, [Uuid, Fun1, Fun2]).

%%--------------------------------------------------------------------
%% @doc
%% Applies function Fun1 on synchronizer if it exists,
%% applies Fun2 on caller proc otherwise.
%% @end
%%--------------------------------------------------------------------
-spec apply_internal(file_meta:uuid(), fun(() -> term()), fun(() -> term())) ->
    term().
apply_internal(Uuid, Fun1, Fun2) ->
    case gproc:lookup_local_name({Uuid, undefined}) of
        undefined ->
            Fun2();
        Process ->
            gen_server2:call(Process, {apply, Fun1}, infinity)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv apply_internal(Uuid, Fun1) on chosen node.
%% @end
%%--------------------------------------------------------------------
-spec apply(file_ctx:ctx(), fun(() -> term())) ->
    term().
apply(FileCtx, Fun) ->
    case get(file_locations) of
        undefined ->
            Uuid = file_ctx:get_uuid_const(FileCtx),
            Node = consistent_hasing:get_node(Uuid),
            rpc:call(Node, ?MODULE, apply_internal, [FileCtx, Fun]);
        _ ->
            Fun()
    end.

%%--------------------------------------------------------------------
%% @doc
%% Applies function Fun on synchronizer.
%% @end
%%--------------------------------------------------------------------
-spec apply_internal(file_ctx:ctx(), fun(() -> term())) ->
    term().
apply_internal(FileCtx, Fun) ->
    try
        UserCtx = undefined,
        {ok, Process} = get_process(UserCtx, FileCtx),
        gen_server2:call(Process, {apply, Fun}, infinity)

    catch
        %% The process we called was already terminating because of idle timeout,
        %% there's nothing to worry about.
        exit:{{shutdown, timeout}, _} ->
            ?debug("Process stopped because of a timeout, retrying with a new one"),
            apply_internal(FileCtx, Fun)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init({user_ctx:ctx(), file_ctx:ctx()}) ->
    {ok, #state{}, Timeout :: non_neg_integer()}.
% TODO VFS-4412 - delete UserCtx
init({_UserCtx, FileCtx}) ->
    fslogic_blocks:init_cache(),
    %% trigger creation of local file location
    {_LocalDoc, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx2),
    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    {DestStorageId, FileCtx3} = file_ctx:get_storage_id(FileCtx2),
    {DestFileId, FileCtx4} = file_ctx:get_storage_file_id(FileCtx3),
    {ok, #state{file_ctx = FileCtx4,
        file_guid = FileGuid,
        space_id = SpaceId,
        dest_storage_id = DestStorageId,
        dest_file_id = DestFileId,
        in_progress = ordsets:new()}, ?DIE_AFTER}.

%%--------------------------------------------------------------------
%% @doc
%% Synchronize a file block.
%% Only schedules synchronization of fragments where newer data is
%% available on other providers AND they are not already being
%% synchronized by this process.
%% The caller is notified once whole range he requested is
%% synchronized to the newest version.
%% @end
%%--------------------------------------------------------------------
handle_call({synchronize, FileCtx, Block, Prefetch, TransferId, Session}, From,
    #state{from_sessions = FS} = State0) ->
    {message_queue_len, QL} = erlang:process_info(self(), message_queue_len),
    State = State0#state{file_ctx = FileCtx},
    TransferId =/= undefined andalso (catch gproc:add_local_counter(TransferId, 1)),
    OverlappingInProgress = find_overlapping(Block, State),
    {OverlappingBlocks, ExistingRefs} = lists:unzip(OverlappingInProgress),
    Holes = get_holes(Block, OverlappingBlocks),
    NewTransfers = start_transfers(Holes, TransferId, Prefetch, State),
    {_, NewRefs} = lists:unzip(NewTransfers),
    case ExistingRefs ++ NewRefs of
        [] ->
            {FileLocation, _FileCtx2} =
                file_ctx:get_or_create_local_file_location_doc(FileCtx),
            ReturnedBlocks = fslogic_blocks:get_blocks(FileLocation,
                #{overlapping_sorted_blocks => [Block]}),
            {EventOffset, EventSize} =
                fslogic_blocks:get_blocks_range(ReturnedBlocks, [Block]),
            FLC = #file_location_changed{
                file_location = fslogic_blocks:set_final_blocks(
                    FileLocation#document.value, ReturnedBlocks),
                change_beg_offset = EventOffset, change_end_offset = EventSize},
            {reply, {ok, FLC}, State, ?DIE_AFTER};
        RelevantRefs ->
            State1 = associate_from_with_refs(From, RelevantRefs, State),
            State2 = associate_from_with_tid(From, TransferId, State1),
            State3 = add_in_progress(NewTransfers, State2),
            State4 = adjust_sequence_hits(Block, NewRefs, State3),
            State5 = prefetch(NewTransfers, TransferId, Prefetch, State4),
            State6 = State5#state{from_sessions = maps:put(From, Session, FS)},
            {noreply, State6, ?DIE_AFTER}
    end;

handle_call(flush_location, _From, State) ->
    Ans = fslogic_blocks:flush(),
    {reply, Ans, State, ?DIE_AFTER};

handle_call({apply, Fun}, _From, State) ->
    Ans = Fun(),
    {reply, Ans, State, ?DIE_AFTER}.

handle_cast({cancel, TransferId}, State) ->
    try
        NewState = cancel_transfer_id(TransferId, State),
        {noreply, NewState, ?DIE_AFTER}
    catch
        _:Reason ->
            ?error_stacktrace("Unable to cancel ~p: ~p", [TransferId, Reason]),
            {noreply, State, ?DIE_AFTER}
    end;

handle_cast(Msg, State) ->
    ?log_bad_request(Msg),
    {noreply, State, ?DIE_AFTER}.

handle_info(timeout, #state{in_progress = []} = State) ->
    ?debug("Exiting due to inactivity with state: ~p", [State]),
    {stop, {shutdown, timeout}, State};

handle_info(timeout, State) ->
    {noreply, State, ?DIE_AFTER};

handle_info({Ref, active, ProviderId, Block}, State) ->
    #state{ref_to_froms = RefToFroms, from_to_transfer_id = FromToTransferId} = State,
    AffectedFroms = maps:get(Ref, RefToFroms, []),
    TransferIds = case maps:values(maps:with(AffectedFroms, FromToTransferId)) of
        [] -> [undefined];
        Values -> Values
    end,
    {noreply, cache_stats(TransferIds, ProviderId, Block, State), ?DIE_AFTER};

handle_info(?FLUSH_STATS, State) ->
    {_, State2} = flush_stats(State, []),
    {noreply, State2, ?DIE_AFTER};

handle_info({Ref, complete, {ok, _} = _Status}, #state{from_sessions = FS} = State) ->
    #state{from_to_transfer_id = FromToTransferId} = State,
    {__Block, __AffectedFroms, FinishedFroms, State1} = disassociate_ref(Ref, State),

    {ExcludeSessions, FS2} = lists:foldl(fun(From, {Acc, TmpFS}) ->
        case maps:get(From, TmpFS, undefined) of
            undefined ->
                {Acc, TmpFS};
            S ->
                {[S | Acc -- [S]], maps:remove(From, TmpFS)}
        end
    end, {[], FS}, FinishedFroms),

    {Location, State2} = flush_stats(State1, ExcludeSessions),
    TransferIds = maps:with(FinishedFroms, FromToTransferId),
    [transfer:increase_files_transferred_counter(TID) || TID <- maps:values(TransferIds)],
    [gen_server2:reply(From, {ok, Location}) || From <- FinishedFroms],
    {noreply, State2#state{from_sessions = FS2}, ?DIE_AFTER};

handle_info({FailedRef, complete, {error, disconnected}}, State) ->
    {Block, AffectedFroms, _FinishedFroms, State1} = disassociate_ref(FailedRef, State),
    NewTransfers = start_transfers([Block], undefined, false, State1),

    ?warning("Replaced failed transfer ~p (~p) with new transfers ~p",
        [FailedRef, Block, NewTransfers]),

    {_, NewRefs} = lists:unzip(NewTransfers),
    State2 = associate_froms_with_refs(AffectedFroms, NewRefs, State1),
    State3 = add_in_progress(NewTransfers, State2),
    {noreply, State3};

handle_info({Ref, complete, ErrorStatus}, State) ->
    ?error("Transfer ~p failed: ~p", [Ref, ErrorStatus]),
    {_Block, AffectedFroms, FinishedFroms, State1} = disassociate_ref(Ref, State),
    State2 = disassociate_transfer_ids(FinishedFroms, State1),

    %% Cancel TransferIds that are still left (i.e. the failed ref was only a part of the transfer)
    AffectedTransferIds = maps:values(maps:with(AffectedFroms, State2#state.from_to_transfer_id)),
    [gen_server2:reply(From, ErrorStatus) || From <- FinishedFroms],
    State3 = lists:foldl(fun cancel_transfer_id/2, State2, AffectedTransferIds),

    {noreply, State3, ?DIE_AFTER};

handle_info(check_flush, State) ->
    fslogic_blocks:check_flush(),
    {noreply, State, ?DIE_AFTER};

handle_info(Msg, State) ->
    ?log_bad_request(Msg),
    {noreply, State, ?DIE_AFTER}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    flush_stats(State, []),
    fslogic_blocks:flush(),
    ignore.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes a transfer request from most of the internal state and
%% returns From tuples for further handling.
%% @end
%%--------------------------------------------------------------------
-spec disassociate_ref(fetch_ref(), #state{}) -> {block(), AffectedFroms :: [from()],
    FinishedFroms :: [from()], #state{}}.
disassociate_ref(Ref, State) ->
    {Block, InProgress} =
        case lists:keytake(Ref, 2, State#state.in_progress) of
            {value, {B, _}, IP} -> {B, IP};
            false -> {undefined, State#state.in_progress}
        end,
    AffectedFroms = maps:get(Ref, State#state.ref_to_froms, []),
    RefToFroms = maps:remove(Ref, State#state.ref_to_froms),
    {FinishedFroms, FromToRefs} =
        lists:foldl(
            fun(From, {FF, FTR}) ->
                WaitingForRefs = maps:get(From, FTR, []),
                case lists:delete(Ref, WaitingForRefs) of
                    [] -> {[From | FF], maps:remove(From, FTR)};
                    Remaining -> {FF, maps:put(From, Remaining, FTR)}
                end
            end,
            {[], State#state.from_to_refs},
            AffectedFroms),
    {Block, AffectedFroms, FinishedFroms, State#state{in_progress = InProgress,
        ref_to_froms = RefToFroms,
        from_to_refs = FromToRefs}}.

disassociate_transfer_ids(Froms, State) ->
    TransferIds = maps:values(maps:with(Froms, State#state.from_to_transfer_id)),
    FromToTransferId = maps:without(Froms, State#state.from_to_transfer_id),
    TransferIdToFrom = maps:without(TransferIds, State#state.transfer_id_to_from),
    State#state{from_to_transfer_id = FromToTransferId,
        transfer_id_to_from = TransferIdToFrom}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Cancels a transfer.
%% @end
%%--------------------------------------------------------------------
-spec cancel_transfer_id(transfer:id(), #state{}) -> #state{}.
cancel_transfer_id(TransferId, State) ->
    From = maps:get(TransferId, State#state.transfer_id_to_from),
    AffectedRefs = maps:get(From, State#state.from_to_refs, []),
    OrphanedRefs =
        lists:filter(fun(Ref) ->
            maps:get(Ref, State#state.ref_to_froms) == [From] end,
            AffectedRefs),

    [rtransfer_link:cancel(Ref) || Ref <- OrphanedRefs],
    gen_server2:reply(From, {error, canceled}),

    InProgress =
        lists:filter(fun({_Block, Ref}) -> lists:member(Ref, OrphanedRefs) end,
            State#state.in_progress),
    FromToRefs = maps:remove(From, State#state.from_to_refs),
    RefToFroms = maps:without(OrphanedRefs, State#state.ref_to_froms),
    FromToTransferId = maps:remove(From, State#state.from_to_transfer_id),
    TransferIdToFrom = maps:remove(TransferId, State#state.transfer_id_to_from),
    State#state{in_progress = InProgress,
        from_to_refs = FromToRefs,
        ref_to_froms = RefToFroms,
        from_to_transfer_id = FromToTransferId,
        transfer_id_to_from = TransferIdToFrom}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Associates synchronization requests with existing rtransfer
%% requests.
%% @end
%%--------------------------------------------------------------------
-spec associate_froms_with_refs(Froms :: [from()], Refs :: [fetch_ref()], #state{}) -> #state{}.
associate_froms_with_refs(Froms, Refs, State) when is_list(Froms) ->
    lists:foldl(fun(From, S) -> associate_from_with_refs(From, Refs, S) end,
        State, Froms).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Associates a new synchronization request with existing rtransfer
%% requests.
%% @end
%%--------------------------------------------------------------------
-spec associate_from_with_refs(From :: from(), Refs :: [fetch_ref()], #state{}) -> #state{}.
associate_from_with_refs(From, Refs, State) when is_list(Refs) ->
    OldFromToRefs = maps:get(From, State#state.from_to_refs, []),
    FromToRefs = maps:put(From, OldFromToRefs ++ Refs, State#state.from_to_refs),
    RefToFroms =
        lists:foldl(
            fun(Ref, Acc) ->
                OldList = maps:get(Ref, Acc, []),
                maps:put(Ref, [From | OldList], Acc)
            end,
            State#state.ref_to_froms,
            Refs),
    State#state{from_to_refs = FromToRefs, ref_to_froms = RefToFroms}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Associates a new synchronization with a transfer id.
%% @end
%%--------------------------------------------------------------------
-spec associate_from_with_tid(From :: from(), transfer:id(), #state{}) -> #state{}.
associate_from_with_tid(_From, undefined, State) -> State;
associate_from_with_tid(From, TransferId, State) ->
    FromToTransferId = maps:put(From, TransferId, State#state.from_to_transfer_id),
    TransferIdToFrom = maps:put(TransferId, From, State#state.transfer_id_to_from),
    State#state{from_to_transfer_id = FromToTransferId,
        transfer_id_to_from = TransferIdToFrom}.

add_in_progress(NewTransfers, State) ->
    InProgress = ordsets:union(State#state.in_progress, ordsets:from_list(NewTransfers)),
    State#state{in_progress = InProgress}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Increases or resets the in-sequence counter for prefetching.
%% The counter is only moved when the new synchronization request
%% actually results in a rtransfer request.
%% @end
%%--------------------------------------------------------------------
-spec adjust_sequence_hits(block(), [fetch_ref()], #state{}) -> #state{}.
adjust_sequence_hits(_Block, [], State) ->
    State;
adjust_sequence_hits(Block, _NewRefs, #state{in_sequence_hits = Hits} = State) ->
    NewState =
        case is_sequential(Block, State) of
            true -> State#state{in_sequence_hits = Hits + 1};
            false -> State#state{in_sequence_hits = 0}
        end,
    NewState#state{last_transfer = Block}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether a new request is in-sequence.
%% @end
%%--------------------------------------------------------------------
-spec is_sequential(block(), #state{}) -> boolean().
is_sequential(#file_block{offset = NextOffset, size = NewSize},
    #state{last_transfer = #file_block{offset = O, size = S}})
    when NextOffset =< O + S andalso NextOffset + NewSize > O + S ->
    true;
is_sequential(_, _State) ->
    false.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a new or existing process for synchronization of this file,
%% by this user session.
%% @end
%%--------------------------------------------------------------------
-spec get_process(user_ctx:ctx(), file_ctx:ctx()) ->
    {ok, pid()} | {error, Reason :: any()}.
get_process(UserCtx, FileCtx) ->
    proc_lib:start(?MODULE, init_or_return_existing, [UserCtx, FileCtx], 10000).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns an existing process for synchronization of this file,
%% by this user session, or continues and makes this process the one.
%% @end
%%--------------------------------------------------------------------
-spec init_or_return_existing(user_ctx:ctx(), file_ctx:ctx()) ->
    no_return() | normal.
init_or_return_existing(UserCtx, FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
%%    SessionId = user_ctx:get_session_id(UserCtx),
    SessionId = undefined,
    {Pid, _} = gproc:reg_or_locate({n, l, {FileUuid, SessionId}}),
    ok = proc_lib:init_ack({ok, Pid}),
    case self() of
        Pid ->
            {ok, State, Timeout} = init({UserCtx, FileCtx}),
            gen_server2:enter_loop(?MODULE, [], State, Timeout);
        _ ->
            normal
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts new rtransfer transfers.
%% @end
%%--------------------------------------------------------------------
-spec start_transfers([block()], transfer:id() | undefined,
    Prefetch :: boolean(), #state{}) ->
    NewRequests :: [{block(), fetch_ref()}].
start_transfers(InitialBlocks, TransferId, Prefetch, State) ->
    {LocationDocs, FileCtx2} = file_ctx:get_file_location_docs(State#state.file_ctx),
    ProvidersAndBlocks = replica_finder:get_blocks_for_sync(LocationDocs, InitialBlocks),
    FileGuid = State#state.file_guid,
    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    DestStorageId = State#state.dest_storage_id,
    DestFileId = State#state.dest_file_id,
    Priority = case Prefetch of true -> high_priority; false ->
        medium_priority end,
    lists:flatmap(
        fun({ProviderId, Blocks, {SrcStorageId, SrcFileId}}) ->
            lists:map(
                fun(#file_block{offset = O, size = S} = FetchedBlock) ->
                    Request = #{
                        provider_id => ProviderId,
                        file_guid => FileGuid,
                        src_storage_id => SrcStorageId,
                        src_file_id => SrcFileId,
                        dest_storage_id => DestStorageId,
                        dest_file_id => DestFileId,
                        space_id => SpaceId,
                        offset => O,
                        size => S,
                        priority => Priority
                    },
                    Self = self(),
                    NotifyFun = make_notify_fun(Self, ProviderId),
                    CompleteFun = make_complete_fun(Self),
                    {ok, NewRef} = rtransfer_config:fetch(Request, NotifyFun, CompleteFun,
                        TransferId, SpaceId, FileGuid),
                    {FetchedBlock, NewRef}
                end, Blocks)
        end, ProvidersAndBlocks).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a new notification function called on transfer update.
%% @end
%%--------------------------------------------------------------------
-spec make_notify_fun(Self :: pid(), od_provider:id()) -> rtransfer_link:notify_fun().
make_notify_fun(Self, ProviderId) ->
    fun(Ref, Offset, Size) ->
        Self ! {Ref, active, ProviderId, #file_block{offset = Offset, size = Size}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates a new completion function called on transfer completion.
%% @end
%%--------------------------------------------------------------------
-spec make_complete_fun(Self :: pid()) -> rtransfer_link:on_complete_fun().
make_complete_fun(Self) ->
    fun(Ref, Status) -> Self ! {Ref, complete, Status} end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules a prefetch transfer.
%% @end
%%--------------------------------------------------------------------
-spec prefetch(NewTransfers :: list(), transfer:id() | undefined,
    Prefetch :: boolean(), #state{}) -> #state{}.
prefetch([], _TransferId, _Prefetch, State) -> State;
prefetch(_NewTransfers, _TransferId, false, State) -> State;
prefetch(_NewTransfers, _TransferId, _Prefetch, #state{in_sequence_hits = 0} = State) ->
    State;
prefetch(_, TransferId, _, #state{in_sequence_hits = Hits, last_transfer = Block} = State) ->
    #file_block{offset = O, size = S} = Block,
    Offset = O + S,
    Size = min(?MINIMAL_SYNC_REQUEST * round(math:pow(2, Hits)), ?PREFETCH_SIZE),
    PrefetchBlock = #file_block{offset = Offset, size = Size},
    NewTransfers = start_transfers([PrefetchBlock], TransferId, true, State),
    InProgress = ordsets:union(State#state.in_progress, ordsets:from_list(NewTransfers)),
    State#state{last_transfer = PrefetchBlock, in_progress = InProgress}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Enlarges a block to minimal synchronization size.
%% @end
%%--------------------------------------------------------------------
-spec enlarge_block(block(), Prefetch :: boolean()) -> block().
enlarge_block(Block = #file_block{size = RequestedSize}, _Prefetch = true) ->
    Block#file_block{size = max(RequestedSize, ?MINIMAL_SYNC_REQUEST)};
enlarge_block(Block, _Prefetch) ->
    Block.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Finds parts of a block that are already in progress.
%% @end
%%--------------------------------------------------------------------
-spec find_overlapping(block(), #state{}) -> Overlapping :: [{block(), fetch_ref()}].
find_overlapping(#file_block{offset = Offset, size = Size}, #state{in_progress = InProgress}) ->
    lists:filter(
        fun({#file_block{offset = O, size = S}, _Ref}) ->
            (O =< Offset andalso Offset < O + S) orelse
                (Offset =< O andalso O < Offset + Size)
        end,
        InProgress).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Finds parts of request that are not yet being fulfilled.
%% @end
%%--------------------------------------------------------------------
-spec get_holes(block(), Existing :: [block()]) -> Holes :: [block()].
get_holes(#file_block{offset = Offset, size = Size}, ExistingBlocks) ->
    get_holes(Offset, Size, ExistingBlocks, []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Finds parts of request that are not yet being fulfilled.
%% @end
%%--------------------------------------------------------------------
-spec get_holes(Offset :: non_neg_integer(), Size :: non_neg_integer(),
    Existing :: [block()], Acc :: [block()]) -> Holes :: [block()].
get_holes(_Offset, Size, [], Acc) when Size =< 0 ->
    lists:reverse(Acc);
get_holes(Offset, Size, [], Acc) ->
    get_holes(Offset + Size, 0, [],
        [#file_block{offset = Offset, size = Size} | Acc]);
get_holes(Offset, Size, [#file_block{offset = Offset, size = S} | Blocks], Acc) ->
    get_holes(Offset + S, Size - S, Blocks, Acc);
get_holes(Offset, Size, [#file_block{offset = O} | Blocks], Acc) ->
    HoleSize = O - Offset,
    get_holes(Offset + HoleSize, Size - HoleSize, Blocks,
        [#file_block{offset = Offset, size = HoleSize} | Acc]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Cache transferred block in state to avoid updating transfer documents
%% hundred of times per second and set timeout after which cached stats will
%% be flushed.
%% @end
%%--------------------------------------------------------------------
-spec cache_stats([transfer:id() | undefined], od_provider:id(), block(),
    #state{}) -> #state{}.
cache_stats(TransferIds, ProviderId, Block, State) ->
    UpdatedStats = lists:foldl(fun(TransferId, Stats) ->
        TransferStats = maps:get(TransferId, Stats, #{}),
        NewTransferStats = TransferStats#{
            ProviderId => [Block | maps:get(ProviderId, TransferStats, [])]
        },
        Stats#{TransferId => NewTransferStats}
    end, State#state.cached_stats, TransferIds),

    NewState = State#state{cached_stats = UpdatedStats},
    set_caching_timer(NewState).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Aggregate cached transfer stats and update transfer documents.
%% @end
%%--------------------------------------------------------------------
-spec flush_stats(#state{}, [session:id()]) ->
    {#file_location_changed{} | undefined, #state{}}.
flush_stats(#state{cached_stats = Stats, file_ctx = FileCtx} = State, _ExcludeSessions)
    when map_size(Stats) == 0 ->
    {#document{value = Location}, _FileCtx2} =
        file_ctx:get_or_create_local_file_location_doc(FileCtx, false),
    {#file_location_changed{
        file_location = Location,
        change_beg_offset = 0, change_end_offset = 0}, State};
flush_stats(State, ExcludeSessions) ->
    #state{
        file_ctx = FileCtx,
        space_id = SpaceId,
        cached_stats = Cache
    } = State,

    {Stats, AllBlocks} =
        maps:fold(fun(TransferId, BlocksPerProvider, {Stats0, AllBlocks0}) ->
            {BytesPerProvider, TransferredBlocks} = maps:fold(
                fun(ProviderId, Blocks, {BytesPerProvider0, TransferredBlocks0}) ->
                    Bytes = get_summarized_blocks_size(Blocks),
                    BytesPerProvider1 = BytesPerProvider0#{ProviderId => Bytes},
                    TransferredBlocks1 = Blocks ++ TransferredBlocks0,
                    {BytesPerProvider1, TransferredBlocks1}
                end, {#{}, []}, BlocksPerProvider),

            Stats1 = Stats0#{TransferId => BytesPerProvider},
            AllBlocks1 = TransferredBlocks ++ AllBlocks0,
            {Stats1, AllBlocks1}
        end, {#{}, []}, Cache),

    {ok, _} = replica_updater:update(FileCtx, AllBlocks, undefined, false),
    {Location, _FileCtx2} = file_ctx:get_file_location_with_filled_gaps(FileCtx, AllBlocks),
    {EventOffset, EventSize} = fslogic_blocks:get_blocks_range(Location, AllBlocks),
    ok = fslogic_event_emitter:emit_file_location_changed(Location,
        ExcludeSessions, EventOffset, EventSize),

    lists:foreach(fun({TransferId, BytesPerProvider}) ->
        case transfer:mark_data_transfer_finished(TransferId, SpaceId, BytesPerProvider) of
            {ok, _} ->
                ok;
            {error, Error} ->
                ?error("Failed to update trasnfer statistics for ~p transfer "
                "due to ~p", [TransferId, Error])
        end
    end, maps:to_list(Stats)),

    % TODO VFS-4412 emit rtransfer statistics
%%    monitoring_event:emit_rtransfer_statistics(
%%        SpaceId, UserId, get_summarized_blocks_size(AllBlocks)
%%    ),

    erlang:garbage_collect(),

    Ans = #file_location_changed{
        file_location = Location,
        change_beg_offset = EventOffset, change_end_offset = EventSize},

    {Ans, cancel_caching_timer(State#state{cached_stats = #{}})}.

-spec set_caching_timer(#state{}) -> #state{}.
set_caching_timer(#state{caching_timer = undefined} = State) ->
    TimerRef = erlang:send_after(?STATS_AGGREGATION_TIME, self(), ?FLUSH_STATS),
    State#state{caching_timer = TimerRef};
set_caching_timer(State) ->
    State.

-spec cancel_caching_timer(#state{}) -> #state{}.
cancel_caching_timer(#state{caching_timer = undefined} = State) ->
    State;
cancel_caching_timer(#state{caching_timer = TimerRef} = State) ->
    erlang:cancel_timer(TimerRef, [{async, true}, {info, false}]),
    State#state{caching_timer = undefined}.

-spec get_summarized_blocks_size([block()]) -> non_neg_integer().
get_summarized_blocks_size(Blocks) ->
    lists:foldl(fun(#file_block{size = Size}, Acc) ->
        Acc + Size end, 0, Blocks).
