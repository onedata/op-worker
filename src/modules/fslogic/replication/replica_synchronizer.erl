-module(replica_synchronizer).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").

-behaviour(gen_server).

-define(MINIMAL_SYNC_REQUEST, application:get_env(?APP_NAME, minimal_sync_request, 4194304)).
-define(TRIGGER_BYTE, application:get_env(?APP_NAME, trigger_byte, 52428800)).
-define(PREFETCH_SIZE, application:get_env(?APP_NAME, prefetch_size, 104857600)).
-define(DIE_AFTER, 60000).

-record(state, {
          file_ctx,
          file_guid,
          space_id,
          user_id,
          session_id,
          dest_storage_id,
          dest_file_id,
          last_transfer,
          in_progress,
          in_sequence_hits = 0,
          from_to_refs = #{},
          ref_to_froms = #{},
          from_to_transfer_id = #{},
          transfer_id_to_from = #{}
         }).

-export([synchronize/5, cancel/1, init/1, handle_call/3, handle_cast/2,
         handle_info/2, code_change/3, terminate/2, init_or_return_existing/2]).

%%%===================================================================
%%% API
%%%===================================================================

synchronize(UserCtx, FileCtx, Block, Prefetch, TransferId) ->
    EnlargedBlock = enlarge_block(Block, Prefetch),
    try
        {ok, Process} = get_process(UserCtx, FileCtx),
        gen_server2:call(Process, {synchronize, FileCtx, EnlargedBlock,
                                   Prefetch, TransferId}, infinity)
    catch
        exit:{{shutdown, timeout}, _} ->
            synchronize(UserCtx, FileCtx, Block, Prefetch, TransferId)
    end.

cancel(TransferId) ->
    lists:foreach(
      fun(Pid) -> gen_server2:cast(Pid, {cancel, TransferId}) end,
      gproc:lookup_pids({c, l, TransferId})).

init({UserCtx, FileCtx}) ->
    %% trigger creation of local file location
    {_LocalDoc, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx2),
    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    UserId = user_ctx:get_user_id(UserCtx),
    SessId = user_ctx:get_session_id(UserCtx),
    {DestStorageId, FileCtx3} = file_ctx:get_storage_id(FileCtx2),
    {DestFileId, FileCtx4} = file_ctx:get_storage_file_id(FileCtx3),
    {ok, #state{file_ctx = FileCtx4,
                file_guid = FileGuid,
                space_id = SpaceId,
                user_id = UserId,
                session_id = SessId,
                dest_storage_id = DestStorageId,
                dest_file_id = DestFileId,
                in_progress = ordsets:new()}, ?DIE_AFTER}.

handle_call({synchronize, FileCtx, Block, Prefetch, TransferId}, From, State0) ->
    State = State0#state{file_ctx = FileCtx},
    TransferId =/= undefined andalso (catch gproc:add_local_counter(TransferId, 1)),
    OverlappingInProgress = find_overlapping(Block, State),
    {OverlappingBlocks, ExistingRefs} = lists:unzip(OverlappingInProgress),
    Holes = get_holes(Block, OverlappingBlocks),
    NewTransfers = start_transfers(Holes, TransferId, Prefetch, State),
    {_, NewRefs} = lists:unzip(NewTransfers),
    case ExistingRefs ++ NewRefs of
        [] -> {reply, ok, State, ?DIE_AFTER};
        RelevantRefs ->
            State1 = associate_from_with_refs(From, RelevantRefs, TransferId, State),
            InProgress = ordsets:union(State1#state.in_progress, ordsets:from_list(NewTransfers)),
            State2 = adjust_sequence_hits(Block, NewRefs, State1),
            State3 = State2#state{in_progress = InProgress},
            State4 = prefetch(NewTransfers, TransferId, Prefetch, State3),
            {noreply, State4, ?DIE_AFTER}
    end.

handle_cast({cancel, TransferId}, State) ->
    try
        NewState = cancel_transfer_id(TransferId, State),
        {noreply, NewState, ?DIE_AFTER}
    catch
        _:Reason ->
            ?error_stacktrace("Unable to cancel ~p: ~p", [TransferId, Reason]),
            {noreply, State, ?DIE_AFTER}
    end;

handle_cast(_, State) ->
    {noreply, State, ?DIE_AFTER}.

handle_info(timeout, #state{in_progress = []} = State) ->
    ?debug("Exiting due to inactivity with state: ~p", [State]),
    {stop, {shutdown, timeout}, State};
handle_info(timeout, State) ->
    {noreply, State, ?DIE_AFTER};
handle_info({Ref, active, ProviderId, Block}, State) ->
    AffectedFroms = maps:get(Ref, State#state.ref_to_froms, []),
    lists:foreach(
      fun(From) ->
              TransferId = maps:get(From, State#state.from_to_transfer_id, undefined),
              {ok, _} = transfer:mark_data_transfer_finished(TransferId, ProviderId,
                                                             Block#file_block.size)
      end,
      AffectedFroms),
    {noreply, State, ?DIE_AFTER};
handle_info({Ref, complete, {ok, _} = _Status}, State) ->
    {Block, FinishedFroms, State1} = disassociate_ref(Ref, State),
    SessId = State1#state.session_id,
    fslogic_event_emitter:emit_file_location_changed(State#state.file_ctx, [SessId], Block),
    lists:foreach(
      fun(From) ->
              TransferId = maps:get(From, State#state.from_to_transfer_id, undefined),
              transfer:increase_files_transferred_counter(TransferId)
      end,
      FinishedFroms),
    [gen_server2:reply(From, ok) || From <- FinishedFroms],
    {noreply, State1, ?DIE_AFTER};
handle_info({Ref, complete, ErrorStatus}, State) ->
    ?debug("Transfer: ~p", [Ref]),
    ?error("Transfer failed: ~p", [ErrorStatus]),
    AffectedFroms = maps:get(Ref, State#state.ref_to_froms, []),
    {_Block, FinishedFroms, State1} = disassociate_ref(Ref, State),

    AffectedTransferIds = maps:values(maps:with(AffectedFroms, State1#state.from_to_transfer_id)),
    [gen_server2:reply(From, ErrorStatus) || From <- FinishedFroms],
    State2 = lists:foldl(fun(TID, S) -> cancel_transfer_id(TID, S) end,
                         State1, AffectedTransferIds),

    {noreply, State2, ?DIE_AFTER}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ignore.

%%%===================================================================
%%% Internal functions
%%%===================================================================

disassociate_ref(Ref, State) ->
    {Block, InProgress} =
        case lists:keytake(Ref, 2, State#state.in_progress) of
            {value, {B, _}, IP} -> {B, IP};
            false -> {undefined, State#state.in_progress}
        end,
    AffectedFroms = maps:get(Ref, State#state.ref_to_froms, []),
    RefsToFrom = maps:remove(Ref, State#state.ref_to_froms),
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
    TransferIds = maps:values(maps:with(FinishedFroms, State#state.from_to_transfer_id)),
    FromToTransferId = maps:without(FinishedFroms, State#state.from_to_transfer_id),
    TransferIdToFrom = maps:without(TransferIds, State#state.transfer_id_to_from),
    {Block, FinishedFroms,
     State#state{in_progress = InProgress, ref_to_froms = RefsToFrom,
                 from_to_refs = FromToRefs, from_to_transfer_id = FromToTransferId,
                 transfer_id_to_from = TransferIdToFrom}}.

cancel_transfer_id(TransferId, State) ->
    From = maps:get(TransferId, State#state.transfer_id_to_from),
    AffectedRefs = maps:get(From, State#state.from_to_refs, []),
    OrphanedRefs =
        lists:filter(fun(Ref) -> maps:get(Ref, State#state.ref_to_froms) == [From] end,
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

associate_from_with_refs(From, Refs, TransferId, State) ->
    FromToRefs = maps:put(From, Refs, State#state.from_to_refs),
    FromToTransferId = maps:put(From, TransferId, State#state.from_to_transfer_id),
    TransferIdToFrom = maps:put(TransferId, From, State#state.transfer_id_to_from),
    RefToFroms =
        lists:foldl(
          fun(Ref, Acc) ->
                  OldList = maps:get(Ref, Acc, []),
                  maps:put(Ref, [From | OldList], Acc)
          end,
          State#state.ref_to_froms,
          Refs),
    State#state{from_to_refs = FromToRefs, ref_to_froms = RefToFroms,
                from_to_transfer_id = FromToTransferId,
                transfer_id_to_from = TransferIdToFrom}.

adjust_sequence_hits(_Block, [], State) ->
    State;
adjust_sequence_hits(Block, _NewRefs, #state{in_sequence_hits = Hits} = State) ->
    NewState =
        case is_sequential(Block, State) of
            true -> State#state{in_sequence_hits = Hits + 1};
            false -> State#state{in_sequence_hits = 0}
        end,
    NewState#state{last_transfer = Block}.

is_sequential(#file_block{offset = NextOffset, size = NewSize},
              #state{last_transfer = #file_block{offset = O, size = S}})
  when NextOffset =< O + S andalso NextOffset + NewSize > O + S ->
    true;
is_sequential(_, _State) ->
    false.

get_process(UserCtx, FileCtx) ->
    proc_lib:start(?MODULE, init_or_return_existing, [UserCtx, FileCtx], 10000).

init_or_return_existing(UserCtx, FileCtx) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    SessionId = user_ctx:get_session_id(UserCtx),
    {Pid, _} = gproc:reg_or_locate({n, l, {FileGuid, SessionId}}),
    ok = proc_lib:init_ack({ok, Pid}),
    case self() of
        Pid ->
            {ok, State, Timeout} = init({UserCtx, FileCtx}),
            gen_server2:enter_loop(?MODULE, [], State, Timeout);
        _ ->
            normal
    end.

start_transfers(InitialBlocks, TransferId, Prefetch, State) ->
    {LocationDocs, FileCtx2} = file_ctx:get_file_location_docs(State#state.file_ctx),
    ProvidersAndBlocks = replica_finder:get_blocks_for_sync(LocationDocs, InitialBlocks),
    FileGuid = State#state.file_guid,
    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    UserId = State#state.user_id,
    DestStorageId = State#state.dest_storage_id,
    DestFileId = State#state.dest_file_id,
    Priority = case Prefetch of true -> high_priority; false -> medium_priority end,
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
                    NotifyFun = make_notify_fun(Self, ProviderId, SpaceId, UserId, FileCtx2),
                    CompleteFun = make_complete_fun(Self),
                    {ok, NewRef} = rtransfer_config:fetch(Request, NotifyFun, CompleteFun,
                                                          TransferId, SpaceId, FileGuid),
                    {FetchedBlock, NewRef}
                end, Blocks)
        end, ProvidersAndBlocks).

make_notify_fun(Self, ProviderId, SpaceId, UserId, FileCtx) ->
    fun(Ref, Offset, Size) ->
            monitoring_event:emit_rtransfer_statistics(SpaceId, UserId, Size),
            replica_updater:update(FileCtx,
                                   [#file_block{offset = Offset, size = Size}],
                                   undefined, false),
            Self ! {Ref, active, ProviderId, #file_block{offset = Offset, size = Size}}
    end.

make_complete_fun(Self) ->
    fun(Ref, Status) -> Self ! {Ref, complete, Status} end.

prefetch([], _TransferId, _Prefetch, State) -> State;
prefetch(_NewTransfers, _TransferId, false, State) -> State;
prefetch(_NewTransfers, _TransferId, _Prefetch, #state{in_sequence_hits = 0} = State) -> State;
prefetch(_, TransferId, _, #state{in_sequence_hits = Hits, last_transfer = Block} = State) ->
    #file_block{offset = O, size = S} = Block,
    Offset = O + S,
    Size = min(?MINIMAL_SYNC_REQUEST * round(math:pow(2, Hits)), ?PREFETCH_SIZE),
    PrefetchBlock = #file_block{offset = Offset, size = Size},
    NewTransfers = start_transfers([PrefetchBlock], TransferId, true, State),
    InProgress = ordsets:union(State#state.in_progress, ordsets:from_list(NewTransfers)),
    State#state{last_transfer = PrefetchBlock, in_progress = InProgress}.

enlarge_block(Block = #file_block{size = RequestedSize}, _Prefetch = true) ->
    Block#file_block{size = max(RequestedSize, ?MINIMAL_SYNC_REQUEST)};
enlarge_block(Block, _Prefetch) ->
    Block.

find_overlapping(#file_block{offset = Offset, size = Size}, #state{in_progress = InProgress}) ->
    lists:filter(
      fun({#file_block{offset = O, size = S}, _Ref}) ->
              (O =< Offset andalso Offset < O + S) orelse
                  (Offset =< O andalso O < Offset + Size)
      end,
      InProgress).

get_holes(#file_block{offset = Offset, size = Size}, ExistingBlocks) ->
    get_holes(Offset, Size, ExistingBlocks, []).

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
