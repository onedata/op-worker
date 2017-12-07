%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module for synchronizing file replicas
%%% @end
%%%--------------------------------------------------------------------
-module(replica_synchronizer).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/dbsync/common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").

%% API
-export([synchronize/5]).

-define(MINIMAL_SYNC_REQUEST, application:get_env(?APP_NAME, minimal_sync_request, 4194304)).
-define(TRIGGER_BYTE, application:get_env(?APP_NAME, trigger_byte, 52428800)).
-define(PREFETCH_SIZE, application:get_env(?APP_NAME, prefetch_size, 104857600)).
-define(MAX_BLOCK_SIZE, 104857600). % 100MB

-define(CHECK_STATUS_INTERVAL, timer:minutes(5)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sychronizes File on given range. Does prefetch data if requested.
%% Fetch request for rtansfer are scheduled per MAX_BLOCK_SIZE of data.
%% Before each request, process checks whether given transfer hasn't been
%% marked as failed or cancelled.
%% @end
%%--------------------------------------------------------------------
-spec synchronize(user_ctx:ctx(), file_ctx:ctx(), fslogic_blocks:block(),
    boolean(), undefined | transfer:id()) -> ok.
synchronize(UserCtx, FileCtx, Block = #file_block{size = RequestedSize}, Prefetch, TransferId) ->
    EnlargedBlock =
        case Prefetch of
            true ->
                Block#file_block{size = max(RequestedSize, ?MINIMAL_SYNC_REQUEST)};
            _ ->
                Block
        end,
    trigger_prefetching(UserCtx, FileCtx, EnlargedBlock, Prefetch),
    % trigger creation of local file location
    {_LocalDoc, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx),
    {LocationDocs, FileCtx3} = file_ctx:get_file_location_docs(FileCtx2),
    ProvidersAndBlocks = replica_finder:get_blocks_for_sync(LocationDocs, [EnlargedBlock]),
    FileGuid = file_ctx:get_guid_const(FileCtx3),
    UserId = user_ctx:get_user_id(UserCtx),
    lists:foreach(
        fun({ProviderId, Blocks}) ->
            lists:foreach(fun(FileBlock) ->
                foreach_chunk(FileBlock, ?MAX_BLOCK_SIZE, fun(Chunk) ->
                    maybe_transfer_chunk(Chunk, ProviderId, FileGuid, UserId, FileCtx, TransferId)
                end)
            end, Blocks)
        end, ProvidersAndBlocks),
    SessId = user_ctx:get_session_id(UserCtx),
    fslogic_event_emitter:emit_file_location_changed(FileCtx3, [SessId], Block).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Recursively calls Fun on chunks of size smaller or equal to ChunkSize.
%% @end
%%-------------------------------------------------------------------
-spec foreach_chunk(fslogic_blocks:block(), non_neg_integer(),
    fun((Chunk :: fslogic_blocks:block()) -> ok)) -> atom().
foreach_chunk(#file_block{size = 0}, _ChunkSize_ , _Fun) ->
    ok;
foreach_chunk(FB = #file_block{size = S}, ChunkSize , Fun) when S =< ChunkSize ->
    Fun(FB),
    ok;
foreach_chunk(#file_block{offset = O, size = S}, ChunkSize ,  Fun) ->
    Chunk = #file_block{offset = O, size = ChunkSize},
    RestOfBlock = #file_block{offset = O + ChunkSize, size = S - ChunkSize},
    Fun(Chunk),
    foreach_chunk(RestOfBlock, ChunkSize, Fun).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether transfer should be continued.
%% If yes, it transfers given Chunk. Otherwise throws {transfer_cancelled, TransferId}.
%% @end
%%-------------------------------------------------------------------
-spec maybe_transfer_chunk(fslogic_blocks:block(), oneprovider:id(), file_meta:uuid(),
    od_user:id(), file_ctx:ctx(), transfer:id() | undefined) -> ok.
maybe_transfer_chunk(Chunk = #file_block{size = BlockSize}, ProviderId, FileGuid,
    UserId, FileCtx, TransferId
) ->
    case transfer:should_continue(TransferId) of
        true ->
            try
                {ok, _} = transfer:mark_data_transfer_scheduled(TransferId, BlockSize),
                transfer_chunk(Chunk, ProviderId, FileGuid, UserId, FileCtx, TransferId)
            catch
                Error:Reason ->
                    transfer:mark_data_transfer_scheduled(TransferId, -BlockSize),
                    erlang:Error(Reason)
            end;
        false ->
            throw({transfer_cancelled, TransferId})
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Transfers one chunk of data.
%% @end
%%-------------------------------------------------------------------
-spec transfer_chunk(fslogic_blocks:block(), oneprovider:id(), file_meta:uuid(),
    od_user:id(), file_ctx:ctx(), transfer:id() | undefined) -> ok.
transfer_chunk(#file_block{offset = O, size = S}, ProviderId, FileGuid, UserId,
    FileCtx, TransferId
) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    Ref0 = rtransfer:prepare_request(ProviderId, FileGuid, O, S),
    Self = self(),
    NotifyFun = fun(Ref, Offset, Size) ->
        monitoring_event:emit_rtransfer_statistics(SpaceId, UserId, Size),
        replica_updater:update(FileCtx,
            [#file_block{offset = Offset, size = Size}], undefined, false),
        {ok, _} = transfer:mark_data_transfer_finished(TransferId, Size),
        Self ! {Ref, active, #file_block{offset = Offset, size = Size}}
    end,
    CompleteFun = fun(Ref, Status) ->
        Self ! {Ref, complete, Status}
    end,
    NewRef = rtransfer:fetch(Ref0, NotifyFun, CompleteFun),
    {ok, _} = receive_rtransfer_notification(NewRef, TransferId),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Trigger prefetching if block includes trigger bytes.
%% @end
%%--------------------------------------------------------------------
-spec trigger_prefetching(user_ctx:ctx(), file_ctx:ctx(),
    fslogic_blocks:block(), boolean()) -> ok.
trigger_prefetching(UserCtx, FileCtx, Block, true) ->
    case contains_trigger_byte(Block) of
        true ->
            spawn(prefetch_data_fun(UserCtx, FileCtx, Block)),
            ok;
        false ->
            ok
    end;
trigger_prefetching(_, _, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns function that prefetches data starting at given block.
%% @end
%%--------------------------------------------------------------------
-spec prefetch_data_fun(user_ctx:ctx(), file_ctx:ctx(), fslogic_blocks:block()) -> function().
prefetch_data_fun(UserCtx, FileCtx, #file_block{offset = O, size = _S}) ->
    fun() ->
        try
            replica_synchronizer:synchronize(UserCtx, FileCtx, #file_block{offset = O, size = ?PREFETCH_SIZE}, false, undefined)
        catch
            _:Error ->
                ?error_stacktrace("Prefetching of ~p at offset ~p with size ~p failed due to: ~p",
                    [file_ctx:get_guid_const(FileCtx), O, ?PREFETCH_SIZE, Error])
        end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns true if given blocks contains trigger byte.
%% @end
%%--------------------------------------------------------------------
-spec contains_trigger_byte(fslogic_blocks:block()) -> boolean().
contains_trigger_byte(#file_block{offset = O, size = S}) ->
    ((O rem ?TRIGGER_BYTE) == 0) orelse
        ((O rem ?TRIGGER_BYTE) + S >= ?TRIGGER_BYTE).

%%--------------------------------------------------------------------
%% @doc
%% Wait for Rtransfer notification. Every ?CHECK_STATUS_INTERVAL period process
%% checks whether it should continue waiting (whether transfer hasn't been
%% cancelled or failed).
%% @end
%%--------------------------------------------------------------------
-spec receive_rtransfer_notification(rtransfer:ref(), transfer:id()) -> term().
receive_rtransfer_notification(Ref, TransferId) ->
    receive
        {Ref, active, _Block} ->
            receive_rtransfer_notification(Ref, TransferId);
        {Ref, complete, Status} ->
            Status
    after
         ?CHECK_STATUS_INTERVAL ->
            case transfer:should_continue(TransferId) of
                true ->
                    receive_rtransfer_notification(Ref, TransferId);
                false ->
                    throw({transfer_cancelled, TransferId})
            end
    end.