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
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").

%% API
-export([synchronize/5]).

-define(MINIMAL_SYNC_REQUEST, application:get_env(?APP_NAME, minimal_sync_request, 4194304)).
-define(TRIGGER_BYTE, application:get_env(?APP_NAME, trigger_byte, 52428800)).
-define(PREFETCH_SIZE, application:get_env(?APP_NAME, prefetch_size, 104857600)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sychronizes File on given range. Does prefetch data if requested.
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
    SpaceId = file_ctx:get_space_id_const(FileCtx3),
    UserId = user_ctx:get_user_id(UserCtx),
    BlockSizes = [BlockSize || {_, Blocks} <- ProvidersAndBlocks,  #file_block{size = BlockSize} <- Blocks],
    {ok, _} = transfer:mark_data_transfer_scheduled(TransferId, lists:sum(BlockSizes)),
    lists:foreach(
        fun({ProviderId, Blocks}) ->
            lists:foreach(
                fun(#file_block{offset = O, size = S}) ->
                    Ref0 = rtransfer:prepare_request(ProviderId, FileGuid, O, S),
                    Self = self(),
                    NotifyFun = fun(Ref, Offset, Size) ->
                        monitoring_event:emit_rtransfer_statistics(SpaceId, UserId, Size),
                        replica_updater:update(FileCtx3,
                            [#file_block{offset = Offset, size = Size}], undefined, false),
                        {ok, _} = transfer:mark_data_transfer_finished(TransferId, ProviderId, Size),
                        Self ! {Ref, active, #file_block{offset = Offset, size = Size}}
                    end,
                    CompleteFun = fun(Ref, Status) ->
                        Self ! {Ref, complete, Status}
                    end,
                    NewRef = rtransfer:fetch(Ref0, NotifyFun, CompleteFun),
                    {ok, _} = receive_rtransfer_notification(NewRef)
                end, Blocks)
        end, ProvidersAndBlocks),
    SessId = user_ctx:get_session_id(UserCtx),
    fslogic_event_emitter:emit_file_location_changed(FileCtx3, [SessId], Block).

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
%% Wait for Rtransfer notification.
%% @end
%%--------------------------------------------------------------------
-spec receive_rtransfer_notification(rtransfer:ref()) -> term().
receive_rtransfer_notification(Ref) ->
    receive
        {Ref, active, _Block} ->
            receive_rtransfer_notification(Ref);
        {Ref, complete, Status} ->
            Status
    end.