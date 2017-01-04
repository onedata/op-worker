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
-export([synchronize/4]).

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
-spec synchronize(fslogic_context:ctx(), file_meta:uuid(), fslogic_blocks:block(),
    boolean()) -> ok.
synchronize(CTX, Uuid, Block = #file_block{offset = RequestedOffset, size = RequestedSize}, Prefetch) ->
    EnlargedBlock =
        case Prefetch of
            true ->
                Block#file_block{size = max(RequestedSize, ?MINIMAL_SYNC_REQUEST)};
            _ ->
                Block
        end,
    trigger_prefetching(CTX, Uuid, EnlargedBlock, Prefetch),
    {ok, Locations} = file_meta:get_locations({uuid, Uuid}), %todo VFS-2813 support multi location
    LocationDocs = lists:map(
        fun(LocationId) ->
            {ok, Loc} = file_location:get(LocationId),
            Loc
        end, Locations),
    LocalProviderId = oneprovider:get_provider_id(),
    [#document{value = #file_location{version_vector = LocalVersion}}] = [Loc || Loc = #document{value = #file_location{provider_id = Id}}
        <- LocationDocs, Id =:= LocalProviderId],
    ProvidersAndBlocks = replica_finder:get_blocks_for_sync(LocationDocs, [EnlargedBlock]),
    lists:foreach(
        fun({ProviderId, Blocks}) ->
            lists:foreach(
                fun(BlockToSync = #file_block{offset = O, size = S}) ->
                    Ref = rtransfer:prepare_request(ProviderId, fslogic_uuid:uuid_to_guid(Uuid), O, S),
                    NewRef = rtransfer:fetch(Ref, fun notify_fun/3, on_complete_fun()),
                    case receive_rtransfer_notification(NewRef, ?SYNC_TIMEOUT) of
                        {ok, Size} ->
                            monitoring_event:emit_rtransfer_statistics(CTX, Size),
                            replica_updater:update(Uuid, [BlockToSync#file_block{size = Size}], undefined, false, LocalVersion);
                        {error, Error} ->
                            ?error("Transfer of ~p range (~p, ~p) failed with error: ~p.", [Uuid, RequestedOffset, RequestedSize, Error]),
                            throw(?EIO)
                    end
                end, Blocks)
        end, ProvidersAndBlocks),
    SessId = fslogic_context:get_session_id(CTX),
    fslogic_event:emit_file_location_changed({uuid, Uuid}, [SessId], Block).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Trigger prefetching if block includes trigger bytes.
%% @end
%%--------------------------------------------------------------------
-spec trigger_prefetching(fslogic_context:ctx(), file_meta:uuid(),
    fslogic_blocks:block(), boolean()) -> ok.
trigger_prefetching(CTX, FileUuid, Block, true) ->
    case contains_trigger_byte(Block) of
        true ->
            spawn(prefetch_data_fun(CTX, FileUuid, Block)),
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
-spec prefetch_data_fun(fslogic_context:ctx(), file_meta:uuid(), fslogic_blocks:block()) -> function().
prefetch_data_fun(CTX, FileUuid, #file_block{offset = O, size = S}) ->
    fun() ->
        try
            replica_synchronizer:synchronize(CTX, FileUuid, #file_block{offset = O, size = ?PREFETCH_SIZE}, false)
        catch
            _:Error ->
                ?error_stacktrace("Prefetching of ~p at offset ~p with size ~p failed due to: ~p",
                    [FileUuid, O+S, ?PREFETCH_SIZE, Error])
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
%% RTransfer notify fun
%% @end
%%--------------------------------------------------------------------
-spec notify_fun(any(), any(), any()) -> ok.
notify_fun(_Ref, _Offset, _Size) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Rtransfer on complete fun
%% @end
%%--------------------------------------------------------------------
-spec on_complete_fun() -> function().
on_complete_fun() ->
    Self = self(),
    fun(Ref, Status) ->
        Self ! {Ref, Status}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Wait for Rtransfer notification.
%% @end
%%--------------------------------------------------------------------
-spec receive_rtransfer_notification(rtransfer:ref(), non_neg_integer()) -> term().
receive_rtransfer_notification(Ref, Timeout) ->
    receive
        {Ref, Status} ->
            Status
    after
        Timeout -> {error, timeout}
    end.
