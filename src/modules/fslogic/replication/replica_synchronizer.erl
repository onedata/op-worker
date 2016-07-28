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
-export([synchronize/2, synchronize/3]).

-define(MINIMAL_SYNC_REQUEST, application:get_env(?APP_NAME, minimal_sync_request, 4194304)).
-define(TRIGGER_BYTE, application:get_env(?APP_NAME, trigger_byte, 52428800)).
-define(PREFETCH_SIZE, application:get_env(?APP_NAME, prefetch_size, 104857600)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv synchronize(Uuid, Block, true).
%%--------------------------------------------------------------------
-spec synchronize(file_meta:uuid(), fslogic_blocks:block()) -> ok.
synchronize(Uuid, Block) ->
    synchronize(Uuid, Block, true).

%%--------------------------------------------------------------------
%% @doc
%% Sychronizes File on given range. Does prefetch data if requested.
%% @end
%%--------------------------------------------------------------------
-spec synchronize(file_meta:uuid(), fslogic_blocks:block(), boolean()) -> ok.
synchronize(Uuid, Block = #file_block{offset = RequestedOffset, size = RequestedSize}, Prefetch) ->
    EnlargedBlock =
        case Prefetch of
            true ->
                Block#file_block{size = max(RequestedSize, ?MINIMAL_SYNC_REQUEST)};
            _ ->
                Block
        end,
    {ok, Locations} = file_meta:get_locations({uuid, Uuid}),
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
                    Ref = rtransfer:prepare_request(ProviderId, fslogic_uuid:to_file_guid(Uuid), O, S),
                    NewRef = rtransfer:fetch(Ref, fun notify_fun/3, on_complete_fun()),
                    case receive_rtransfer_notification(NewRef, ?SYNC_TIMEOUT) of
                        {ok, Size} ->
                            replica_updater:update(Uuid, [BlockToSync#file_block{size = Size}], undefined, false, LocalVersion);
                        {error, Error} ->
                            ?error("Transfer of ~p range (~p, ~p) failed with error: ~p.", [Uuid, RequestedOffset, RequestedSize, Error]),
                            throw(?EIO)
                    end
                end, Blocks)
        end, ProvidersAndBlocks),
    fslogic_event:emit_file_location_update({uuid, Uuid}, [], Block).

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
