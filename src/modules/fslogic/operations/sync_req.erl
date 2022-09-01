%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling requests synchronizing and getting
%%% synchronization state of files.
%%% @end
%%%--------------------------------------------------------------------
-module(sync_req).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

-type block() :: undefined | fslogic_blocks:block().
-type transfer_id() :: undefined | transfer:id().
-type provider_id() :: undefined | od_provider:id().
-type query_view_params() :: transfer:query_view_params().
-type fuse_response() :: fslogic_worker:fuse_response().
-type provider_response() :: fslogic_worker:provider_response().

-export_type([block/0, transfer_id/0, provider_id/0, query_view_params/0, provider_response/0]).

%% API
-export([
    synchronize_block/6,
    request_block_synchronization/6,
    synchronize_block_and_compute_checksum/5
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block(user_ctx:ctx(), file_ctx:ctx(), block(),
    Prefetch :: boolean(), transfer_id(), non_neg_integer()) -> fuse_response().
synchronize_block(UserCtx, FileCtx, undefined, Prefetch, TransferId, Priority) ->
    % trigger file_location creation
    {#document{value = #file_location{size = Size} = FL} = FLDoc, FileCtx2} =
        file_ctx:get_or_create_local_file_location_doc(FileCtx, skip_local_blocks),
    case fslogic_location_cache:get_blocks(FLDoc, #{count => 2}) of
        [#file_block{offset = 0, size = Size}] ->
            LogicalUuid = file_ctx:get_logical_uuid_const(FileCtx2),
            FLC = #file_location_changed{file_location = FL#file_location{uuid = LogicalUuid},
                change_beg_offset = 0, change_end_offset = Size},
            #fuse_response{status = #status{code = ?OK}, fuse_response = FLC};
        _ ->
            synchronize_block(UserCtx, FileCtx2, #file_block{offset = 0, size = Size},
                Prefetch, TransferId, Priority)
    end;
synchronize_block(UserCtx, FileCtx, Block, Prefetch, TransferId, Priority) ->
    case replica_synchronizer:synchronize(UserCtx, FileCtx, Block,
        Prefetch, TransferId, Priority, transfer) of
        {ok, #file_location_changed{file_location = FL} = Ans} ->
            LogicalUuid = file_ctx:get_logical_uuid_const(FileCtx),
            #fuse_response{status = #status{code = ?OK},
                fuse_response = Ans#file_location_changed{file_location = FL#file_location{uuid = LogicalUuid}}};
        {error, Reason} ->
            throw(Reason)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Requests synchronization of given block with remote replicas.
%% Does not wait for sync.
%% @end
%%--------------------------------------------------------------------
-spec request_block_synchronization(user_ctx:ctx(), file_ctx:ctx(), block(),
    Prefetch :: boolean(), transfer_id(), non_neg_integer()) -> fuse_response().
request_block_synchronization(UserCtx, FileCtx, undefined, Prefetch, TransferId, Priority) ->
    % trigger file_location creation
    {#document{value = #file_location{size = Size}} = FLDoc, FileCtx2} =
        file_ctx:get_or_create_local_file_location_doc(FileCtx, skip_local_blocks),
    case fslogic_location_cache:get_blocks(FLDoc, #{count => 2}) of
        [#file_block{offset = 0, size = Size}] ->
            #fuse_response{status = #status{code = ?OK}};
        _ ->
            request_block_synchronization(UserCtx, FileCtx2, #file_block{offset = 0, size = Size},
                Prefetch, TransferId, Priority)
    end;
request_block_synchronization(UserCtx, FileCtx, Block, Prefetch, TransferId, Priority) ->
    case replica_synchronizer:request_synchronization(UserCtx, FileCtx, Block,
        Prefetch, TransferId, Priority, transfer) of
        ok ->
            #fuse_response{status = #status{code = ?OK}};
        {error, Reason} ->
            throw(Reason)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas and returns checksum of
%% synchronized data.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block_and_compute_checksum(user_ctx:ctx(), file_ctx:ctx(),
    block(), boolean(), non_neg_integer()) -> fuse_response().
synchronize_block_and_compute_checksum(UserCtx, FileCtx,
    Range = #file_block{offset = Offset, size = Size}, Prefetch, Priority
) ->
    SessId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),

    {ok, #file_location_changed{file_location = FL} = Ans} =
        replica_synchronizer:synchronize(UserCtx, FileCtx, Range, Prefetch, undefined, Priority, transfer),

    %TODO VFS-7393 do not use lfm, operate on fslogic directly
    {ok, Handle} = lfm:open(SessId, ?FILE_REF(FileGuid), read),
    % does sync internally
    {ok, _, Data} = lfm_files:read_without_events(Handle, Offset, Size, off),
    lfm:release(Handle),

    Checksum = crypto:hash(md4, Data),
    LogicalUuid = file_ctx:get_logical_uuid_const(FileCtx),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #sync_response{
            checksum = Checksum,
            file_location_changed = Ans#file_location_changed{file_location = FL#file_location{uuid = LogicalUuid}}
        }
    }.
