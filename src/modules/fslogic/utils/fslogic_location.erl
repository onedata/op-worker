%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides functions operating on file_location.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_location).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/common_messages.hrl").

%% API
-export([create_doc/3, is_file_created/2, mark_file_created/3]).
-export([create_imported_file_doc/7, update_imported_file_doc/2]).
-export([get_local_blocks_and_fill_location_gaps/4, fill_location_gaps/5]).

%%%===================================================================
%%% API
%%%===================================================================


-spec create_doc(file_ctx:ctx(), StorageFileCreated :: boolean(),
    GeneratedKey :: boolean()) -> {{ok, file_location:record()} | {error, already_exists}, file_ctx:ctx()}.
create_doc(FileCtx, StorageFileCreated, GeneratedKey) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    {StorageFileId, FileCtx2} = file_ctx:get_new_storage_file_id(FileCtx),
    {StorageId, FileCtx3} = file_ctx:get_storage_id(FileCtx2),
    {Size, FileCtx4} = file_ctx:get_file_size_from_remote_locations(FileCtx3),
    Location = #file_location{
        provider_id = oneprovider:get_id(),
        file_id = StorageFileId,
        storage_id = StorageId,
        uuid = FileUuid,
        space_id = SpaceId,
        storage_file_created = StorageFileCreated,
        size = Size
    },
    storage_sync_info:maybe_set_guid(StorageFileId, SpaceId, StorageId, FileGuid),
    LocId = file_location:local_id(FileUuid),
    case fslogic_location_cache:create_location(#document{
        key = LocId,
        value = Location
    }, GeneratedKey) of
        {ok, _LocId} ->
            FileCtx5 = file_ctx:set_file_location(FileCtx4, LocId),
            {{ok, Location}, FileCtx5};
        {error, already_exists} = Error ->
            {Error, FileCtx4}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks whether file is marked as created on storage.
%% @end
%%--------------------------------------------------------------------
-spec is_file_created(file_meta:uuid(), file_location:id()) -> boolean().
is_file_created(FileUuid, FileLocationId) ->
    case fslogic_location_cache:get_location(FileLocationId, FileUuid, false) of
        {ok, #document{
            value = #file_location{storage_file_created = Created}
        }} ->
            Created;
        {ok, _} ->
            false
    end.

%%--------------------------------------------------------------------
%% @doc
%% Marks that file is created on storage.
%% @end
%%--------------------------------------------------------------------
-spec mark_file_created(file_meta:uuid(), file_location:id(),
    helpers:file_id()) -> {ok, file_location:doc()} | {error, term()}.
mark_file_created(FileUuid, FileLocationId, StorageFileId) ->
    fslogic_location_cache:update_location(FileUuid, FileLocationId,
        fun(FileLocation = #file_location{storage_file_created = false}) ->
            {ok, FileLocation#file_location{storage_file_created = true,
                file_id = StorageFileId}}
        end, false).


-spec create_imported_file_doc(od_space:id(), storage:id(), file_meta:uuid(),
    helpers:file_id(), file_meta:size(), od_user:id(), luma:gid() | undefined) -> ok.
create_imported_file_doc(SpaceId, StorageId, FileUuid, StorageFileId, Size, OwnerId, SyncedGid) ->
    Location = #file_location{
        provider_id = oneprovider:get_id(),
        file_id = StorageFileId,
        storage_id = StorageId,
        uuid = FileUuid,
        space_id = SpaceId,
        size = Size,
        storage_file_created = true,
        synced_gid = SyncedGid
    },
    LocationDoc = #document{
        key = file_location:local_id(FileUuid),
        value = Location,
        scope = SpaceId
    },
    LocationDoc2 = fslogic_location_cache:set_blocks(LocationDoc, create_file_blocks(Size)),
    {ok, _LocId} = file_location:save_and_bump_version(LocationDoc2, OwnerId),
    ok.


-spec update_imported_file_doc(file_ctx:ctx(), non_neg_integer()) -> ok.
update_imported_file_doc(FileCtx, StorageSize) ->
    NewFileBlocks = create_file_blocks(StorageSize),
    {ok, _} = replica_updater:update(FileCtx, NewFileBlocks, StorageSize, true),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% @equiv fill_location_gaps/5 but checks requested range and gets local blocks first.
%% @end
%%--------------------------------------------------------------------
-spec get_local_blocks_and_fill_location_gaps(fslogic_blocks:blocks() | undefined, file_location:doc(),
    [file_location:doc()], file_location:id()) -> #file_location{}.
get_local_blocks_and_fill_location_gaps(ReqRange0, #document{value = FileLocation = #file_location{
    size = Size}} = FileLocationDoc, Locations, Uuid) ->
    ReqRange = utils:ensure_defined(ReqRange0, [#file_block{offset = 0, size = Size}]),
    Blocks = fslogic_location_cache:get_blocks(FileLocationDoc,
        #{overlapping_blocks => ReqRange}),

    fill_location_gaps(ReqRange, FileLocation, Blocks, Locations, Uuid).

%%--------------------------------------------------------------------
%% @doc
%% Returns location that can be understood by client. It has gaps filled, and
%% stores guid instead of uuid.
%% @end
%%--------------------------------------------------------------------
-spec fill_location_gaps(fslogic_blocks:blocks(), file_location:record(), fslogic_blocks:blocks(),
    [file_location:doc()], file_location:id()) -> #file_location{}.
fill_location_gaps(ReqRange, FileLocation, LocalBlocks, Locations, Uuid) ->
    % find gaps
    AllRanges = lists:foldl(
        fun(Doc, Acc) ->
            fslogic_blocks:merge(Acc, fslogic_location_cache:get_blocks(Doc,
                #{overlapping_blocks => ReqRange}))
        end, [], Locations),
    Gaps = fslogic_blocks:consolidate(
        fslogic_blocks:invalidate(ReqRange, AllRanges)
    ),
    BlocksWithFilledGaps = fslogic_blocks:merge(LocalBlocks, Gaps),

    % fill gaps transform uid and emit
    fslogic_location_cache:set_final_blocks(FileLocation#file_location{
        uuid = Uuid}, BlocksWithFilledGaps).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns list containing one block with given size.
%% Is Size == 0 returns empty list.
%% @end
%%-------------------------------------------------------------------
-spec create_file_blocks(non_neg_integer()) -> fslogic_blocks:blocks().
create_file_blocks(0) -> [];
create_file_blocks(Size) -> [#file_block{offset = 0, size = Size}].