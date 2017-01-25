%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module for updating blocks of file replicas.
%%% @end
%%%--------------------------------------------------------------------
-module(replica_updater).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([update/4, rename/3, fill_blocks_with_storage_info/2]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Appends given blocks to the file's local location and invalidates those blocks in its remote locations.
%% This function in synchronized on the file.
%% FileSize argument may be used to truncate file's blocks if needed.
%% Return value tells whether file size has been changed by this call.
%% @end
%%--------------------------------------------------------------------
-spec update(file_ctx:ctx(), fslogic_blocks:blocks(),
    FileSize :: non_neg_integer() | undefined, BumpVersion :: boolean()) ->
    {ok, size_changed} | {ok, size_not_changed} | {error, Reason :: term()}.
update(FileCtx, Blocks, FileSize, BumpVersion) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    file_location:critical_section(FileUuid,
        fun() ->
            {[Location = #document{ %todo VFS-2813 support multi location, get location as argument, instead of operating on first one
                value = #file_location{
                    size = OldSize
                }
            }], _FileCtx2} = file_ctx:get_local_file_location_docs(FileCtx),
            FullBlocks = fill_blocks_with_storage_info(Blocks, Location),
            UpdatedLocation = append(Location, FullBlocks, BumpVersion),

            case FileSize of
                undefined ->
                    file_location:save(UpdatedLocation),
                    case fslogic_blocks:upper(FullBlocks) > OldSize of
                        true -> {ok, size_changed};
                        false -> {ok, size_not_changed}
                    end;
                _ ->
                    TruncatedLocation = do_local_truncate(FileSize, UpdatedLocation),
                    file_location:save(TruncatedLocation),
                    {ok, size_changed}
            end
            % todo VFS-2813 support multi location, reconcile other local replicas according to this one
        end).


%%--------------------------------------------------------------------
%% @doc
%% Renames file's local location.
%% This function in synchronized on the file.
%% @end
%%--------------------------------------------------------------------
-spec rename(file_ctx:ctx(), TargetFileId :: helpers:file(),
    TargetSpaceId :: binary()) -> ok | {error, Reason :: term()}.
rename(FileCtx, TargetFileId, TargetSpaceId) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    file_location:critical_section(FileUuid,
        fun() ->
            {[LocationDoc = #document{ %todo VFS-2813 support multi location, get location as argument, instead of operating on first one
                value = Location = #file_location{
                    blocks = Blocks
                }}], _FileCtx2} = file_ctx:get_local_file_location_docs(FileCtx),

            {ok, #document{key = TargetStorageId}} = fslogic_storage:select_storage(TargetSpaceId),

            UpdatedBlocks = lists:map(fun(Block) ->
                Block#file_block{file_id = TargetFileId, storage_id = TargetStorageId}
            end, Blocks),

            fslogic_file_location:set_last_rename(
                version_vector:bump_version(
                    LocationDoc#document{value = Location#file_location{
                            file_id = TargetFileId,
                            space_id = TargetSpaceId,
                            storage_id = TargetStorageId,
                            blocks = UpdatedBlocks
                    }}
                ), TargetFileId, TargetSpaceId
            )
        %todo VFS-2813 support multi location, reconcile other local replicas according to this one
        end).


%%--------------------------------------------------------------------
%% @doc
%% Make sure that storage_id and file_id are set (assume defaults form location
%% if not)
%% @end
%%--------------------------------------------------------------------
-spec fill_blocks_with_storage_info(fslogic_blocks:blocks(), file_location:doc()) ->
    fslogic_blocks:blocks().
fill_blocks_with_storage_info(Blocks, #document{value = #file_location{storage_id = DSID, file_id = DFID}}) ->
    FilledBlocks = lists:map(
        fun(#file_block{storage_id = SID, file_id = FID} = B) ->
            NewSID =
                case SID of
                    undefined -> DSID;
                    _ -> SID
                end,
            NewFID =
                case FID of
                    undefined -> DFID;
                    _ -> FID
                end,

            B#file_block{storage_id = NewSID, file_id = NewFID}
        end, Blocks),

    fslogic_blocks:consolidate(lists:usort(FilledBlocks)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Truncates blocks from given location. Works for both shrinking and growing file.
%% @end
%%--------------------------------------------------------------------
-spec do_local_truncate(FileSize :: non_neg_integer(), file_location:doc()) -> file_location:doc().
do_local_truncate(FileSize, Doc = #document{value = #file_location{size = FileSize}}) ->
    Doc;
do_local_truncate(FileSize, #document{value = #file_location{size = LocalSize, file_id = FileId, storage_id = StorageId}} = LocalLocation) when LocalSize < FileSize ->
    append(LocalLocation, [#file_block{offset = LocalSize, size = FileSize - LocalSize, file_id = FileId, storage_id = StorageId}], true);
do_local_truncate(FileSize, #document{value = #file_location{size = LocalSize}} = LocalLocation) when LocalSize > FileSize ->
    shrink(LocalLocation, [#file_block{offset = FileSize, size = LocalSize - FileSize}], FileSize).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Appends given blocks to given locations and updates file size for those locations.
%% @end
%%--------------------------------------------------------------------
-spec append(file_location:doc(), fslogic_blocks:blocks(), boolean()) -> file_location:doc().
append(Doc, [], _) ->
    Doc;
append(#document{value = #file_location{blocks = OldBlocks, size = OldSize} = Loc} = Doc, Blocks, BumpVersion) ->
    NewBlocks = fslogic_blocks:merge(Blocks, OldBlocks),
    NewSize = fslogic_blocks:upper(Blocks),

    case BumpVersion of
        true ->
            version_vector:bump_version(
                fslogic_file_location:add_change(
                    Doc#document{value =
                    Loc#file_location{blocks = NewBlocks, size = max(OldSize, NewSize)}},
                    Blocks
                ));
        false ->
            Doc#document{value =
                Loc#file_location{blocks = NewBlocks, size = max(OldSize, NewSize)}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Inavlidates given blocks in given locations. File size is also updated.
%% @end
%%--------------------------------------------------------------------
-spec shrink(#document{value :: #file_location{}}, [#file_block{}] | lists:list(), non_neg_integer()) ->
    file_location:doc().
shrink(Doc = #document{value = Loc = #file_location{blocks = OldBlocks}}, Blocks, NewSize) ->
    NewBlocks = fslogic_blocks:invalidate(OldBlocks, Blocks),
    NewBlocks1 = fslogic_blocks:consolidate(NewBlocks),
    version_vector:bump_version(
        fslogic_file_location:add_change(
            Doc#document{value =
            Loc#file_location{blocks = NewBlocks1, size = NewSize}},
            {shrink, NewSize}
        )
    ).