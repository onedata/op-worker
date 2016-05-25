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
-export([update/4]).

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
-spec update(FileUUID :: file_meta:uuid(), Blocks :: fslogic_blocks:blocks(),
    FileSize :: non_neg_integer() | undefined, BumpVersion :: boolean()) ->
    {ok, size_changed} | {ok, size_not_changed} | {error, Reason :: term()}.
update(FileUUID, Blocks, FileSize, BumpVersion) ->
    fslogic_utils:wait_for_local_file_location(FileUUID),
    file_location:run_synchronized(FileUUID,
        fun() ->
                [Location = #document{value = #file_location{size = OldSize}} | _] =
                    fslogic_utils:get_local_file_locations_once({uuid, FileUUID}), %todo get location as argument, insted operating on first one
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
                % todo reconcile other local replicas according to this one
        end).

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
    NewBlocks = fslogic_blocks:invalidate(OldBlocks, Blocks) ++ Blocks,
    NewBlocks1 = fslogic_blocks:consolidate(lists:sort(NewBlocks)),
    NewSize = fslogic_blocks:upper(Blocks),

    case BumpVersion of
        true ->
            version_vector:bump_version(
                fslogic_file_location:add_change(
                    Doc#document{value =
                    Loc#file_location{blocks = NewBlocks1, size = max(OldSize, NewSize)}},
                    Blocks
                ));
        false ->
            Doc#document{value =
                Loc#file_location{blocks = NewBlocks1, size = max(OldSize, NewSize)}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Inavlidates given blocks in given locations. File size is also updated.
%% @end
%%--------------------------------------------------------------------
-spec shrink(#document{value :: #file_location{}}, [#file_block{}] | [], non_neg_integer()) ->
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