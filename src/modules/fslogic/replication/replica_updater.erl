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

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([update/4, rename/2, has_replication_status_changed/4]).

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
    {ok, ignore | emit_size_change | {emit_replica_status_change, EmissionParam :: boolean()}} |
    {error, Reason :: term()}.
update(FileCtx, Blocks, FileSize, BumpVersion) ->
    replica_synchronizer:apply(FileCtx,
        fun() ->
            case fslogic_cache:get_local_location() of
                Location = #document{
                    value = #file_location{
                        size = OldSize
                    }
                } ->
                    FirestLocalBlocksBeforeAppend = fslogic_location_cache:get_blocks(Location, #{count => 2}),
                    UpdatedLocation = append(Location, Blocks, BumpVersion),
                    case FileSize of
                        undefined ->
                            fslogic_location_cache:save_location(UpdatedLocation),
                            #document{value = #file_location{size = UpdatedSize}} = UpdatedLocation,
                            FirestLocalBlocks = fslogic_location_cache:get_blocks(UpdatedLocation, #{count => 2}),
                            ReplicationStatusChanged = has_replication_status_changed(
                                FirestLocalBlocksBeforeAppend, FirestLocalBlocks, OldSize, UpdatedSize),
                            case {ReplicationStatusChanged, UpdatedSize > OldSize} of
                                {true, SizeChanged} -> {ok, {emit_replica_status_change, SizeChanged}};
                                {_, true} -> {ok, emit_size_change};
                                _ -> {ok, ignore}
                            end;
                        _ ->
                            TruncatedLocation = do_local_truncate(FileSize, UpdatedLocation),
                            fslogic_location_cache:save_location(TruncatedLocation),
                            FirestLocalBlocks = fslogic_location_cache:get_blocks(TruncatedLocation, #{count => 2}),
                            ReplicationStatusChanged = has_replication_status_changed(
                                FirestLocalBlocksBeforeAppend, FirestLocalBlocks, OldSize, FileSize),
                            case ReplicationStatusChanged of
                                true -> {ok, {emit_replica_status_change, true}};
                                false -> {ok, emit_size_change}
                            end
                    end;
                Error ->
                    Error
            end
        end).


%%--------------------------------------------------------------------
%% @doc
%% Renames file's local location.
%% This function in synchronized on the file.
%% @end
%%--------------------------------------------------------------------
-spec rename(file_ctx:ctx(), TargetFileId :: helpers:file_id()) ->
    ok | {error, Reason :: term()}.
rename(FileCtx, TargetFileId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    replica_synchronizer:apply(FileCtx,
        fun() ->
            {LocationDoc, _FileCtx2} = file_ctx:get_or_create_local_file_location_doc(file_ctx:reset(FileCtx)),

            replica_changes:set_last_rename(
                version_vector:bump_version(
                    LocationDoc#document{value = LocationDoc#document.value#file_location{
                            file_id = TargetFileId
                    }}
                ), TargetFileId, SpaceId
            )
        %todo VFS-2813 support multi location, reconcile other local replicas according to this one
        end).

-spec has_replication_status_changed(fslogic_blocks:blocks(), fslogic_blocks:blocks(),
    non_neg_integer(), non_neg_integer()) -> boolean().
has_replication_status_changed(FirstLocalBlocksBeforeUpdate, FirestLocalBlocks, OldSize, NewSize) ->
    is_fully_replicated(FirstLocalBlocksBeforeUpdate, OldSize) =/= is_fully_replicated(FirestLocalBlocks, NewSize).

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
do_local_truncate(FileSize, LocalLocation = #document{value = #file_location{size = LocalSize}}) when LocalSize < FileSize ->
    append(LocalLocation, [#file_block{offset = LocalSize, size = FileSize - LocalSize}], true);
do_local_truncate(FileSize, LocalLocation = #document{value = #file_location{size = LocalSize}}) when LocalSize > FileSize ->
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
append(#document{value = #file_location{size = OldSize} = Loc} = Doc, Blocks, BumpVersion) ->
    % TODO VFS-4743 - some changes are published but blocks does not appear (blocks are private)
    OldBlocks = fslogic_location_cache:get_blocks(Doc, #{overlapping_blocks => Blocks}),
    NewBlocks = fslogic_blocks:merge(Blocks, OldBlocks),
    NewSize = fslogic_blocks:upper(Blocks),

    case BumpVersion of
        true ->
            version_vector:bump_version(
                replica_changes:add_change(
                    fslogic_location_cache:update_blocks(Doc#document{value =
                    Loc#file_location{size = max(OldSize, NewSize)}}, NewBlocks),
                    Blocks));
        false ->
            fslogic_location_cache:update_blocks(Doc#document{value =
                Loc#file_location{size = max(OldSize, NewSize)}}, NewBlocks)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Inavlidates given blocks in given locations. File size is also updated.
%% @end
%%--------------------------------------------------------------------
-spec shrink(#document{value :: #file_location{}}, [#file_block{}], non_neg_integer()) ->
    file_location:doc().
shrink(Doc = #document{value = Loc}, Blocks, NewSize) ->
    OldBlocks = fslogic_location_cache:get_blocks(Doc, #{overlapping_blocks => Blocks}),
    NewBlocks = fslogic_blocks:invalidate(OldBlocks, Blocks),
    NewBlocks1 = fslogic_blocks:consolidate(NewBlocks),
    version_vector:bump_version(
        replica_changes:add_change(
            fslogic_location_cache:update_blocks(Doc#document{value =
            Loc#file_location{size = NewSize}}, NewBlocks1),
            {shrink, NewSize})).

%% @private
-spec is_fully_replicated(fslogic_blocks:blocks(), non_neg_integer()) -> boolean().
is_fully_replicated([], 0) ->
    true;
is_fully_replicated([#file_block{offset = 0, size = Size}], Size) ->
    true;
is_fully_replicated(_, _) ->
    false.