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

%% API - file location updates
-export([update/4, rename/2]).
%% API - blocks analysis
-export([has_replica_status_changed/4]).
%% API - replica update result analysis
-export([get_location_changes/1, has_size_changed/1, has_replica_status_changed/1]).

% Description of update result containing information required to produce events
% describing changes of local file replica
-type report() :: #{
    location_changes := location_changes_description(),
    size_changed => boolean(),
    replica_status_changed => boolean()
}.

% Types describing local changes required to produce file_location_changed event
-type location_change_description() :: {
    file_location:record(), % File location record containing blocks from ChangesOffset to ChangesEnd
    ChangesOffset :: non_neg_integer(),
    ChangesEnd :: non_neg_integer()
}.
-type location_changes_description() :: [location_change_description()].

% Description of blocks' changes. Each tuple contain information about blocks that triggered change
% and information about final blocks that has been saved as a result.
-type blocks_changes_description() :: [{BlocksTriggeringChange :: fslogic_blocks:blocks(),
    BlocksSaved :: fslogic_blocks:blocks()}].

-export_type([report/0, location_change_description/0, location_changes_description/0]).

%%%===================================================================
%%% API - file location updates
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
    {ok, report()} | {error, Reason :: term()}.
update(FileCtx, Blocks, FileSize, BumpVersion) ->
    replica_synchronizer:apply(FileCtx,
        fun() ->
            case fslogic_cache:get_local_location() of
                Location = #document{
                    value = #file_location{
                        size = OldSize
                    }
                } ->
                    FirstLocalBlocksBeforeAppend = fslogic_location_cache:get_blocks(Location, #{count => 2}),
                    {TruncatedLocation, LocationTruncateChanges} = do_local_truncate(FileSize, Location),

                    {UpdatedLocation, LocationAppendChanges} = append(TruncatedLocation, Blocks, BumpVersion),
                    UpdateDescription = #{location_changes => LocationTruncateChanges ++ LocationAppendChanges},
                    fslogic_location_cache:save_location(UpdatedLocation),
                    #document{value = #file_location{size = UpdatedSize}} = UpdatedLocation,
                    FirstLocalBlocks = fslogic_location_cache:get_blocks(UpdatedLocation, #{count => 2}),
                    ReplicaStatusChanged = has_replica_status_changed(
                        FirstLocalBlocksBeforeAppend, FirstLocalBlocks, OldSize, UpdatedSize),
                    case {ReplicaStatusChanged, UpdatedSize =/= OldSize} of
                        {true, SizeChanged} ->
                            {ok, UpdateDescription#{size_changed => SizeChanged, replica_status_changed => true}};
                        {_, true} ->
                            {ok, UpdateDescription#{size_changed => true}};
                        _ ->
                            {ok, UpdateDescription}
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

%%%===================================================================
%%% API - blocks analysis
%%%===================================================================

-spec has_replica_status_changed(fslogic_blocks:blocks(), fslogic_blocks:blocks(),
    non_neg_integer(), non_neg_integer()) -> boolean().
has_replica_status_changed(FirstLocalBlocksBeforeUpdate, FirstLocalBlocks, OldSize, NewSize) ->
    is_fully_replicated(FirstLocalBlocksBeforeUpdate, OldSize) =/= is_fully_replicated(FirstLocalBlocks, NewSize).

%%%===================================================================
%%% API - replica update result analysis
%%%===================================================================

-spec get_location_changes(report()) -> location_changes_description().
get_location_changes(#{location_changes := LocationChanges} = _ReplicaUpdateResult) ->
    LocationChanges.

-spec has_size_changed(report()) -> boolean().
has_size_changed(ReplicaUpdateResult) ->
    maps:get(size_changed, ReplicaUpdateResult, false).

-spec has_replica_status_changed(report()) -> boolean().
has_replica_status_changed(ReplicaUpdateResult) ->
    maps:get(replica_status_changed, ReplicaUpdateResult, false).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Truncates blocks from given location. Works for both shrinking and growing file.
%% @end
%%--------------------------------------------------------------------
-spec do_local_truncate(FileSize :: non_neg_integer() | undefined, file_location:doc()) ->
    {file_location:doc(), location_changes_description()}.
do_local_truncate(undefined, Doc) ->
    {Doc, []};
do_local_truncate(FileSize, Doc = #document{value = #file_location{size = FileSize}}) ->
    {Doc, []};
do_local_truncate(FileSize, Doc = #document{value = Record = #file_location{size = LocalSize}}) when LocalSize < FileSize ->
    append(Doc#document{value = Record#file_location{size = FileSize}}, [#file_block{offset = LocalSize, size = 0}], true);
do_local_truncate(FileSize, LocalLocation = #document{value = #file_location{size = LocalSize}}) when LocalSize > FileSize ->
    shrink(LocalLocation, [#file_block{offset = FileSize, size = LocalSize - FileSize}], FileSize).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Appends given blocks to given locations and updates file size for those locations.
%% @end
%%--------------------------------------------------------------------
-spec append(file_location:doc(), fslogic_blocks:blocks(), boolean()) ->
    {file_location:doc(), location_changes_description()}.
append(Doc, [], _) ->
    {Doc, []};
append(#document{key = Key, value = #file_location{size = OldSize} = Loc} = Doc, Blocks, BumpVersion) ->
    % TODO VFS-4743 - some changes are published but blocks does not appear (blocks are private)
    OverlappingBlocksSequence = fslogic_location_cache:get_overlapping_blocks_sequence(Key, Blocks),
    NewSize = fslogic_blocks:upper(Blocks),

    DocWithUpdatedSize = Doc#document{value = Loc#file_location{size = max(OldSize, NewSize)}},
    {FinalDoc, BlocksChanges} = lists:foldl(fun({NewBlocks, OverlappingBlocks}, {TmpDoc, TmpBlocksChanges}) ->
        BlocksToSave = fslogic_blocks:merge(NewBlocks, OverlappingBlocks),
        {fslogic_location_cache:update_blocks(TmpDoc, BlocksToSave), [{NewBlocks, BlocksToSave} | TmpBlocksChanges]}
    end, {DocWithUpdatedSize, []}, OverlappingBlocksSequence),

    #document{value = FinalRecord} = BumpedFinalDoc = case BumpVersion of
        true -> version_vector:bump_version(replica_changes:add_change(FinalDoc, Blocks));
        false -> FinalDoc
    end,

    {BumpedFinalDoc, blocks_changes_to_location_changes_description(FinalRecord, lists:reverse(BlocksChanges))}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Inavlidates given blocks in given locations. File size is also updated.
%% @end
%%--------------------------------------------------------------------
-spec shrink(#document{value :: #file_location{}}, [#file_block{}], non_neg_integer()) ->
    {file_location:doc(), location_changes_description()}.
shrink(Doc = #document{key = Key, value = Loc}, Blocks, NewSize) ->
    OverlappingBlocksSequence = fslogic_location_cache:get_overlapping_blocks_sequence(Key, Blocks),

    DocWithUpdatedSize = Doc#document{value = Loc#file_location{size = NewSize}},
    {FinalDoc, BlocksChanges} = lists:foldl(fun({NewBlocks, OverlappingBlocks}, {TmpDoc, TmpBlocksChanges}) ->
        InvalidatedNewBlocks = fslogic_blocks:invalidate(OverlappingBlocks, NewBlocks),
        BlocksToSave = fslogic_blocks:consolidate(InvalidatedNewBlocks),
        {fslogic_location_cache:update_blocks(TmpDoc, BlocksToSave), [{NewBlocks, BlocksToSave} | TmpBlocksChanges]}
    end, {DocWithUpdatedSize, []}, OverlappingBlocksSequence),

    #document{value = FinalRecord} = BumpedFinalDoc =
        version_vector:bump_version(replica_changes:add_change(FinalDoc, {shrink, NewSize})),
    {BumpedFinalDoc, blocks_changes_to_location_changes_description(FinalRecord, lists:reverse(BlocksChanges))}.

%% @private
-spec is_fully_replicated(fslogic_blocks:blocks(), non_neg_integer()) -> boolean().
is_fully_replicated([], 0) ->
    true;
is_fully_replicated([#file_block{offset = 0, size = Size}], Size) ->
    true;
is_fully_replicated(_, _) ->
    false.

%% @private
-spec blocks_changes_to_location_changes_description(file_location:record(), blocks_changes_description()) ->
    location_changes_description().
blocks_changes_to_location_changes_description(Location, ChangeDescription) ->
    lists:map(fun({BlocksTriggeringChange, BlocksSaved}) ->
        LocationWithFilledGaps = fslogic_location:fill_location_gaps(BlocksTriggeringChange, Location, BlocksSaved,
            fslogic_cache:get_all_locations(), fslogic_cache:get_uuid()),
        {EventOffset, EventSize} = fslogic_location_cache:get_blocks_range(LocationWithFilledGaps, BlocksTriggeringChange),
        {LocationWithFilledGaps, EventOffset, EventSize}
    end, ChangeDescription).