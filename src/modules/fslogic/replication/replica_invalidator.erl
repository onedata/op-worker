%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module for invalidating blocks of file replicas
%%% @end
%%%--------------------------------------------------------------------
-module(replica_invalidator).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([invalidate_changes/5]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Invalidate given list of changes in blocks of file_location.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_changes(file_ctx:ctx(), file_location:doc(), Changes :: list(),
    Size :: non_neg_integer(), [fslogic_blocks:block()]) ->
    {file_location:doc() | deleted, file_ctx:ctx(), [fslogic_blocks:block()]}.
invalidate_changes(FileCtx, Doc = #document{value = Loc}, [], NewSize, ChangedBlocks) ->
    NewDoc = Doc#document{value = Loc#file_location{size = NewSize}},
    {ok, _} = fslogic_blocks:save_location(NewDoc),
    {NewDoc, file_ctx:reset(FileCtx), ChangedBlocks};
invalidate_changes(FileCtx, Doc = #document{value = Loc}, [{rename, Rename}],
    NewSize, ChangedBlocks) ->
    % if rename is present, it is always last element of changes list
    NewDoc = Doc#document{value = Loc#file_location{size = NewSize}},
    case replica_changes:rename_or_delete(FileCtx, NewDoc, Rename) of
        {deleted, FileCtx2} ->
            {deleted, FileCtx2, ChangedBlocks};
        {skipped, FileCtx2} ->
            {ok, _} = fslogic_blocks:save_location(NewDoc),
            {NewDoc, file_ctx:reset(FileCtx2), ChangedBlocks};
        {{renamed, RenamedDoc, _FileUuid, _TargetSpaceId}, FileCtx2} ->
            {ok, _} = fslogic_blocks:save_location(RenamedDoc),
            {RenamedDoc, file_ctx:reset(FileCtx2), ChangedBlocks}
    end;
invalidate_changes(FileCtx, Doc = #document{
    value = #file_location{
        size = OldSize
    }}, [{shrink, ShrinkSize} | Rest], Size, ChangedBlocks
) when OldSize > ShrinkSize ->
    ChangeBlocks = [#file_block{offset = ShrinkSize, size = OldSize - ShrinkSize}],
    OldBlocks = fslogic_blocks:get_blocks(Doc, #{overlapping_blocks => ChangeBlocks}),
    NewBlocks = fslogic_blocks:invalidate(OldBlocks, ChangeBlocks),
    NewBlocks1 = fslogic_blocks:consolidate(NewBlocks),
    invalidate_changes(FileCtx, fslogic_blocks:update_blocks(Doc, NewBlocks1),
        Rest, Size, ChangedBlocks ++ ChangeBlocks);
invalidate_changes(FileCtx, Doc, [{shrink, _} | Rest], Size, ChangedBlocks) ->
    invalidate_changes(FileCtx, Doc, Rest, Size, ChangedBlocks);
invalidate_changes(FileCtx, Doc, [Blocks | Rest], Size, ChangedBlocks
) ->
    OldBlocks = fslogic_blocks:get_blocks(Doc, #{overlapping_blocks => Blocks}),
    NewBlocks = fslogic_blocks:invalidate(OldBlocks, Blocks),
    NewBlocks1 = fslogic_blocks:consolidate(NewBlocks),
    invalidate_changes(FileCtx, fslogic_blocks:update_blocks(Doc, NewBlocks1),
        Rest, Size, ChangedBlocks ++ Blocks).

%%%===================================================================
%%% Internal functions
%%%===================================================================