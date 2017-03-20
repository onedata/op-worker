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

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([invalidate_changes/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Invalidate given list of changes in blocks of file_location.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_changes(file_ctx:ctx(), file_location:doc(), Changes :: list(),
    Size :: non_neg_integer()) -> {file_location:doc() | deleted, file_ctx:ctx()}.
invalidate_changes(FileCtx, Doc = #document{value = Loc}, [], NewSize) ->
    NewDoc = Doc#document{value = Loc#file_location{size = NewSize}},
    {ok, _} = file_location:save(NewDoc),
    {NewDoc, file_ctx:reset(FileCtx)};
invalidate_changes(FileCtx, Doc = #document{value = Loc}, [{rename, Rename}], NewSize) ->
    % if rename is present, it is always last element of changes list
    NewDoc = Doc#document{value = Loc#file_location{size = NewSize}},
    case replica_changes:rename_or_delete(FileCtx, NewDoc, Rename) of
        {deleted, FileCtx2} ->
            {deleted, FileCtx2};
        {skipped, FileCtx2} ->
            {ok, _} = file_location:save(NewDoc),
            {NewDoc, file_ctx:reset(FileCtx2)};
        {{renamed, RenamedDoc, _FileUuid, _TargetSpaceId}, FileCtx2} ->
            {ok, _} = file_location:save(RenamedDoc),
            {RenamedDoc, file_ctx:reset(FileCtx2)}
    end;
invalidate_changes(FileCtx, Doc = #document{
    value = Loc = #file_location{
        blocks = OldBlocks,
        size = OldSize
    }}, [{shrink, ShrinkSize} | Rest], Size
) when OldSize > ShrinkSize ->
    NewBlocks = fslogic_blocks:invalidate(OldBlocks, [#file_block{offset = ShrinkSize, size = OldSize - ShrinkSize}]),
    NewBlocks1 = fslogic_blocks:consolidate(NewBlocks),
    invalidate_changes(FileCtx, Doc#document{
        value = Loc#file_location{
            blocks = NewBlocks1
        }}, Rest, Size);
invalidate_changes(FileCtx, Doc, [{shrink, _} | Rest], Size) ->
    invalidate_changes(FileCtx, Doc, Rest, Size);
invalidate_changes(FileCtx, Doc = #document{
    value = Loc = #file_location{
        blocks = OldBlocks
    }}, [Blocks | Rest], Size
) ->
    NewBlocks = fslogic_blocks:invalidate(OldBlocks, Blocks),
    NewBlocks1 = fslogic_blocks:consolidate(NewBlocks),
    invalidate_changes(FileCtx, Doc#document{
        value = Loc#file_location{
            blocks = NewBlocks1
        }
    }, Rest, Size).

%%%===================================================================
%%% Internal functions
%%%===================================================================