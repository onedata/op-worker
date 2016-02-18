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

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([invalidate_changes/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Invalidate given list of changes in blocks of file_location.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_changes(file_location:doc(), Changes :: list(), Size :: non_neg_integer()) -> file_location:doc().
invalidate_changes(Doc = #document{value = Loc}, [], NewSize) ->
    NewDoc = Doc#document{value = Loc#file_location{size = NewSize}},
    {ok, _} = file_location:save(NewDoc),
    NewDoc;
invalidate_changes(Doc = #document{value = Loc = #file_location{blocks = OldBlocks, size = OldSize}}, [{shrink, ShrinkSize} | Rest], Size) when OldSize > ShrinkSize ->
    NewBlocks = fslogic_blocks:invalidate(OldBlocks, [#file_block{offset = ShrinkSize, size = OldSize - ShrinkSize}]),
    NewBlocks1 = fslogic_blocks:consolidate(NewBlocks),
    invalidate_changes(Doc#document{value = Loc#file_location{blocks = NewBlocks1}}, Rest, Size);
invalidate_changes(Doc, [{shrink, _} | Rest], Size) ->
    invalidate_changes(Doc, Rest, Size);
invalidate_changes(Doc = #document{value = Loc = #file_location{blocks = OldBlocks}}, [Blocks | Rest], Size) ->
    NewBlocks = fslogic_blocks:invalidate(OldBlocks, Blocks),
    NewBlocks1 = fslogic_blocks:consolidate(NewBlocks),
    invalidate_changes(Doc#document{value = Loc#file_location{blocks = NewBlocks1}}, Rest, Size).

%%%===================================================================
%%% Internal functions
%%%===================================================================