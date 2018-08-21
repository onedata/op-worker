%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module provides tools for blocks management inside locations.
%%% It covers cache and decides if updates of blocks (or location that includes
%%% blocks) should be saved to cache or directly to datastore.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_location_cache).
-author("Michał Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("global_definitions.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

-type block() :: fslogic_blocks:block().
-type blocks() :: fslogic_blocks:blocks().
-type blocks_tree() :: gb_sets:set(). % TODO - use gb_trees (it is faster)
-type stored_blocks() :: blocks() | blocks_tree(). % set only when used by blocks' cache
-type location() :: file_location:doc().
-type location_or_record() :: location() | file_location:record().
-type get_doc_opts() :: boolean() | {blocks_num, non_neg_integer()}.
-type get_blocks_opts() :: #{overlapping_blocks => blocks(),
    count => non_neg_integer()}.

-export_type([block/0, blocks/0, blocks_tree/0, stored_blocks/0]).

%% Location getters/setters
-export([get_location/2, get_location/3, save_location/1, cache_location/1,
    update_location/4, create_location/1, create_location/2, delete_location/2,
    force_flush/1]).
%% Blocks getters/setters
-export([get_blocks/1, get_blocks/2, set_blocks/2, set_final_blocks/2,
    update_blocks/2]).
%% Blocks API
-export([get_location_size/2, get_blocks_range/1, get_blocks_range/2]).

%%%===================================================================
%%% Location getters/setters
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% @equiv get_location(LocId, FileUuid, true).
%% @end
%%-------------------------------------------------------------------
-spec get_location(file_location:id(), file_meta:uuid()) ->
    {ok, file_location:doc()} | {error, term()}.
get_location(LocId, FileUuid) ->
    get_location(LocId, FileUuid, true).

%%-------------------------------------------------------------------
%% @doc
%% Returns location document from cache or datastore. Third parameter
%% tells if blocks should be included in document (possible limiting of blocks num).
%% @end
%%-------------------------------------------------------------------
-spec get_location(file_location:id(), file_meta:uuid(), get_doc_opts()) ->
    {ok, file_location:doc()} | {error, term()}.
get_location(LocId, FileUuid, BlocksOptions) ->
    replica_synchronizer:apply_or_run_locally(FileUuid, fun() ->
        case fslogic_cache:get_doc(LocId) of
            #document{} = LocationDoc ->
                {ok, LocationDoc};
            Error ->
                Error
        end
    end, fun() ->
        case fslogic_cache:get_doc(LocId) of
            #document{key = Key, value = Location} = LocationDoc ->
                case BlocksOptions of
                    false ->
                        {ok, LocationDoc};
                    true ->
                        {ok, fslogic_cache:attach_blocks(LocationDoc)};
                    {blocks_num, Num} ->
                        {ok, LocationDoc#document{value =
                        Location#file_location{blocks =
                        get_blocks(Key, #{count => Num})}}}
                end;
            Error ->
                Error
        end
    end, fun() ->
        get_location_not_cached(LocId, BlocksOptions)
    end).

%%-------------------------------------------------------------------
%% @doc
%% Saves location document in cache or datastore.
%% @end
%%-------------------------------------------------------------------
-spec save_location(file_location:doc()) -> {ok, file_location:id()} | {error, term()}.
save_location(#document{value = #file_location{uuid = Uuid}} = FileLocation) ->
    replica_synchronizer:apply_or_run_locally(Uuid, fun() ->
        fslogic_cache:save_doc(FileLocation)
    end, fun() ->
        fslogic_cache:save_doc(FileLocation)
    end, fun() ->
        file_location:save_and_update_quota(FileLocation)
    end).

%%-------------------------------------------------------------------
%% @doc
%% Caches location document.
%% @end
%%-------------------------------------------------------------------
-spec cache_location(file_location:doc()) -> ok.
cache_location(#document{key = Key, value = #file_location{uuid = Uuid, blocks = Blocks}} = FileLocation) ->
    replica_synchronizer:apply_if_alive(Uuid, fun() ->
        fslogic_cache:cache_doc(FileLocation),
        fslogic_cache:cache_blocks(Key, Blocks),
        ok
    end).

%%-------------------------------------------------------------------
%% @doc
%% Updates location document
%% @end
%%-------------------------------------------------------------------
-spec update_location(file_meta:uuid(), file_location:id(), file_location:diff(),
    boolean()) -> {ok, file_location:doc()} | {error, term()}.
update_location(FileUuid, LocId, Diff, ModifyBlocks) ->
    % TODO 4743 - Cannot update local blocks with update
    replica_synchronizer:apply_or_run_locally(FileUuid, fun() ->
        case fslogic_cache:flush(LocId, ModifyBlocks) of
            ok ->
                case file_location:update(LocId, Diff) of
                    {ok, #document{value = #file_location{blocks = Blocks}} = Doc} = Ans ->
                        fslogic_cache:cache_doc(Doc),
                        case ModifyBlocks of
                            true -> fslogic_cache:cache_blocks(LocId, Blocks);
                            _ -> ok
                        end,
                        Ans;
                    Error ->
                        Error
                end;
            FlushError ->
                FlushError
        end
    end, fun() ->
        file_location:update(LocId, Diff)
    end).

%%-------------------------------------------------------------------
%% @doc
%% @equiv create_location(Doc, false).
%% @end
%%-------------------------------------------------------------------
-spec create_location(file_location:doc()) ->
    {ok, file_location:id()} | {error, term()}.
create_location(Doc) ->
    create_location(Doc, false).

%%-------------------------------------------------------------------
%% @doc
%% Creates location document.
%% @end
%%-------------------------------------------------------------------
-spec create_location(file_location:doc(), boolean()) ->
    {ok, file_location:id()} | {error, term()}.
create_location(#document{key = Key, value = #file_location{uuid = Uuid}} = Doc,
    GeneratedKey) ->
    replica_synchronizer:apply_or_run_locally(Uuid, fun() ->
        case fslogic_cache:get_doc(Key) of
            #document{} ->
                {error, already_exists};
            _ ->
                case file_location:create(Doc, GeneratedKey) of
                    {ok, #document{key = Key, value = #file_location{blocks = Blocks}} = FileLocation} ->
                        fslogic_cache:cache_blocks(Key, Blocks),
                        fslogic_cache:update_size(Key, fslogic_blocks:size(Blocks)),
                        fslogic_cache:cache_doc(FileLocation);
                    Other ->
                        Other
                end
        end
    end, fun() ->
        ?extract_key(file_location:create_and_update_quota(Doc, GeneratedKey))
    end).

%%-------------------------------------------------------------------
%% @doc
%% Deletes location document.
%% @end
%%-------------------------------------------------------------------
-spec delete_location(file_meta:uuid(), file_location:id()) ->
    ok | {error, term()}.
delete_location(Uuid, LocId) ->
    replica_synchronizer:apply_or_run_locally(Uuid, fun() ->
        fslogic_cache:delete_doc(LocId)
    end, fun() ->
        file_location:delete_and_update_quota(LocId)
    end).

%%-------------------------------------------------------------------
%% @doc
%% Forces flush of cache.
%% @end
%%-------------------------------------------------------------------
-spec force_flush(file_meta:uuid()) -> ok | flush_error.
force_flush(Uuid) ->
    replica_synchronizer:apply_if_alive(Uuid, fun fslogic_cache:flush/0).

%%%===================================================================
%%% Blocks getters/setters
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% @equiv get_blocks(FileLocation, [])
%% @end
%%-------------------------------------------------------------------
-spec get_blocks(location() | file_location:id()) -> blocks().
get_blocks(FileLocation) ->
    get_blocks(FileLocation, #{}).

%%-------------------------------------------------------------------
%% @doc
%% @equiv Returns blocks from cache or document.
%% @end
%%-------------------------------------------------------------------
-spec get_blocks(location() | file_location:id(), get_blocks_opts()) -> blocks().
get_blocks(#document{key = Key, value = #file_location{blocks = Blocks}}, Options) ->
    case fslogic_cache:is_current_proc_cache() of
        false ->
            Blocks;
        _ ->
            get_blocks(Key, Options)
    end;
get_blocks(Key, #{overlapping_blocks := OverlappingBlocks}) ->
    case OverlappingBlocks of
        [] ->
            [];
        [#file_block{offset = Offset1, size = Size1} | OverlappingBlocksTail] ->
            Blocks = fslogic_cache:get_blocks_tree(Key),
            {Start, Stop} = lists:foldl(fun(#file_block{offset = Offset, size = Size},
                {#file_block{offset = TmpO} = TmpStart, TmpStop}) ->
                Start2 = case Offset < TmpO of
                    true -> #file_block{offset = Offset, size = 0};
                    _ -> TmpStart
                end,
                End = Offset + Size,
                Stop2 = case End > TmpStop of
                    true -> End;
                    _ -> TmpStop
                end,
                {Start2, Stop2}
            end, {#file_block{offset = Offset1, size = 0}, Offset1 + Size1}, OverlappingBlocksTail),
            Iter = gb_sets:iterator_from(Start, Blocks),
            Ans = get_block_while(Iter, Stop),
            fslogic_cache:use_blocks(Key, Ans),
            Ans
    end;
get_blocks(Key, #{count := Num}) ->
            Blocks = fslogic_cache:get_blocks_tree(Key),
            Iter = gb_sets:iterator(Blocks),
            Ans = get_blocks_num(Iter, Num),
            Ans;
get_blocks(Key, _Options) ->
    fslogic_cache:get_blocks(Key).

%%-------------------------------------------------------------------
%% @doc
%% Sets blocks in location document.
%% @end
%%-------------------------------------------------------------------
-spec set_blocks(location(), blocks()) -> location().
set_blocks(#document{key = Key, value = FileLocation} = Doc, Blocks) ->
    case fslogic_cache:is_current_proc_cache() of
        false ->
            Doc#document{value = FileLocation#file_location{blocks = Blocks}};
        _ ->
            fslogic_cache:save_blocks(Key, Blocks),
            fslogic_cache:mark_changed_blocks(Key, all, all),
            Doc
    end.

%%-------------------------------------------------------------------
%% @doc
%% Updates blocks in location document.
%% @end
%%-------------------------------------------------------------------
-spec update_blocks(location(), blocks()) -> {location(), boolean()}.
update_blocks(#document{key = LocID, value = FileLocation} = Doc, NewBlocks) ->
    case fslogic_cache:is_current_proc_cache() of
        false ->
            Doc#document{value = FileLocation#file_location{blocks = NewBlocks}};
        _ ->
            Blocks = fslogic_cache:get_blocks_tree(LocID),
            OldBlocks = fslogic_cache:finish_blocks_usage(LocID),

            {BlocksToSave, BlocksToDel} = fslogic_cache:get_changed_blocks(LocID),

            {Blocks2, Exclude, SizeChange, BlocksToDel2, BlocksToSave2} =
                lists:foldl(fun(#file_block{offset = O, size = S} = B,
                    {TmpBlocks, NotChanged, TmpSize, TmpBlocksToDel, TmpBlocksToSave}) ->
                    B2 = #file_block{offset = O+S, size = S},
                    case lists:member(B, NewBlocks) of
                        true ->
                            {TmpBlocks, [B | NotChanged], TmpSize, TmpBlocksToDel, TmpBlocksToSave};
                        _ ->
                            case sets:is_element(B, TmpBlocksToSave) of
                                true ->
                                    {gb_sets:delete(B2, TmpBlocks), NotChanged, TmpSize - S,
                                        TmpBlocksToDel, sets:del_element(B, TmpBlocksToSave)};
                                _ ->
                                    {gb_sets:delete(B2, TmpBlocks), NotChanged, TmpSize - S,
                                        sets:add_element(B, TmpBlocksToDel), TmpBlocksToSave}
                            end
                    end
                end, {Blocks, [], 0, BlocksToDel, BlocksToSave}, OldBlocks),

            {Blocks3, SizeChange2, BlocksToSave3} = lists:foldl(fun(#file_block{offset = O, size = S} = B,
                {Acc, TmpSize, TmpBlocksToSave}) ->
                B2 = #file_block{offset = O+S, size = S},
                {gb_sets:add(B2, Acc), TmpSize + S, sets:add_element(B, TmpBlocksToSave)}
            end, {Blocks2, SizeChange, BlocksToSave2}, NewBlocks -- Exclude),

            fslogic_cache:update_size(LocID, SizeChange2),
            fslogic_cache:save_blocks(LocID, Blocks3),
            MarkAns = fslogic_cache:mark_changed_blocks(LocID, BlocksToSave3, BlocksToDel2),
            {Doc, MarkAns}
    end.

%%-------------------------------------------------------------------
%% @doc
%% Sets blocks in location document. The blocks are not translated to
%% to set even if the process is used as cache.
%% @end
%%-------------------------------------------------------------------
-spec set_final_blocks(location_or_record(), blocks()) -> location_or_record().
set_final_blocks(#file_location{} = FileLocation, Blocks) ->
    FileLocation#file_location{blocks = Blocks};
set_final_blocks(#document{value = Record} = Doc, Blocks) ->
    Doc#document{value = set_final_blocks(Record, Blocks)}.

%%%===================================================================
%%% Blocks API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns size of local file.
%% @end
%%-------------------------------------------------------------------
-spec get_location_size(file_location:id(), file_meta:uuid()) -> non_neg_integer().
get_location_size(LocId, Uuid) ->
    replica_synchronizer:apply_or_run_locally(Uuid, fun() ->
        fslogic_cache:get_local_size(LocId)
    end, fun() ->
        case file_location:get(LocId) of
            {ok, #document{value = #file_location{blocks = Blocks}}} ->
                fslogic_blocks:size(Blocks);
            _ ->
                0
        end
    end).

%%-------------------------------------------------------------------
%% @doc
%% @equiv get_blocks_range(Location, undefined).
%% @end
%%-------------------------------------------------------------------
-spec get_blocks_range(location_or_record() | blocks()) ->
    {non_neg_integer() | undefined, non_neg_integer() | undefined}.
get_blocks_range(Location) ->
    get_blocks_range(Location, undefined).

%%-------------------------------------------------------------------
%% @doc
%% Returns max range of blocks from location and request.
%% @end
%%-------------------------------------------------------------------
-spec get_blocks_range(location_or_record() | blocks(), blocks() | undefined) ->
    {non_neg_integer() | undefined, non_neg_integer() | undefined}.
get_blocks_range(Location, RequestRange) ->
    case get_blocks_range_helper(RequestRange) of
        {undefined, undefined} ->
            {undefined, undefined};
        {Start1, Stop1} ->
            case get_blocks_range_helper(Location) of
                {undefined, undefined} ->
                    {Start1, Stop1};
                {Start2, Stop2} ->
                    {min(Start1, Start2), max(Stop1, Stop2)}
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Gets blocks using iterator.
%% @end
%%-------------------------------------------------------------------
-spec get_block_while(gb_sets:iter(), non_neg_integer()) -> blocks().
get_block_while(Iter, Stop) ->
    case gb_sets:next(Iter) of
        none ->
            [];
        {#file_block{offset = Offset, size = Size}, Iter2} ->
            Offset2 = Offset - Size,
            case Offset2 =< Stop of
                true ->
                    [#file_block{offset = Offset2, size = Size} |
                        get_block_while(Iter2, Stop)];
                _ ->
                    []
            end
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Gets blocks using iterator.
%% @end
%%-------------------------------------------------------------------
-spec get_blocks_num(gb_sets:iter(), non_neg_integer()) -> blocks().
get_blocks_num(_Iter, 0) ->
    [];
get_blocks_num(Iter, Num) ->
    case gb_sets:next(Iter) of
        none ->
            [];
        {#file_block{offset = Offset, size = Size}, Iter2} ->
            [#file_block{offset = Offset - Size, size = Size} |
                get_blocks_num(Iter2, Num - 1)]
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns range of blocks.
%% @end
%%-------------------------------------------------------------------
-spec get_blocks_range_helper(location_or_record() | blocks() | undefined) ->
    {non_neg_integer() | undefined, non_neg_integer() | undefined}.
get_blocks_range_helper(undefined) ->
    {undefined, undefined};
get_blocks_range_helper(#document{value = Record}) ->
    get_blocks_range_helper(Record);
get_blocks_range_helper(#file_location{blocks = Blocks}) ->
    get_blocks_range_helper(Blocks);
get_blocks_range_helper(Blocks0) ->
    Blocks = lists:sort(Blocks0),
    case Blocks of
        [] ->
            {undefined, undefined};
        [#file_block{offset = StartOffset} | _] ->
            #file_block{offset = StopOffset, size = StopSize} = lists:last(Blocks),
            {StartOffset, StopOffset + StopSize}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns location document from datastore. Third parameter
%% tells if blocks should be included in document (possible limiting of blocks num).
%% @end
%%-------------------------------------------------------------------
-spec get_location_not_cached(file_location:id(), get_doc_opts()) ->
    {ok, file_location:doc()} | {error, term()}.
get_location_not_cached(LocId, false) ->
    case file_location:get(LocId) of
        {ok, #document{value = Location} = LocationDoc} ->
            {ok, LocationDoc#document{value =
            Location#file_location{blocks = []}}};
        Error ->
            Error
    end;
get_location_not_cached(LocId, true) ->
    case file_location:get(LocId) of
        {ok, Doc} ->
            {ok, fslogic_cache:attach_local_blocks(Doc)};
        Error ->
            Error
    end;
get_location_not_cached(LocId, {blocks_num, Num}) ->
    case file_location:get(LocId) of
        {ok, LocationDoc} ->
            #document{value = #file_location{blocks = Blocks} = Location}
                = LocationDoc2 = fslogic_cache:attach_local_blocks(LocationDoc),
            {ok, LocationDoc2#document{value =
            Location#file_location{blocks = lists:sublist(Blocks, Num)}}};
        Error ->
            Error
    end.