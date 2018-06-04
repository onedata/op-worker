%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module provides tools for blocks management.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_blocks).
-author("Rafal Slota").

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").

-type block() :: #file_block{}.
-type blocks() :: [block()].
-type location() :: file_location:doc() | #file_location{}.
-type stored_blocks() :: blocks() | gb_sets:set(). % set only when used by blocks' cache

-export_type([block/0, blocks/0, stored_blocks/0]).

%% API
-export([merge/2, aggregate/2, consolidate/1, invalidate/2, upper/1, lower/1, size/1]).
-export([get_blocks_range/2, get_blocks/1, get_blocks/2,
    set_blocks/2, set_final_blocks/2, update_blocks/2,
    save_location/1, update_location/3, create_location/2, get_location/2,
    get_location/3, cache_location/1, init_cache/0, flush/0, check_flush/0,
    delete_location/2, get_size/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns size of local file.
%% @end
%%-------------------------------------------------------------------
-spec get_size(file_location:id(), file_meta:uuid()) -> non_neg_integer().
get_size(LocId, Uuid) ->
    case get(file_locations) of
        undefined ->
            Fun1 = fun() ->
                get_size(LocId, Uuid)
            end,
            Fun2 = fun() ->
                case file_location:get(LocId) of
                    {ok, #document{value = #file_location{blocks = Blocks}}} ->
                        ?MODULE:size(Blocks);
                    _ ->
                        0
                end
            end,
            replica_synchronizer:apply(Uuid, Fun1, Fun2);
        _ ->
            get_location(LocId, Uuid),
            Sizes = get(file_location_sizes),
            proplists:get_value(LocId, Sizes, 0)
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns max range of blocks from location and request.
%% @end
%%-------------------------------------------------------------------
-spec get_blocks_range(location() | blocks(), blocks() | undefined) ->
    {non_neg_integer() | undefined, non_neg_integer() | undefined}.
get_blocks_range(Location, RequestRange) ->
    case get_blocks_range(RequestRange) of
        {undefined, undefined} ->
            {undefined, undefined};
        {Start1, Stop1} ->
            case get_blocks_range(Location) of
                {undefined, undefined} ->
                    {Start1, Stop1};
                {Start2, Stop2} ->
                    {min(Start1, Start2), max(Stop1, Stop2)}
            end
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns range of blocks.
%% @end
%%-------------------------------------------------------------------
-spec get_blocks_range(location() | blocks() | undefined) ->
    {non_neg_integer() | undefined, non_neg_integer() | undefined}.
get_blocks_range(undefined) ->
    {undefined, undefined};
get_blocks_range(#document{value = Record}) ->
    get_blocks_range(Record);
get_blocks_range(#file_location{blocks = Blocks}) ->
    get_blocks_range(Blocks);
get_blocks_range(Blocks) ->
    case Blocks of
        [] ->
            {undefined, undefined};
        [#file_block{offset = StartOffset} | _] ->
            #file_block{offset = StopOffset, size = StopSize} = lists:last(Blocks),
            {StartOffset, StopOffset + StopSize}
    end.

%%-------------------------------------------------------------------
%% @doc
%% @equiv get_blocks(FileLocation, [])
%% @end
%%-------------------------------------------------------------------
-spec get_blocks(location()) -> blocks().
get_blocks(FileLocation) ->
    get_blocks(FileLocation, #{}).

get_blocks(#document{key = Key, value = Record}, Options) ->
    get_blocks(Record, Options, Key);
get_blocks(Location, Options) ->
    get_blocks(Location, Options, undefined).

%%-------------------------------------------------------------------
%% @doc
%% Returns blocks from location document/record. Blocks in record can be in
%% stored as set.
%% @end
%%-------------------------------------------------------------------
-spec get_blocks(location(), map()) -> blocks().
get_blocks(#file_location{blocks = Blocks}, _Options, _LocKey) when is_list(Blocks) ->
    Blocks;
get_blocks(#file_location{blocks = Blocks}, #{overlapping_sorted_blocks := OB}, LocKey) ->
    case OB of
        [] ->
            [];
        [Start | _] ->
            #file_block{offset = StopOffset, size = StopSize} = lists:last(OB),
            Iter = gb_sets:iterator_from(Start, Blocks),
            Ans = get_block_while(Iter, StopOffset + StopSize),
            case LocKey of
                undefined ->
                    ok;
                _ ->
                    BIU = get(blocks_in_use),
                    put(blocks_in_use, [{LocKey, Ans} | proplists:delete(LocKey, BIU)])
            end,
            Ans
    end;
get_blocks(#file_location{blocks = Blocks}, #{overlapping_blocks := OB}, LocKey) ->
    case OB of
        [] ->
            [];
        [#file_block{offset = O1, size = S1} = Start0 | OBTail] ->
            {Start, Stop} = lists:foldl(fun(#file_block{offset = O, size = S} = Block,
                {#file_block{offset = TmpO} = TmpStart, TmpStop}) ->
                Start2 = case O < TmpO of
                    true -> Block;
                    _ -> TmpStart
                end,
                End = O + S,
                Stop2 = case End > TmpStop of
                    true -> End;
                    _ -> TmpStop
                end,
                {Start2, Stop2}
            end, {Start0, O1 + S1}, OBTail),
            Iter = gb_sets:iterator_from(Start, Blocks),
            Ans = get_block_while(Iter, Stop),
            case LocKey of
                undefined ->
                    ok;
                _ ->
                    BIU = get(blocks_in_use),
                    put(blocks_in_use, [{LocKey, Ans} | proplists:delete(LocKey, BIU)])
            end,
            Ans
    end;
get_blocks(#file_location{blocks = Blocks}, _Options, _LocKey) ->
    lists:map(
        fun(#file_block{offset = O, size = S}) ->
            #file_block{offset = O-S, size = S}
        end, gb_sets:to_list(Blocks)).

%%-------------------------------------------------------------------
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
%% @doc
%% Sets blocks in location document.
%% @end
%%-------------------------------------------------------------------
-spec set_blocks(location(), blocks()) -> location().
set_blocks(#file_location{blocks = Blocks} = FileLocation, NewBlocks)
    when is_list(Blocks) -> FileLocation#file_location{blocks = NewBlocks};
set_blocks(#file_location{} = FileLocation, NewBlocks) ->
    FileLocation#file_location{blocks = gb_sets:from_ordset(lists:map(
        fun(#file_block{offset = O, size = S}) ->
            #file_block{offset = O+S, size = S}
        end, NewBlocks))};
set_blocks(#document{value = Record} = Doc, Blocks) ->
    Doc#document{value = set_blocks(Record, Blocks)}.

%%-------------------------------------------------------------------
%% @doc
%% Updates blocks in location document.
%% @end
%%-------------------------------------------------------------------
-spec update_blocks(location(), blocks()) -> location().
update_blocks(#document{value = #file_location{blocks = Blocks} =
    FileLocation} = Doc, NewBlocks) when is_list(Blocks) ->
    Doc#document{value = FileLocation#file_location{blocks = NewBlocks}};
update_blocks(#document{key = LocID, value = #file_location{blocks = Blocks}
    = FileLocation} = Doc, NewBlocks) ->
    BIU = get(blocks_in_use),
    put(blocks_in_use, proplists:delete(LocID, BIU)),
    OldBlocks = proplists:get_value(LocID, BIU, []),

    Sizes = get(file_location_sizes),
    Size = proplists:get_value(LocID, Sizes, 0),

    {Blocks2, Exclude, Size2} = lists:foldl(fun(#file_block{offset = O,
        size = S} = B, {TmpBlocks, NotChanged, TmpSize}) ->
        B2 = #file_block{offset = O+S, size = S},
        case lists:member(B, NewBlocks) of
            true ->
                {TmpBlocks, [B | NotChanged], TmpSize};
            _ ->
                {gb_sets:delete(B2, TmpBlocks), NotChanged, TmpSize - S}
        end
    end, {Blocks, [], Size}, OldBlocks),

    {Blocks3, Size3} = lists:foldl(fun(#file_block{offset = O, size = S},
        {Acc, TmpSize}) ->
        B2 = #file_block{offset = O+S, size = S},
        {gb_sets:add(B2, Acc), TmpSize + S}
    end, {Blocks2, Size2}, NewBlocks -- Exclude),

    put(file_location_sizes, [{LocID, Size3} | proplists:delete(LocID, Sizes)]),
    Doc#document{value = FileLocation#file_location{blocks = Blocks3}}.

%%-------------------------------------------------------------------
%% @doc
%% Sets blocks in location document. The blocks are not translated to
%% to set even if the process is used as cache.
%% @end
%%-------------------------------------------------------------------
-spec set_final_blocks(location(), blocks()) -> location().
set_final_blocks(#file_location{} = FileLocation, Blocks) ->
    FileLocation#file_location{blocks = Blocks};
set_final_blocks(#document{value = Record} = Doc, Blocks) ->
    Doc#document{value = set_final_blocks(Record, Blocks)}.

%%-------------------------------------------------------------------
%% @doc
%% Saves location document in cache or datastore.
%% @end
%%-------------------------------------------------------------------
-spec save_location(file_location:doc()) -> {ok, file_location:id()} | {error, term()}.
save_location(#document{key = Key, value = Location =
    #file_location{uuid = Uuid, blocks = Blocks}} = FileLocation) ->
    case get(file_locations) of
        undefined ->
            Fun1 = fun() ->
                save_location(FileLocation)
            end,
            Fun2 = fun() ->
                file_location:save(FileLocation)
            end,
            replica_synchronizer:apply(Uuid, Fun1, Fun2);
        Locations ->
            init_flush_check(),

            FileLocation2 = case is_list(Blocks) of
                true ->
                    put(file_location_sizes, [{Key, ?MODULE:size(Blocks)} |
                        proplists:delete(Key, get(file_location_sizes))]),
                    FileLocation#document{value =
                    Location#file_location{blocks =
                    gb_sets:from_ordset(lists:map(
                        fun(#file_block{offset = O, size = S}) ->
                            #file_block{offset = O+S, size = S}
                        end, Blocks))}};
                _ ->
                    FileLocation
            end,
            put(file_locations,
                [{Key, {FileLocation2, updated}} | proplists:delete(Key, Locations)]),
            {ok, Key}
    end.

%%-------------------------------------------------------------------
%% @doc
%% Caches location document.
%% @end
%%-------------------------------------------------------------------
-spec cache_location(file_location:doc()) -> ok.
cache_location(#document{key = Key, value = Location =
    #file_location{uuid = Uuid, blocks = Blocks}} = FileLocation) ->
    case get(file_locations) of
        undefined ->
            Fun1 = fun() ->
                cache_location(FileLocation)
            end,
            Fun2 = fun() ->
                ok
            end,
            replica_synchronizer:apply(Uuid, Fun1, Fun2);
        Locations ->
            FileLocation2 = case is_list(Blocks) of
                true ->
                    FileLocation#document{value =
                    Location#file_location{blocks =
                    gb_sets:from_ordset(lists:map(
                        fun(#file_block{offset = O, size = S}) ->
                            #file_block{offset = O+S, size = S}
                        end, Blocks))}};
                _ ->
                    FileLocation
            end,
            put(file_locations,
                [{Key, {FileLocation2, ok}} | proplists:delete(Key, Locations)]),
            put(file_location_sizes, [{Key, ?MODULE:size(Blocks)} |
                proplists:delete(Key, get(file_location_sizes))]),
            ok
    end.

%%-------------------------------------------------------------------
%% @doc
%% Updates location document
%% @end
%%-------------------------------------------------------------------
-spec update_location(file_meta:uuid(), file_location:id(), file_location:diff()) ->
    {ok, file_location:doc()} | {error, term()}.
update_location(FileUuid, LocId, Diff) ->
    case get(file_locations) of
        undefined ->
            Fun1 = fun() ->
                update_location(FileUuid, LocId, Diff)
            end,
            Fun2 = fun() ->
                file_location:update(LocId, Diff)
            end,
            replica_synchronizer:apply(FileUuid, Fun1, Fun2);
        Locations ->
            GetAns = case proplists:get_value(LocId, Locations) of
                undefined ->
                    file_location:get(LocId);
                {#document{value = Location = #file_location{blocks = Blocks}} =
                    LocationDoc, _} ->
                    LocationDoc2 = LocationDoc#document{value =
                    Location#file_location{blocks =
                    lists:map(
                        fun(#file_block{offset = O, size = S}) ->
                            #file_block{offset = O-S, size = S}
                        end, gb_sets:to_list(Blocks))}},
                    {ok, LocationDoc2}
            end,

            case GetAns of
                {ok, Document = #document{value = Value}} ->
                    try Diff(Value) of
                        {ok, Value2} ->
                            Document2 = Document#document{value = Value2},
                            case save_location(Document2) of
                                {ok, _} -> {ok, Document};
                                Other -> Other
                            end;
                        {error, Reason} ->
                            {error, Reason}
                    catch
                        _:Reason ->
                            {error, {Reason, erlang:get_stacktrace()}}
                    end;
                _ ->
                    GetAns
            end
    end.

%%-------------------------------------------------------------------
%% @doc
%% Creates location document.
%% @end
%%-------------------------------------------------------------------
-spec create_location(file_location:doc(), boolean()) ->
    {ok, file_location:id()} | {error, term()}.
create_location(#document{key = Key, value = #file_location{uuid = Uuid}} = Doc,
    GeneratedKey) ->
    case get(file_locations) of
        undefined ->
            Fun1 = fun() ->
                create_location(Doc, GeneratedKey)
            end,
            Fun2 = fun() ->
                file_location:create(Doc, GeneratedKey)
            end,
            replica_synchronizer:apply(Uuid, Fun1, Fun2);
        Locations ->
            GetAns = case proplists:get_value(Key, Locations) of
                undefined ->
                    file_location:get(Key);
                {Document, _} ->
                    {ok, Document}
            end,

            case GetAns of
                {ok, _} ->
                    {error, already_exists};
                {error, not_found} ->
                    case file_location:create(Doc, GeneratedKey) of
                        {ok, #document{value = Location = #file_location{blocks =
                        Blocks}} = FileLocation} ->
                            FileLocation2 = FileLocation#document{value =
                            Location#file_location{blocks =
                            gb_sets:from_ordset(lists:map(
                                fun(#file_block{offset = O, size = S}) ->
                                    #file_block{offset = O+S, size = S}
                                end, Blocks))}},
                            put(file_locations,
                                [{Key, {FileLocation2, updated}} |
                                    proplists:delete(Key, Locations)]),
                            put(file_location_sizes, [{Key, ?MODULE:size(Blocks)} |
                                proplists:delete(Key, get(file_location_sizes))]),
                            {ok, FileLocation};
                        Other ->
                            Other
                    end;
                Error ->
                    Error
            end
    end.

%%-------------------------------------------------------------------
%% @doc
%% Deletes location document.
%% @end
%%-------------------------------------------------------------------
-spec delete_location(file_meta:uuid(), file_location:id()) ->
    ok | {error, term()}.
delete_location(FileUuid, LocId) ->
    case get(file_locations) of
        undefined ->
            Fun1 = fun() ->
                delete_location(FileUuid, LocId)
            end,
            Fun2 = fun() ->
                file_location:delete(LocId)
            end,
            replica_synchronizer:apply(FileUuid, Fun1, Fun2);
        Locations ->
            put(file_locations, proplists:delete(LocId, Locations)),
            put(file_location_sizes, proplists:delete(LocId,
                get(file_location_sizes))),
            file_location:delete(LocId)
    end.

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
%% tells if blocks should be included in document.
%% @end
%%-------------------------------------------------------------------
-spec get_location(file_location:id(), file_meta:uuid(), boolean()) ->
    {ok, file_location:doc()} | {error, term()}.
% TODO VFS-4412 - allow specification of blocks number to be fetched
get_location(LocId, FileUuid, true) ->
    case get(file_locations) of
        undefined ->
            replica_synchronizer:flush_location(FileUuid), % TODO VFS-4412 - kills performance
            file_location:get(LocId);
        Locations ->
            case proplists:get_value(LocId, Locations) of
                undefined ->
                    case file_location:get(LocId) of
                        {ok, #document{value = Location =
                            #file_location{blocks = Blocks}} = LocationDoc} ->
                            LocationDoc2 = LocationDoc#document{value =
                                Location#file_location{blocks =
                                    gb_sets:from_ordset(lists:map(
                                        fun(#file_block{offset = O, size = S}) ->
                                            #file_block{offset = O+S, size = S}
                                        end, Blocks))}},
                            put(file_locations,
                                [{LocId, {LocationDoc2, ok}} | Locations]),
                            put(file_location_sizes, [{LocId, ?MODULE:size(Blocks)} |
                                proplists:delete(LocId, get(file_location_sizes))]),
                            {ok, LocationDoc2};
                        Error ->
                            Error
                    end;
                {Doc, _} ->
                    {ok, Doc}
            end
    end;
get_location(LocId, FileUuid, _) ->
    case get(file_locations) of
        undefined ->
            Fun1 = fun() ->
                get_location(LocId, FileUuid, false)
            end,
            Fun2 = fun() ->
                case file_location:get(LocId) of
                    {ok, #document{value = Location} = LocationDoc} ->
                        {ok, LocationDoc#document{value =
                        Location#file_location{blocks = []}}};
                    Error ->
                        Error
                end
            end,
            replica_synchronizer:apply(FileUuid, Fun1, Fun2);
        Locations ->
            case proplists:get_value(LocId, Locations) of
                undefined ->
                    case file_location:get(LocId) of
                        {ok, #document{value = Location =
                            #file_location{blocks = Blocks}} = LocationDoc} ->
                            LocationDoc2 = LocationDoc#document{value =
                            Location#file_location{blocks =
                            gb_sets:from_ordset(lists:map(
                                fun(#file_block{offset = O, size = S}) ->
                                    #file_block{offset = O+S, size = S}
                                end, Blocks))}},
                            put(file_locations,
                                [{LocId, {LocationDoc2, ok}} | Locations]),
                            put(file_location_sizes, [{LocId, ?MODULE:size(Blocks)} |
                                proplists:delete(LocId, get(file_location_sizes))]),
                            LocationDoc3 = LocationDoc#document{value =
                            Location#file_location{blocks = []}},
                            {ok, LocationDoc3};
                        Error ->
                            Error
                    end;
                {#document{value = Location} = LocationDoc, _} ->
                    LocationDoc2 = LocationDoc#document{value =
                    Location#file_location{blocks = []}},
                    {ok, LocationDoc2}
            end
    end.

%%-------------------------------------------------------------------
%% @doc
%% Initializes blocks cache in process.
%% @end
%%-------------------------------------------------------------------
-spec init_cache() -> ok.
init_cache() ->
    put(flush_time, {0,0,0}),
    put(file_locations, []),
    put(file_location_sizes, []),
    put(blocks_in_use, []),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Flushes cache.
%% @end
%%-------------------------------------------------------------------
-spec flush() -> ok.
flush() ->
    Locations = get(file_locations),
    flush(Locations).

%%-------------------------------------------------------------------
%% @doc
%% Checks if flush should be performed and flushes cache if needed.
%% @end
%%-------------------------------------------------------------------
-spec check_flush() -> ok.
check_flush() ->
    FlushTime = get(flush_time),
    Now = os:timestamp(),
    case {timer:now_diff(Now, FlushTime) > 3000000, get(flush_planned)} of
        {true, _} ->
            flush();
        {_, true} ->
            erlang:send_after(timer:seconds(3), self(), check_flush),
            ok;
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Merges given blocks to one blocks list. Blocks from he first list override
%% blocks with the same ranges from second list. This function returns
%% consolidated list.
%% @end
%%--------------------------------------------------------------------
-spec merge(blocks(), blocks()) -> blocks().
merge(Blocks1, Blocks2) ->
    NewBlocks = fslogic_blocks:invalidate(Blocks2, Blocks1) ++ Blocks1,
    fslogic_blocks:consolidate(lists:sort(NewBlocks)).

%%--------------------------------------------------------------------
%% @doc
%% Aggregates lists of 'file_block' records.
%% IMPORTANT! Both list should contain disjoint file blocks sorted in ascending
%% order of block offset.
%% @end
%%--------------------------------------------------------------------
-spec aggregate(Blocks1 :: blocks(), Blocks2 :: blocks()) -> AggBlocks :: blocks().
aggregate([], Blocks) ->
    Blocks;
aggregate(Blocks, []) ->
    Blocks;
aggregate(Blocks1, Blocks2) ->
    aggregate_blocks(Blocks1, Blocks2, []).

%%-------------------------------------------------------------------
%% @doc
%% Returns aggregated size of give list of blocks.
%% @end
%%-------------------------------------------------------------------
-spec size(blocks()) -> non_neg_integer().
size(Blocks) ->
    size(Blocks, 0).

%%--------------------------------------------------------------------
%% @doc
%% For given blocks, returns last byte number + 1 (counting from 0).
%% @end
%%--------------------------------------------------------------------
-spec upper(#file_block{} | [#file_block{}]) -> non_neg_integer().
upper(#file_block{offset = Offset, size = Size}) ->
    Offset + Size;
upper([_ | _] = Blocks) ->
    lists:max([upper(Block) || Block <- Blocks]);
upper([]) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% For given blocks, returns first block number.
%% @end
%%--------------------------------------------------------------------
-spec lower(#file_block{} | [#file_block{}]) -> non_neg_integer().
lower(#file_block{offset = Offset}) ->
    Offset;
lower([_ | _] = Blocks) ->
    lists:min([lower(Block) || Block <- Blocks]);
lower([]) ->
    0.

%%--------------------------------------------------------------------
%% @doc
%% Invalidates second list's blocks in first list.
%% @end
%%--------------------------------------------------------------------
-spec invalidate(blocks(), blocks() | block()) -> blocks().
invalidate(OldBlocks, []) ->
    OldBlocks;
invalidate(OldBlocks, [#file_block{} = B | T]) ->
    invalidate(invalidate(OldBlocks, B), T);
invalidate([], #file_block{}) ->
    [];
invalidate([#file_block{offset = CO, size = CS} = C | T], #file_block{offset = DO, size = _DS} = D) when CO + CS =< DO ->
    [C | invalidate(T, D)];
invalidate([#file_block{offset = CO, size = _CS} = C | T], #file_block{offset = DO, size = DS} = D) when DO + DS =< CO ->
    [C | invalidate(T, D)];
invalidate([#file_block{offset = CO, size = CS} = _C | T], #file_block{offset = DO, size = DS} = D) when CO >= DO, CO + CS =< DO + DS ->
    invalidate(T, D);
invalidate([#file_block{offset = CO, size = CS} = C | T], #file_block{offset = DO, size = DS} = D) when CO >= DO, CO + CS > DO + DS ->
    [C#file_block{offset = DO + DS, size = CS - (DO + DS - CO)} | invalidate(T, D)];
invalidate([#file_block{offset = CO, size = CS} = C | T], #file_block{offset = DO, size = DS} = D) when CO < DO, CO + CS =< DO + DS ->
    [C#file_block{size = DO - CO} | invalidate(T, D)];
invalidate([#file_block{offset = CO, size = CS} = C | T], #file_block{offset = DO, size = DS} = D) when CO =< DO, CO + CS >= DO + DS ->
    [C#file_block{size = DO - CO}, C#file_block{offset = DO + DS, size = CO + CS - (DO + DS)} | invalidate(T, D)].

%%--------------------------------------------------------------------
%% @doc
%% Removes empty and invalid blocks and merges them whenever it is possible.
%% @end
%%--------------------------------------------------------------------
-spec consolidate(blocks()) -> blocks().
consolidate([]) ->
    [];
consolidate([#file_block{size = Size} | Rest]) when Size =< 0  ->
    consolidate(Rest);
consolidate([
    #file_block{offset = LO, size = LS} = FirstBlock,
    #file_block{offset = RO, size = RS} | Rest]
) when LO + LS >= RO + RS ->
    consolidate([FirstBlock | Rest]);
consolidate([
    #file_block{offset = LO, size = LS} = FirstBlock,
    #file_block{offset = RO, size = RS} | Rest]
) when LO + LS >= RO ->
    consolidate([FirstBlock#file_block{size = RO + RS - LO} | Rest]);
consolidate([B | Rest]) ->
    [B | consolidate(Rest)].


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Initializes flush procedure.
%% @end
%%-------------------------------------------------------------------
-spec save_doc(file_location:doc()) -> {ok, file_location:id()} | {error, term()}.
save_doc(#document{value = #file_location{blocks = Blocks}} = LocationDoc) when is_list(Blocks) ->
    file_location:save(LocationDoc);
save_doc(#document{value = Location = #file_location{blocks = Blocks}} = LocationDoc) ->
    LocationDoc2 = LocationDoc#document{value = Location#file_location{blocks =
    lists:map(
        fun(#file_block{offset = O, size = S}) ->
            #file_block{offset = O-S, size = S}
        end, gb_sets:to_list(Blocks))}},
    file_location:save(LocationDoc2).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Flushes list of documents.
%% @end
%%-------------------------------------------------------------------
-spec flush([datastore:document()]) -> ok.
flush(Locations) ->
    Locations2 = lists:map(fun
        ({ID, {LocationDoc, updated}}) ->
            save_doc(LocationDoc),
            {ID, {LocationDoc, ok}};
        (Element) ->
            Element
    end, Locations),
    put(file_locations, Locations2),
    put(flush_time, os:timestamp()),
    put(flush_planned, undefined),
    ok.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Initializes flush procedure.
%% @end
%%-------------------------------------------------------------------
-spec init_flush_check() -> ok.
init_flush_check() ->
    case {get(flush_time), get(flush_planned)} of
        {undefined, _} ->
            ok;
        {_, undefined} ->
            put(flush_planned, true),
            erlang:send_after(timer:seconds(3), self(), check_flush),
            ok;
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Aggregates lists of 'file_block' records using acumulator AggBlocks.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_blocks(Blocks1 :: blocks(), Blocks2 :: blocks(), AggBlocks :: blocks()) ->
    NewAggBlocks :: blocks().
aggregate_blocks([], [], AggBlocks) ->
    lists:reverse(AggBlocks);
aggregate_blocks([], [Block | Blocks], AggBlocks) ->
    aggregate_blocks([], Blocks, aggregate_block(Block, AggBlocks));
aggregate_blocks([Block | Blocks], [], AggBlocks) ->
    aggregate_blocks(Blocks, [], aggregate_block(Block, AggBlocks));
aggregate_blocks([#file_block{offset = Offset1} = Block1 | Blocks1],
    [#file_block{offset = Offset2} | _] = Blocks2, AggBlocks)
    when Offset1 < Offset2 ->
    aggregate_blocks(Blocks1, Blocks2, aggregate_block(Block1, AggBlocks));
aggregate_blocks(Blocks1, [#file_block{} = Block2 | Blocks2], AggBlocks) ->
    aggregate_blocks(Blocks1, Blocks2, aggregate_block(Block2, AggBlocks)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Aggregates 'file_block' record  with list of 'file_block' records.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_block(Block :: block(), Blocks :: blocks()) -> AggBlocks :: blocks().
aggregate_block(Block, []) ->
    [Block];
aggregate_block(#file_block{offset = Offset1, size = Size1} = Block1,
    [#file_block{offset = Offset2, size = Size2} | Blocks])
    when Offset1 =< Offset2 + Size2 ->
    [Block1#file_block{
        offset = Offset2,
        size = max(Offset1 + Size1, Offset2 + Size2) - Offset2
    } | Blocks];
aggregate_block(Block, Blocks) ->
    [Block | Blocks].

%%-------------------------------------------------------------------
%% @doc
%% Tail recursive helper function for size/1 function.
%% @end
%%-------------------------------------------------------------------
-spec size(blocks(), non_neg_integer()) -> non_neg_integer().
size([], AggregatedSize) -> AggregatedSize;
size([#file_block{size = Size} | RestBlocks], AggregatedSize) ->
    size(RestBlocks, Size + AggregatedSize).