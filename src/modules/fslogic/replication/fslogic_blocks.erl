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

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").


-type block() :: #file_block{}.
-type blocks() :: [block()].

-export_type([block/0, blocks/0]).

%% API
-export([aggregate/2, update/3, get_file_size/1]).

%% Test API
-export([upper/1, lower/1]).

%%%===================================================================
%%% API
%%%===================================================================

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
%% For given file / location or multiple locations, reads file size assigned to those locations.
%% @end
%%--------------------------------------------------------------------
-spec get_file_size(datastore:document() | #file_location{} | [#file_location{}] | fslogic_worker:file()) ->
    Size :: non_neg_integer() | no_return().
get_file_size(#document{value = #file_location{} = Value}) ->
    get_file_size(Value);
get_file_size(#file_location{size = undefined} = Location) ->
    calculate_file_size(Location);
get_file_size(#file_location{size = Size}) ->
    Size;
get_file_size([Location | T]) ->
    max(get_file_size(Location), get_file_size(T));
get_file_size([]) ->
    0;
get_file_size(Entry) ->
    {ok, LocIds} = file_meta:get_locations(Entry),
    Locations = [file_location:get(LocId) || LocId <- LocIds],
    Locations1 = [Location || {ok, #document{value = #file_location{} = Location}} <- Locations],
    get_file_size(Locations1).


%%--------------------------------------------------------------------
%% @doc
%% Appends given blocks to the file's local location and invalidates those blocks in its remote locations.
%% This function in synchronized on the file.
%% FileSize argument may be used to truncate file's blocks if needed.
%% Return value tells whether file size has been changed by this call.
%% @end
%%--------------------------------------------------------------------
-spec update(FileUUID :: file_meta:uuid(), Blocks :: blocks(), FileSize :: non_neg_integer() | undefined) ->
    {ok, size_changed} | {ok, size_not_changed} | {error, Reason :: term()}.
update(FileUUID, Blocks, FileSize) ->
    file_location:run_synchronized(FileUUID, fun() ->
        try
            [Location | _] = fslogic_utils:get_local_file_locations({uuid, FileUUID}), %todo get location as argument, insted operating on first one
            FullBlocks = fill_blocks_with_storage_info(Blocks, Location),

            ok = append([Location], FullBlocks),

            case FileSize of
                undefined ->
                    case upper(FullBlocks) > calculate_file_size(Location) of
                        true -> {ok, size_changed};
                        false -> {ok, size_not_changed}
                    end;
                _ ->
                    do_local_truncate(FileSize, Location),
                    {ok, size_changed}
            end
            % todo reconcile other local replicas according to this one
        catch
            _:Reason ->
                ?error_stacktrace("Failed to update blocks for file ~p (blocks ~p, file_size ~p) due to: ~p", [FileUUID, Blocks, FileSize, Reason]),
                {error, Reason}
        end
                                             end).


%%--------------------------------------------------------------------
%% @doc
%% Inavlidates given blocks in given locations. File size is also updated.
%% @end
%%--------------------------------------------------------------------
-spec invalidate(datastore:document() | [datastore:document()], Blocks :: blocks()) ->
    ok | no_return().
invalidate([Location | T], Blocks) ->
    ok = invalidate(Location, Blocks),
    ok = invalidate(T, Blocks);
invalidate(_, []) ->
    ok;
invalidate(#document{value = #file_location{blocks = OldBlocks} = Loc} = Doc, Blocks) ->
    ?debug("OldBlocks invalidate ~p, new ~p", [OldBlocks, Blocks]),
    NewBlocks = invalidate(Doc, OldBlocks, Blocks),
    ?debug("NewBlocks invalidate ~p", [NewBlocks]),
    NewBlocks1 = consolidate(NewBlocks),
    ?debug("NewBlocks1 invalidate ~p", [NewBlocks1]),
    {ok, _} = file_location:save(Doc#document{rev = undefined, value = Loc#file_location{blocks = NewBlocks1}}), %do not change size
    ok;
invalidate([], _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Inavlidates given blocks in given locations. File size is also updated.
%% @end
%%--------------------------------------------------------------------
-spec shrink(datastore:document() | [datastore:document()], Blocks :: blocks(), NewSize :: non_neg_integer()) ->
    ok | no_return().
shrink([Location | T], Blocks, NewSize) ->
    ok = shrink(Location, Blocks, NewSize),
    ok = shrink(T, Blocks, NewSize);
shrink(#document{value = #file_location{size = NewSize}}, [], NewSize) ->
    ok;
shrink(#document{value = #file_location{blocks = OldBlocks, size = OldSize} = Loc} = Doc, Blocks, NewSize) ->
    ?debug("OldBlocks shrink ~p, new ~p", [OldBlocks, Blocks]),
    NewBlocks = invalidate(Doc, OldBlocks, Blocks),
    ?debug("NewBlocks shrink ~p", [NewBlocks]),
    NewBlocks1 = consolidate(NewBlocks),
    ?debug("NewBlocks1 shrink ~p", [NewBlocks1]),
    {ok, _} = file_location:save_and_bump_version(
        Doc#document{rev = undefined, value =
        Loc#file_location{blocks = NewBlocks1, size = NewSize}}
    ),
    ok;
shrink([], _, _) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Internal impl. of invalidate/2
%% @end
%%--------------------------------------------------------------------
-spec invalidate(datastore:document(), blocks(), blocks() | block()) -> blocks().
invalidate(_Doc, OldBlocks, []) ->
    OldBlocks;
invalidate(Doc, OldBlocks, [#file_block{} = B | T]) ->
    invalidate(Doc, invalidate(Doc, OldBlocks, B), T);
invalidate(_Doc, [], #file_block{}) ->
    [];
invalidate(Doc, [#file_block{offset = CO, size = CS} = C | T], #file_block{offset = DO, size = _DS} = D) when CO + CS =< DO ->
    [C | invalidate(Doc, T, D)];
invalidate(Doc, [#file_block{offset = CO, size = _CS} = C | T], #file_block{offset = DO, size = DS} = D) when DO + DS =< CO ->
    [C | invalidate(Doc, T, D)];
invalidate(Doc, [#file_block{offset = CO, size = CS} = _C | T], #file_block{offset = DO, size = _DS} = D) when CO >= DO, CO + CS =< DO + DO ->
    invalidate(Doc, T, D);
invalidate(Doc, [#file_block{offset = CO, size = CS} = C | T], #file_block{offset = DO, size = DS} = D) when CO >= DO, CO + CS > DO + DO ->
    [C#file_block{offset = DO + DS, size = CS - (DO + DS - CO)} | invalidate(Doc, T, D)];
invalidate(Doc, [#file_block{offset = CO, size = CS} = C | T], #file_block{offset = DO, size = _DS} = D) when CO < DO, CO + CS =< DO + DO ->
    [C#file_block{size = DO - CO} | invalidate(Doc, T, D)];
invalidate(Doc, [#file_block{offset = CO, size = CS} = C | T], #file_block{offset = DO, size = DS} = D) when CO =< DO, CO + CS >= DO + DO ->
    [C#file_block{size = DO - CO}, C#file_block{offset = DO + DS, size = CO + CS - (DO + DS)} | invalidate(Doc, T, D)].


%%--------------------------------------------------------------------
%% @doc
%% Removes empty and invalid blocks and merges them whenever it is possible.
%% @end
%%--------------------------------------------------------------------
-spec consolidate(blocks()) -> blocks().
consolidate([]) ->
    [];
consolidate([#file_block{size = 0} | T]) ->
    consolidate(T);
consolidate([#file_block{size = Size} = B | T]) when Size < 0 ->
    ?warning("Skipping invalid block: ~p", [B]),
    consolidate(T);
consolidate([B]) ->
    [B];
consolidate([#file_block{offset = LO, size = LS, file_id = FID, storage_id = SID} = B,
    #file_block{offset = RO, size = RS, file_id = FID, storage_id = SID} | T]) when LO + LS >= RO + RS ->
    consolidate([B | T]);
consolidate([#file_block{offset = LO, size = LS, file_id = FID, storage_id = SID} = B,
    #file_block{offset = RO, size = RS, file_id = FID, storage_id = SID} | T]) when LO + LS >= RO ->
    consolidate([B#file_block{size = RO + RS - LO} | T]);
consolidate([B | T]) ->
    [B | consolidate(T)].


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% For given file / location or multiple locations, calculates file size based on blocks assigned to those locations.
%% @end
%%--------------------------------------------------------------------
-spec calculate_file_size(datastore:document() | #file_location{} | [#file_location{}] | fslogic_worker:file()) ->
    Size :: non_neg_integer() | no_return().
calculate_file_size(#document{value = #file_location{} = Value}) ->
    calculate_file_size(Value);
calculate_file_size(#file_location{blocks = []}) ->
    0;
calculate_file_size(#file_location{blocks = Blocks}) ->
    upper(Blocks);
calculate_file_size([Location | T]) ->
    max(calculate_file_size(Location), calculate_file_size(T));
calculate_file_size([]) ->
    0;
calculate_file_size(Entry) ->
    {ok, LocIds} = file_meta:get_locations(Entry),
    Locations = [file_location:get(LocId) || LocId <- LocIds],
    Locations1 = [Location || {ok, #document{value = #file_location{} = Location}} <- Locations],
    calculate_file_size(Locations1).

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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Appends given blocks to given locations and updates file size for those locations.
%% @end
%%--------------------------------------------------------------------
-spec append(datastore:document() | [datastore:document()], blocks()) -> ok | no_return().
append([], _Blocks) ->
    ok;
append([Location | T], Blocks) ->
    ok = append(Location, Blocks),
    ok = append(T, Blocks);
append(_, []) ->
    ok;
append(#document{value = #file_location{blocks = OldBlocks, size = OldSize} = Loc} = Doc, Blocks) ->
    ?debug("OldBlocks ~p, NewBlocks ~p", [OldBlocks, Blocks]),
    NewBlocks = invalidate(Doc, OldBlocks, Blocks) ++ Blocks,
    NewSize = upper(Blocks),
    ?debug("NewBlocks ~p", [NewBlocks]),
    NewBlocks1 = consolidate(lists:sort(NewBlocks)),
    ?debug("NewBlocks1 ~p", [NewBlocks1]),
    {ok, _} = file_location:save_and_bump_version(
        Doc#document{rev = undefined, value =
        Loc#file_location{blocks = NewBlocks1, size = max(OldSize, NewSize)}}
    ),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Truncates blocks from given location. Works for both shrinking and growing file.
%% @end
%%--------------------------------------------------------------------
-spec do_local_truncate(FileSize :: non_neg_integer(), datastore:document()) -> ok | no_return().
do_local_truncate(FileSize, #document{value = #file_location{size = FileSize}}) ->
    ok;
do_local_truncate(FileSize, #document{value = #file_location{size = LocalSize, file_id = FileId, storage_id = StorageId}} = LocalLocation) when LocalSize < FileSize ->
    append(LocalLocation, [#file_block{offset = LocalSize, size = FileSize - LocalSize, file_id = FileId, storage_id = StorageId}]);
do_local_truncate(FileSize, #document{value = #file_location{size = LocalSize}} = LocalLocation) when LocalSize > FileSize ->
    shrink(LocalLocation, [#file_block{offset = FileSize, size = LocalSize - FileSize}], FileSize).


%%--------------------------------------------------------------------
%% @doc
%% Make sure that storage_id and file_id are set (assume defaults form location
%% if not)
%% @end
%%--------------------------------------------------------------------
-spec fill_blocks_with_storage_info([#file_block{}], file_location:doc()) ->
    [#file_block{}].
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

    consolidate(lists:usort(FilledBlocks)).