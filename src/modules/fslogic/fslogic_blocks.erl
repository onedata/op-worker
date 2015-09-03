%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_blocks).
-author("Rafal Slota").

-include("modules/datastore/datastore.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").

-type snapshot_id() :: integer().

-export_type([snapshot_id/0]).

-record(byte_range, {
    offset = 0,
    size = 0
}).

%% API
-export([calculate_file_size/1, update/3, get_file_size/1]).
-export([upper/1, lower/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec upper(#file_block{} | [#file_block{}]) -> non_neg_integer().
upper(#file_block{offset = Offset, size = Size}) ->
    Offset + Size;
upper([Block | T]) ->
    max(upper(Block), upper(T));
upper([]) ->
    0.


-spec lower(#file_block{} | [#file_block{}]) -> non_neg_integer().
lower(#file_block{offset = Offset}) ->
    Offset;
lower([Block | T]) ->
    min(upper(Block), upper(T));
lower([]) ->
    0.


-spec calculate_file_size(File :: fslogic_worker:file()) -> Size :: non_neg_integer() | no_return().
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


update(FileUUID, Blocks, FileSize) ->
    file_location:run_synchronized(FileUUID, fun() ->
        try
            LProviderId = oneprovider:get_provider_id(),
            {ok, LocIds} = file_meta:get_locations({uuid, FileUUID}),
            Locations = [file_location:get(LocId) || LocId <- LocIds],
            Locations1 = [Loc || {ok, Loc} <- Locations],
            [LocalLocation] = [Location || #document{value = #file_location{provider_id = ProviderId}} = Location <- Locations1, LProviderId =:= ProviderId],
            RemoteLocations = Locations1 -- [LocalLocation],

            BlocksSorted = lists:usort(Blocks),

            ok = invalidate(RemoteLocations, BlocksSorted),
            ok = append([LocalLocation], BlocksSorted),

            case FileSize of
                undefined -> ok;
                _ ->
                    do_truncate(FileSize, LocalLocation, RemoteLocations)
            end,

            ok
        catch
            _:Reason ->
                ?error_stacktrace("Failed to update blocks for file ~p (blocks ~p, file_size ~p) due to: ~p", [FileUUID, Blocks, FileSize, Reason]),
                {error, Reason}
        end
    end).

invalidate_and_truncate(#document{value = #file_location{blocks = OldBlocks} = Loc} = Doc, Blocks) ->
    NewBlocks = invalidate(Doc, OldBlocks, Blocks),
    NewBlocks1 = consolidate(lists:sort(NewBlocks)),
    {ok, _} = file_location:save(Doc#document{value = Loc#file_location{blocks = NewBlocks1, size = upper(NewBlocks1)}}),
    ok.

invalidate([Location | T], Blocks) ->
    [invalidate(Location, Blocks) | invalidate(T, Blocks)],
    ok;
invalidate(#document{value = #file_location{blocks = OldBlocks} = Loc} = Doc, Blocks) ->
    NewBlocks = invalidate(Doc, OldBlocks, Blocks),
    NewBlocks1 = consolidate(lists:sort(NewBlocks)),
    {ok, _} = file_location:save(Doc#document{value = Loc#file_location{blocks = NewBlocks1}}),
    ok;
invalidate([], _) ->
    ok.

invalidate(_Doc, OldBlocks, []) ->
    OldBlocks;
invalidate(Doc, OldBlocks, [#file_block{} = B | T]) ->
    invalidate(Doc, invalidate(Doc, OldBlocks, B), T);
invalidate(_Doc, [], #file_block{}) ->
    [];
invalidate(Doc, [#file_block{offset = CO, size = CS} = C | T], #file_block{offset = DO, size = DS} = D) when CO + CS =< DO ->
    [C | invalidate(Doc, T, D)];
invalidate(Doc, [#file_block{offset = CO, size = CS} = C | T], #file_block{offset = DO, size = DS} = D) when DO + DS =< CO ->
    [C | invalidate(Doc, T, D)];
invalidate(Doc, [#file_block{offset = CO, size = CS} = C | T], #file_block{offset = DO, size = DS} = D) when CO >= DO, CO + CS =< DO + DO ->
    invalidate(Doc, T, D);
invalidate(Doc, [#file_block{offset = CO, size = CS} = C | T], #file_block{offset = DO, size = DS} = D) when CO >= DO, CO + CS > DO + DO ->
    [C#file_block{offset = DO + DS, size = CS - (DO + DS - CO)} | invalidate(Doc, T, D)];
invalidate(Doc, [#file_block{offset = CO, size = CS} = C | T], #file_block{offset = DO, size = DS} = D) when CO < DO, CO + CS =< DO + DO ->
    [C#file_block{size = DO - CO} | invalidate(Doc, T, D)];
invalidate(Doc, [#file_block{offset = CO, size = CS} = C | T], #file_block{offset = DO, size = DS} = D) when CO =< DO, CO + CS >= DO + DO ->
    [C#file_block{size = DO - CO}, C#file_block{offset = DO + DS, size = CO + CS - (DO + DS)} | invalidate(Doc, T, D)].



append([], _Blocks) ->
    ok;
append([Location | T], Blocks) ->
    [append(Location, Blocks) | append(T, Blocks)],
    ok;
append(#document{value = #file_location{blocks = OldBlocks, size = OldSize} = Loc} = Doc, Blocks) ->
    ?info("OldBlocks ~p, NewBlocks ~p", [OldBlocks, Blocks]),
    NewBlocks = invalidate(Doc, OldBlocks, Blocks) ++ Blocks,
    NewSize = upper(Blocks),
    ?info("NewBlocks ~p", [NewBlocks]),
    NewBlocks1 = consolidate(lists:sort(NewBlocks)),
    ?info("NewBlocks1 ~p", [NewBlocks1]),
    {ok, _} = file_location:save(Doc#document{value = Loc#file_location{blocks = NewBlocks1, size = max(OldSize, NewSize)}}),
    ok.


consolidate([]) ->
    [];
consolidate([#file_block{size = 0} | T]) ->
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


do_truncate(FileSize, #document{value = #file_location{}} = LocalLocation, RemoteLocations) ->
    {do_local_truncate(FileSize, LocalLocation), do_remote_truncate(FileSize, RemoteLocations)}.

do_local_truncate(FileSize, #document{value = #file_location{size = FileSize}}) ->
    ok;
do_local_truncate(FileSize, #document{value =
                    #file_location{size = LocalSize, file_id = FileId, storage_id = StorageId}} = LocalLocation) when LocalSize < FileSize ->
    append(LocalLocation, [#file_block{offset = LocalSize, size = FileSize - LocalSize, file_id = FileId, storage_id = StorageId}]);
do_local_truncate(FileSize, #document{value = #file_location{size = LocalSize}} = LocalLocation) when LocalSize > FileSize ->
    invalidate_and_truncate(LocalLocation, [#file_block{offset = FileSize, size = LocalSize - FileSize}]).


do_remote_truncate(_FileSize, []) ->
    [];
do_remote_truncate(FileSize, [Location | T]) ->
    [do_remote_truncate(FileSize, Location) | do_remote_truncate(FileSize, T)];
do_remote_truncate(FileSize, #document{value = #file_location{provider_id = ProviderId, size = FileSize}}) ->
    {ProviderId, ok};
do_remote_truncate(FileSize, #document{value = #file_location{provider_id = ProviderId, size = RemoteSize}}) when RemoteSize < FileSize ->
    {ProviderId, ok};
do_remote_truncate(FileSize, #document{key = LocId, value = #file_location{provider_id = ProviderId, size = RemoteSize}} = Loc) when RemoteSize > FileSize ->
    {ProviderId, invalidate_and_truncate(Loc, [#file_block{offset = FileSize, size = RemoteSize - FileSize}])}.