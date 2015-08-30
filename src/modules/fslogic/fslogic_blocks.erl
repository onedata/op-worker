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
-export([get_file_size/1, update/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_file_size(File :: fslogic_worker:file()) -> Size :: non_neg_integer() | no_return().
get_file_size(Entry) ->
    {ok, LocIds} = file_meta:get_locations(Entry),
    Locations = [file_location:get(LocId) || LocId <- LocIds],
    LastBlocks = [lists:last(Blocks) || {ok, #document{value = #file_location{blocks = [_ | _] = Blocks}}} <- Locations],
    lists:foldl(
        fun(Elem, Acc) ->
            max(Elem, Acc)
        end, 0, [Offset + Size || #file_block{offset = Offset, size = Size} <- LastBlocks]).


update(FileUUID, Blocks) ->
    file_location:run_synchronized(FileUUID, fun() ->
        LProviderId = oneprovider:get_provider_id(),
        {ok, LocIds} = file_meta:get_locations({uuid, FileUUID}),
        Locations = [file_location:get(LocId) || LocId <- LocIds],
        Locations1 = [Loc || {ok, Loc} <- Locations],
        [LocalLocation] = [Location || #document{value = #file_location{provider_id = ProviderId}} = Location <- Locations1, LProviderId =:= ProviderId],
        RemoteLocations = Locations1 -- [LocalLocation],

        ok = invalidate(RemoteLocations, Blocks),
        ok = append([LocalLocation], Blocks),

        ok
    end).

invalidate([Location | T], Blocks) ->
    [invalidate(Location, Blocks) | invalidate(T, Blocks)],
    ok;
invalidate(#document{value = #file_location{blocks = OldBlocks} = Loc} = Doc, Blocks) ->
    NewBlocks = invalidate(Doc, OldBlocks, Blocks),
    NewBlocks1 = consolidate(lists:sort(NewBlocks)),
    {ok, _} = file_location:save(Doc#document{value = Loc#file_location{blocks = NewBlocks1}});
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
append(#document{value = #file_location{blocks = OldBlocks} = Loc} = Doc, Blocks) ->
    ?info("OldBlocks ~p, NewBlocks ~p", [OldBlocks, Blocks]),
    NewBlocks = invalidate(Doc, OldBlocks, Blocks) ++ Blocks,
    ?info("NewBlocks ~p", [NewBlocks]),
    NewBlocks1 = consolidate(lists:sort(NewBlocks)),
    ?info("NewBlocks1 ~p", [NewBlocks1]),
    {ok, _} = file_location:save(Doc#document{value = Loc#file_location{blocks = NewBlocks1}}).


consolidate([]) ->
    [];
consolidate([B]) ->
    [B];
consolidate([#file_block{size = 0} | T]) ->
    consolidate(T);
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