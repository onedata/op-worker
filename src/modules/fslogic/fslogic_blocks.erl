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

-include("cluster/worker/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").


-type block() :: #file_block{}.
-type blocks() :: [block()].

-export_type([block/0, blocks/0]).

%% API
-export([calculate_file_size/1, update/3, get_file_size/1]).
-export([upper/1, lower/1]).
-export([consolidate/1, invalidate/3]).

%%%===================================================================
%%% API
%%%===================================================================

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
            LProviderId = oneprovider:get_provider_id(),
            {ok, LocIds} = file_meta:get_locations({uuid, FileUUID}),
            Locations = [file_location:get(LocId) || LocId <- LocIds],
            Locations1 = [Loc || {ok, Loc} <- Locations],
            [LocalLocation] = [Location || #document{value = #file_location{provider_id = ProviderId}} = Location <- Locations1, LProviderId =:= ProviderId],
            RemoteLocations = Locations1 -- [LocalLocation],

            BlocksSorted = consolidate(lists:usort(Blocks)),

            ok = invalidate(RemoteLocations, BlocksSorted),
            ok = append([LocalLocation], BlocksSorted),

            case FileSize of
                undefined ->
                    case upper(BlocksSorted) > calculate_file_size(Locations1) of
                        true -> {ok, size_changed};
                        false -> {ok, size_not_changed}
                    end;
                _ ->
                    do_truncate(FileSize, LocalLocation, RemoteLocations),
                    {ok, size_changed}
            end
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
invalidate(#document{value = #file_location{blocks = OldBlocks} = Loc} = Doc, Blocks) ->
    ?debug("OldBlocks invalidate ~p, new ~p", [OldBlocks, Blocks]),
    NewBlocks = invalidate(Doc, OldBlocks, Blocks),
    ?debug("NewBlocks invalidate ~p", [NewBlocks]),
    NewBlocks1 = consolidate(NewBlocks),
    ?debug("NewBlocks1 invalidate ~p", [NewBlocks1]),
    {ok, _} = file_location:save(Doc#document{value = Loc#file_location{blocks = NewBlocks1, size = upper(NewBlocks1)}}),
    ok;
invalidate([], _) ->
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
%% Appends given blocks to given locations and updates file size for those locations.
%% @end
%%--------------------------------------------------------------------
-spec append(datastore:document() | [datastore:document()], blocks()) -> ok | no_return().
append([], _Blocks) ->
    ok;
append([Location | T], Blocks) ->
    ok = append(Location, Blocks),
    ok = append(T, Blocks);
append(#document{value = #file_location{blocks = OldBlocks, size = OldSize} = Loc} = Doc, Blocks) ->
    ?debug("OldBlocks ~p, NewBlocks ~p", [OldBlocks, Blocks]),
    NewBlocks = invalidate(Doc, OldBlocks, Blocks) ++ Blocks,
    NewSize = upper(Blocks),
    ?debug("NewBlocks ~p", [NewBlocks]),
    NewBlocks1 = consolidate(lists:sort(NewBlocks)),
    ?debug("NewBlocks1 ~p", [NewBlocks1]),
    {ok, _} = file_location:save(Doc#document{value = Loc#file_location{blocks = NewBlocks1, size = max(OldSize, NewSize)}}),
    ok.



%%--------------------------------------------------------------------
%% @doc
%% Performs truncate operation on given locations. Note that on remote locations only shrinking will be done.
%% @end
%%--------------------------------------------------------------------
-spec do_truncate(FileSize :: non_neg_integer(), LocalLocation :: datastore:document(), RemoteLocations :: [datastore:document()]) ->
    {LocalResult :: ok, RemoteResults :: [{ProviderId :: oneprovider:id(), ok}]} | no_return().
do_truncate(FileSize, #document{value = #file_location{}} = LocalLocation, RemoteLocations) ->
    {do_local_truncate(FileSize, LocalLocation), do_remote_truncate(FileSize, RemoteLocations)}.

%%--------------------------------------------------------------------
%% @doc
%% Truncates blocks from given location. Works for both shrinking and growing file.
%% @end
%%--------------------------------------------------------------------
-spec do_local_truncate(FileSize :: non_neg_integer(), datastore:document()) -> ok | no_return().
do_local_truncate(FileSize, #document{value = #file_location{size = FileSize}}) ->
    ok;
do_local_truncate(FileSize, #document{value =
                    #file_location{size = LocalSize, file_id = FileId, storage_id = StorageId}} = LocalLocation) when LocalSize < FileSize ->
    append(LocalLocation, [#file_block{offset = LocalSize, size = FileSize - LocalSize, file_id = FileId, storage_id = StorageId}]);
do_local_truncate(FileSize, #document{value = #file_location{size = LocalSize}} = LocalLocation) when LocalSize > FileSize ->
    invalidate(LocalLocation, [#file_block{offset = FileSize, size = LocalSize - FileSize}]).


%%--------------------------------------------------------------------
%% @doc
%% Truncates blocks from given location. Works only for shrinking file. Growing is ignored.
%% @end
%%--------------------------------------------------------------------
-spec do_remote_truncate(FileSize :: non_neg_integer(), [datastore:document()] | datastore:document()) ->
    [{ProviderId :: oneprovider:id(), ok}] | {ProviderId :: oneprovider:id(), ok} | no_return().
do_remote_truncate(_FileSize, []) ->
    [];
do_remote_truncate(FileSize, [Location | T]) ->
    [do_remote_truncate(FileSize, Location) | do_remote_truncate(FileSize, T)];
do_remote_truncate(FileSize, #document{value = #file_location{provider_id = ProviderId, size = FileSize}}) ->
    {ProviderId, ok};
do_remote_truncate(FileSize, #document{value = #file_location{provider_id = ProviderId, size = RemoteSize}}) when RemoteSize < FileSize ->
    {ProviderId, ok};
do_remote_truncate(FileSize, #document{key = _LocId, value = #file_location{provider_id = ProviderId, size = RemoteSize}} = Loc) when RemoteSize > FileSize ->
    {ProviderId, invalidate(Loc, [#file_block{offset = FileSize, size = RemoteSize - FileSize}])}.