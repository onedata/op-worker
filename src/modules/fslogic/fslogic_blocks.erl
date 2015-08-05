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

-type snapshot_id() :: integer().

-export_type([snapshot_id/0]).

-record(byte_range, {
    offset = 0,
    size = 0
}).

%% API
-export([get_file_size/1]).

%%%===================================================================
%%% API
%%%===================================================================

get_file_size(Entry) ->
    {ok, LocIds} = file_meta:get_locations(Entry),
    Locations = [file_location:get(LocId) || LocId <- LocIds],
    LastBlocks = [lists:last(Blocks) || {ok, #document{value = #file_location{blocks = [_ | _] = Blocks}}} <- Locations],
    lists:foldl(
        fun(Elem, Acc) ->
            max(Elem, Acc)
        end, 0, [Offset + Size || #file_block{offset = Offset, size = Size} <- LastBlocks]).


update(FileUUID, Blocks) ->
    ProviderId = cluster_manager:provider_id(),
    {ok, LocIds} = file_meta:get_locations({uuid, FileUUID}),
    Locations = [file_location:get(LocId) || LocId <- LocIds],
    [LocalLocation] = [Location || #document{value = #file_location{provider_id = ProviderId}} = Location <- Locations],
    RemoteLocations = Locations -- [LocalLocation],

    ok = invalidate(Locations, Blocks),
    ok = append([LocalLocation], Blocks),

    ok.

invalidate(Locations, Blocks) ->
    ok.


append(Locations, Blocks) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================