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
-export([merge/2, aggregate/2, consolidate/1, invalidate/2, get_file_size/1, upper/1,
    lower/1]).

%%%===================================================================
%%% API
%%%===================================================================

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
-spec get_file_size(file_ctx:ctx() | datastore:document() | [datastore:document()]
| #file_location{} | [#file_location{}] | fslogic_worker:file()) ->
    Size :: non_neg_integer() | no_return().
get_file_size(#document{value = #file_location{} = Value}) ->
    get_file_size(Value);
get_file_size(#document{value = #file_meta{type = ?DIRECTORY_TYPE}}) ->
    0;
get_file_size(#document{value = #file_meta{type = ?SYMLINK_TYPE}}) ->
    0;
get_file_size(#file_location{size = undefined, blocks = Blocks}) ->
    upper(Blocks);
get_file_size(#file_location{size = Size}) ->
    Size;
get_file_size([Location]) ->
    get_file_size(Location);
get_file_size([Location | T]) ->
    max(get_file_size(Location), get_file_size(T));
get_file_size([]) ->
    throw(locations_not_found);
get_file_size(Entry) ->
    case file_ctx:is_file_ctx_const(Entry) of
        true ->
            case file_ctx:is_dir(Entry) of
                {true, _FileCtx2} -> %todo return FileCtx from this funciton
                    0;
                {false, FileCtx2} ->
                    {LocalLocations, _FileCtx3} = file_ctx:get_local_file_location_docs(FileCtx2),
                    get_file_size(LocalLocations)
            end;
        false ->
            LocalLocations = file_meta:get_local_locations(Entry),
            get_file_size(LocalLocations)
    end.

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