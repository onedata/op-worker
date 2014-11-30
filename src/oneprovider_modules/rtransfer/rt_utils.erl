%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc Common utilities for rtransfer and gateway modules.
%% ===================================================================

-module(rt_utils).

-include("oneprovider_modules/rtransfer/rt_container.hrl").

-export([partition/2, pop/1, push/2]).


%% ====================================================================
%% API functions
%% ====================================================================


%% partition/2
%% ====================================================================
%% @doc Partitions a BaseBlock into sub-blocks based on existing blocks from
%% range.<br>
%% Preconditions:<br>
%% * ExistingBlocks is sorted by block offset,<br>
%% * blocks from ExistingBlocks are fully contained in BaseBlock range,<br>
%% * blocks from ExistingBlocks don't overlap.<br>
%% These conditions are fulfilled by block lists returned by rt_map:get.
%% @end
-spec partition(ExistingBlocks :: [#rt_block{}], BaseBlock :: #rt_block{}) ->
    [#rt_block{}].
partition([#rt_block{offset = Offset} = Block | ExistingBlocks], #rt_block{offset = Offset} = BaseBlock) ->
    #rt_block{size = BSize, priority = BPriority, terms = BTerms} = Block,
    #rt_block{size = Size, provider_ref = ProviderRef, priority = Priority, terms = Terms} = BaseBlock,
    UpdatedBlock = Block#rt_block{provider_ref = ProviderRef, priority = erlang:max(BPriority, Priority), terms = ordsets:union(ordsets:from_list(BTerms), ordsets:from_list(Terms))},
    ShrunkBaseBlock = BaseBlock#rt_block{offset = Offset + BSize, size = Size - BSize},
    [UpdatedBlock | partition(ExistingBlocks, ShrunkBaseBlock)];

partition([#rt_block{offset = NextOffset} | _] = ExistingBlocks, #rt_block{offset = Offset, size = Size} = BaseBlock) ->
    BlockSize = NextOffset - Offset,
    NewBlock = BaseBlock#rt_block{offset = Offset, size = BlockSize},
    ShrunkBaseBlock = BaseBlock#rt_block{offset = NextOffset, size = Size - BlockSize},
    [NewBlock | partition(ExistingBlocks, ShrunkBaseBlock)];

partition([], #rt_block{size = Size} = BaseBlock) when Size > 0 ->
    [BaseBlock];

partition([], #rt_block{}) ->
    [].


%% push/2
%% ====================================================================
%% @doc A meck-friendly wrapper.
%% @equiv rt_priority_queue:push(ContainerRef, Block)
-spec push(ContainerRef, Block :: #rt_block{}) -> ok when
    ContainerRef :: container_ref().
push(ContainerRef, Block) ->
    rt_priority_queue:push(ContainerRef, Block).


%% pop/1
%% ====================================================================
%% @doc A meck-friendly wrapper.
%% @equiv rt_priority_queue:pop(ContainerRef)
-spec pop(ContainerRef) -> {ok, #rt_block{}} | {error, Error :: term()} when
    ContainerRef :: container_ref().
pop(ContainerRef) ->
    rt_priority_queue:pop(ContainerRef).
