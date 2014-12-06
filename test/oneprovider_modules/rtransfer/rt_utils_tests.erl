%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module tests the functionality of helper functions contained
%% in rt_utils module.
%% @end
%% ===================================================================
-module(rt_utils_tests).

-ifdef(TEST).

-include("oneprovider_modules/rtransfer/rt_container.hrl").
-include_lib("eunit/include/eunit.hrl").


%% ===================================================================
%% Tests description
%% ===================================================================

rt_utils_partition_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            {"should return the base block on empty existing blocks", fun partition_should_return_base_block/0},
            {"should fill in missing block at the beginning", fun partition_should_fill_in_starting_block/0},
            {"should fill in missing block at the end", fun partition_should_fill_in_last_block/0},
            {"should fill in missing blocks in the middle of range", fun partition_should_fill_in_middle_blocks/0},
            {"should rewrite provider_ref from base block", fun partition_should_override_provider_ref/0},
            {"should rewrite take higher priority from base block", fun partition_should_use_higher_priority/0}
        ]
    }.

%% ===================================================================
%% Setup/teardown functions
%% ===================================================================

setup() ->
    ok.

teardown(_) ->
    ok.

%% ===================================================================
%% Tests functions
%% ===================================================================

partition_should_return_base_block() ->
    BaseBlock = #rt_block{provider_ref = qwerty, terms = [a, l], file_id = "FileId",
                          offset = 123, size = 456},

    ?assertEqual([BaseBlock], rt_utils:partition([], BaseBlock)).

partition_should_fill_in_starting_block() ->
    BaseBlock = #rt_block{provider_ref = qwerty, terms = [a, l], file_id = "FileId",
                          offset = 0, size = 200},
    ExistingBlock = BaseBlock#rt_block{offset = 50, size = 150},
    ExpectedBlock = BaseBlock#rt_block{offset = 0, size = 50},

    ?assertEqual(ordsets:from_list([ExistingBlock, ExpectedBlock]),
                 ordsets:from_list(rt_utils:partition([ExistingBlock], BaseBlock))).

partition_should_fill_in_last_block() ->
    BaseBlock = #rt_block{provider_ref = qwerty, terms = [a, l], file_id = "FileId",
                          offset = 0, size = 200},
    ExistingBlock = BaseBlock#rt_block{offset = 0, size = 150},
    ExpectedBlock = BaseBlock#rt_block{offset = 150, size = 50},

    ?assertEqual(ordsets:from_list([ExistingBlock, ExpectedBlock]),
        ordsets:from_list(rt_utils:partition([ExistingBlock], BaseBlock))).

partition_should_fill_in_middle_blocks() ->
    BaseBlock = #rt_block{provider_ref = qwerty, terms = [a, l], file_id = "FileId",
                          offset = 0, size = 200},
    ExistingBlocks =
        [
            BaseBlock#rt_block{offset = 0, size = 75},
            BaseBlock#rt_block{offset = 100, size = 50},
            BaseBlock#rt_block{offset = 175, size = 25}
        ],

    ExpectedBlocks =
        [
            BaseBlock#rt_block{offset = 75, size = 25},
            BaseBlock#rt_block{offset = 150, size = 25}
        ],

    ?assertEqual(ordsets:from_list(ExistingBlocks ++ ExpectedBlocks),
        ordsets:from_list(rt_utils:partition(ExistingBlocks, BaseBlock))).

partition_should_override_provider_ref() ->
    BaseBlock = #rt_block{provider_ref = newref, terms = [a, l], file_id = "FileId",
                          offset = 0, size = 200},
    ExistingBlock = BaseBlock#rt_block{provider_ref = other},

    ?assertMatch([#rt_block{provider_ref = newref}],
        rt_utils:partition([ExistingBlock], BaseBlock)).

partition_should_use_higher_priority() ->
    BaseBlock = #rt_block{provider_ref = qwerty, terms = [a, l], file_id = "FileId",
                          offset = 0, size = 200, priority = 25},
    ExistingBlocks =
        [
            BaseBlock#rt_block{offset = 0,   size = 100, priority = 20},
            BaseBlock#rt_block{offset = 100, size = 100, priority = 30}
        ],
    ExpectedBlocks =
        [
            BaseBlock#rt_block{offset = 0, size = 100, priority = 25},
            BaseBlock#rt_block{offset = 100, size = 100, priority = 30}
        ],

    ?assertEqual(ordsets:from_list(ExpectedBlocks),
        ordsets:from_list(rt_utils:partition(ExistingBlocks, BaseBlock))).

-endif.