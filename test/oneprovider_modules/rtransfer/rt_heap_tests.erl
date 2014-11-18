%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module tests the functionality of rt_heap module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(rt_heap_tests).

-ifdef(TEST).

-include("oneprovider_modules/rtransfer/rt_container.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_RT_BLOCK_SIZE, 10).
-define(TEST_HEAP, test_heap).

%% ===================================================================
%% Tests description
%% ===================================================================

rt_heap_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            {"should create empty heap", fun should_create_empty_heap/0},
            {"should push single block", fun should_push_single_block/0},
            {"should push block with large offset", fun should_push_block_with_large_offset/0},
            {"should push blocks from different files", fun should_push_blocks_from_different_files/0},
            {"should push many blocks", fun should_push_many_blocks/0},
            {"should split large block", fun should_split_large_block/0},
            {"should merge small blocks", fun should_merge_small_blocks/0},
            {"should increase counter on blocks overlap", fun should_increase_counter_on_blocks_overlap/0},
            {"should change priority 1", fun should_change_priority_1/0},
            {"should change priority 2", fun should_change_priority_2/0},
            {"should change priority 3", fun should_change_priority_3/0},
            {"should change priority 4", fun should_change_priority_4/0},
            {"should change priority 5", fun should_change_priority_5/0},
            {"should concatenate block pids", fun should_concatenate_block_pids/0}
        ]
    }.

%% ===================================================================
%% Setup/teardown functions
%% ===================================================================

setup() ->
    {ok, _} = rt_container:new({local, ?TEST_HEAP}, "../", ?TEST_RT_BLOCK_SIZE).

teardown(_) ->
    ok = rt_container:delete(?TEST_HEAP).

%% ===================================================================
%% Tests functions
%% ===================================================================

should_create_empty_heap() ->
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)).

should_push_single_block() ->
    Block = #rt_block{file_id = "test_file", offset = 0, size = 5, priority = 2},

    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block)),
    ?assertEqual({ok, Block}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)).

should_push_block_with_large_offset() ->
    Block = #rt_block{file_id = "test_file", offset = 9223372036854775807, size = 5, priority = 2},

    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block)),
    ?assertEqual({ok, Block}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)).

should_push_blocks_from_different_files() ->
    Block1 = #rt_block{file_id = "test_file_1", offset = 0, size = 5, priority = 2},
    Block2 = #rt_block{file_id = "test_file_2", offset = 0, size = 5, priority = 2},

    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block1)),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block2)),
    ?assertEqual({ok, Block1}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block2}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)).

should_push_many_blocks() ->
    BlocksAmount = 10000,
    Block = #rt_block{file_id = "test_file", size = ?TEST_RT_BLOCK_SIZE, priority = 2},

    lists:foreach(fun(N) ->
        ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block#rt_block{offset = N * ?TEST_RT_BLOCK_SIZE}))
    end, lists:seq(0, BlocksAmount)),
    lists:foreach(fun(N) ->
        ?assertEqual({ok, Block#rt_block{offset = N * ?TEST_RT_BLOCK_SIZE}}, rt_container:pop(?TEST_HEAP))
    end, lists:seq(0, BlocksAmount)),
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)).

should_merge_small_blocks() ->
    Block = #rt_block{file_id = "test_file", size = 1, priority = 2},

    lists:foreach(fun(N) ->
        ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block#rt_block{offset = N}))
    end, lists:seq(0, ?TEST_RT_BLOCK_SIZE - 1)),
    ?assertEqual({ok, Block#rt_block{offset = 0, size = ?TEST_RT_BLOCK_SIZE}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)).

should_split_large_block() ->
    FullBlocksAmount = 10,
    LastBlockSize = random:uniform(?TEST_RT_BLOCK_SIZE),
    Block = #rt_block{file_id = "test_file", offset = 0, size = FullBlocksAmount * ?TEST_RT_BLOCK_SIZE + LastBlockSize, priority = 2},

    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block)),
    lists:foreach(fun(N) ->
        ?assertEqual({ok, Block#rt_block{offset = N * ?TEST_RT_BLOCK_SIZE, size = ?TEST_RT_BLOCK_SIZE}}, rt_container:pop(?TEST_HEAP))
    end, lists:seq(0, FullBlocksAmount - 1)),
    ?assertEqual({ok, Block#rt_block{offset = FullBlocksAmount * ?TEST_RT_BLOCK_SIZE, size = LastBlockSize}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)).

should_increase_counter_on_blocks_overlap() ->
    Block1 = #rt_block{file_id = "test_file", offset = 0, size = 5, priority = 2},
    Block2 = #rt_block{file_id = "test_file", offset = 5, size = 5, priority = 2},

    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block1)),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block2)),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block2)),
    ?assertEqual({ok, Block2}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block1}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)).

should_change_priority_1() ->
    Block1 = #rt_block{file_id = "test_file", offset = 0, size = 10, priority = 2},
    Block2 = #rt_block{file_id = "test_file", offset = 3, size = 3, priority = 5},

    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block1)),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block2)),
    ?assertEqual({ok, Block2}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block1#rt_block{size = 3}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block1#rt_block{offset = 6, size = 4}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)),

    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block2)),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block1)),
    ?assertEqual({ok, Block2#rt_block{priority = 2}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block1#rt_block{size = 3}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block1#rt_block{offset = 6, size = 4}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)).

should_change_priority_2() ->
    Block1 = #rt_block{file_id = "test_file", offset = 0, size = 3, priority = 2},
    Block2 = #rt_block{file_id = "test_file", offset = 2, size = 4, priority = 5},
    Block3 = #rt_block{file_id = "test_file", offset = 4, size = 3, priority = 3},

    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block1)),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block2)),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block3)),
    ?assertEqual({ok, Block2#rt_block{size = 5}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block1#rt_block{size = 2}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)).

should_change_priority_3() ->
    Block1 = #rt_block{file_id = "test_file", offset = 0, size = 10, priority = 1},
    Block2 = #rt_block{file_id = "test_file", offset = 1, size = 8, priority = 2},
    Block3 = #rt_block{file_id = "test_file", offset = 2, size = 6, priority = 3},
    Block4 = #rt_block{file_id = "test_file", offset = 3, size = 4, priority = 4},
    Block5 = #rt_block{file_id = "test_file", offset = 4, size = 2, priority = 5},

    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block1)),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block2)),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block3)),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block4)),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block5)),
    ?assertEqual({ok, Block5}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block4#rt_block{size = 1}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block4#rt_block{offset = 6, size = 1}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block3#rt_block{size = 1}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block3#rt_block{offset = 7, size = 1}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block2#rt_block{size = 1}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block2#rt_block{offset = 8, size = 1}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block1#rt_block{size = 1}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block1#rt_block{offset = 9, size = 1}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)).

should_change_priority_4() ->
    Block = #rt_block{file_id = "test_file", size = 1, priority = 2},

    lists:foreach(fun(N) ->
        ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block#rt_block{offset = N}))
    end, lists:seq(0, ?TEST_RT_BLOCK_SIZE - 1)),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block#rt_block{offset = 0, size = ?TEST_RT_BLOCK_SIZE})),
    ?assertEqual({ok, Block#rt_block{offset = 0, size = ?TEST_RT_BLOCK_SIZE}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)).

should_change_priority_5() ->
    Block1 = #rt_block{file_id = "test_file", offset = 0, size = 6, priority = 1},
    Block2 = #rt_block{file_id = "test_file", offset = 2, size = 6, priority = 2},

    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block1)),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block2)),
    ?assertEqual({ok, Block2}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({ok, Block1#rt_block{size = 2}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)),

    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block1#rt_block{priority = 2})),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block2#rt_block{priority = 1})),
    ?assertEqual({ok, Block1#rt_block{size = 8, priority = 2}}, rt_container:pop(?TEST_HEAP)),
    ?assertEqual({error, "Empty heap"}, rt_container:pop(?TEST_HEAP)).

should_concatenate_block_pids() ->
    Pid1 = list_to_pid("<0.1.0>"),
    Pid2 = list_to_pid("<0.2.0>"),
    PidsFilterFunction = fun(_) -> true end,

    Block1 = #rt_block{file_id = "test_file", offset = 0, size = 10, priority = 1, pids = [Pid1]},
    Block2 = #rt_block{file_id = "test_file", offset = 3, size = 3, priority = 2, pids = [Pid2]},

    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block1)),
    ?assertEqual(ok, rt_container:push(?TEST_HEAP, Block2)),

    ?assertEqual({ok, Block2#rt_block{pids = [Pid1, Pid2]}}, rt_container:pop(?TEST_HEAP, PidsFilterFunction)),
    ?assertEqual({ok, Block1#rt_block{size = 3}}, rt_container:pop(?TEST_HEAP, PidsFilterFunction)),
    ?assertEqual({ok, Block1#rt_block{offset = 6, size = 4}}, rt_container:pop(?TEST_HEAP, PidsFilterFunction)).

-endif.