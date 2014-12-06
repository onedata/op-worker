%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module tests the functionality of rt_priority_queue module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(rt_priority_queue_tests).

-ifdef(TEST).

-include("registered_names.hrl").
-include("oneprovider_modules/rtransfer/rt_container.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_RT_BLOCK_SIZE, 1024).
-define(TEST_PRIORITY_QUEUE, test_priority_queue).

%% ===================================================================
%% Tests description
%% ===================================================================

rt_priority_queue_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            {"should create empty priority queue", fun should_create_empty_priority_queue/0},
            {"should increase container size", fun should_increase_container_size/0},
            {"should push single block", fun should_push_single_block/0},
            {"should push block with large offset", fun should_push_block_with_large_offset/0},
            {"should push blocks from different files", fun should_push_blocks_from_different_files/0},
            {"should push many blocks", fun should_push_many_blocks/0},
            {"should split large block 1", fun should_split_large_block/0},
            {"should merge small blocks", fun should_merge_small_blocks/0},
            {"should increase counter on blocks overlap", fun should_increase_counter_on_blocks_overlap/0},
            {"should change priority 1", fun should_change_priority_1/0},
            {"should change priority 2", fun should_change_priority_2/0},
            {"should change priority 3", fun should_change_priority_3/0},
            {"should change priority 4", fun should_change_priority_4/0},
            {"should change priority 5", fun should_change_priority_5/0},
            {"should concatenate block pids", fun should_concatenate_block_pids/0},
            {"should remove repeated pids", fun should_remove_repeated_pids/0},
            {"should subscribe and unsubscribe process", fun should_subscribe_and_unsubscribe_process/0},
            {"should change counter 1", fun should_change_counter_1/0},
            {"should change counter 2", fun should_change_counter_2/0},
            {"should change counter 2", fun should_change_counter_3/0}
        ]
    }.

%% ===================================================================
%% Setup/teardown functions
%% ===================================================================

setup() ->
    application:set_env(?APP_Name, rt_nif_prefix, "../c_lib"),
    {ok, _} = rt_priority_queue:new({local, ?TEST_PRIORITY_QUEUE}, ?TEST_RT_BLOCK_SIZE).

teardown(_) ->
    ok = rt_priority_queue:delete(?TEST_PRIORITY_QUEUE),
    application:set_env(?APP_Name, rt_nif_prefix, "c_lib").

%% ===================================================================
%% Tests functions
%% ===================================================================

should_create_empty_priority_queue() ->
    ?assertEqual({ok, 0}, rt_priority_queue:size(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_increase_container_size() ->
    Block1 = #rt_block{file_id = "test_file", offset = 0, size = 10, priority = 2},
    Block2 = #rt_block{file_id = "test_file", offset = 3, size = 3, priority = 3},

    ?assertEqual({ok, 0}, rt_priority_queue:size(?TEST_PRIORITY_QUEUE)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block1)),
    ?assertEqual({ok, 1}, rt_priority_queue:size(?TEST_PRIORITY_QUEUE)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block2)),
    ?assertEqual({ok, 3}, rt_priority_queue:size(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block2}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, 2}, rt_priority_queue:size(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block1#rt_block{size = 3}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, 1}, rt_priority_queue:size(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block1#rt_block{offset = 6, size = 4}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, 0}, rt_priority_queue:size(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_push_single_block() ->
    Block = #rt_block{file_id = "test_file", offset = 0, size = 5, priority = 2},

    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block)),
    ?assertEqual({ok, Block}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_push_block_with_large_offset() ->
    Block = #rt_block{file_id = "test_file", offset = 9223372036854775807, size = 5, priority = 2},

    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block)),
    ?assertEqual({ok, Block}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_push_blocks_from_different_files() ->
    Block1 = #rt_block{file_id = "test_file_1", offset = 0, size = 5, priority = 2},
    Block2 = #rt_block{file_id = "test_file_2", offset = 0, size = 5, priority = 2},

    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block1)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block2)),
    ?assertEqual({ok, Block1}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block2}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_push_many_blocks() ->
    BlocksAmount = 10000,
    Block = #rt_block{file_id = "test_file", size = ?TEST_RT_BLOCK_SIZE, priority = 2},

    lists:foreach(fun(N) ->
        ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block#rt_block{offset = N * ?TEST_RT_BLOCK_SIZE}))
    end, lists:seq(0, BlocksAmount)),
    lists:foreach(fun(N) ->
        ?assertEqual({ok, Block#rt_block{offset = N * ?TEST_RT_BLOCK_SIZE}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE))
    end, lists:seq(0, BlocksAmount)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_merge_small_blocks() ->
    Block = #rt_block{file_id = "test_file", size = 1, priority = 2},

    lists:foreach(fun(N) ->
        ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block#rt_block{offset = N}))
    end, lists:seq(0, ?TEST_RT_BLOCK_SIZE - 1)),
    ?assertEqual({ok, Block#rt_block{offset = 0, size = ?TEST_RT_BLOCK_SIZE}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_split_large_block() ->
    FullBlocksAmount = 10000,
    LastBlockSize = random:uniform(?TEST_RT_BLOCK_SIZE),
    Block = #rt_block{file_id = "test_file", offset = 0, size = FullBlocksAmount * ?TEST_RT_BLOCK_SIZE + LastBlockSize, priority = 2},

    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block)),
    lists:foreach(fun(N) ->
        ?assertEqual({ok, Block#rt_block{offset = N * ?TEST_RT_BLOCK_SIZE, size = ?TEST_RT_BLOCK_SIZE}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE))
    end, lists:seq(0, FullBlocksAmount - 1)),
    ?assertEqual({ok, Block#rt_block{offset = FullBlocksAmount * ?TEST_RT_BLOCK_SIZE, size = LastBlockSize}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_increase_counter_on_blocks_overlap() ->
    Block1 = #rt_block{file_id = "test_file", offset = 0, size = 5, priority = 2},
    Block2 = #rt_block{file_id = "test_file", offset = 5, size = 5, priority = 2},

    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block1)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block2)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block2)),
    ?assertEqual({ok, Block2}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block1}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_change_priority_1() ->
    Block1 = #rt_block{file_id = "test_file", offset = 0, size = 10, priority = 2},
    Block2 = #rt_block{file_id = "test_file", offset = 3, size = 3, priority = 5},

    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block1)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block2)),
    ?assertEqual({ok, Block2}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block1#rt_block{size = 3}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block1#rt_block{offset = 6, size = 4}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),

    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block2)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block1)),
    ?assertEqual({ok, Block2#rt_block{priority = 2}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block1#rt_block{size = 3}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block1#rt_block{offset = 6, size = 4}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_change_priority_2() ->
    Block1 = #rt_block{file_id = "test_file", offset = 0, size = 3, priority = 2},
    Block2 = #rt_block{file_id = "test_file", offset = 2, size = 4, priority = 5},
    Block3 = #rt_block{file_id = "test_file", offset = 4, size = 3, priority = 3},

    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block1)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block2)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block3)),
    ?assertEqual({ok, Block2#rt_block{size = 5}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block1#rt_block{size = 2}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_change_priority_3() ->
    Block1 = #rt_block{file_id = "test_file", offset = 0, size = 10, priority = 1},
    Block2 = #rt_block{file_id = "test_file", offset = 1, size = 8, priority = 2},
    Block3 = #rt_block{file_id = "test_file", offset = 2, size = 6, priority = 3},
    Block4 = #rt_block{file_id = "test_file", offset = 3, size = 4, priority = 4},
    Block5 = #rt_block{file_id = "test_file", offset = 4, size = 2, priority = 5},

    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block1)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block2)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block3)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block4)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block5)),
    ?assertEqual({ok, Block5}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block4#rt_block{size = 1}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block4#rt_block{offset = 6, size = 1}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block3#rt_block{size = 1}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block3#rt_block{offset = 7, size = 1}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block2#rt_block{size = 1}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block2#rt_block{offset = 8, size = 1}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block1#rt_block{size = 1}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block1#rt_block{offset = 9, size = 1}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_change_priority_4() ->
    Block = #rt_block{file_id = "test_file", size = 1, priority = 2},

    lists:foreach(fun(N) ->
        ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block#rt_block{offset = N}))
    end, lists:seq(0, ?TEST_RT_BLOCK_SIZE - 1)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block#rt_block{offset = 0, size = ?TEST_RT_BLOCK_SIZE})),
    ?assertEqual({ok, Block#rt_block{offset = 0, size = ?TEST_RT_BLOCK_SIZE}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_change_priority_5() ->
    Block1 = #rt_block{file_id = "test_file", offset = 0, size = 6, priority = 1},
    Block2 = #rt_block{file_id = "test_file", offset = 2, size = 6, priority = 2},

    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block1)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block2)),
    ?assertEqual({ok, Block2}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block1#rt_block{size = 2}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),

    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block1#rt_block{priority = 2})),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block2#rt_block{priority = 1})),
    ?assertEqual({ok, Block1#rt_block{size = 8, priority = 2}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_concatenate_block_pids() ->
    Pid1 = list_to_pid("<0.1.0>"),
    Pid2 = list_to_pid("<0.2.0>"),
    PidsFilterFunction = fun(_) -> true end,

    Block1 = #rt_block{file_id = "test_file", offset = 0, size = 10, priority = 1, terms = [Pid1]},
    Block2 = #rt_block{file_id = "test_file", offset = 3, size = 3, priority = 2, terms = [Pid2]},

    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block1)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block2)),

    PopAns = rt_priority_queue:pop(?TEST_PRIORITY_QUEUE, PidsFilterFunction),
    ?assertMatch({ok, _}, PopAns),
    {ok, PoppedBlock} = PopAns,
    ?assertEqual([Pid1, Pid2], lists:sort(PoppedBlock#rt_block.terms)),
    ?assertEqual({ok, Block1#rt_block{size = 3}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE, PidsFilterFunction)),
    ?assertEqual({ok, Block1#rt_block{offset = 6, size = 4}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE, PidsFilterFunction)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_remove_repeated_pids() ->
    Pid1 = list_to_pid("<0.1.0>"),
    Pid2 = list_to_pid("<0.2.0>"),
    Pid3 = list_to_pid("<0.3.0>"),
    PidsFilterFunction = fun(_) -> true end,

    Block = #rt_block{file_id = "test_file", offset = 0, size = 10, priority = 1},

    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block#rt_block{terms = [Pid1]})),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block#rt_block{terms = [Pid2]})),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block#rt_block{terms = [Pid3]})),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block#rt_block{terms = [Pid2]})),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block#rt_block{terms = [Pid1]})),

    PopAns = rt_priority_queue:pop(?TEST_PRIORITY_QUEUE, PidsFilterFunction),
    ?assertMatch({ok, _}, PopAns),
    {ok, PoppedBlock} = PopAns,
    ?assertEqual([Pid1, Pid2, Pid3], lists:sort(PoppedBlock#rt_block.terms)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_subscribe_and_unsubscribe_process() ->
    Reference = make_ref(),
    Block = #rt_block{file_id = "test_file", offset = 0, size = 10, priority = 1},
    ?assertEqual(ok, rt_priority_queue:subscribe(?TEST_PRIORITY_QUEUE, self(), Reference)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block)),
    NotificationReceived = receive
                               {not_empty, Reference} -> true
                           after
                               1000 -> false
                           end,
    ?assert(NotificationReceived),

    ?assertEqual({ok, Block}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, 0}, rt_priority_queue:size(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),

    ?assertEqual(ok, rt_priority_queue:unsubscribe(?TEST_PRIORITY_QUEUE, Reference)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, #rt_block{})),
    NotificationNotReceived = receive
                                  {not_empty, Reference} -> false
                              after
                                  1000 -> true
                              end,
    ?assert(NotificationNotReceived).

should_change_counter_1() ->
    FileId = "test_file",
    Block = #rt_block{file_id = FileId, offset = 0, size = 10, priority = 1},
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block)),
    ?assertEqual(ok, rt_priority_queue:change_counter(?TEST_PRIORITY_QUEUE, FileId, 3, 3, 2)),
    ?assertEqual({ok, 3}, rt_priority_queue:size(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block#rt_block{offset = 3, size = 3}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block#rt_block{size = 3}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block#rt_block{offset = 6, size = 4}}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_change_counter_2() ->
    FileId = "test_file",
    Block = #rt_block{file_id = FileId, offset = 3, size = 3, priority = 1},
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block)),
    ?assertEqual(ok, rt_priority_queue:change_counter(?TEST_PRIORITY_QUEUE, FileId, 0, 10, 2)),
    ?assertEqual({ok, 1}, rt_priority_queue:size(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

should_change_counter_3() ->
    FileId = "test_file",
    Block1 = #rt_block{file_id = FileId, offset = 0, size = 3, priority = 1},
    Block2 = #rt_block{file_id = FileId, offset = 5, size = 3, priority = 1},
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block1)),
    ?assertEqual(ok, rt_priority_queue:push(?TEST_PRIORITY_QUEUE, Block2)),
    ?assertEqual(ok, rt_priority_queue:change_counter(?TEST_PRIORITY_QUEUE, FileId, 4, 6)),
    ?assertEqual({ok, 2}, rt_priority_queue:size(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block2}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({ok, Block1}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)),
    ?assertEqual({error, empty}, rt_priority_queue:pop(?TEST_PRIORITY_QUEUE)).

-endif.