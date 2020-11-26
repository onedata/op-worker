%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for fslogic_location_cache module.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_location_cache_test).
-author("Michal Wrzeszcz").

-ifdef(TEST).

-include("global_definitions.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(KEY, <<"KEY">>).
-define(BLOCK(Offset, Size), #file_block{offset = Offset, size = Size}).
-define(BLOCK(Offset), ?BLOCK(Offset, 3)).

%%%===================================================================
%%% Test functions - basic functionality
%%%===================================================================

save_and_get_test() ->
    TestBlocks = gen_test_blocks(3),
    ?assertEqual(TestBlocks, fslogic_location_cache:get_blocks(?KEY, #{})).

get_blocks_count_test() ->
    TestBlocks = gen_test_blocks(5),
    lists:foreach(fun(N) ->
        ?assertEqual(lists:sublist(TestBlocks, N), fslogic_location_cache:get_blocks(?KEY, #{count => N}))
    end, lists:seq(0, 6)).

%%%===================================================================
%%% Test functions - overlapping blocks
%%%===================================================================

get_overlapping_blocks_equal_size_test() ->
    get_overlapping_blocks_template(fun(Offset) -> ?BLOCK(Offset) end).

get_overlapping_blocks_smaller_size_test() ->
    get_overlapping_blocks_template(fun(Offset) -> ?BLOCK(Offset + 1, 1) end).

get_overlapping_blocks_smaller_size_overlapping_offset_test() ->
    get_overlapping_blocks_template(fun(Offset) -> ?BLOCK(Offset, 2) end).

get_overlapping_blocks_smaller_size_overlapping_end_test() ->
    get_overlapping_blocks_template(fun(Offset) -> ?BLOCK(Offset - 1, 5) end).

get_overlapping_blocks_larger_size_test() ->
    get_overlapping_blocks_template(fun(Offset) -> ?BLOCK(Offset + 1, 1) end).

get_overlapping_blocks_larger_size_overlapping_offset_test() ->
    get_overlapping_blocks_template(fun(Offset) -> ?BLOCK(Offset, 4) end).

get_overlapping_blocks_larger_size_overlapping_end_test() ->
    get_overlapping_blocks_template(fun(Offset) -> ?BLOCK(Offset - 1, 4) end).

get_overlapping_blocks_shifted_left_test() ->
    get_overlapping_blocks_template(fun(Offset) -> ?BLOCK(Offset - 1) end).

get_overlapping_blocks_shifted_right_test() ->
    get_overlapping_blocks_template(fun(Offset) -> ?BLOCK(Offset + 1) end).

get_overlapping_blocks_adjacent_left_test() ->
    get_overlapping_blocks_template(fun(Offset) -> ?BLOCK(Offset - 1, 1) end).

get_overlapping_blocks_adjacent_right_test() ->
    get_overlapping_blocks_template(fun(Offset) -> ?BLOCK(Offset + 3, 1) end).

get_overlapping_blocks_template(InputBlockProducer) ->
    TestBlocks = gen_test_blocks(5),
    lists:foreach(fun(N) ->
        #file_block{offset = Offset} = BlockToCheck = lists:nth(N, TestBlocks),
        InputBlock = InputBlockProducer(Offset),
        ?assertEqual([BlockToCheck], fslogic_location_cache:get_blocks(?KEY, #{overlapping_blocks => [InputBlock]}))
    end, lists:seq(1, 5)).

get_not_overlapping_blocks_test() ->
    TestBlocks = gen_test_blocks(2),
    #file_block{offset = Offset} = lists:nth(1, TestBlocks),
    ?assertEqual([], fslogic_location_cache:get_blocks(?KEY, #{overlapping_blocks => [?BLOCK(Offset + 4, 1)]})).

get_two_overlapping_blocks_test() ->
    TestBlocks = gen_test_blocks(5),
    [#file_block{offset = Offset} | _] = ExpectedBlocks = lists:sublist(TestBlocks, 2),
    ?assertEqual(ExpectedBlocks, fslogic_location_cache:get_blocks(?KEY, #{overlapping_blocks => [?BLOCK(Offset + 3, 7)]})).

get_multiple_overlapping_blocks_test() ->
    TestBlocks = gen_test_blocks(5),
    [_, #file_block{offset = Offset} | _] = [_ | ExpectedBlocks] = lists:sublist(TestBlocks, 4),
    ?assertEqual(ExpectedBlocks, fslogic_location_cache:get_blocks(?KEY, #{overlapping_blocks => [?BLOCK(Offset, 23)]})).

get_overlapping_blocks_by_full_sequence_test() ->
    get_overlapping_blocks_by_sequence_template(fun(Blocks) -> Blocks end).

get_overlapping_blocks_by_two_blocks_sequence_test() ->
    InputBlocksProducer = fun([#file_block{offset = Offset} | _] = Blocks) ->
        [#file_block{offset = LastBlockOffset} | _] = lists:reverse(Blocks),
        [?BLOCK(Offset + 3), ?BLOCK(LastBlockOffset - 1, 1)]
    end,
    get_overlapping_blocks_by_sequence_template(InputBlocksProducer).

get_overlapping_blocks_by_holes_sequence_test() ->
    InputBlocksProducer = fun([_ | Blocks]) ->
        lists:map(fun(#file_block{offset = Offset}) -> ?BLOCK(Offset - 7, 7) end, Blocks)
    end,
    get_overlapping_blocks_by_sequence_template(InputBlocksProducer).

get_overlapping_blocks_by_sequence_template(InputBlocksProducer) ->
    get_overlapping_blocks_by_sequence_template(InputBlocksProducer, 1, 5, 5),
    get_overlapping_blocks_by_sequence_template(InputBlocksProducer, 2, 6, 7).

get_overlapping_blocks_by_sequence_template(InputBlocksProducer, BlockToStart, BlockToStop, BlocksNumber) ->
    TestBlocks = gen_test_blocks(BlocksNumber),
    lists:foreach(fun(M) ->
        lists:foreach(fun(N) ->
            ExpectedBlocks = lists:sublist(TestBlocks, M, N - M + 1),
            InputBlocks = InputBlocksProducer(ExpectedBlocks),
            ?assertEqual(ExpectedBlocks, fslogic_location_cache:get_blocks(?KEY, #{overlapping_blocks => InputBlocks}))
        end, lists:seq(M + 1, BlockToStop))
    end, lists:seq(BlockToStart, BlockToStop - 1)).

get_overlapping_blocks_by_sequence_with_holes_test() ->
    TestBlocks = gen_test_blocks(5),
    InputBlocks = [lists:nth(1, TestBlocks), lists:nth(3, TestBlocks), lists:nth(5, TestBlocks)],
    ?assertEqual(TestBlocks, fslogic_location_cache:get_blocks(?KEY, #{overlapping_blocks => InputBlocks})).

%%%===================================================================
%%% Test functions - overlapping sequences
%%%===================================================================

get_overlapping_sequence_equal_size_test() ->
    get_overlapping_sequence_template(fun(Offset) -> ?BLOCK(Offset) end).

get_overlapping_sequence_smaller_size_test() ->
    get_overlapping_sequence_template(fun(Offset) -> ?BLOCK(Offset + 1, 1) end).

get_overlapping_sequence_smaller_size_overlapping_offset_test() ->
    get_overlapping_sequence_template(fun(Offset) -> ?BLOCK(Offset, 2) end).

get_overlapping_sequence_smaller_size_overlapping_end_test() ->
    get_overlapping_sequence_template(fun(Offset) -> ?BLOCK(Offset - 1, 5) end).

get_overlapping_sequence_larger_size_test() ->
    get_overlapping_sequence_template(fun(Offset) -> ?BLOCK(Offset + 1, 1) end).

get_overlapping_sequence_larger_size_overlapping_offset_test() ->
    get_overlapping_sequence_template(fun(Offset) -> ?BLOCK(Offset, 4) end).

get_overlapping_sequence_larger_size_overlapping_end_test() ->
    get_overlapping_sequence_template(fun(Offset) -> ?BLOCK(Offset - 1, 4) end).

get_overlapping_sequence_shifted_left_test() ->
    get_overlapping_sequence_template(fun(Offset) -> ?BLOCK(Offset - 1) end).

get_overlapping_sequence_shifted_right_test() ->
    get_overlapping_sequence_template(fun(Offset) -> ?BLOCK(Offset + 1) end).

get_overlapping_sequence_adjacent_left_test() ->
    get_overlapping_sequence_template(fun(Offset) -> ?BLOCK(Offset - 1, 1) end).

get_overlapping_sequence_adjacent_right_test() ->
    get_overlapping_sequence_template(fun(Offset) -> ?BLOCK(Offset + 3, 1) end).

get_overlapping_sequence_template(InputBlockProducer) ->
    TestBlocks = gen_test_blocks(5),
    lists:foreach(fun(N) ->
        #file_block{offset = Offset} = BlockToCheck = lists:nth(N, TestBlocks),
        InputBlock = InputBlockProducer(Offset),
        ?assertEqual([{[InputBlock], [BlockToCheck]}],
            fslogic_location_cache:get_overlapping_blocks_sequence(?KEY, [InputBlock]))
    end, lists:seq(1, 5)).

get_not_overlapping_sequence_test() ->
    TestBlocks = gen_test_blocks(2),
    #file_block{offset = Offset} = lists:nth(1, TestBlocks),
    InputBlocks = [?BLOCK(Offset + 4, 1)],
    ?assertEqual([{InputBlocks, []}], fslogic_location_cache:get_overlapping_blocks_sequence(?KEY, InputBlocks)).

get_two_overlapping_blocks_sequence_test() ->
    TestBlocks = gen_test_blocks(5),
    [#file_block{offset = Offset} | _] = ExpectedBlocks = lists:sublist(TestBlocks, 2),
    InputBlocks = [?BLOCK(Offset + 3, 7)],
    ?assertEqual([{InputBlocks, ExpectedBlocks}], fslogic_location_cache:get_overlapping_blocks_sequence(?KEY, InputBlocks)).

get_multiple_overlapping_blocks_sequence_test() ->
    TestBlocks = gen_test_blocks(5),
    [_, #file_block{offset = Offset} | _] = [_ | ExpectedBlocks] = lists:sublist(TestBlocks, 4),
    InputBlocks = [?BLOCK(Offset, 23)],
    ?assertEqual([{InputBlocks, ExpectedBlocks}], fslogic_location_cache:get_overlapping_blocks_sequence(?KEY, InputBlocks)).

get_overlapping_sequences_one_to_one_test() ->
    InputBlocksProducer = fun(Blocks) -> Blocks end,
    ExpectedBlocksProducer = fun(Blocks, InputBlocks, _) -> [{InputBlocks, Blocks}] end,
    get_multiple_overlapping_sequences_template(InputBlocksProducer, ExpectedBlocksProducer, [0, 5]).

get_overlapping_sequences_between_two_blocks_test() ->
    InputBlocksProducer = fun([#file_block{offset = Offset} | _] = Blocks) ->
        [#file_block{offset = LastBlockOffset} | _] = lists:reverse(Blocks),
        [?BLOCK(Offset + 3), ?BLOCK(LastBlockOffset - 1, 1)]
    end,
    ExpectedBlocksProducer = fun([FirstBlock | _] = Blocks, InputBlocks, MaxHole) ->
        case MaxHole < length(Blocks) - 2 of
            true ->
                [LastBlock | _] = lists:reverse(Blocks),
                [FirstInputBlock, LastInputBlock] = InputBlocks,
                [{[FirstInputBlock], [FirstBlock]}, {[LastInputBlock], [LastBlock]}];
            false ->
                [{InputBlocks, Blocks}]
        end
    end,
    get_multiple_overlapping_sequences_template(InputBlocksProducer, ExpectedBlocksProducer, [0, 1, 2, 3, 4, 5]).

get_overlapping_sequences_using_holes_test() ->
    InputBlocksProducer = fun([_ | Blocks]) ->
        lists:map(fun(#file_block{offset = Offset}) -> ?BLOCK(Offset - 7, 7) end, Blocks)
    end,
    ExpectedBlocksProducer = fun(Blocks, InputBlocks, _) -> [{InputBlocks, Blocks}] end,
    get_multiple_overlapping_sequences_template(InputBlocksProducer, ExpectedBlocksProducer, [0, 5]).

get_multiple_overlapping_sequences_template(InputBlocksProducer, ExpectedBlocksProducer, MaxHoles) ->
    get_multiple_overlapping_sequences_template(InputBlocksProducer, ExpectedBlocksProducer, MaxHoles, 1, 5, 5),
    get_multiple_overlapping_sequences_template(InputBlocksProducer, ExpectedBlocksProducer, MaxHoles, 2, 6, 7).

get_multiple_overlapping_sequences_template(InputBlocksProducer, ExpectedBlocksProducer, MaxHoles,
    BlockToStart, BlockToStop, BlocksNumber) ->
    TestBlocks = gen_test_blocks(BlocksNumber),
    lists:foreach(fun(MaxHole) ->
        application:set_env(?APP_NAME, overlapping_seqiences_max_hole, MaxHole),
        lists:foreach(fun(M) ->
            lists:foreach(fun(N) ->
                BasicBlocksList = lists:sublist(TestBlocks, M, N - M + 1),
                InputBlocks = InputBlocksProducer(BasicBlocksList),
                ExpectedBlocks = ExpectedBlocksProducer(BasicBlocksList, InputBlocks, MaxHole),
                ?assertEqual(ExpectedBlocks, fslogic_location_cache:get_overlapping_blocks_sequence(?KEY, InputBlocks))
            end, lists:seq(M + 1, BlockToStop))
        end, lists:seq(BlockToStart, BlockToStop - 1))
    end, MaxHoles).

get_overlapping_sequences_using_three_blocks_test() ->
    TestBlocks = gen_test_blocks(12),
    Block1 = lists:nth(2, TestBlocks),
    Block2 = lists:nth(6, TestBlocks),
    Block3 = lists:nth(11, TestBlocks),
    InputBlocks = [Block1, Block2, Block3],

    lists:foreach(fun(MaxHole) ->
        application:set_env(?APP_NAME, overlapping_seqiences_max_hole, MaxHole),
        ExpectedBlocks = case {MaxHole < 3, MaxHole < 4} of
            {true, true} ->
                [{[Block1], [Block1]}, {[Block2], [Block2]}, {[Block3], [Block3]}];
            {false, true} ->
                [{[Block1, Block2], lists:sublist(TestBlocks, 2, 5)}, {[Block3], [Block3]}];
            _ ->
                [{InputBlocks, lists:sublist(TestBlocks, 2, 10)}]
        end,
        ?assertEqual(ExpectedBlocks, fslogic_location_cache:get_overlapping_blocks_sequence(?KEY, InputBlocks))
    end, [0, 1, 2, 3, 4, 5]).

get_overlapping_sequences_using_multiple_blocks_test() ->
    TestBlocks = gen_test_blocks(30),
    MaxHole = 3,
    application:set_env(?APP_NAME, overlapping_seqiences_max_hole, MaxHole),

    Block1 = lists:nth(3, TestBlocks),
    Block2 = lists:nth(8, TestBlocks),
    Block3 = lists:nth(10, TestBlocks),
    Block4 = lists:nth(13, TestBlocks),
    Block5 = lists:nth(15, TestBlocks),
    Block6 = lists:nth(21, TestBlocks),
    Block7 = lists:nth(22, TestBlocks),
    Block8 = lists:nth(25, TestBlocks),
    Block9 = lists:nth(30, TestBlocks),

    InputBlock1 = ?BLOCK(Block1#file_block.offset + 3),
    InputBlock3 = ?BLOCK(Block3#file_block.offset - 2),
    InputBlock5 = ?BLOCK(Block5#file_block.offset - 1, 1),
    InputBlock9 = ?BLOCK(Block9#file_block.offset + 3),
    InputBlocks = [InputBlock1, Block2, InputBlock3, Block4, InputBlock5, Block6, Block7, Block8, InputBlock9],

    ExpectedAns = [{[InputBlock1], [Block1]},
        {[Block2, InputBlock3, Block4, InputBlock5], lists:sublist(TestBlocks, 8, 8)},
        {[Block6, Block7, Block8], lists:sublist(TestBlocks, 21, 5)},
        {[InputBlock9], [Block9]}],

    ?assertEqual(ExpectedAns, fslogic_location_cache:get_overlapping_blocks_sequence(?KEY, InputBlocks)).

%%%===================================================================
%%% Test functions - update
%%%===================================================================

get_overlapping_and_update_test() ->
    TestBlocks = gen_test_blocks(7),
    [_ | BlocksToMap] = BlocksToChange = lists:sublist(TestBlocks, 2, 5),
    InputBlocks = lists:map(fun(#file_block{offset = Offset}) -> ?BLOCK(Offset - 7, 7) end, BlocksToMap),
    verify_blocks_in_use(undefined),
    verify_changed_blocks([], []),

    UpdatedBlock = ?BLOCK(20, 43),
    FinalBlocks = [lists:nth(1, TestBlocks), UpdatedBlock, lists:nth(7, TestBlocks)],
    ?assertEqual(FinalBlocks, fslogic_blocks:merge(TestBlocks, InputBlocks)),
    check_overlapping_blocks_update(InputBlocks, BlocksToChange, [UpdatedBlock], FinalBlocks).


get_overlapping_and_double_update_test() ->
    TestBlocks = gen_test_blocks(8),
    [_ | BlocksToMap] = BlocksToChange = lists:sublist(TestBlocks, 2, 6),
    InputBlocks = lists:map(fun(#file_block{offset = Offset}) -> ?BLOCK(Offset - 7, 7) end, BlocksToMap),
    verify_blocks_in_use(undefined),
    verify_changed_blocks([], []),

    Input1 = lists:sublist(InputBlocks, 2) ++ lists:sublist(InputBlocks, 4, 2),
    UpdatedBlocks = [?BLOCK(20, 23), ?BLOCK(50, 23)],
    FinalBlocks = [lists:nth(1, TestBlocks)] ++ UpdatedBlocks ++ [lists:nth(8, TestBlocks)],
    ?assertEqual(FinalBlocks, fslogic_blocks:merge(TestBlocks, Input1)),
    check_overlapping_blocks_update(Input1, BlocksToChange, UpdatedBlocks, FinalBlocks),

    Input2 = [lists:nth(3, InputBlocks)],
    UpdatedBlock = ?BLOCK(20, 53),
    FinalBlocks2 = [lists:nth(1, TestBlocks), UpdatedBlock, lists:nth(8, TestBlocks)],
    ?assertEqual(FinalBlocks2, fslogic_blocks:merge(FinalBlocks, Input2)),
    check_overlapping_blocks_update(Input2, UpdatedBlocks, [UpdatedBlock],
        FinalBlocks2, [UpdatedBlock], BlocksToChange).

get_overlapping_and_triple_update_test() ->
    TestBlocks = gen_test_blocks(8),
    [_ | BlocksToMap] = BlocksToChange = lists:sublist(TestBlocks, 2, 6),
    InputBlocks = lists:map(fun(#file_block{offset = Offset}) -> ?BLOCK(Offset - 7, 7) end, BlocksToMap),
    verify_changed_blocks([], []),
    verify_blocks_in_use(undefined),

    Input1 = lists:sublist(InputBlocks, 2),
    UpdatedBlock1 = ?BLOCK(20, 23),
    FinalBlocks = [lists:nth(1, TestBlocks), UpdatedBlock1] ++ lists:sublist(TestBlocks, 5, 4),
    ?assertEqual(FinalBlocks, fslogic_blocks:merge(TestBlocks, Input1)),
    ExpectedGetResult = lists:sublist(BlocksToChange, 3),
    check_overlapping_blocks_update(Input1, ExpectedGetResult, [UpdatedBlock1], FinalBlocks),

    Input2 = lists:sublist(InputBlocks, 4, 2),
    UpdatedBlock2 = ?BLOCK(50, 23),
    FinalBlocks2 = [lists:nth(1, TestBlocks), UpdatedBlock1, UpdatedBlock2, lists:nth(8, TestBlocks)],
    ?assertEqual(FinalBlocks2, fslogic_blocks:merge(FinalBlocks, Input2)),
    check_overlapping_blocks_update(Input2, lists:sublist(BlocksToChange, 4, 3), [UpdatedBlock2],
        FinalBlocks2, [UpdatedBlock1, UpdatedBlock2], BlocksToChange),

    Input3 = [lists:nth(3, InputBlocks)],
    UpdatedBlock3 = ?BLOCK(20, 53),
    FinalBlocks3 = [lists:nth(1, TestBlocks), UpdatedBlock3, lists:nth(8, TestBlocks)],
    ?assertEqual(FinalBlocks3, fslogic_blocks:merge(FinalBlocks2, Input3)),
    check_overlapping_blocks_update(Input3, [UpdatedBlock1, UpdatedBlock2], [UpdatedBlock3],
        FinalBlocks3, [UpdatedBlock3], BlocksToChange).

get_overlapping_sequences_and_update_test() ->
    TestBlocks = gen_test_blocks(25),
    MaxHole = 3,
    application:set_env(?APP_NAME, overlapping_seqiences_max_hole, MaxHole),

    Block1 = lists:nth(3, TestBlocks),
    Block2 = lists:nth(8, TestBlocks),
    Block3 = lists:nth(10, TestBlocks),
    Block4 = lists:nth(13, TestBlocks),
    Block5 = lists:nth(15, TestBlocks),
    Block6 = lists:nth(20, TestBlocks),

    InputBlock1 = ?BLOCK(Block1#file_block.offset + 3),
    InputBlock3 = ?BLOCK(Block3#file_block.offset - 2),
    InputBlock5 = ?BLOCK(Block5#file_block.offset - 1, 1),
    InputBlocks = [InputBlock1, Block2, InputBlock3, Block4, InputBlock5, Block6],

    InputTestList = [Block2, InputBlock3, Block4, InputBlock5],
    ExpectedAns = [{[InputBlock1], [Block1]},
        {InputTestList, lists:sublist(TestBlocks, 8, 8)},
        {[Block6], [Block6]}],

    verify_blocks_in_use(undefined),
    verify_changed_blocks([], []),
    ?assertEqual(ExpectedAns, fslogic_location_cache:get_overlapping_blocks_sequence(?KEY, InputBlocks)),
    verify_blocks_in_use([[Block1], lists:sublist(TestBlocks, 8, 8), [Block6]]),

    UpdatedBlock1 = ?BLOCK(Block1#file_block.offset, 6),
    ?assertMatch(#document{},
        fslogic_location_cache:update_blocks(#document{key = ?KEY}, [UpdatedBlock1])),
    NewBlocks = lists:sublist(TestBlocks, 1, 2) ++ [UpdatedBlock1 | lists:sublist(TestBlocks, 4, 22)],
    ?assertEqual(NewBlocks, fslogic_blocks:merge(TestBlocks, [InputBlock1])),
    ?assertEqual(NewBlocks, fslogic_location_cache:get_blocks(?KEY, #{})),
    verify_changed_blocks([UpdatedBlock1], [Block1]),
    verify_blocks_in_use([lists:sublist(TestBlocks, 8, 8), [Block6]]),

    UpdatedBlock2 = ?BLOCK(InputBlock3#file_block.offset, 5),
    UpdatedBlock3 = ?BLOCK(InputBlock5#file_block.offset, 4),
    ?assertMatch(#document{},
        fslogic_location_cache:update_blocks(#document{key = ?KEY}, lists:sublist(TestBlocks, 8, 2) ++
        [UpdatedBlock2 | lists:sublist(TestBlocks, 11, 4)] ++ [UpdatedBlock3])),
    NewBlocks2 = lists:sublist(TestBlocks, 1, 2) ++ [UpdatedBlock1 | lists:sublist(TestBlocks, 4, 6)]
        ++ [UpdatedBlock2 | lists:sublist(TestBlocks, 11, 4)] ++ [UpdatedBlock3 | lists:sublist(TestBlocks, 16, 10)],
    ?assertEqual(NewBlocks2, fslogic_blocks:merge(NewBlocks, InputTestList)),
    ?assertEqual(NewBlocks2, fslogic_location_cache:get_blocks(?KEY, #{})),
    verify_changed_blocks([UpdatedBlock1, UpdatedBlock2, UpdatedBlock3], [Block1, Block3, Block5]),
    verify_blocks_in_use([[Block6]]),

    ?assertMatch(#document{},
        fslogic_location_cache:update_blocks(#document{key = ?KEY}, [Block6])),
    ?assertEqual(NewBlocks2, fslogic_location_cache:get_blocks(?KEY, #{})),
    verify_changed_blocks([UpdatedBlock1, UpdatedBlock2, UpdatedBlock3], [Block1, Block3, Block5]),
    verify_blocks_in_use(undefined).

%%%===================================================================
%%% Helper functions
%%%===================================================================

gen_test_blocks(N) ->
    erase({fslogic_cache_saved_blocks, ?KEY}),
    erase({fslogic_cache_deleted_blocks, ?KEY}),
    erase({fslogic_cache_blocks_in_use, ?KEY}),
    put(fslogic_cache, ?KEY),
    put(fslogic_cache_modified_blocks_keys, []),

    ReversedBlocks = gen_blocks_reversted_list(N),
    Blocks = lists:reverse(ReversedBlocks),
    ?assertEqual(ok, fslogic_cache:cache_blocks(?KEY, Blocks)),
    Blocks.

gen_blocks_reversted_list(0) ->
    [];
gen_blocks_reversted_list(N) ->
    [?BLOCK(10 * N) | gen_blocks_reversted_list(N - 1)].

check_overlapping_blocks_update(InputBlocks, ExpectedGetResult, BlocksToUpdate, FinalBlocks) ->
    check_overlapping_blocks_update(InputBlocks, ExpectedGetResult, BlocksToUpdate, FinalBlocks, BlocksToUpdate, ExpectedGetResult).

check_overlapping_blocks_update(InputBlocks, ExpectedGetResult, BlocksToUpdate, FinalBlocks, ExpectedBlocksToSave, OverridenBlocks) ->
    ?assertEqual(ExpectedGetResult, fslogic_location_cache:get_blocks(?KEY, #{overlapping_blocks => InputBlocks})),
    verify_blocks_in_use([ExpectedGetResult]),
    ?assertMatch(#document{}, fslogic_location_cache:update_blocks(#document{key = ?KEY}, BlocksToUpdate)),
    ?assertEqual(FinalBlocks, fslogic_location_cache:get_blocks(?KEY, #{})),
    verify_changed_blocks(ExpectedBlocksToSave, OverridenBlocks),
    verify_blocks_in_use(undefined).

verify_changed_blocks(ExpectedSaved, ExpectedDeleted) ->
    ChangedBlocks = fslogic_cache:get_changed_blocks(?KEY),
    ?assertMatch({_, _}, ChangedBlocks),
    {Saved, Deleted} = ChangedBlocks,
    ?assertEqual(ExpectedSaved, lists:sort(sets:to_list(Saved))),
    ?assertEqual(ExpectedDeleted, lists:sort(sets:to_list(Deleted))).

verify_blocks_in_use(Expected) ->
    ?assertEqual(Expected, get({fslogic_cache_blocks_in_use, ?KEY})).

-endif.