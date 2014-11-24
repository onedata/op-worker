%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module tests the functionality of rt_map module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(rt_map_tests).

-ifdef(TEST).

-include("registered_names.hrl").
-include("oneprovider_modules/rtransfer/rt_container.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_MAP, test_map).

%% ===================================================================
%% Tests description
%% ===================================================================

rt_map_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            {"should push single block", fun should_push_single_block/0},
            {"should push many blocks", fun should_push_many_blocks/0},
            {"should merge blocks", fun should_merge_blocks/0},
            {"should remove blocks", fun should_remove_blocks/0}
        ]
    }.

%% ===================================================================
%% Setup/teardown functions
%% ===================================================================

setup() ->
    application:set_env(?APP_Name, rt_nif_prefix, "../c_lib"),
    {ok, _} = rt_map:new({local, ?TEST_MAP}).

teardown(_) ->
    ok = rt_map:delete(?TEST_MAP),
    application:set_env(?APP_Name, rt_nif_prefix, "c_lib").

%% ===================================================================
%% Tests functions
%% ===================================================================

should_push_single_block() ->
    FileId = "test_file",
    Offset = 0,
    Size = 10,
    Block = #rt_block{file_id = FileId, offset = Offset, size = Size, priority = 2},

    ?assertEqual(ok, rt_map:put(?TEST_MAP, Block)),
    ?assertEqual({ok, [Block]}, rt_map:get(?TEST_MAP, FileId, Offset, Size)),
    ?assertEqual({ok, []}, rt_map:get(?TEST_MAP, FileId, Size, Size)).

should_push_many_blocks() ->
    FileId = "test_file",
    BlocksAmount = 10000,
    Block = #rt_block{file_id = FileId, size = 1, priority = 2},

    lists:foreach(fun(N) ->
        ?assertEqual(ok, rt_map:put(?TEST_MAP, Block#rt_block{offset = N}))
    end, lists:seq(0, BlocksAmount)),
    ?assertEqual({ok, [Block#rt_block{size = BlocksAmount}]}, rt_map:get(?TEST_MAP, FileId, 0, BlocksAmount)).

should_merge_blocks() ->
    FileId = "test_file",
    Block1 = #rt_block{file_id = FileId, offset = 0, size = 4, priority = 2},
    Block2 = #rt_block{file_id = FileId, offset = 6, size = 4, priority = 2},
    Block3 = #rt_block{file_id = FileId, offset = 3, size = 4, priority = 2},

    ?assertEqual(ok, rt_map:put(?TEST_MAP, Block1)),
    ?assertEqual(ok, rt_map:put(?TEST_MAP, Block2)),
    ?assertEqual(ok, rt_map:put(?TEST_MAP, Block3)),
    ?assertEqual({ok, [Block1#rt_block{offset = 2, size = 6}]}, rt_map:get(?TEST_MAP, FileId, 2, 6)).

should_remove_blocks() ->
    FileId = "test_file",
    Block = #rt_block{file_id = FileId, offset = 0, size = 10, priority = 2},

    ?assertEqual(ok, rt_map:put(?TEST_MAP, Block)),
    ?assertEqual(ok, rt_map:remove(?TEST_MAP, FileId, 3, 3)),
    ?assertEqual({ok, [
        Block#rt_block{offset = 0, size = 3},
        Block#rt_block{offset = 6, size = 4}
    ]}, rt_map:get(?TEST_MAP, FileId, 0, 10)).

-endif.