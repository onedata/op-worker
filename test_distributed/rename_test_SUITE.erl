%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of rename
%%% @end
%%%-------------------------------------------------------------------
-module(rename_test_SUITE).
-author("Mateusz Paciorek").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    rename_file_test/1,
    move_file_test/1,
    move_file_interspace_test/1,
    move_file_interprovider_test/1,
    rename_dir_test/1,
    move_dir_test/1,
    move_dir_interspace_test/1,
    move_dir_interprovider_test/1,
    attributes_retaining_test/1
]).

all() ->
    ?ALL([
        rename_file_test,
        move_file_test,
        move_file_interspace_test,
        move_file_interprovider_test,
        rename_dir_test,
        move_dir_test,
        move_dir_interspace_test,
        move_dir_interprovider_test,
        attributes_retaining_test
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

rename_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, 1}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t1">>)),
    {_, File1Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t1/renamed_file1">>, 8#770)),
    {_, File2Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t1/renamed_file2">>, 8#770)),
    {_, File3Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t1/renamed_file3">>, 8#770)),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {uuid, File1Uuid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {uuid, File2Uuid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t1/renamed_file1_target">>, 8#770)),
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {uuid, File1Uuid}, <<"/spaces/space_name1/t1/renamed_file1_target">>)),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, <<"/spaces/space_name1/t1/renamed_file1_target">>}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {uuid, File2Uuid}, <<"/spaces/space_name1/t1/renamed_file2_target">>)),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, <<"/spaces/space_name1/t1/renamed_file2_target">>}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t1/renamed_file3_target">>)),
    ?assertEqual({error, ?EISDIR}, lfm_proxy:mv(W, SessId, {uuid, File3Uuid}, <<"/spaces/space_name1/t1/renamed_file3_target">>)),
    
    ok.

move_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, 1}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t2">>)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t2/target_dir">>)),
    {_, File1Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t2/moved_file1">>, 8#770)),
    {_, File2Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t2/moved_file2">>, 8#770)),
    {_, File3Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t2/moved_file3">>, 8#770)),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {uuid, File1Uuid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {uuid, File2Uuid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t2/target_dir/moved_file1_target">>, 8#770)),
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {uuid, File1Uuid}, <<"/spaces/space_name1/t2/target_dir/moved_file1_target">>)),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, <<"/spaces/space_name1/t2/target_dir/moved_file1_target">>}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {uuid, File2Uuid}, <<"/spaces/space_name1/t2/target_dir/moved_file2_target">>)),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, <<"/spaces/space_name1/t2/target_dir/moved_file2_target">>}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t2/target_dir/moved_file3_target">>)),
    ?assertEqual({error, ?EISDIR}, lfm_proxy:mv(W, SessId, {uuid, File3Uuid}, <<"/spaces/space_name1/t2/target_dir/moved_file3_target">>)),

    ok.

move_file_interspace_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, 1}, Config),

    test_utils:mock_expect(W, oz_spaces, get_providers,
        fun
            (provider, _SpaceId) ->
                {ok, [oneprovider:get_provider_id()]};
            (_Client, _SpaceId) ->
                meck:passthrough([_Client, _SpaceId])
        end),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name2/t3">>)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name2/t3/target_dir">>)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t3">>)),
    {_, File1Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t3/moved_file1">>, 8#770)),
    {_, File2Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t3/moved_file2">>, 8#770)),
    {_, File3Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t3/moved_file3">>, 8#770)),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {uuid, File1Uuid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {uuid, File2Uuid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name2/t3/target_dir/moved_file1_target">>, 8#770)),
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {uuid, File1Uuid}, <<"/spaces/space_name2/t3/target_dir/moved_file1_target">>)),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, <<"/spaces/space_name2/t3/target_dir/moved_file1_target">>}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {uuid, File2Uuid}, <<"/spaces/space_name2/t3/target_dir/moved_file2_target">>)),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, <<"/spaces/space_name2/t3/target_dir/moved_file2_target">>}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name2/t3/target_dir/moved_file3_target">>)),
    ?assertEqual({error, ?EISDIR}, lfm_proxy:mv(W, SessId, {uuid, File3Uuid}, <<"/spaces/space_name2/t3/target_dir/moved_file3_target">>)),

    ok.

move_file_interprovider_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, 1}, Config),

    test_utils:mock_expect([W1, W2], oz_spaces, get_providers,
        fun
            (provider, <<"space_id1">>) ->
                {ok, [rpc:call(W1, oneprovider, get_provider_id, [])]};
            (provider, <<"space_id2">>) ->
                {ok, [rpc:call(W2, oneprovider, get_provider_id, [])]};
            (_Client, _SpaceId) ->
                meck:passthrough([_Client, _SpaceId])
        end),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, <<"/spaces/space_name2/t4">>)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, <<"/spaces/space_name2/t4/target_dir">>)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/spaces/space_name1/t4">>)),
    {_, File1Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, <<"/spaces/space_name1/t4/moved_file1">>, 8#770)),
    {_, File2Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, <<"/spaces/space_name1/t4/moved_file2">>, 8#770)),
    {_, File3Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, <<"/spaces/space_name1/t4/moved_file3">>, 8#770)),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {uuid, File1Uuid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {uuid, File2Uuid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W1, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W1, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, <<"/spaces/space_name2/t4/target_dir/moved_file1_target">>, 8#770)),
    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {uuid, File1Uuid}, <<"/spaces/space_name2/t4/target_dir/moved_file1_target">>)),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {path, <<"/spaces/space_name2/t4/target_dir/moved_file1_target">>}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W1, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {uuid, File2Uuid}, <<"/spaces/space_name2/t4/target_dir/moved_file2_target">>)),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {path, <<"/spaces/space_name2/t4/target_dir/moved_file2_target">>}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W1, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/spaces/space_name2/t4/target_dir/moved_file3_target">>)),
    ?assertEqual({error, ?EISDIR}, lfm_proxy:mv(W1, SessId, {uuid, File3Uuid}, <<"/spaces/space_name2/t4/target_dir/moved_file3_target">>)),

    ok.

rename_dir_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, 1}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t5">>)),
    {_, Dir1Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t5/renamed_dir1">>)),
    {_, File1Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t5/renamed_dir1/inner_file1">>, 8#770)),
    {_, Dir2Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t5/renamed_dir2">>)),
    {_, File2Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t5/renamed_dir2/inner_file2">>, 8#770)),
    {_, Dir3Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t5/renamed_dir3">>)),
    {_, Dir4Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t5/renamed_dir4">>)),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {uuid, File1Uuid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {uuid, File2Uuid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t5/renamed_dir1_target">>)),
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {uuid, Dir1Uuid}, <<"/spaces/space_name1/t5/renamed_dir1_target">>)),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, <<"/spaces/space_name1/t5/renamed_dir1_target/inner_file1">>}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {uuid, Dir2Uuid}, <<"/spaces/space_name1/t5/renamed_dir2_target">>)),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, <<"/spaces/space_name1/t5/renamed_dir2_target/inner_file2">>}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t5/renamed_dir3_target">>, 8#770)),
    ?assertEqual({error, ?ENOTDIR}, lfm_proxy:mv(W, SessId, {uuid, Dir3Uuid}, <<"/spaces/space_name1/t5/renamed_dir3_target">>)),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t5/renamed_dir4_target">>)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t5/renamed_dir4_target/some_file">>, 8#770)),
    ?assertEqual({error, ?ENOTEMPTY}, lfm_proxy:mv(W, SessId, {uuid, Dir4Uuid}, <<"/spaces/space_name1/t5/renamed_dir4_target">>)),

    ok.

move_dir_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, 1}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t6">>)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t6/target_dir">>)),
    {_, Dir1Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t6/moved_dir1">>)),
    {_, File1Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t6/moved_dir1/inner_file1">>, 8#770)),
    {_, Dir2Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t6/moved_dir2">>)),
    {_, File2Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t6/moved_dir2/inner_file2">>, 8#770)),
    {_, Dir3Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t6/moved_dir3">>)),
    {_, Dir4Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t6/moved_dir4">>)),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {uuid, File1Uuid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {uuid, File2Uuid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t6/target_dir/moved_dir1_target">>)),
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {uuid, Dir1Uuid}, <<"/spaces/space_name1/t6/target_dir/moved_dir1_target">>)),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, <<"/spaces/space_name1/t6/target_dir/moved_dir1_target/inner_file1">>}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {uuid, Dir2Uuid}, <<"/spaces/space_name1/t6/target_dir/moved_dir2_target">>)),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, <<"/spaces/space_name1/t6/target_dir/moved_dir2_target/inner_file2">>}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t6/target_dir/moved_dir3_target">>, 8#770)),
    ?assertEqual({error, ?ENOTDIR}, lfm_proxy:mv(W, SessId, {uuid, Dir3Uuid}, <<"/spaces/space_name1/t6/target_dir/moved_dir3_target">>)),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t6/target_dir/moved_dir4_target">>)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t6/target_dir/moved_dir4_target/some_file">>, 8#770)),
    ?assertEqual({error, ?ENOTEMPTY}, lfm_proxy:mv(W, SessId, {uuid, Dir4Uuid}, <<"/spaces/space_name1/t6/target_dir/moved_dir4_target">>)),

    ok.

move_dir_interspace_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, 1}, Config),

    test_utils:mock_expect(W, oz_spaces, get_providers,
        fun
            (provider, _SpaceId) ->
                {ok, [oneprovider:get_provider_id()]};
            (_Client, _SpaceId) ->
                meck:passthrough([_Client, _SpaceId])
        end),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name2/t7">>)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name2/t7/target_dir">>)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t7">>)),
    {_, Dir1Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t7/moved_dir1">>)),
    {_, File1Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t7/moved_dir1/inner_file1">>, 8#770)),
    {_, Dir2Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t7/moved_dir2">>)),
    {_, File2Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name1/t7/moved_dir2/inner_file2">>, 8#770)),
    {_, Dir3Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t7/moved_dir3">>)),
    {_, Dir4Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name1/t7/moved_dir4">>)),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {uuid, File1Uuid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {uuid, File2Uuid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name2/t7/target_dir/moved_dir1_target">>)),
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {uuid, Dir1Uuid}, <<"/spaces/space_name2/t7/target_dir/moved_dir1_target">>)),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, <<"/spaces/space_name2/t7/target_dir/moved_dir1_target/inner_file1">>}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {uuid, Dir2Uuid}, <<"/spaces/space_name2/t7/target_dir/moved_dir2_target">>)),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, <<"/spaces/space_name2/t7/target_dir/moved_dir2_target/inner_file2">>}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name2/t7/target_dir/moved_dir3_target">>, 8#770)),
    ?assertEqual({error, ?ENOTDIR}, lfm_proxy:mv(W, SessId, {uuid, Dir3Uuid}, <<"/spaces/space_name2/t7/target_dir/moved_dir3_target">>)),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, <<"/spaces/space_name2/t7/target_dir/moved_dir4_target">>)),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, <<"/spaces/space_name2/t7/target_dir/moved_dir4_target/some_file">>, 8#770)),
    ?assertEqual({error, ?ENOTEMPTY}, lfm_proxy:mv(W, SessId, {uuid, Dir4Uuid}, <<"/spaces/space_name2/t7/target_dir/moved_dir4_target">>)),

    ok.

move_dir_interprovider_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, 1}, Config),

    test_utils:mock_expect([W1, W2], oz_spaces, get_providers,
        fun
            (provider, <<"space_id1">>) ->
                {ok, [rpc:call(W1, oneprovider, get_provider_id, [])]};
            (provider, <<"space_id2">>) ->
                {ok, [rpc:call(W2, oneprovider, get_provider_id, [])]};
            (_Client, _SpaceId) ->
                meck:passthrough([_Client, _SpaceId])
        end),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, <<"/spaces/space_name2/t8">>)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, <<"/spaces/space_name2/t8/target_dir">>)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/spaces/space_name1/t8">>)),
    {_, Dir1Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/spaces/space_name1/t8/moved_dir1">>)),
    {_, File1Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, <<"/spaces/space_name1/t8/moved_dir1/inner_file1">>, 8#770)),
    {_, Dir2Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/spaces/space_name1/t8/moved_dir2">>)),
    {_, File2Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, <<"/spaces/space_name1/t8/moved_dir2/inner_file2">>, 8#770)),
    {_, Dir3Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/spaces/space_name1/t8/moved_dir3">>)),
    {_, Dir4Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/spaces/space_name1/t8/moved_dir4">>)),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {uuid, File1Uuid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {uuid, File2Uuid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W1, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W1, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/spaces/space_name2/t8/target_dir/moved_dir1_target">>)),
    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {uuid, Dir1Uuid}, <<"/spaces/space_name2/t8/target_dir/moved_dir1_target">>)),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {path, <<"/spaces/space_name2/t8/target_dir/moved_dir1_target/inner_file1">>}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W1, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {uuid, Dir2Uuid}, <<"/spaces/space_name2/t8/target_dir/moved_dir2_target">>)),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {path, <<"/spaces/space_name2/t8/target_dir/moved_dir2_target/inner_file2">>}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W1, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, <<"/spaces/space_name2/t8/target_dir/moved_dir3_target">>, 8#770)),
    ?assertEqual({error, ?ENOTDIR}, lfm_proxy:mv(W1, SessId, {uuid, Dir3Uuid}, <<"/spaces/space_name2/t8/target_dir/moved_dir3_target">>)),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/spaces/space_name2/t8/target_dir/moved_dir4_target">>)),
    ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, <<"/spaces/space_name2/t8/target_dir/moved_dir4_target/some_file">>, 8#770)),
    ?assertEqual({error, ?ENOTEMPTY}, lfm_proxy:mv(W1, SessId, {uuid, Dir4Uuid}, <<"/spaces/space_name2/t8/target_dir/moved_dir4_target">>)),

    ok.

attributes_retaining_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, 1}, Config),
    UserId = ?config({user_id, 1}, Config),

    test_utils:mock_expect([W1, W2], oz_spaces, get_providers,
        fun
            (provider, <<"space_id1">>) ->
                {ok, [rpc:call(W1, oneprovider, get_provider_id, [])]};
            (provider, <<"space_id2">>) ->
                {ok, [rpc:call(W1, oneprovider, get_provider_id, [])]};
            (provider, <<"space_id3">>) ->
                {ok, [rpc:call(W2, oneprovider, get_provider_id, [])]};
            (_Client, _SpaceId) ->
                meck:passthrough([_Client, _SpaceId])
        end),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/spaces/space_name1/t9">>)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/spaces/space_name2/t9">>)),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, <<"/spaces/space_name3/t9">>)),
    {_, Dir1Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/spaces/space_name1/t9/dir1">>)),
    {_, File1Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, <<"/spaces/space_name1/t9/dir1/file1">>, 8#770)),
    {_, Dir2Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/spaces/space_name1/t9/dir2">>)),
    {_, File2Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, <<"/spaces/space_name1/t9/dir2/file2">>, 8#770)),
    {_, Dir3Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/spaces/space_name1/t9/dir3">>)),
    {_, File3Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, <<"/spaces/space_name1/t9/dir3/file3">>, 8#770)),
    
    PreRenameUuids = [Dir1Uuid, File1Uuid, Dir2Uuid, File2Uuid, Dir3Uuid, File3Uuid],

    Ace = #accesscontrolentity{
        acetype = ?allow_mask,
        aceflags = ?no_flags_mask,
        identifier = UserId,
        acemask = (?read_mask bor ?write_mask bor ?execute_mask)
    },
    Mimetype = <<"text/html">>,
    TransferEncoding = <<"base64">>,
    CompletionStatus = <<"Completed">>,
    Xattrs = [
        #xattr{name = <<"xattr_name1">>, value = <<"xattr1">>},
        #xattr{name = <<"xattr_name2">>, value = <<"xattr2">>}
    ],

    lists:foreach(
      fun(Uuid) ->
          ?assertEqual(ok, lfm_proxy:set_acl(W1, SessId, {uuid, Uuid}, [Ace])),
          ?assertEqual(ok, lfm_proxy:set_mimetype(W1, SessId, {uuid, Uuid}, Mimetype)),
          ?assertEqual(ok, lfm_proxy:set_transfer_encoding(W1, SessId, {uuid, Uuid}, TransferEncoding)),
          ?assertEqual(ok, lfm_proxy:set_cdmi_completion_status(W1, SessId, {uuid, Uuid}, CompletionStatus)),
          lists:foreach(
              fun(Xattr) ->
                  ?assertEqual(ok, lfm_proxy:set_xattr(W1, SessId, {uuid, Uuid}, Xattr))
              end, Xattrs)
      end, PreRenameUuids),

    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {uuid, Dir1Uuid}, <<"/spaces/space_name1/t9/dir1_target">>)),
    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {uuid, Dir2Uuid}, <<"/spaces/space_name2/t9/dir2_target">>)),
    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {uuid, Dir3Uuid}, <<"/spaces/space_name3/t9/dir3_target">>)),

    PostRenamePaths = [
        <<"/spaces/space_name1/t9/dir1_target">>,
        <<"/spaces/space_name1/t9/dir1_target/file1">>,
        <<"/spaces/space_name2/t9/dir2_target">>,
        <<"/spaces/space_name2/t9/dir2_target/file2">>,
        <<"/spaces/space_name3/t9/dir3_target">>,
        <<"/spaces/space_name3/t9/dir3_target/file3">>
    ],

    lists:foreach(
        fun(Path) ->
            ?assertEqual({ok, [Ace]}, lfm_proxy:get_acl(W1, SessId, {path, Path})),
            ?assertEqual({ok, Mimetype}, lfm_proxy:get_mimetype(W1, SessId, {path, Path})),
            ?assertEqual({ok, TransferEncoding}, lfm_proxy:get_transfer_encoding(W1, SessId, {path, Path})),
            ?assertEqual({ok, CompletionStatus}, lfm_proxy:get_cdmi_completion_status(W1, SessId, {path, Path})),
            lists:foreach(
                fun(#xattr{name = XattrName} = Xattr) ->
                    ?assertEqual({ok, Xattr}, lfm_proxy:get_xattr(W1, SessId, {path, Path}, XattrName))
                end, Xattrs)
        end, PostRenamePaths),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================
