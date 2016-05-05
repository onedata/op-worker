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
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-define(FAILURE_RETURN_VALUE, deliberate_failure).
-define(LINK_FAILURE_SUFFIX, "_with_failing_link").
-define(LINK_AND_MV_FAILURE_SUFFIX, "_with_failing_link_and_mv").

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    rename_file_test/1,
    rename_file_test_with_failing_link/1,
    rename_file_test_with_failing_link_and_mv/1,
    move_file_test/1,
    move_file_test_with_failing_link/1,
    move_file_test_with_failing_link_and_mv/1,
    move_file_interspace_test/1,
    move_file_interspace_test_with_failing_link/1,
    move_file_interspace_test_with_failing_link_and_mv/1,
    move_file_interprovider_test/1,
    rename_dir_test/1,
    rename_dir_test_with_failing_link/1,
    rename_dir_test_with_failing_link_and_mv/1,
    move_dir_test/1,
    move_dir_test_with_failing_link/1,
    move_dir_test_with_failing_link_and_mv/1,
    move_dir_interspace_test/1,
    move_dir_interspace_test_with_failing_link/1,
    move_dir_interspace_test_with_failing_link_and_mv/1,
    move_dir_interprovider_test/1,
    attributes_retaining_test/1,
    attributes_retaining_test_with_failing_link/1,
    attributes_retaining_test_with_failing_link_and_mv/1,
    times_update_test/1,
    moving_dir_into_itself_test/1]).

all() ->
    ?ALL([
        rename_file_test,
        rename_file_test_with_failing_link,
        rename_file_test_with_failing_link_and_mv,
        move_file_test,
        move_file_test_with_failing_link,
        move_file_test_with_failing_link_and_mv,
        move_file_interspace_test,
        move_file_interspace_test_with_failing_link,
        move_file_interspace_test_with_failing_link_and_mv,
        move_file_interprovider_test,
        rename_dir_test,
        rename_dir_test_with_failing_link,
        rename_dir_test_with_failing_link_and_mv,
        move_dir_test,
        move_dir_test_with_failing_link,
        move_dir_test_with_failing_link_and_mv,
        move_dir_interspace_test,
        move_dir_interspace_test_with_failing_link,
        move_dir_interspace_test_with_failing_link_and_mv,
        move_dir_interprovider_test,
        attributes_retaining_test,
        attributes_retaining_test_with_failing_link,
        attributes_retaining_test_with_failing_link_and_mv,
        times_update_test,
        moving_dir_into_itself_test
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

rename_file_test(Config) ->
    [W | _] = sorted_workers(Config),
    TestDir = ?config(test_dir, Config),
    SessId = ?config({session_id, <<"user1">>}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, ""))),
    {_, File1Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/renamed_file1"), 8#770)),
    {_, File2Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/renamed_file2"), 8#770)),
    {_, File3Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/renamed_file3"), 8#770)),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {guid, File1Guid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {guid, File2Guid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/renamed_file1_target"), 8#770)),
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {guid, File1Guid}, filename(1, TestDir, "/renamed_file1_target"))),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, filename(1, TestDir, "/renamed_file1_target")}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {guid, File2Guid}, filename(1, TestDir, "/renamed_file2_target"))),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, filename(1, TestDir, "/renamed_file2_target")}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/renamed_file3_target"))),
    ?assertEqual({error, ?EISDIR}, lfm_proxy:mv(W, SessId, {guid, File3Guid}, filename(1, TestDir, "/renamed_file3_target"))),
    
    ok.

rename_file_test_with_failing_link(Config) ->
    rename_file_test(Config).

rename_file_test_with_failing_link_and_mv(Config) ->
    rename_file_test(Config).

move_file_test(Config) ->
    [W | _] = sorted_workers(Config),
    TestDir = ?config(test_dir, Config),
    SessId = ?config({session_id, <<"user1">>}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, ""))),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/target_dir"))),
    {_, File1Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/moved_file1"), 8#770)),
    {_, File2Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/moved_file2"), 8#770)),
    {_, File3Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/moved_file3"), 8#770)),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {guid, File1Guid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {guid, File2Guid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/target_dir/moved_file1_target"), 8#770)),
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {guid, File1Guid}, filename(1, TestDir, "/target_dir/moved_file1_target"))),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, filename(1, TestDir, "/target_dir/moved_file1_target")}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {guid, File2Guid}, filename(1, TestDir, "/target_dir/moved_file2_target"))),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, filename(1, TestDir, "/target_dir/moved_file2_target")}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/target_dir/moved_file3_target"))),
    ?assertEqual({error, ?EISDIR}, lfm_proxy:mv(W, SessId, {guid, File3Guid}, filename(1, TestDir, "/target_dir/moved_file3_target"))),

    ok.

move_file_test_with_failing_link(Config) ->
    move_file_test(Config).

move_file_test_with_failing_link_and_mv(Config) ->
    move_file_test(Config).

move_file_interspace_test(Config) ->
    [W | _] = sorted_workers(Config),
    TestDir = ?config(test_dir, Config),
    SessId = ?config({session_id, <<"user1">>}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(2, TestDir, ""))),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(2, TestDir, "/target_dir"))),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, ""))),
    {_, File1Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/moved_file1"), 8#770)),
    {_, File2Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/moved_file2"), 8#770)),
    {_, File3Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/moved_file3"), 8#770)),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {guid, File1Guid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {guid, File2Guid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(2, TestDir, "/target_dir/moved_file1_target"), 8#770)),
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {guid, File1Guid}, filename(2, TestDir, "/target_dir/moved_file1_target"))),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, filename(2, TestDir, "/target_dir/moved_file1_target")}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {guid, File2Guid}, filename(2, TestDir, "/target_dir/moved_file2_target"))),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, filename(2, TestDir, "/target_dir/moved_file2_target")}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(2, TestDir, "/target_dir/moved_file3_target"))),
    ?assertEqual({error, ?EISDIR}, lfm_proxy:mv(W, SessId, {guid, File3Guid}, filename(2, TestDir, "/target_dir/moved_file3_target"))),

    ok.

move_file_interspace_test_with_failing_link(Config) ->
    move_file_interspace_test(Config).

move_file_interspace_test_with_failing_link_and_mv(Config) ->
    move_file_interspace_test(Config).

move_file_interprovider_test(Config) ->
    [W1, W2] = sorted_workers(Config),
    TestDir = ?config(test_dir, Config),
    SessId = ?config({session_id, <<"user1">>}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, filename(3, TestDir, ""))),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, filename(3, TestDir, "/target_dir"))),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, ""))),
    {_, File1Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, filename(1, TestDir, "/moved_file1"), 8#770)),
    {_, File2Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, filename(1, TestDir, "/moved_file2"), 8#770)),
    {_, File3Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, filename(1, TestDir, "/moved_file3"), 8#770)),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, File1Guid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, File2Guid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W1, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W1, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId, filename(3, TestDir, "/target_dir/moved_file1_target"), 8#770)),
    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {guid, File1Guid}, filename(3, TestDir, "/target_dir/moved_file1_target"))),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId, {path, filename(3, TestDir, "/target_dir/moved_file1_target")}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W2, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {guid, File2Guid}, filename(3, TestDir, "/target_dir/moved_file2_target"))),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId, {path, filename(3, TestDir, "/target_dir/moved_file2_target")}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W2, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, filename(3, TestDir, "/target_dir/moved_file3_target"))),
    ?assertEqual({error, ?EISDIR}, lfm_proxy:mv(W1, SessId, {guid, File3Guid}, filename(3, TestDir, "/target_dir/moved_file3_target"))),

    ok.

rename_dir_test(Config) ->
    [W | _] = sorted_workers(Config),
    TestDir = ?config(test_dir, Config),
    SessId = ?config({session_id, <<"user1">>}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, ""))),
    {_, Dir1Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/renamed_dir1"))),
    {_, File1Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/renamed_dir1/inner_file1"), 8#770)),
    {_, Dir2Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/renamed_dir2"))),
    {_, File2Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/renamed_dir2/inner_file2"), 8#770)),
    {_, Dir3Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/renamed_dir3"))),
    {_, Dir4Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/renamed_dir4"))),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {guid, File1Guid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {guid, File2Guid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/renamed_dir1_target"))),
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {guid, Dir1Guid}, filename(1, TestDir, "/renamed_dir1_target"))),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, filename(1, TestDir, "/renamed_dir1_target/inner_file1")}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {guid, Dir2Guid}, filename(1, TestDir, "/renamed_dir2_target"))),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, filename(1, TestDir, "/renamed_dir2_target/inner_file2")}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/renamed_dir3_target"), 8#770)),
    ?assertEqual({error, ?ENOTDIR}, lfm_proxy:mv(W, SessId, {guid, Dir3Guid}, filename(1, TestDir, "/renamed_dir3_target"))),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/renamed_dir4_target"))),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/renamed_dir4_target/some_file"), 8#770)),
    ?assertEqual({error, ?ENOTEMPTY}, lfm_proxy:mv(W, SessId, {guid, Dir4Guid}, filename(1, TestDir, "/renamed_dir4_target"))),

    ok.

rename_dir_test_with_failing_link(Config) ->
    rename_dir_test(Config).

rename_dir_test_with_failing_link_and_mv(Config) ->
    rename_dir_test(Config).

move_dir_test(Config) ->
    [W | _] = sorted_workers(Config),
    TestDir = ?config(test_dir, Config),
    SessId = ?config({session_id, <<"user1">>}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, ""))),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/target_dir"))),
    {_, Dir1Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/moved_dir1"))),
    {_, File1Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/moved_dir1/inner_file1"), 8#770)),
    {_, Dir2Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/moved_dir2"))),
    {_, File2Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/moved_dir2/inner_file2"), 8#770)),
    {_, Dir3Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/moved_dir3"))),
    {_, Dir4Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/moved_dir4"))),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {guid, File1Guid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {guid, File2Guid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/target_dir/moved_dir1_target"))),
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {guid, Dir1Guid}, filename(1, TestDir, "/target_dir/moved_dir1_target"))),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, filename(1, TestDir, "/target_dir/moved_dir1_target/inner_file1")}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {guid, Dir2Guid}, filename(1, TestDir, "/target_dir/moved_dir2_target"))),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, filename(1, TestDir, "/target_dir/moved_dir2_target/inner_file2")}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/target_dir/moved_dir3_target"), 8#770)),
    ?assertEqual({error, ?ENOTDIR}, lfm_proxy:mv(W, SessId, {guid, Dir3Guid}, filename(1, TestDir, "/target_dir/moved_dir3_target"))),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/target_dir/moved_dir4_target"))),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/target_dir/moved_dir4_target/some_file"), 8#770)),
    ?assertEqual({error, ?ENOTEMPTY}, lfm_proxy:mv(W, SessId, {guid, Dir4Guid}, filename(1, TestDir, "/target_dir/moved_dir4_target"))),

    ok.

move_dir_test_with_failing_link(Config) ->
    move_dir_test(Config).

move_dir_test_with_failing_link_and_mv(Config) ->
    move_dir_test(Config).

move_dir_interspace_test(Config) ->
    [W | _] = sorted_workers(Config),
    TestDir = ?config(test_dir, Config),
    SessId = ?config({session_id, <<"user1">>}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(2, TestDir, ""))),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(2, TestDir, "/target_dir"))),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, ""))),
    {_, Dir1Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/moved_dir1"))),
    {_, File1Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/moved_dir1/inner_file1"), 8#770)),
    {_, Dir2Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/moved_dir2"))),
    {_, File2Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(1, TestDir, "/moved_dir2/inner_file2"), 8#770)),
    {_, Dir3Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/moved_dir3"))),
    {_, Dir4Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/moved_dir4"))),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {guid, File1Guid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {guid, File2Guid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(2, TestDir, "/target_dir/moved_dir1_target"))),
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {guid, Dir1Guid}, filename(2, TestDir, "/target_dir/moved_dir1_target"))),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, filename(2, TestDir, "/target_dir/moved_dir1_target/inner_file1")}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W, SessId, {guid, Dir2Guid}, filename(2, TestDir, "/target_dir/moved_dir2_target"))),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId, {path, filename(2, TestDir, "/target_dir/moved_dir2_target/inner_file2")}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(2, TestDir, "/target_dir/moved_dir3_target"), 8#770)),
    ?assertEqual({error, ?ENOTDIR}, lfm_proxy:mv(W, SessId, {guid, Dir3Guid}, filename(2, TestDir, "/target_dir/moved_dir3_target"))),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(2, TestDir, "/target_dir/moved_dir4_target"))),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, filename(2, TestDir, "/target_dir/moved_dir4_target/some_file"), 8#770)),
    ?assertEqual({error, ?ENOTEMPTY}, lfm_proxy:mv(W, SessId, {guid, Dir4Guid}, filename(2, TestDir, "/target_dir/moved_dir4_target"))),

    ok.

move_dir_interspace_test_with_failing_link(Config) ->
    move_dir_interspace_test(Config).

move_dir_interspace_test_with_failing_link_and_mv(Config) ->
    move_dir_interspace_test(Config).

move_dir_interprovider_test(Config) ->
    [W1, W2] = sorted_workers(Config),
    TestDir = ?config(test_dir, Config),
    SessId = ?config({session_id, <<"user1">>}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, filename(3, TestDir, ""))),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, filename(3, TestDir, "/target_dir"))),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, ""))),
    {_, Dir1Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, "/moved_dir1"))),
    {_, File1Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, filename(1, TestDir, "/moved_dir1/inner_file1"), 8#770)),
    {_, Dir2Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, "/moved_dir2"))),
    {_, File2Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, filename(1, TestDir, "/moved_dir2/inner_file2"), 8#770)),
    {_, Dir3Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, "/moved_dir3"))),
    {_, Dir4Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, "/moved_dir4"))),
    {_, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, File1Guid}, write)),
    {_, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, File2Guid}, write)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W1, Handle1, 0, <<"test1">>)),
    ?assertEqual({ok, 5}, lfm_proxy:write(W1, Handle2, 0, <<"test2">>)),

    %% with overwrite
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, filename(3, TestDir, "/target_dir/moved_dir1_target"))),
    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {guid, Dir1Guid}, filename(3, TestDir, "/target_dir/moved_dir1_target"))),
    {_, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId, {path, filename(3, TestDir, "/target_dir/moved_dir1_target/inner_file1")}, read)),
    ?assertEqual({ok, <<"test1">>}, lfm_proxy:read(W2, Handle3, 0, 10)),

    %% without overwrite
    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {guid, Dir2Guid}, filename(3, TestDir, "/target_dir/moved_dir2_target"))),
    {_, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId, {path, filename(3, TestDir, "/target_dir/moved_dir2_target/inner_file2")}, read)),
    ?assertEqual({ok, <<"test2">>}, lfm_proxy:read(W2, Handle4, 0, 10)),

    %% with illegal overwrite
    ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId, filename(3, TestDir, "/target_dir/moved_dir3_target"), 8#770)),
    ?assertEqual({error, ?ENOTDIR}, lfm_proxy:mv(W1, SessId, {guid, Dir3Guid}, filename(3, TestDir, "/target_dir/moved_dir3_target"))),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, filename(3, TestDir, "/target_dir/moved_dir4_target"))),
    ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId, filename(3, TestDir, "/target_dir/moved_dir4_target/some_file"), 8#770)),
    ?assertEqual({error, ?ENOTEMPTY}, lfm_proxy:mv(W1, SessId, {guid, Dir4Guid}, filename(3, TestDir, "/target_dir/moved_dir4_target"))),

    ok.

attributes_retaining_test(Config) ->
    [W1, W2] = sorted_workers(Config),
    TestDir = ?config(test_dir, Config),
    SessId = ?config({session_id, <<"user1">>}, Config),
    UserId = ?config({user_id, <<"user1">>}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, ""))),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(2, TestDir, ""))),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, filename(3, TestDir, ""))),
    {_, Dir1Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, "/dir1"))),
    {_, File1Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, filename(1, TestDir, "/dir1/file1"), 8#770)),
    {_, Dir2Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, "/dir2"))),
    {_, File2Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, filename(1, TestDir, "/dir2/file2"), 8#770)),
    {_, Dir3Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, "/dir3"))),
    {_, File3Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, filename(1, TestDir, "/dir3/file3"), 8#770)),
    
    PreRenameGuids = [Dir1Guid, File1Guid, Dir2Guid, File2Guid, Dir3Guid, File3Guid],

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
      fun(Guid) ->
          ?assertEqual(ok, lfm_proxy:set_acl(W1, SessId, {guid, Guid}, [Ace])),
          ?assertEqual(ok, lfm_proxy:set_mimetype(W1, SessId, {guid, Guid}, Mimetype)),
          ?assertEqual(ok, lfm_proxy:set_transfer_encoding(W1, SessId, {guid, Guid}, TransferEncoding)),
          ?assertEqual(ok, lfm_proxy:set_cdmi_completion_status(W1, SessId, {guid, Guid}, CompletionStatus)),
          lists:foreach(
              fun(Xattr) ->
                  ?assertEqual(ok, lfm_proxy:set_xattr(W1, SessId, {guid, Guid}, Xattr))
              end, Xattrs)
      end, PreRenameGuids),

    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {guid, Dir1Guid}, filename(1, TestDir, "/dir1_target"))),
    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {guid, Dir2Guid}, filename(2, TestDir, "/dir2_target"))),
    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {guid, Dir3Guid}, filename(3, TestDir, "/dir3_target"))),

    PostRenamePathsAndWorkers = [
        {filename(1, TestDir, "/dir1_target"), W1},
        {filename(1, TestDir, "/dir1_target/file1"), W1},
        {filename(2, TestDir, "/dir2_target"), W1},
        {filename(2, TestDir, "/dir2_target/file2"), W1},
        {filename(3, TestDir, "/dir3_target"), W2},
        {filename(3, TestDir, "/dir3_target/file3"), W2}
    ],

    lists:foreach(
        fun({Path, Worker}) ->
            ?assertEqual({ok, [Ace]}, lfm_proxy:get_acl(Worker, SessId, {path, Path})),
            ?assertEqual({ok, Mimetype}, lfm_proxy:get_mimetype(Worker, SessId, {path, Path})),
            ?assertEqual({ok, TransferEncoding}, lfm_proxy:get_transfer_encoding(Worker, SessId, {path, Path})),
            ?assertEqual({ok, CompletionStatus}, lfm_proxy:get_cdmi_completion_status(Worker, SessId, {path, Path})),
            lists:foreach(
                fun(#xattr{name = XattrName} = Xattr) ->
                    ?assertEqual({ok, Xattr}, lfm_proxy:get_xattr(Worker, SessId, {path, Path}, XattrName))
                end, Xattrs)
        end, PostRenamePathsAndWorkers),

    ok.

attributes_retaining_test_with_failing_link(Config) ->
    attributes_retaining_test(Config).

attributes_retaining_test_with_failing_link_and_mv(Config) ->
    attributes_retaining_test(Config).

times_update_test(Config) ->
    [W1, W2] = sorted_workers(Config),
    TestDir = ?config(test_dir, Config),
    SessId = ?config({session_id, <<"user1">>}, Config),

    {_, SourceParentGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, ""))),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(2, TestDir, ""))),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, filename(3, TestDir, ""))),
    {_, TargetParent1Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, "/target1"))),
    {_, TargetParent2Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(2, TestDir, "/target2"))),
    {_, TargetParent3Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId, filename(3, TestDir, "/target3"))),
    {_, Dir1Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, "/dir1"))),
    {_, InnerFile1Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, filename(1, TestDir, "/dir1/inner_file1"), 8#770)),
    {_, Dir2Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, "/dir2"))),
    {_, InnerFile2Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, filename(1, TestDir, "/dir2/inner_file2"), 8#770)),
    {_, Dir3Guid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, filename(1, TestDir, "/dir3"))),
    {_, InnerFile3Guid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, filename(1, TestDir, "/dir3/inner_file3"), 8#770)),

    ParentGuids = [SourceParentGuid, TargetParent1Guid, TargetParent2Guid, TargetParent3Guid],
    PreRenameDirGuids = [Dir1Guid, Dir2Guid, Dir3Guid],
    PreRenameInnerGuids = [InnerFile1Guid, InnerFile2Guid, InnerFile3Guid],

    PreRenameParentTimes = get_times(W1, SessId, guid, ParentGuids),
    PreRenameDirTimes = get_times(W1, SessId, guid, PreRenameDirGuids),
    PreRenameInnerTimes = get_times(W1, SessId, guid, PreRenameInnerGuids),

    %% ensure time difference
    ct:sleep(timer:seconds(1)),

    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {guid, Dir1Guid}, filename(1, TestDir, "/target1/dir1_target"))),
    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {guid, Dir2Guid}, filename(2, TestDir, "/target2/dir2_target"))),
    ?assertEqual(ok, lfm_proxy:mv(W1, SessId, {guid, Dir3Guid}, filename(3, TestDir, "/target3/dir3_target"))),

    PostRenameDirPaths = [
        filename(1, TestDir, "/target1/dir1_target"),
        filename(2, TestDir, "/target2/dir2_target"),
        filename(3, TestDir, "/target3/dir3_target")
    ],
    PostRenameInnerPaths = [
        filename(1, TestDir, "/target1/dir1_target/inner_file1"),
        filename(2, TestDir, "/target2/dir2_target/inner_file2"),
        filename(3, TestDir, "/target3/dir3_target/inner_file3")
    ],

    PostRenameParentTimes = get_times(W1, SessId, guid, ParentGuids),
    PostRenameDirTimes = get_times(W1, SessId, path, PostRenameDirPaths),
    PostRenameInnerTimes = get_times(W1, SessId, path, PostRenameInnerPaths),

    lists:foreach(
        fun({{PreATime, PreMTime, PreCTime}, {PostATime, PostMTime, PostCTime}}) ->
            ?assert(PreATime =:= PostATime),
            ?assert(PreMTime < PostMTime),
            ?assert(PreCTime < PostCTime)
        end, lists:zip(PreRenameParentTimes, PostRenameParentTimes)),

    lists:foreach(
        fun({{PreATime, PreMTime, PreCTime}, {PostATime, PostMTime, PostCTime}}) ->
            ?assert(PreATime =:= PostATime),
            ?assert(PreMTime =:= PostMTime),
            ?assert(PreCTime < PostCTime)
        end, lists:zip(PreRenameDirTimes, PostRenameDirTimes)),

    lists:foreach(
        fun({{PreATime, PreMTime, PreCTime}, {PostATime, PostMTime, PostCTime}}) ->
            ?assert(PreATime =:= PostATime),
            ?assert(PreMTime =:= PostMTime),
            ?assert(PreCTime =:= PostCTime)
        end, lists:zip(PreRenameInnerTimes, PostRenameInnerTimes)),


    ok.

moving_dir_into_itself_test(Config) ->
    [W | _] = sorted_workers(Config),
    TestDir = ?config(test_dir, Config),
    SessId = ?config({session_id, <<"user1">>}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, ""))),
    {_, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, filename(1, TestDir, "/dir"))),

    ?assertEqual({error, ?EINVAL}, lfm_proxy:mv(W, SessId, {guid, DirGuid}, filename(1, TestDir, "/dir/dir_target"))),
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

init_per_testcase(CaseName, Config) ->
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    initializer:enable_grpca_based_communication(Config),
    NewConfig = lfm_proxy:init(ConfigWithSessionInfo),
    Workers = ?config(op_worker_nodes, NewConfig),

    CaseNameString = atom_to_list(CaseName),
    case lists:suffix(?LINK_AND_MV_FAILURE_SUFFIX, CaseNameString) of
        true ->
            test_utils:mock_new(Workers, storage_file_manager),
            test_utils:mock_expect(Workers, storage_file_manager, link,
                fun(_, _) -> ?FAILURE_RETURN_VALUE end),
            test_utils:mock_expect(Workers, storage_file_manager, mv,
                    fun(_, _) -> ?FAILURE_RETURN_VALUE end);
        false ->
            case lists:suffix(?LINK_FAILURE_SUFFIX, CaseNameString) of
                true ->
                    test_utils:mock_new(Workers, storage_file_manager),
                    test_utils:mock_expect(Workers, storage_file_manager, link,
                        fun(_, _) -> ?FAILURE_RETURN_VALUE end);
                false ->
                    ok
            end
    end,

    CaseNameBinary = list_to_binary(CaseNameString),
    [{test_dir, <<CaseNameBinary/binary, "_dir">>} | NewConfig].

end_per_testcase(CaseName, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    CaseNameString = atom_to_list(CaseName),

    case lists:suffix(?LINK_AND_MV_FAILURE_SUFFIX, CaseNameString) orelse
        lists:suffix(?LINK_FAILURE_SUFFIX, CaseNameString) of
        true ->
            test_utils:mock_unload(Workers, storage_file_manager);
        false ->
            ok
    end,

    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces(Config),
    initializer:disable_grpca_based_communication(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

filename(SpaceNo, TestDir, Suffix) ->
    SpaceNoBinary = integer_to_binary(SpaceNo),
    SuffixBinary = list_to_binary(Suffix),
    <<"/spaces/space", SpaceNoBinary/binary, "/", TestDir/binary, SuffixBinary/binary>>.

get_times(W, SessId, IdType, Ids) ->
    lists:map(
        fun(Id) ->
            {_, Stat} = ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId, {IdType, Id})),
            #file_attr{atime = ATime, mtime = MTime, ctime = CTime} = Stat,
            {ATime, MTime, CTime}
        end, Ids).

sorted_workers(Config) ->
    [W1, W2] = ?config(op_worker_nodes, Config),
    [P1Domain | _] = [V || {K, V} <- ?config(domain_mappings, Config), K =:= p1],
    case ?GET_DOMAIN(W1) of
        P1Domain ->
            [W1, W1];
        _ ->
            [W2, W1]
    end.