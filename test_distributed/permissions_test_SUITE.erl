%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Permissions tests
%%% @end
%%%-------------------------------------------------------------------
-module(permissions_test_SUITE).
-author("Mateusz Paciorek").

-define(acl_all(UserId),
    #accesscontrolentity{
        acetype = ?allow_mask,
        aceflags = ?no_flags_mask,
        identifier = UserId,
        acemask = (?read_mask bor ?write_mask bor ?execute_mask)
    }).

-define(allow_user(UserId, Mask),
    #accesscontrolentity{
        acetype = ?allow_mask,
        aceflags = ?no_flags_mask,
        identifier = UserId,
        acemask = Mask
    }).


-define(deny_user(UserId, Mask),
    #accesscontrolentity{
        acetype = ?deny_mask,
        aceflags = ?no_flags_mask,
        identifier = UserId,
        acemask = Mask
    }).

-define(allow_group(GroupId, Mask),
    #accesscontrolentity{
        acetype = ?allow_mask,
        aceflags = ?identifier_group_mask,
        identifier = GroupId,
        acemask = Mask
    }).


-define(deny_group(GroupId, Mask),
    #accesscontrolentity{
        acetype = ?deny_mask,
        aceflags = ?identifier_group_mask,
        identifier = GroupId,
        acemask = Mask
    }).

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    posix_read_file_user_test/1,
    posix_read_file_group_test/1,
    posix_write_file_user_test/1,
    posix_write_file_group_test/1,
    posix_read_dir_user_test/1,
    posix_read_dir_group_test/1,
    posix_write_dir_user_test/1,
    posix_write_dir_group_test/1,
    posix_execute_dir_user_test/1,
    posix_execute_dir_group_test/1,
    acl_read_object_user_test/1,
    acl_read_object_group_test/1,
    acl_list_container_user_test/1,
    acl_list_container_group_test/1,
    acl_write_object_user_test/1,
    acl_write_object_group_test/1,
    acl_add_object_user_test/1,
    acl_add_object_group_test/1,
    acl_add_subcontainer_user_test/1,
    acl_add_subcontainer_group_test/1,
    acl_read_metadata_user_test/1,
    acl_read_metadata_group_test/1,
    acl_write_metadata_user_test/1,
    acl_write_metadata_group_test/1,
    acl_traverse_container_user_test/1,
    acl_traverse_container_group_test/1,
    acl_delete_object_user_test/1,
    acl_delete_object_group_test/1,
    acl_delete_subcontainer_user_test/1,
    acl_delete_subcontainer_group_test/1,
    acl_read_attributes_user_test/1,
    acl_read_attributes_group_test/1,
    acl_write_attributes_user_test/1,
    acl_write_attributes_group_test/1,
    acl_delete_user_test/1,
    acl_delete_group_test/1,
    acl_read_acl_user_test/1,
    acl_read_acl_group_test/1,
    acl_write_acl_user_test/1,
    acl_write_acl_group_test/1
]).

-performance({test_cases, []}).
all() ->
    [
        posix_read_file_user_test,
        posix_read_file_group_test,
        posix_write_file_user_test,
        posix_write_file_group_test,
        posix_read_dir_user_test,
        posix_read_dir_group_test,
        posix_write_dir_user_test,
        posix_write_dir_group_test,
        posix_execute_dir_user_test,
        posix_execute_dir_group_test,
        acl_read_object_user_test,
        acl_read_object_group_test,
        acl_list_container_user_test,
        acl_list_container_group_test,
        acl_write_object_user_test,
        acl_write_object_group_test,
        acl_add_object_user_test,
        acl_add_object_group_test,
        acl_add_subcontainer_user_test,
        acl_add_subcontainer_group_test,
        acl_read_metadata_user_test,
        acl_read_metadata_group_test,
        acl_write_metadata_user_test,
        acl_write_metadata_group_test,
        acl_traverse_container_user_test,
        acl_traverse_container_group_test,
        acl_delete_object_user_test,
        acl_delete_object_group_test,
        acl_delete_subcontainer_user_test,
        acl_delete_subcontainer_group_test,
        acl_read_attributes_user_test,
        acl_read_attributes_group_test,
        acl_write_attributes_user_test,
        acl_write_attributes_group_test,
        acl_delete_user_test,
        acl_delete_group_test,
        acl_read_acl_user_test,
        acl_read_acl_group_test,
        acl_write_acl_user_test,
        acl_write_acl_group_test
    ].


%%%===================================================================
%%% Test functions
%%%===================================================================

% posix tests

posix_read_file_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    {ok, Uuid} = lfm_proxy:create(W, SessId1, <<"/file">>, 8#370),
    {ok, H1} = lfm_proxy:open(W, SessId1, {uuid, Uuid}, write),
    {ok, 1} = lfm_proxy:write(W, H1, 0, <<255:8>>),

    Ans1 = lfm_proxy:open(W, SessId1, {uuid, Uuid}, read),
    ?assertEqual({error, ?EACCES}, Ans1),

    ok = lfm_proxy:set_perms(W, SessId1, {uuid, Uuid}, 8#470),
    {ok, H2} = lfm_proxy:open(W, SessId1, {uuid, Uuid}, read),

    Ans3 = lfm_proxy:read(W, H2, 0, 1),
    ?assertEqual({ok, <<255:8>>}, Ans3).

posix_read_file_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    {ok, Uuid} = lfm_proxy:create(W, SessId2, <<"/file">>, 8#730),


    {ok, H1} = lfm_proxy:open(W, SessId1, {uuid, Uuid}, write),
    {ok, 1} = lfm_proxy:write(W, H1, 0, <<255:8>>),

    Ans1 = lfm_proxy:open(W, SessId1, {uuid, Uuid}, read),
    ?assertEqual({error, ?EACCES}, Ans1),

    ok = lfm_proxy:set_perms(W, SessId2, {uuid, Uuid}, 8#740),
    {ok, H2} = lfm_proxy:open(W, SessId1, {uuid, Uuid}, read),

    Ans2 = lfm_proxy:read(W, H2, 0, 1),
    ?assertEqual({ok, <<255:8>>}, Ans2).

posix_write_file_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    {ok, Uuid} =
        lfm_proxy:create(W, SessId1, <<"/spaces/space_name1/file">>, 8#570),

    Ans1 = lfm_proxy:open(W, SessId1, {uuid, Uuid}, write),
    ?assertEqual({error, ?EACCES}, Ans1),

    ok = lfm_proxy:set_perms(W, SessId1, {uuid, Uuid}, 8#270),
    {ok, H1} = lfm_proxy:open(W, SessId1, {uuid, Uuid}, write),

    Ans2 = lfm_proxy:write(W, H1, 0, <<255:8>>),
    ?assertEqual({ok, 1}, Ans2),

    ok = lfm_proxy:set_perms(W, SessId1, {uuid, Uuid}, 8#770),
    {ok, H2} = lfm_proxy:open(W, SessId1, {uuid, Uuid}, read),
    Ans3 = lfm_proxy:read(W, H2, 0, 1),
    ?assertEqual({ok, <<255:8>>}, Ans3).

posix_write_file_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    SessId4 = ?config({session_id, 4}, Config),
    {ok, Uuid} =
        lfm_proxy:create(W, SessId1, <<"/spaces/space_name4/file">>, 8#750),

    Ans1 = lfm_proxy:open(W, SessId4, {uuid, Uuid}, write),
    ?assertEqual({error, ?EACCES}, Ans1),

    ok = lfm_proxy:set_perms(W, SessId1, {uuid, Uuid}, 8#270),
    {ok, H1} = lfm_proxy:open(W, SessId4, {uuid, Uuid}, write),

    Ans2 = lfm_proxy:write(W, H1, 0, <<255:8>>),
    ?assertEqual({ok, 1}, Ans2),

    ok = lfm_proxy:set_perms(W, SessId1, {uuid, Uuid}, 8#770),
    {ok, H2} = lfm_proxy:open(W, SessId1, {uuid, Uuid}, read),
    Ans3 = lfm_proxy:read(W, H2, 0, 1),
    ?assertEqual({ok, <<255:8>>}, Ans3).

posix_read_dir_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    {ok, DirUuid} = mkdir(W, SessId2, <<"/spaces/space_name2/dir">>, 8#370),
    {ok, FileUuid} =
        lfm_proxy:create(W, SessId2, <<"/spaces/space_name2/dir/file">>, 8#770),

    Ans1 = lfm_proxy:ls(W, SessId2, {uuid, DirUuid}, 5, 0),
    ?assertEqual({error, ?EACCES}, Ans1),

    ok = lfm_proxy:set_perms(W, SessId2, {uuid, DirUuid}, 8#470),
    Ans2 = lfm_proxy:ls(W, SessId2, {uuid, DirUuid}, 5, 0),
    ?assertMatch({ok, [{FileUuid, _}]}, Ans2).

posix_read_dir_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    {ok, DirUuid} = mkdir(W, SessId2, <<"/spaces/space_name2/dir">>, 8#730),
    {ok, FileUuid} =
        lfm_proxy:create(W, SessId2, <<"/spaces/space_name2/dir/file">>, 8#770),

    Ans1 = lfm_proxy:ls(W, SessId1, {uuid, DirUuid}, 5, 0),
    ?assertEqual({error, ?EACCES}, Ans1),

    ok = lfm_proxy:set_perms(W, SessId2, {uuid, DirUuid}, 8#740),
    Ans2 = lfm_proxy:ls(W, SessId1, {uuid, DirUuid}, 5, 0),
    ?assertMatch({ok, [{FileUuid, _}]}, Ans2).

posix_write_dir_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    {ok, DirUuid} = mkdir(W, SessId3, <<"/dir">>, 8#770),
    {ok, File1Uuid} = lfm_proxy:create(W, SessId3, <<"/dir/file1">>, 8#770),
    ok = lfm_proxy:set_perms(W, SessId3, {uuid, DirUuid}, 8#570),

    Ans1 = lfm_proxy:create(W, SessId3, <<"/dir/file2">>, 8#770),
    ?assertEqual({error, ?EACCES}, Ans1),
    % TODO: assert eacces on mv when implemented
    Ans2 = lfm_proxy:unlink(W, SessId3, {uuid, File1Uuid}),
    ?assertEqual({error, ?EACCES}, Ans2),

    ok = lfm_proxy:set_perms(W, SessId3, {uuid, DirUuid}, 8#370),
    Ans3 = lfm_proxy:create(W, SessId3, <<"/dir/file2">>, 8#770),
    ?assertMatch({ok, _Uuid}, Ans3),
    % TODO: assert ok on mv when implemented
    Ans4 = lfm_proxy:unlink(W, SessId3, {uuid, File1Uuid}),
    ?assertEqual(ok, Ans4).

posix_write_dir_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    {ok, DirUuid} = mkdir(W, SessId3, <<"/dir">>, 8#770),
    {ok, File1Uuid} = lfm_proxy:create(W, SessId3, <<"/dir/file1">>, 8#770),
    ok = lfm_proxy:set_perms(W, SessId3, {uuid, DirUuid}, 8#750),

    Ans1 =
        lfm_proxy:create(W, SessId1, <<"/spaces/space_name3/dir/file2">>, 8#770),
    ?assertEqual({error, ?EACCES}, Ans1),
    % TODO: assert eacces on mv when implemented
    Ans2 = lfm_proxy:unlink(W, SessId1, {uuid, File1Uuid}),
    ?assertEqual({error, ?EACCES}, Ans2),

    ok = lfm_proxy:set_perms(W, SessId3, {uuid, DirUuid}, 8#730),
    Ans3 =
        lfm_proxy:create(W, SessId1, <<"/spaces/space_name3/dir/file2">>, 8#770),
    ?assertMatch({ok, _Uuid}, Ans3),
    % TODO: assert ok on mv when implemented
    Ans4 = lfm_proxy:unlink(W, SessId1, {uuid, File1Uuid}),
    ?assertEqual(ok, Ans4).

posix_execute_dir_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    {ok, Dir1Uuid} = mkdir(W, SessId2, <<"/spaces/space_name3/dir1">>, 8#770),
    {ok, Dir2Uuid} = mkdir(W, SessId2, <<"/spaces/space_name3/dir1/dir2">>, 8#770),
    {ok, FileUuid} =
        lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/dir1/dir2/file">>, 8#770),
    ok = lfm_proxy:set_perms(W, SessId2, {uuid, Dir1Uuid}, 8#670),

    Ans1 = lfm_proxy:ls(W, SessId2, {uuid, Dir2Uuid}, 5, 0),
    ?assertEqual({error, ?EACCES}, Ans1),
    Ans2 = lfm_proxy:open(W, SessId2, {uuid, FileUuid}, rdwr),
    ?assertEqual({error, ?EACCES}, Ans2),

    ok = lfm_proxy:set_perms(W, SessId2, {uuid, Dir1Uuid}, 8#170),
    Ans3 = lfm_proxy:ls(W, SessId2, {uuid, Dir2Uuid}, 5, 0),
    ?assertMatch({ok, _List}, Ans3),
    {ok, H} = lfm_proxy:open(W, SessId2, {uuid, FileUuid}, rdwr),
    Ans4 = lfm_proxy:write(W, H, 0, <<255:8>>),
    ?assertEqual({ok, 1}, Ans4),
    Ans5 = lfm_proxy:read(W, H, 0, 1),
    ?assertEqual({ok, <<255:8>>}, Ans5).

posix_execute_dir_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    SessId3 = ?config({session_id, 3}, Config),
    {ok, Dir1Uuid} = mkdir(W, SessId2, <<"/spaces/space_name3/dir1">>, 8#770),
    {ok, Dir2Uuid} = mkdir(W, SessId2, <<"/spaces/space_name3/dir1/dir2">>, 8#770),
    {ok, FileUuid} =
        lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/dir1/dir2/file">>, 8#770),
    ok = lfm_proxy:set_perms(W, SessId2, {uuid, Dir1Uuid}, 8#760),

    Ans1 = lfm_proxy:ls(W, SessId3, {uuid, Dir2Uuid}, 5, 0),
    ?assertEqual({error, ?EACCES}, Ans1),
    Ans2 = lfm_proxy:open(W, SessId3, {uuid, FileUuid}, rdwr),
    ?assertEqual({error, ?EACCES}, Ans2),

    ok = lfm_proxy:set_perms(W, SessId2, {uuid, Dir1Uuid}, 8#710),
    Ans3 = lfm_proxy:ls(W, SessId3, {uuid, Dir2Uuid}, 5, 0),
    ?assertMatch({ok, _List}, Ans3),
    {ok, H} = lfm_proxy:open(W, SessId3, {uuid, FileUuid}, rdwr),
    Ans4 = lfm_proxy:write(W, H, 0, <<255:8>>),
    ?assertEqual({ok, 1}, Ans4),
    Ans5 = lfm_proxy:read(W, H, 0, 1),
    ?assertEqual({ok, <<255:8>>}, Ans5).

% acl tests

acl_read_object_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {ok, Uuid} = lfm_proxy:create(W, SessId2, <<"/file">>, 8#777),
    {ok, H1} = lfm_proxy:open(W, SessId2, {uuid, Uuid}, write),
    {ok, 1} = lfm_proxy:write(W, H1, 0, <<255:8>>),

    Ace1 = ?deny_user(UserId1, ?read_object_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:open(W, SessId1, {uuid, Uuid}, read),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace2 = ?allow_user(UserId1, ?read_object_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace2]),
    {ok, H2} = lfm_proxy:open(W, SessId1, {uuid, Uuid}, read),
    Ans2 = lfm_proxy:read(W, H2, 0, 1),
    ?assertEqual({ok, <<255:8>>}, Ans2).

acl_read_object_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [{GroupId1, _} | _] = ?config({groups, 1}, Config),
    {ok, Uuid} = lfm_proxy:create(W, SessId2, <<"/file">>, 8#777),
    {ok, H1} = lfm_proxy:open(W, SessId2, {uuid, Uuid}, write),
    {ok, 1} = lfm_proxy:write(W, H1, 0, <<255:8>>),

    Ace1 = ?deny_group(GroupId1, ?read_object_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:open(W, SessId1, {uuid, Uuid}, read),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace2 = ?allow_group(GroupId1, ?read_object_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace2]),
    {ok, H2} = lfm_proxy:open(W, SessId1, {uuid, Uuid}, read),
    Ans2 = lfm_proxy:read(W, H2, 0, 1),
    ?assertEqual({ok, <<255:8>>}, Ans2).

acl_list_container_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {ok, DirUuid} = mkdir(W, SessId2, <<"/spaces/space_name2/dir">>, 8#777),
    {ok, FileUuid} =
        lfm_proxy:create(W, SessId2, <<"/spaces/space_name2/dir/file">>, 8#777),

    Ace1 = ?deny_user(UserId1, ?list_container_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:ls(W, SessId1, {uuid, DirUuid}, 5, 0),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace2 = ?allow_user(UserId1, ?list_container_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2]),
    Ans2 = lfm_proxy:ls(W, SessId1, {uuid, DirUuid}, 5, 0),
    ?assertMatch({ok, [{FileUuid, _}]}, Ans2).


acl_list_container_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [{GroupId1, _} | _] = ?config({groups, 1}, Config),
    {ok, DirUuid} = mkdir(W, SessId2, <<"/spaces/space_name2/dir">>, 8#777),
    {ok, FileUuid} =
        lfm_proxy:create(W, SessId2, <<"/spaces/space_name2/dir/file">>, 8#777),

    Ace1 = ?deny_group(GroupId1, ?list_container_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:ls(W, SessId1, {uuid, DirUuid}, 5, 0),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace2 = ?allow_group(GroupId1, ?list_container_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2]),
    Ans2 = lfm_proxy:ls(W, SessId1, {uuid, DirUuid}, 5, 0),
    ?assertMatch({ok, [{FileUuid, _}]}, Ans2).

acl_write_object_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {ok, Uuid} =
        lfm_proxy:create(W, SessId2, <<"/spaces/space_name4/file">>, 8#777),

    Ace1 = ?deny_user(UserId1, ?write_object_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:open(W, SessId1, {uuid, Uuid}, write),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace2 = ?allow_user(UserId1, ?write_object_mask),

        ok = lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace2]),
    {ok, H1} = lfm_proxy:open(W, SessId1, {uuid, Uuid}, write),
    Ans2 = lfm_proxy:write(W, H1, 0, <<255:8>>),
    ?assertEqual({ok, 1}, Ans2),

    {ok, H2} = lfm_proxy:open(W, SessId2, {uuid, Uuid}, read),
    Ans3 = lfm_proxy:read(W, H2, 0, 1),
    ?assertEqual({ok, <<255:8>>}, Ans3).

acl_write_object_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [{GroupId1, _} | _] = ?config({groups, 1}, Config),
    {ok, Uuid} =
        lfm_proxy:create(W, SessId2, <<"/spaces/space_name4/file">>, 8#777),

    Ace1 = ?deny_group(GroupId1, ?write_object_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:open(W, SessId1, {uuid, Uuid}, write),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace2 = ?allow_group(GroupId1, ?write_object_mask),

        ok = lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace2]),
    {ok, H1} = lfm_proxy:open(W, SessId1, {uuid, Uuid}, write),
    Ans2 = lfm_proxy:write(W, H1, 0, <<255:8>>),
    ?assertEqual({ok, 1}, Ans2),


    {ok, H2} = lfm_proxy:open(W, SessId2, {uuid, Uuid}, read),
    Ans3 = lfm_proxy:read(W, H2, 0, 1),
    ?assertEqual({ok, <<255:8>>}, Ans3).

acl_add_object_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    {ok, DirUuid} = mkdir(W, SessId3, <<"/dir">>, 8#777),

    Ace1 = ?deny_user(UserId2, ?add_object_mask),
    Ace2 = ?allow_user(UserId2, ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace1, Ace2]),
    Ans1 = lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/dir/file">>, 8#777),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace3 = ?allow_user(UserId2, ?add_object_mask bor ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace3]),
    Ans2 = lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/dir/file">>, 8#777),
    ?assertMatch({ok, _FileUuid}, Ans2).

acl_add_object_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    [_, {GroupId2, _} | _] = ?config({groups, 1}, Config),
    {ok, DirUuid} = mkdir(W, SessId3, <<"/dir">>, 8#777),

    Ace1 = ?deny_group(GroupId2, ?add_object_mask),
    Ace2 = ?allow_group(GroupId2, ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace1, Ace2]),
    Ans1 = lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/dir/file">>, 8#777),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace3 = ?allow_group(GroupId2, ?add_object_mask bor ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace3]),
    Ans2 = lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/dir/file">>, 8#777),
    ?assertMatch({ok, _FileUuid}, Ans2).

acl_add_subcontainer_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    {ok, Dir1Uuid} = mkdir(W, SessId3, <<"/spaces/space_name4/dir1">>, 8#777),

    Ace1 = ?allow_user(UserId2, ?add_subcontainer_mask),
    Ace2 = ?deny_user(UserId2, ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId3, {uuid, Dir1Uuid}, [?acl_all(UserId3), Ace1, Ace2]),
    Ans1 = mkdir(W, SessId2, <<"/spaces/space_name4/dir1/dir2">>, 8#777),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace3 = ?allow_user(UserId2, ?add_subcontainer_mask bor ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId3, {uuid, Dir1Uuid}, [?acl_all(UserId3), Ace3]),
    Ans2 = mkdir(W, SessId2, <<"/spaces/space_name4/dir1/dir2">>, 8#777),
    ?assertMatch({ok, _Dir2Uuid}, Ans2).

acl_add_subcontainer_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    [_, {GroupId2, _} | _] = ?config({groups, 1}, Config),
    {ok, Dir1Uuid} = mkdir(W, SessId3, <<"/spaces/space_name4/dir1">>, 8#777),

    Ace1 = ?allow_group(GroupId2, ?add_subcontainer_mask),
    Ace2 = ?deny_group(GroupId2, ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId3, {uuid, Dir1Uuid}, [?acl_all(UserId3), Ace1, Ace2]),
    Ans1 = mkdir(W, SessId2, <<"/spaces/space_name4/dir1/dir2">>, 8#777),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace3 = ?allow_group(GroupId2, ?add_subcontainer_mask bor ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId3, {uuid, Dir1Uuid}, [?acl_all(UserId3), Ace3]),
    Ans2 = mkdir(W, SessId2, <<"/spaces/space_name4/dir1/dir2">>, 8#777),
    ?assertMatch({ok, _Dir2Uuid}, Ans2).

acl_read_metadata_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId4 = ?config({session_id, 4}, Config),
    UserId4 = ?config({user_id, 4}, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    {ok, FileUuid} = lfm_proxy:create(W, SessId4, <<"/file">>, 8#777),
    Xattr = #xattr{name = <<"XATTR_NAME">>, value = <<42/integer>>},
    ok = lfm_proxy:set_xattr(W, SessId4, {uuid, FileUuid}, Xattr),

    Ace1 = ?deny_user(UserId3, ?read_metadata_mask),

    ok = lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace1]),
    Ans1 = lfm_proxy:get_xattr(W, SessId3, {uuid, FileUuid}, <<"XATTR_NAME">>),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace2 = ?allow_user(UserId3, ?read_metadata_mask),

    ok = lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace2]),
    Ans2 = lfm_proxy:get_xattr(W, SessId3, {uuid, FileUuid}, <<"XATTR_NAME">>),
    ?assertEqual({ok, Xattr}, Ans2).

acl_read_metadata_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId4 = ?config({session_id, 4}, Config),
    UserId4 = ?config({user_id, 4}, Config),
    SessId3 = ?config({session_id, 3}, Config),
    [_, _, _, {GroupId4, _}] = ?config({groups, 1}, Config),
    {ok, FileUuid} = lfm_proxy:create(W, SessId4, <<"/file">>, 8#777),
    Xattr = #xattr{name = <<"XATTR_NAME">>, value = <<42/integer>>},
    ok = lfm_proxy:set_xattr(W, SessId4, {uuid, FileUuid}, Xattr),

    Ace1 = ?deny_group(GroupId4, ?read_metadata_mask),

    ok = lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace1]),
    Ans1 = lfm_proxy:get_xattr(W, SessId3, {uuid, FileUuid}, <<"XATTR_NAME">>),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace2 = ?allow_group(GroupId4, ?read_metadata_mask),

    ok = lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace2]),
    Ans2 = lfm_proxy:get_xattr(W, SessId3, {uuid, FileUuid}, <<"XATTR_NAME">>),
    ?assertEqual({ok, Xattr}, Ans2).

acl_write_metadata_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId4 = ?config({session_id, 4}, Config),
    UserId4 = ?config({user_id, 4}, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    {ok, FileUuid} = lfm_proxy:create(W, SessId4, <<"/spaces/space_name4/file">>, 8#777),
    Xattr = #xattr{name = <<"XATTR_NAME">>, value = <<42/integer>>},
    ok = lfm_proxy:set_xattr(W, SessId4, {uuid, FileUuid}, Xattr),

    Ace1 = ?deny_user(UserId3, ?write_metadata_mask),

    ok = lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace1]),
    Ans1 = lfm_proxy:set_xattr(W, SessId3, {uuid, FileUuid}, Xattr),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace2 = ?allow_user(UserId3, ?write_metadata_mask),

    ok = lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace2]),
    Ans2 = lfm_proxy:set_xattr(W, SessId3, {uuid, FileUuid}, Xattr),
    ?assertEqual(ok, Ans2),

    Ans3 = lfm_proxy:get_xattr(W, SessId4, {uuid, FileUuid}, <<"XATTR_NAME">>),
    ?assertEqual({ok, Xattr}, Ans3).

acl_write_metadata_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId4 = ?config({session_id, 4}, Config),
    UserId4 = ?config({user_id, 4}, Config),
    SessId3 = ?config({session_id, 3}, Config),
    [_, _, {GroupId3, _} | _] = ?config({groups, 1}, Config),
    {ok, FileUuid} = lfm_proxy:create(W, SessId4, <<"/spaces/space_name4/file">>, 8#777),
    Xattr = #xattr{name = <<"XATTR_NAME">>, value = <<42/integer>>},
    ok = lfm_proxy:set_xattr(W, SessId4, {uuid, FileUuid}, Xattr),

    Ace1 = ?deny_group(GroupId3, ?write_metadata_mask),

    ok = lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace1]),
    Ans1 = lfm_proxy:set_xattr(W, SessId3, {uuid, FileUuid}, Xattr),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace2 = ?allow_group(GroupId3, ?write_metadata_mask),

    ok = lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace2]),
    Ans2 = lfm_proxy:set_xattr(W, SessId3, {uuid, FileUuid}, Xattr),
    ?assertEqual(ok, Ans2),

    Ans3 = lfm_proxy:get_xattr(W, SessId4, {uuid, FileUuid}, <<"XATTR_NAME">>),
    ?assertEqual({ok, Xattr}, Ans3).

acl_traverse_container_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    {ok, Dir1Uuid} = mkdir(W, SessId1, <<"/spaces/space_name3/dir1">>, 8#777),
    {ok, Dir2Uuid} = mkdir(W, SessId1, <<"/spaces/space_name3/dir1/dir2">>, 8#777),
    {ok, FileUuid} =
        lfm_proxy:create(W, SessId1, <<"/spaces/space_name3/dir1/dir2/file">>, 8#777),
    {ok, H1} = lfm_proxy:open(W, SessId1, {uuid, FileUuid}, write),
    {ok, 1} = lfm_proxy:write(W, H1, 0, <<255:8>>),

    Ace1 = ?deny_user(UserId3, ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace1]),
    Ans1 = lfm_proxy:ls(W, SessId3, {uuid, Dir2Uuid}, 5, 0),
    ?assertEqual({error, ?EACCES}, Ans1),
    Ans2 = lfm_proxy:open(W, SessId3, {uuid, FileUuid}, read),
    ?assertEqual({error, ?EACCES}, Ans2),

    Ace2 = ?allow_user(UserId3, ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace2]),
    Ans3 = lfm_proxy:ls(W, SessId3, {uuid, Dir2Uuid}, 5, 0),
    ?assertMatch({ok, _List}, Ans3),
    {ok, H2} = lfm_proxy:open(W, SessId3, {uuid, FileUuid}, read),
    Ans4 = lfm_proxy:read(W, H2, 0, 1),
    ?assertEqual({ok, <<255:8>>}, Ans4).

acl_traverse_container_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    SessId3 = ?config({session_id, 3}, Config),
    [_, _, {GroupId3, _} | _] = ?config({groups, 1}, Config),
    {ok, Dir1Uuid} = mkdir(W, SessId1, <<"/spaces/space_name3/dir1">>, 8#777),
    {ok, Dir2Uuid} = mkdir(W, SessId1, <<"/spaces/space_name3/dir1/dir2">>, 8#777),
    {ok, FileUuid} =
        lfm_proxy:create(W, SessId1, <<"/spaces/space_name3/dir1/dir2/file">>, 8#777),
    {ok, H1} = lfm_proxy:open(W, SessId1, {uuid, FileUuid}, write),
    {ok, 1} = lfm_proxy:write(W, H1, 0, <<255:8>>),

    Ace1 = ?deny_group(GroupId3, ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace1]),
    Ans1 = lfm_proxy:ls(W, SessId3, {uuid, Dir2Uuid}, 5, 0),
    ?assertEqual({error, ?EACCES}, Ans1),
    Ans2 = lfm_proxy:open(W, SessId3, {uuid, FileUuid}, read),
    ?assertEqual({error, ?EACCES}, Ans2),

    Ace2 = ?allow_group(GroupId3, ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace2]),
    Ans3 = lfm_proxy:ls(W, SessId3, {uuid, Dir2Uuid}, 5, 0),
    ?assertMatch({ok, _List}, Ans3),
    {ok, H2} = lfm_proxy:open(W, SessId3, {uuid, FileUuid}, read),
    Ans4 = lfm_proxy:read(W, H2, 0, 1),
    ?assertEqual({ok, <<255:8>>}, Ans4).

acl_delete_object_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    {ok, DirUuid} = mkdir(W, SessId3, <<"/dir">>, 8#777),
    {ok, FileUuid} = lfm_proxy:create(W, SessId3, <<"/dir/file">>, 8#777),

    Ace1 = ?deny_user(UserId2, ?delete_object_mask),
    Ace2 = ?allow_user(UserId2, ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace1, Ace2]),
    Ans1 = lfm_proxy:unlink(W, SessId2, {uuid, FileUuid}),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace3 = ?allow_user(UserId2, ?delete_object_mask bor ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace3]),
    Ans2 = lfm_proxy:unlink(W, SessId2, {uuid, FileUuid}),
    ?assertEqual(ok, Ans2).

acl_delete_object_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    [_, _, _, {GroupId4, _}] = ?config({groups, 1}, Config),
    {ok, DirUuid} = mkdir(W, SessId3, <<"/dir">>, 8#777),
    {ok, FileUuid} = lfm_proxy:create(W, SessId3, <<"/dir/file">>, 8#777),

    Ace1 = ?allow_group(GroupId4, ?traverse_container_mask),
    Ace2 = ?deny_group(GroupId4, ?delete_object_mask),

    ok = lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace1, Ace2]),
    Ans1 = lfm_proxy:unlink(W, SessId2, {uuid, FileUuid}),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace3 = ?allow_group(GroupId4, ?traverse_container_mask bor ?delete_object_mask),

    ok = lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace3]),
    Ans2 = lfm_proxy:unlink(W, SessId2, {uuid, FileUuid}),
    ?assertEqual(ok, Ans2).

acl_delete_subcontainer_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    SessId4 = ?config({session_id, 4}, Config),
    UserId4 = ?config({user_id, 4}, Config),
    {ok, Dir1Uuid} = mkdir(W, SessId1, <<"/spaces/space_name4/dir1">>, 8#777),
    {ok, Dir2Uuid} = mkdir(W, SessId1, <<"/spaces/space_name4/dir1/dir2">>, 8#777),

    Ace1 = ?deny_user(UserId4, ?delete_subcontainer_mask),
    Ace2 = ?allow_user(UserId4, ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace1, Ace2]),
    Ans1 = lfm_proxy:unlink(W, SessId4, {uuid, Dir2Uuid}),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace3 = ?allow_user(UserId4, ?delete_subcontainer_mask bor ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace3]),
    Ans2 = lfm_proxy:unlink(W, SessId4, {uuid, Dir2Uuid}),
    ?assertEqual(ok, Ans2).

acl_delete_subcontainer_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    SessId4 = ?config({session_id, 4}, Config),
    [_, _, _, {GroupId4, _}] = ?config({groups, 1}, Config),
    {ok, Dir1Uuid} = mkdir(W, SessId1, <<"/spaces/space_name4/dir1">>, 8#777),
    {ok, Dir2Uuid} = mkdir(W, SessId1, <<"/spaces/space_name4/dir1/dir2">>, 8#777),

    Ace1 = ?deny_group(GroupId4, ?delete_subcontainer_mask),
    Ace2 = ?allow_group(GroupId4, ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace1, Ace2]),
    Ans1 = lfm_proxy:unlink(W, SessId4, {uuid, Dir2Uuid}),
    ?assertEqual({error, ?EACCES}, Ans1),

    Ace3 = ?allow_group(GroupId4, ?delete_subcontainer_mask bor ?traverse_container_mask),

    ok = lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace3]),
    Ans2 = lfm_proxy:unlink(W, SessId4, {uuid, Dir2Uuid}),
    ?assertEqual(ok, Ans2).

acl_read_attributes_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {ok, FileUuid} = lfm_proxy:create(W, SessId2, <<"/file">>, 8#777),
    ok = lfm_proxy:set_transfer_encoding(W, SessId2, {uuid, FileUuid}, <<"base64">>),
    ok = lfm_proxy:set_completion_status(W, SessId2, {uuid, FileUuid}, <<"Completed">>),
    ok = lfm_proxy:set_mimetype(W, SessId2, {uuid, FileUuid}, <<"text/html">>),

    Ace1 = ?deny_user(UserId1, ?read_attributes_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:get_transfer_encoding(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({error, ?EACCES}, Ans1),
    Ans2 = lfm_proxy:get_completion_status(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({error, ?EACCES}, Ans2),
    Ans3 = lfm_proxy:get_mimetype(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({error, ?EACCES}, Ans3),

    Ace2 = ?allow_user(UserId1, ?read_attributes_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2]),
    Ans4 = lfm_proxy:get_transfer_encoding(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({ok, <<"base64">>}, Ans4),
    Ans5 = lfm_proxy:get_completion_status(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({ok, <<"Completed">>}, Ans5),
    Ans6 = lfm_proxy:get_mimetype(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({ok, <<"text/html">>}, Ans6).

acl_read_attributes_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [{GroupId1, _} | _] = ?config({groups, 1}, Config),
    {ok, FileUuid} = lfm_proxy:create(W, SessId2, <<"/file">>, 8#777),
    ok = lfm_proxy:set_transfer_encoding(W, SessId2, {uuid, FileUuid}, <<"base64">>),
    ok = lfm_proxy:set_completion_status(W, SessId2, {uuid, FileUuid}, <<"Completed">>),
    ok = lfm_proxy:set_mimetype(W, SessId2, {uuid, FileUuid}, <<"text/html">>),

    Ace1 = ?deny_group(GroupId1, ?read_attributes_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:get_transfer_encoding(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({error, ?EACCES}, Ans1),
    Ans2 = lfm_proxy:get_completion_status(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({error, ?EACCES}, Ans2),
    Ans3 = lfm_proxy:get_mimetype(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({error, ?EACCES}, Ans3),

    Ace2 = ?allow_group(GroupId1, ?read_attributes_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2]),
    Ans4 = lfm_proxy:get_transfer_encoding(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({ok, <<"base64">>}, Ans4),
    Ans5 = lfm_proxy:get_completion_status(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({ok, <<"Completed">>}, Ans5),
    Ans6 = lfm_proxy:get_mimetype(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({ok, <<"text/html">>}, Ans6).

acl_write_attributes_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {ok, FileUuid} = lfm_proxy:create(W, SessId2, <<"/spaces/space_name4/file">>, 8#777),

    Ace1 = ?deny_user(UserId1, ?write_attributes_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:set_transfer_encoding(W, SessId1, {uuid, FileUuid}, <<"base64">>),
    ?assertEqual({error, ?EACCES}, Ans1),
    Ans2 = lfm_proxy:set_completion_status(W, SessId1, {uuid, FileUuid}, <<"Completed">>),
    ?assertEqual({error, ?EACCES}, Ans2),
    Ans3 = lfm_proxy:set_mimetype(W, SessId1, {uuid, FileUuid}, <<"text/html">>),
    ?assertEqual({error, ?EACCES}, Ans3),

    Ace2 = ?allow_user(UserId1, ?write_attributes_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2]),
    Ans4 = lfm_proxy:set_transfer_encoding(W, SessId1, {uuid, FileUuid}, <<"base64">>),
    ?assertEqual(ok, Ans4),
    Ans5 = lfm_proxy:set_completion_status(W, SessId1, {uuid, FileUuid}, <<"Completed">>),
    ?assertEqual(ok, Ans5),
    Ans6 = lfm_proxy:set_mimetype(W, SessId1, {uuid, FileUuid}, <<"text/html">>),
    ?assertEqual(ok, Ans6),

    Ans7 = lfm_proxy:get_transfer_encoding(W, SessId2, {uuid, FileUuid}),
    ?assertEqual({ok, <<"base64">>}, Ans7),
    Ans8 = lfm_proxy:get_completion_status(W, SessId2, {uuid, FileUuid}),
    ?assertEqual({ok, <<"Completed">>}, Ans8),
    Ans9 = lfm_proxy:get_mimetype(W, SessId2, {uuid, FileUuid}),
    ?assertEqual({ok, <<"text/html">>}, Ans9).

acl_write_attributes_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [_, _, _, {GroupId4, _}] = ?config({groups, 1}, Config),
    {ok, FileUuid} = lfm_proxy:create(W, SessId2, <<"/spaces/space_name4/file">>, 8#777),

    Ace1 = ?deny_group(GroupId4, ?write_attributes_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:set_transfer_encoding(W, SessId1, {uuid, FileUuid}, <<"base64">>),
    ?assertEqual({error, ?EACCES}, Ans1),
    Ans2 = lfm_proxy:set_completion_status(W, SessId1, {uuid, FileUuid}, <<"Completed">>),
    ?assertEqual({error, ?EACCES}, Ans2),
    Ans3 = lfm_proxy:set_mimetype(W, SessId1, {uuid, FileUuid}, <<"text/html">>),
    ?assertEqual({error, ?EACCES}, Ans3),

    Ace2 = ?allow_group(GroupId4, ?write_attributes_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2]),
    Ans4 = lfm_proxy:set_transfer_encoding(W, SessId1, {uuid, FileUuid}, <<"base64">>),
    ?assertEqual(ok, Ans4),
    Ans5 = lfm_proxy:set_completion_status(W, SessId1, {uuid, FileUuid}, <<"Completed">>),
    ?assertEqual(ok, Ans5),
    Ans6 = lfm_proxy:set_mimetype(W, SessId1, {uuid, FileUuid}, <<"text/html">>),
    ?assertEqual(ok, Ans6),

    Ans7 = lfm_proxy:get_transfer_encoding(W, SessId2, {uuid, FileUuid}),
    ?assertEqual({ok, <<"base64">>}, Ans7),
    Ans8 = lfm_proxy:get_completion_status(W, SessId2, {uuid, FileUuid}),
    ?assertEqual({ok, <<"Completed">>}, Ans8),
    Ans9 = lfm_proxy:get_mimetype(W, SessId2, {uuid, FileUuid}),
    ?assertEqual({ok, <<"text/html">>}, Ans9).

acl_delete_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {ok, DirUuid} = mkdir(W, SessId2, <<"/dir">>, 8#777),
    {ok, FileUuid} = lfm_proxy:create(W, SessId2, <<"/file">>, 8#777),

    Ace1 = ?deny_user(UserId1, ?delete_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:unlink(W, SessId1, {uuid, DirUuid}),
    ?assertEqual({error, ?EACCES}, Ans1),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1]),
    Ans2 = lfm_proxy:unlink(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({error, ?EACCES}, Ans2),

    Ace2 = ?allow_user(UserId1, ?delete_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2]),
    Ans3 = lfm_proxy:unlink(W, SessId1, {uuid, DirUuid}),
    ?assertEqual(ok, Ans3),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2]),
    Ans4 = lfm_proxy:unlink(W, SessId1, {uuid, FileUuid}),
    ?assertEqual(ok, Ans4).

acl_delete_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [{GroupId1, _}| _] = ?config({groups, 1}, Config),
    {ok, DirUuid} = mkdir(W, SessId2, <<"/dir">>, 8#777),
    {ok, FileUuid} = lfm_proxy:create(W, SessId2, <<"/file">>, 8#777),

    Ace1 = ?deny_group(GroupId1, ?delete_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:unlink(W, SessId1, {uuid, DirUuid}),
    ?assertEqual({error, ?EACCES}, Ans1),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1]),
    Ans2 = lfm_proxy:unlink(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({error, ?EACCES}, Ans2),

    Ace2 = ?allow_group(GroupId1, ?delete_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2]),
    Ans3 = lfm_proxy:unlink(W, SessId1, {uuid, DirUuid}),
    ?assertEqual(ok, Ans3),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2]),
    Ans4 = lfm_proxy:unlink(W, SessId1, {uuid, FileUuid}),
    ?assertEqual(ok, Ans4).

acl_read_acl_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {ok, DirUuid} = mkdir(W, SessId2, <<"/spaces/space_name2/dir">>, 8#777),
    {ok, FileUuid} = lfm_proxy:create(W, SessId2, <<"/spaces/space_name2/file">>, 8#777),

    Ace1 = ?deny_user(UserId1, ?read_acl_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:get_acl(W, SessId1, {uuid, DirUuid}),
    ?assertEqual({error, ?EACCES}, Ans1),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1]),
    Ans2 = lfm_proxy:get_acl(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({error, ?EACCES}, Ans2),

    Ace2 = ?allow_user(UserId1, ?read_acl_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2]),
    Ans3 = lfm_proxy:get_acl(W, SessId1, {uuid, DirUuid}),
    ?assertMatch({ok, _List}, Ans3),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2]),
    Ans4 = lfm_proxy:get_acl(W, SessId1, {uuid, FileUuid}),
    ?assertMatch({ok, _List}, Ans4).

acl_read_acl_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [_, _, {GroupId3, _}| _] = ?config({groups, 1}, Config),
    {ok, DirUuid} = mkdir(W, SessId2, <<"/spaces/space_name2/dir">>, 8#777),
    {ok, FileUuid} = lfm_proxy:create(W, SessId2, <<"/spaces/space_name2/file">>, 8#777),

    Ace1 = ?deny_group(GroupId3, ?read_acl_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:get_acl(W, SessId1, {uuid, DirUuid}),
    ?assertEqual({error, ?EACCES}, Ans1),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1]),
    Ans2 = lfm_proxy:get_acl(W, SessId1, {uuid, FileUuid}),
    ?assertEqual({error, ?EACCES}, Ans2),

    Ace2 = ?allow_group(GroupId3, ?read_acl_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2]),
    Ans3 = lfm_proxy:get_acl(W, SessId1, {uuid, DirUuid}),
    ?assertMatch({ok, _List}, Ans3),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2]),
    Ans4 = lfm_proxy:get_acl(W, SessId1, {uuid, FileUuid}),
    ?assertMatch({ok, _List}, Ans4).

acl_write_acl_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {ok, DirUuid} = mkdir(W, SessId2, <<"/spaces/space_name3/dir">>, 8#777),
    {ok, FileUuid} = lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/file">>, 8#777),

    Ace1 = ?deny_user(UserId1, ?write_acl_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:set_acl(W, SessId1, {uuid, DirUuid}, []),
    ?assertEqual({error, ?EACCES}, Ans1),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1]),
    Ans2 = lfm_proxy:set_acl(W, SessId1, {uuid, FileUuid}, []),
    ?assertEqual({error, ?EACCES}, Ans2),

    Ace2 = ?allow_user(UserId1, ?write_acl_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2]),
    Ans3 = lfm_proxy:set_acl(W, SessId1, {uuid, DirUuid}, []),
    ?assertEqual(ok, Ans3),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2]),
    Ans4 = lfm_proxy:set_acl(W, SessId1, {uuid, FileUuid}, []),
    ?assertEqual(ok, Ans4).

acl_write_acl_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [_, _, {GroupId3, _}| _] = ?config({groups, 1}, Config),
    {ok, DirUuid} = mkdir(W, SessId2, <<"/spaces/space_name3/dir">>, 8#777),
    {ok, FileUuid} = lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/file">>, 8#777),

    Ace1 = ?deny_group(GroupId3, ?write_acl_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1]),
    Ans1 = lfm_proxy:set_acl(W, SessId1, {uuid, DirUuid}, []),
    ?assertEqual({error, ?EACCES}, Ans1),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1]),
    Ans2 = lfm_proxy:set_acl(W, SessId1, {uuid, FileUuid}, []),
    ?assertEqual({error, ?EACCES}, Ans2),

    Ace2 = ?allow_group(GroupId3, ?write_acl_mask),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2]),
    Ans3 = lfm_proxy:set_acl(W, SessId1, {uuid, DirUuid}, []),
    ?assertEqual(ok, Ans3),

    ok = lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2]),
    Ans4 = lfm_proxy:set_acl(W, SessId1, {uuid, FileUuid}, []),
    ?assertEqual(ok, Ans4).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
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

% Creates directory and returns its uuid
mkdir(Worker, SessId, Path, Mode) ->
    case lfm_proxy:mkdir(Worker, SessId, Path, Mode) of
        ok ->
            {ok, #file_attr{uuid = Uuid}} =
                lfm_proxy:stat(Worker, SessId, {path, Path}),
            {ok, Uuid};
        Error ->
            Error
    end.
