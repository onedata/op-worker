%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of posix and acl 
%%% permissions with corresponding logical_file_manager functions
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_permissions_test_SUITE).
-author("Mateusz Paciorek").

%%%-------------------------------------------------------------------
%%% Macros used in acl tests
%%%-------------------------------------------------------------------
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
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/test/performance.hrl").

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------

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

all() ->
    ?ALL(
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
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

%%%-------------------------------------------------------------------
%%% Posix tests
%%%-------------------------------------------------------------------

posix_read_file_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    {_, Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/t1_file">>, 8#770)),
    {_, H1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, write)),
    ?assertEqual({ok, 1}, lfm_proxy:write(W, H1, 0, <<255:8>>)),

    % Verification
    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId1, {uuid, Uuid}, 8#370)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, read)),

    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId1, {uuid, Uuid}, 8#470)),
    {_, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, read)),
    ?assertEqual({ok, <<255:8>>}, lfm_proxy:read(W, H2, 0, 1)).

posix_read_file_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    {_, Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/t2_file">>, 8#770)),
    {_, H1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {uuid, Uuid}, write)),
    ?assertEqual({ok, 1}, lfm_proxy:write(W, H1, 0, <<255:8>>)),

    % Verification
    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId2, {uuid, Uuid}, 8#730)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, read)),

    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId2, {uuid, Uuid}, 8#740)),
    {_, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, read)),
    ?assertEqual({ok, <<255:8>>}, lfm_proxy:read(W, H2, 0, 1)).

posix_write_file_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    {_, Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/spaces/space_name1/t3_file">>, 8#770)),

    % Verification
    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId1, {uuid, Uuid}, 8#570)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, write)),

    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId1, {uuid, Uuid}, 8#270)),
    {_, H1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, write)),
    ?assertEqual({ok, 1}, lfm_proxy:write(W, H1, 0, <<255:8>>)),

    % Check if written data is present
    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId1, {uuid, Uuid}, 8#770)),
    {_, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, read)),
    ?assertEqual({ok, <<255:8>>}, lfm_proxy:read(W, H2, 0, 1)).

posix_write_file_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    SessId4 = ?config({session_id, 4}, Config),
    {_, Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId1, <<"/spaces/space_name4/t4_file">>, 8#770)),

    % Verification
    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId1, {uuid, Uuid}, 8#750)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:open(W, SessId4, {uuid, Uuid}, write)),

    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId1, {uuid, Uuid}, 8#720)),
    {_, H1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId4, {uuid, Uuid}, write)),
    ?assertEqual({ok, 1}, lfm_proxy:write(W, H1, 0, <<255:8>>)),

    % Check if written data is present
    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId1, {uuid, Uuid}, 8#770)),
    {_, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, read)),
    ?assertEqual({ok, <<255:8>>}, lfm_proxy:read(W, H2, 0, 1)).

posix_read_dir_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name2/t5_dir">>, 8#770)),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name2/t5_dir/file">>, 8#770)),

    % Verification
    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId2, {uuid, DirUuid}, 8#370)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:ls(W, SessId2, {uuid, DirUuid}, 0, 5)),

    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId2, {uuid, DirUuid}, 8#470)),
    ?assertMatch({ok, [{FileUuid, _}]}, lfm_proxy:ls(W, SessId2, {uuid, DirUuid}, 0, 5)).

posix_read_dir_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name2/t6_dir">>, 8#770)),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name2/t6_dir/file">>, 8#770)),

    % Verification
    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId2, {uuid, DirUuid}, 8#730)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:ls(W, SessId1, {uuid, DirUuid}, 0, 5)),

    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId2, {uuid, DirUuid}, 8#740)),
    ?assertMatch({ok, [{FileUuid, _}]}, lfm_proxy:ls(W, SessId1, {uuid, DirUuid}, 0, 5)).

posix_write_dir_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId3, <<"/t7_dir">>, 8#770)),
    {_, File1Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId3, <<"/t7_dir/file1">>, 8#770)),

    % Verification
    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId3, {uuid, DirUuid}, 8#570)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:create(W, SessId3, <<"/t7_dir/file2">>, 8#770)),
    % TODO: assert eacces on mv when implemented
    ?assertEqual({error, ?EACCES}, lfm_proxy:unlink(W, SessId3, {uuid, File1Uuid})),

    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId3, {uuid, DirUuid}, 8#370)),
    ?assertMatch({ok, _Uuid}, lfm_proxy:create(W, SessId3, <<"/t7_dir/file2">>, 8#770)),
    % TODO: assert ok on mv when implemented
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId3, {uuid, File1Uuid})).

posix_write_dir_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId3, <<"/t8_dir">>, 8#770)),
    {_, File1Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId3, <<"/t8_dir/file1">>, 8#770)),

    % Verification
    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId3, {uuid, DirUuid}, 8#750)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:create(W, SessId1, <<"/spaces/space_name3/t8_dir/file2">>, 8#770)),
    % TODO: assert eacces on mv when implemented
    ?assertEqual({error, ?EACCES}, lfm_proxy:unlink(W, SessId1, {uuid, File1Uuid})),

    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId3, {uuid, DirUuid}, 8#730)),
    ?assertMatch({ok, _Uuid}, lfm_proxy:create(W, SessId1, <<"/spaces/space_name3/t8_dir/file2">>, 8#770)),
    % TODO: assert ok on mv when implemented
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId1, {uuid, File1Uuid})).

posix_execute_dir_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    {_, Dir1Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name3/t9_dir1">>, 8#770)),
    {_, Dir2Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name3/t9_dir1/dir2">>, 8#770)),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/t9_dir1/dir2/file">>, 8#770)),

    % Verification
    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId2, {uuid, Dir1Uuid}, 8#670)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:ls(W, SessId2, {uuid, Dir2Uuid}, 0, 5)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:open(W, SessId2, {uuid, FileUuid}, rdwr)),

    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId2, {uuid, Dir1Uuid}, 8#170)),
    ?assertMatch({ok, _List}, lfm_proxy:ls(W, SessId2, {uuid, Dir2Uuid}, 0, 5)),
    {_, H} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {uuid, FileUuid}, rdwr)),
    ?assertEqual({ok, 1}, lfm_proxy:write(W, H, 0, <<255:8>>)),
    ?assertEqual({ok, <<255:8>>}, lfm_proxy:read(W, H, 0, 1)).

posix_execute_dir_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    SessId3 = ?config({session_id, 3}, Config),
    {_, Dir1Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name3/t10_dir1">>, 8#770)),
    {_, Dir2Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name3/t10_dir1/dir2">>, 8#770)),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/t10_dir1/dir2/file">>, 8#770)),

    % Verification
    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId2, {uuid, Dir1Uuid}, 8#760)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:ls(W, SessId3, {uuid, Dir2Uuid}, 0, 5)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:open(W, SessId3, {uuid, FileUuid}, rdwr)),

    ?assertEqual(ok, lfm_proxy:set_perms(W, SessId2, {uuid, Dir1Uuid}, 8#710)),
    ?assertMatch({ok, _List}, lfm_proxy:ls(W, SessId3, {uuid, Dir2Uuid}, 0, 5)),
    {_, H} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId3, {uuid, FileUuid}, rdwr)),
    ?assertEqual({ok, 1}, lfm_proxy:write(W, H, 0, <<255:8>>)),
    ?assertEqual({ok, <<255:8>>}, lfm_proxy:read(W, H, 0, 1)).

%%%-------------------------------------------------------------------
%%% Acl tests
%%%-------------------------------------------------------------------

acl_read_object_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {_, Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/t11_file">>, 8#777)),
    {_, H1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {uuid, Uuid}, write)),
    {_, 1} = ?assertMatch({ok, _}, lfm_proxy:write(W, H1, 0, <<255:8>>)),

    % Verification
    Ace1 = ?deny_user(UserId1, ?read_object_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, read)),

    Ace2 = ?allow_user(UserId1, ?read_object_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace2])),
    {_, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, read)),
    ?assertEqual({ok, <<255:8>>}, lfm_proxy:read(W, H2, 0, 1)).

acl_read_object_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [{GroupId1, _} | _] = ?config({groups, 1}, Config),
    {_, Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/t12_file">>, 8#777)),
    {_, H1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {uuid, Uuid}, write)),
    {_, 1} = ?assertMatch({ok, _}, lfm_proxy:write(W, H1, 0, <<255:8>>)),

    % Verification
    Ace1 = ?deny_group(GroupId1, ?read_object_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, read)),

    Ace2 = ?allow_group(GroupId1, ?read_object_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace2])),
    {_, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, read)),
    ?assertEqual({ok, <<255:8>>}, lfm_proxy:read(W, H2, 0, 1)).

acl_list_container_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name2/t13_dir">>, 8#777)),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name2/t13_dir/file">>, 8#777)),

    % Verification
    Ace1 = ?deny_user(UserId1, ?list_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:ls(W, SessId1, {uuid, DirUuid}, 0, 5)),

    Ace2 = ?allow_user(UserId1, ?list_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2])),
    ?assertMatch({ok, [{FileUuid, _}]}, lfm_proxy:ls(W, SessId1, {uuid, DirUuid}, 0, 5)).

acl_list_container_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [{GroupId1, _} | _] = ?config({groups, 1}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name2/t14_dir">>, 8#777)),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name2/t14_dir/file">>, 8#777)),

    % Verification
    Ace1 = ?deny_group(GroupId1, ?list_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:ls(W, SessId1, {uuid, DirUuid}, 0, 5)),

    Ace2 = ?allow_group(GroupId1, ?list_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2])),
    ?assertMatch({ok, [{FileUuid, _}]}, lfm_proxy:ls(W, SessId1, {uuid, DirUuid}, 0, 5)).

acl_write_object_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {_, Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name4/t15_file">>, 8#777)),

    % Verification
    Ace1 = ?deny_user(UserId1, ?write_object_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, write)),

    Ace2 = ?allow_user(UserId1, ?write_object_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace2])),
    {_, H1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, write)),
    ?assertEqual({ok, 1}, lfm_proxy:write(W, H1, 0, <<255:8>>)),

    % Check if written data is present
    {_, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {uuid, Uuid}, read)),
    ?assertEqual({ok, <<255:8>>}, lfm_proxy:read(W, H2, 0, 1)).

acl_write_object_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [{GroupId1, _} | _] = ?config({groups, 1}, Config),
    {_, Uuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name4/t16_file">>, 8#777)),

    % Verification
    Ace1 = ?deny_group(GroupId1, ?write_object_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, write)),

    Ace2 = ?allow_group(GroupId1, ?write_object_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, Uuid}, [?acl_all(UserId2), Ace2])),
    {_, H1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {uuid, Uuid}, write)),
    ?assertEqual({ok, 1}, lfm_proxy:write(W, H1, 0, <<255:8>>)),

    % Check if written data is present
    {_, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId2, {uuid, Uuid}, read)),
    ?assertEqual({ok, <<255:8>>}, lfm_proxy:read(W, H2, 0, 1)).

acl_add_object_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId3, <<"/t17_dir">>, 8#777)),

    % Verification
    Ace1 = ?deny_user(UserId2, ?add_object_mask),
    Ace2 = ?allow_user(UserId2, ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace1, Ace2])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/t17_dir/file">>, 8#777)),

    Ace3 = ?allow_user(UserId2, ?add_object_mask bor ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace3])),
    ?assertMatch({ok, _FileUuid}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/t17_dir/file">>, 8#777)).

acl_add_object_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    [_, {GroupId2, _} | _] = ?config({groups, 1}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId3, <<"/t18_dir">>, 8#777)),

    % Verification
    Ace1 = ?deny_group(GroupId2, ?add_object_mask),
    Ace2 = ?allow_group(GroupId2, ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace1, Ace2])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/t18_dir/file">>, 8#777)),

    Ace3 = ?allow_group(GroupId2, ?add_object_mask bor ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace3])),
    ?assertMatch({ok, _FileUuid}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/t18_dir/file">>, 8#777)).

acl_add_subcontainer_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    {_, Dir1Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId3, <<"/spaces/space_name4/t19_dir1">>, 8#777)),

    % Verification
    Ace1 = ?allow_user(UserId2, ?add_subcontainer_mask),
    Ace2 = ?deny_user(UserId2, ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId3, {uuid, Dir1Uuid}, [?acl_all(UserId3), Ace1, Ace2])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name4/t19_dir1/dir2">>, 8#777)),

    Ace3 = ?allow_user(UserId2, ?add_subcontainer_mask bor ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId3, {uuid, Dir1Uuid}, [?acl_all(UserId3), Ace3])),
    ?assertMatch({ok, _Dir2Uuid}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name4/t19_dir1/dir2">>, 8#777)).

acl_add_subcontainer_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    [_, {GroupId2, _} | _] = ?config({groups, 1}, Config),
    {_, Dir1Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId3, <<"/spaces/space_name4/t20_dir1">>, 8#777)),

    % Verification
    Ace1 = ?allow_group(GroupId2, ?add_subcontainer_mask),
    Ace2 = ?deny_group(GroupId2, ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId3, {uuid, Dir1Uuid}, [?acl_all(UserId3), Ace1, Ace2])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name4/t20_dir1/dir2">>, 8#777)),

    Ace3 = ?allow_group(GroupId2, ?add_subcontainer_mask bor ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId3, {uuid, Dir1Uuid}, [?acl_all(UserId3), Ace3])),
    ?assertMatch({ok, _Dir2Uuid}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name4/t20_dir1/dir2">>, 8#777)).

acl_read_metadata_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId4 = ?config({session_id, 4}, Config),
    UserId4 = ?config({user_id, 4}, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId4, <<"/t21_file">>, 8#777)),
    Xattr = #xattr{name = <<"XATTR_NAME">>, value = <<42/integer>>},
    ?assertEqual(ok, lfm_proxy:set_xattr(W, SessId4, {uuid, FileUuid}, Xattr)),

    % Verification
    Ace1 = ?deny_user(UserId3, ?read_metadata_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:get_xattr(W, SessId3, {uuid, FileUuid}, <<"XATTR_NAME">>)),

    Ace2 = ?allow_user(UserId3, ?read_metadata_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace2])),
    ?assertEqual({ok, Xattr}, lfm_proxy:get_xattr(W, SessId3, {uuid, FileUuid}, <<"XATTR_NAME">>)).

acl_read_metadata_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId4 = ?config({session_id, 4}, Config),
    UserId4 = ?config({user_id, 4}, Config),
    SessId3 = ?config({session_id, 3}, Config),
    [_, _, _, {GroupId4, _}] = ?config({groups, 1}, Config),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId4, <<"/t22_file">>, 8#777)),
    Xattr = #xattr{name = <<"XATTR_NAME">>, value = <<42/integer>>},
    ?assertEqual(ok, lfm_proxy:set_xattr(W, SessId4, {uuid, FileUuid}, Xattr)),

    % Verification
    Ace1 = ?deny_group(GroupId4, ?read_metadata_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:get_xattr(W, SessId3, {uuid, FileUuid}, <<"XATTR_NAME">>)),

    Ace2 = ?allow_group(GroupId4, ?read_metadata_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace2])),
    ?assertEqual({ok, Xattr}, lfm_proxy:get_xattr(W, SessId3, {uuid, FileUuid}, <<"XATTR_NAME">>)).

acl_write_metadata_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId4 = ?config({session_id, 4}, Config),
    UserId4 = ?config({user_id, 4}, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId4, <<"/spaces/space_name4/t23_file">>, 8#777)),
    Xattr = #xattr{name = <<"XATTR_NAME">>, value = <<42/integer>>},
    ?assertEqual(ok, lfm_proxy:set_xattr(W, SessId4, {uuid, FileUuid}, Xattr)),

    % Verification
    Ace1 = ?deny_user(UserId3, ?write_metadata_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:set_xattr(W, SessId3, {uuid, FileUuid}, Xattr)),

    Ace2 = ?allow_user(UserId3, ?write_metadata_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace2])),
    ?assertEqual(ok, lfm_proxy:set_xattr(W, SessId3, {uuid, FileUuid}, Xattr)),

    % Check if written metadata is present
    ?assertEqual({ok, Xattr}, lfm_proxy:get_xattr(W, SessId4, {uuid, FileUuid}, <<"XATTR_NAME">>)).

acl_write_metadata_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId4 = ?config({session_id, 4}, Config),
    UserId4 = ?config({user_id, 4}, Config),
    SessId3 = ?config({session_id, 3}, Config),
    [_, _, {GroupId3, _} | _] = ?config({groups, 1}, Config),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId4, <<"/spaces/space_name4/t24_file">>, 8#777)),
    Xattr = #xattr{name = <<"XATTR_NAME">>, value = <<42/integer>>},
    ?assertEqual(ok, lfm_proxy:set_xattr(W, SessId4, {uuid, FileUuid}, Xattr)),

    % Verification
    Ace1 = ?deny_group(GroupId3, ?write_metadata_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:set_xattr(W, SessId3, {uuid, FileUuid}, Xattr)),

    Ace2 = ?allow_group(GroupId3, ?write_metadata_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId4, {uuid, FileUuid}, [?acl_all(UserId4), Ace2])),
    ?assertEqual(ok, lfm_proxy:set_xattr(W, SessId3, {uuid, FileUuid}, Xattr)),

    % Check if written metadata is present
    ?assertEqual({ok, Xattr}, lfm_proxy:get_xattr(W, SessId4, {uuid, FileUuid}, <<"XATTR_NAME">>)).

acl_traverse_container_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    {_, Dir1Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId1, <<"/spaces/space_name3/t25_dir1">>, 8#777)),
    {_, Dir2Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId1, <<"/spaces/space_name3/t25_dir1/dir2">>, 8#777)),
    {ok, FileUuid} =
        lfm_proxy:create(W, SessId1, <<"/spaces/space_name3/t25_dir1/dir2/file">>, 8#777),
    {_, H1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {uuid, FileUuid}, write)),
    {_, 1} = ?assertMatch({ok, _}, lfm_proxy:write(W, H1, 0, <<255:8>>)),

    % Verification
    Ace1 = ?deny_user(UserId3, ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:ls(W, SessId3, {uuid, Dir2Uuid}, 0, 5)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:open(W, SessId3, {uuid, FileUuid}, read)),

    Ace2 = ?allow_user(UserId3, ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace2])),
    ?assertMatch({ok, _List}, lfm_proxy:ls(W, SessId3, {uuid, Dir2Uuid}, 0, 5)),
    {_, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId3, {uuid, FileUuid}, read)),
    ?assertEqual({ok, <<255:8>>}, lfm_proxy:read(W, H2, 0, 1)).

acl_traverse_container_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    SessId3 = ?config({session_id, 3}, Config),
    [_, _, {GroupId3, _} | _] = ?config({groups, 1}, Config),
    {_, Dir1Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId1, <<"/spaces/space_name3/t26_dir1">>, 8#777)),
    {_, Dir2Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId1, <<"/spaces/space_name3/t26_dir1/dir2">>, 8#777)),
    {ok, FileUuid} =
        lfm_proxy:create(W, SessId1, <<"/spaces/space_name3/t26_dir1/dir2/file">>, 8#777),
    {_, H1} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId1, {uuid, FileUuid}, write)),
    {_, 1} = ?assertMatch({ok, _}, lfm_proxy:write(W, H1, 0, <<255:8>>)),

    % Verification
    Ace1 = ?deny_group(GroupId3, ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:ls(W, SessId3, {uuid, Dir2Uuid}, 0, 5)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:open(W, SessId3, {uuid, FileUuid}, read)),

    Ace2 = ?allow_group(GroupId3, ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace2])),
    ?assertMatch({ok, _List}, lfm_proxy:ls(W, SessId3, {uuid, Dir2Uuid}, 0, 5)),
    {_, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W, SessId3, {uuid, FileUuid}, read)),
    ?assertEqual({ok, <<255:8>>}, lfm_proxy:read(W, H2, 0, 1)).

acl_delete_object_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId3, <<"/t27_dir">>, 8#777)),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId3, <<"/t27_dir/file">>, 8#777)),

    % Verification
    Ace1 = ?deny_user(UserId2, ?delete_object_mask),
    Ace2 = ?allow_user(UserId2, ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace1, Ace2])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:unlink(W, SessId2, {uuid, FileUuid})),

    Ace3 = ?allow_user(UserId2, ?delete_object_mask bor ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace3])),
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId2, {uuid, FileUuid})).

acl_delete_object_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    UserId3 = ?config({user_id, 3}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    [_, _, _, {GroupId4, _}] = ?config({groups, 1}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId3, <<"/t28_dir">>, 8#777)),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId3, <<"/t28_dir/file">>, 8#777)),

    % Verification
    Ace1 = ?allow_group(GroupId4, ?traverse_container_mask),
    Ace2 = ?deny_group(GroupId4, ?delete_object_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace1, Ace2])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:unlink(W, SessId2, {uuid, FileUuid})),

    Ace3 = ?allow_group(GroupId4, ?traverse_container_mask bor ?delete_object_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId3, {uuid, DirUuid}, [?acl_all(UserId3), Ace3])),
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId2, {uuid, FileUuid})).

acl_delete_subcontainer_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    SessId4 = ?config({session_id, 4}, Config),
    UserId4 = ?config({user_id, 4}, Config),
    {_, Dir1Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId1, <<"/spaces/space_name4/t29_dir1">>, 8#777)),
    {_, Dir2Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId1, <<"/spaces/space_name4/t29_dir1/dir2">>, 8#777)),

    % Verification
    Ace1 = ?deny_user(UserId4, ?delete_subcontainer_mask),
    Ace2 = ?allow_user(UserId4, ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace1, Ace2])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:unlink(W, SessId4, {uuid, Dir2Uuid})),

    Ace3 = ?allow_user(UserId4, ?delete_subcontainer_mask bor ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace3])),
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId4, {uuid, Dir2Uuid})).

acl_delete_subcontainer_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    SessId4 = ?config({session_id, 4}, Config),
    [_, _, _, {GroupId4, _}] = ?config({groups, 1}, Config),
    {_, Dir1Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId1, <<"/spaces/space_name4/t30_dir1">>, 8#777)),
    {_, Dir2Uuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId1, <<"/spaces/space_name4/t30_dir1/dir2">>, 8#777)),

    % Verification
    Ace1 = ?deny_group(GroupId4, ?delete_subcontainer_mask),
    Ace2 = ?allow_group(GroupId4, ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace1, Ace2])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:unlink(W, SessId4, {uuid, Dir2Uuid})),

    Ace3 = ?allow_group(GroupId4, ?delete_subcontainer_mask bor ?traverse_container_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId1, {uuid, Dir1Uuid}, [?acl_all(UserId1), Ace3])),
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId4, {uuid, Dir2Uuid})).

acl_read_attributes_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/t31_file">>, 8#777)),
    ?assertEqual(ok, lfm_proxy:set_transfer_encoding(W, SessId2, {uuid, FileUuid}, <<"base64">>)),
    ?assertEqual(ok, lfm_proxy:set_cdmi_completion_status(W, SessId2, {uuid, FileUuid}, <<"Completed">>)),
    ?assertEqual(ok, lfm_proxy:set_mimetype(W, SessId2, {uuid, FileUuid}, <<"text/html">>)),

    % Verification
    Ace1 = ?deny_user(UserId1, ?read_attributes_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:get_transfer_encoding(W, SessId1, {uuid, FileUuid})),
    ?assertEqual({error, ?EACCES}, lfm_proxy:get_cdmi_completion_status(W, SessId1, {uuid, FileUuid})),
    ?assertEqual({error, ?EACCES}, lfm_proxy:get_mimetype(W, SessId1, {uuid, FileUuid})),

    Ace2 = ?allow_user(UserId1, ?read_attributes_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2])),
    ?assertEqual({ok, <<"base64">>}, lfm_proxy:get_transfer_encoding(W, SessId1, {uuid, FileUuid})),
    ?assertEqual({ok, <<"Completed">>}, lfm_proxy:get_cdmi_completion_status(W, SessId1, {uuid, FileUuid})),
    ?assertEqual({ok, <<"text/html">>}, lfm_proxy:get_mimetype(W, SessId1, {uuid, FileUuid})).

acl_read_attributes_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [{GroupId1, _} | _] = ?config({groups, 1}, Config),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/t32_file">>, 8#777)),
    ?assertEqual(ok, lfm_proxy:set_transfer_encoding(W, SessId2, {uuid, FileUuid}, <<"base64">>)),
    ?assertEqual(ok, lfm_proxy:set_cdmi_completion_status(W, SessId2, {uuid, FileUuid}, <<"Completed">>)),
    ?assertEqual(ok, lfm_proxy:set_mimetype(W, SessId2, {uuid, FileUuid}, <<"text/html">>)),

    % Verification
    Ace1 = ?deny_group(GroupId1, ?read_attributes_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:get_transfer_encoding(W, SessId1, {uuid, FileUuid})),
    ?assertEqual({error, ?EACCES}, lfm_proxy:get_cdmi_completion_status(W, SessId1, {uuid, FileUuid})),
    ?assertEqual({error, ?EACCES}, lfm_proxy:get_mimetype(W, SessId1, {uuid, FileUuid})),

    Ace2 = ?allow_group(GroupId1, ?read_attributes_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2])),
    ?assertEqual({ok, <<"base64">>}, lfm_proxy:get_transfer_encoding(W, SessId1, {uuid, FileUuid})),
    ?assertEqual({ok, <<"Completed">>}, lfm_proxy:get_cdmi_completion_status(W, SessId1, {uuid, FileUuid})),
    ?assertEqual({ok, <<"text/html">>}, lfm_proxy:get_mimetype(W, SessId1, {uuid, FileUuid})).

acl_write_attributes_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name4/t33_file">>, 8#777)),

    % Verification
    Ace1 = ?deny_user(UserId1, ?write_attributes_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:set_transfer_encoding(W, SessId1, {uuid, FileUuid}, <<"base64">>)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:set_cdmi_completion_status(W, SessId1, {uuid, FileUuid}, <<"Completed">>)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:set_mimetype(W, SessId1, {uuid, FileUuid}, <<"text/html">>)),

    Ace2 = ?allow_user(UserId1, ?write_attributes_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2])),
    ?assertEqual(ok, lfm_proxy:set_transfer_encoding(W, SessId1, {uuid, FileUuid}, <<"base64">>)),
    ?assertEqual(ok, lfm_proxy:set_cdmi_completion_status(W, SessId1, {uuid, FileUuid}, <<"Completed">>)),
    ?assertEqual(ok, lfm_proxy:set_mimetype(W, SessId1, {uuid, FileUuid}, <<"text/html">>)),

    % Check if written attributes are present
    ?assertEqual({ok, <<"base64">>}, lfm_proxy:get_transfer_encoding(W, SessId2, {uuid, FileUuid})),
    ?assertEqual({ok, <<"Completed">>}, lfm_proxy:get_cdmi_completion_status(W, SessId2, {uuid, FileUuid})),
    ?assertEqual({ok, <<"text/html">>}, lfm_proxy:get_mimetype(W, SessId2, {uuid, FileUuid})).

acl_write_attributes_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [_, _, _, {GroupId4, _}] = ?config({groups, 1}, Config),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name4/t34_file">>, 8#777)),

    % Verification
    Ace1 = ?deny_group(GroupId4, ?write_attributes_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:set_transfer_encoding(W, SessId1, {uuid, FileUuid}, <<"base64">>)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:set_cdmi_completion_status(W, SessId1, {uuid, FileUuid}, <<"Completed">>)),
    ?assertEqual({error, ?EACCES}, lfm_proxy:set_mimetype(W, SessId1, {uuid, FileUuid}, <<"text/html">>)),

    Ace2 = ?allow_group(GroupId4, ?write_attributes_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2])),
    ?assertEqual(ok, lfm_proxy:set_transfer_encoding(W, SessId1, {uuid, FileUuid}, <<"base64">>)),
    ?assertEqual(ok, lfm_proxy:set_cdmi_completion_status(W, SessId1, {uuid, FileUuid}, <<"Completed">>)),
    ?assertEqual(ok, lfm_proxy:set_mimetype(W, SessId1, {uuid, FileUuid}, <<"text/html">>)),

    % Check if written attributes are present
    ?assertEqual({ok, <<"base64">>}, lfm_proxy:get_transfer_encoding(W, SessId2, {uuid, FileUuid})),
    ?assertEqual({ok, <<"Completed">>}, lfm_proxy:get_cdmi_completion_status(W, SessId2, {uuid, FileUuid})),
    ?assertEqual({ok, <<"text/html">>}, lfm_proxy:get_mimetype(W, SessId2, {uuid, FileUuid})).

acl_delete_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId2, <<"/t35_dir">>, 8#777)),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/t35_file">>, 8#777)),

    % Verification
    Ace1 = ?deny_user(UserId1, ?delete_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:unlink(W, SessId1, {uuid, DirUuid})),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:unlink(W, SessId1, {uuid, FileUuid})),

    Ace2 = ?allow_user(UserId1, ?delete_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2])),
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId1, {uuid, DirUuid})),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2])),
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId1, {uuid, FileUuid})).

acl_delete_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [{GroupId1, _} | _] = ?config({groups, 1}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId2, <<"/t36_dir">>, 8#777)),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/t36_file">>, 8#777)),

    % Verification
    Ace1 = ?deny_group(GroupId1, ?delete_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:unlink(W, SessId1, {uuid, DirUuid})),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:unlink(W, SessId1, {uuid, FileUuid})),

    Ace2 = ?allow_group(GroupId1, ?delete_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2])),
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId1, {uuid, DirUuid})),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2])),
    ?assertEqual(ok, lfm_proxy:unlink(W, SessId1, {uuid, FileUuid})).

acl_read_acl_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name2/t37_dir">>, 8#777)),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name2/t37_file">>, 8#777)),

    % Verification
    Ace1 = ?deny_user(UserId1, ?read_acl_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:get_acl(W, SessId1, {uuid, DirUuid})),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:get_acl(W, SessId1, {uuid, FileUuid})),

    Ace2 = ?allow_user(UserId1, ?read_acl_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2])),
    ?assertMatch({ok, _List}, lfm_proxy:get_acl(W, SessId1, {uuid, DirUuid})),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2])),
    ?assertMatch({ok, _List}, lfm_proxy:get_acl(W, SessId1, {uuid, FileUuid})).

acl_read_acl_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [_, _, {GroupId3, _} | _] = ?config({groups, 1}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name2/t38_dir">>, 8#777)),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name2/t38_file">>, 8#777)),

    % Verification
    Ace1 = ?deny_group(GroupId3, ?read_acl_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:get_acl(W, SessId1, {uuid, DirUuid})),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:get_acl(W, SessId1, {uuid, FileUuid})),

    Ace2 = ?allow_group(GroupId3, ?read_acl_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2])),
    ?assertMatch({ok, _List}, lfm_proxy:get_acl(W, SessId1, {uuid, DirUuid})),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2])),
    ?assertMatch({ok, _List}, lfm_proxy:get_acl(W, SessId1, {uuid, FileUuid})).

acl_write_acl_user_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    UserId1 = ?config({user_id, 1}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name3/t39_dir">>, 8#777)),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/t39_file">>, 8#777)),

    % Verification
    Ace1 = ?deny_user(UserId1, ?write_acl_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:set_acl(W, SessId1, {uuid, DirUuid}, [])),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:set_acl(W, SessId1, {uuid, FileUuid}, [])),

    Ace2 = ?allow_user(UserId1, ?write_acl_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2])),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId1, {uuid, DirUuid}, [])),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2])),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId1, {uuid, FileUuid}, [])).

acl_write_acl_group_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId2 = ?config({session_id, 2}, Config),
    UserId2 = ?config({user_id, 2}, Config),
    SessId1 = ?config({session_id, 1}, Config),
    [_, _, {GroupId3, _} | _] = ?config({groups, 1}, Config),
    {_, DirUuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId2, <<"/spaces/space_name3/t40_dir">>, 8#777)),
    {_, FileUuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId2, <<"/spaces/space_name3/t40_file">>, 8#777)),

    % Verification
    Ace1 = ?deny_group(GroupId3, ?write_acl_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:set_acl(W, SessId1, {uuid, DirUuid}, [])),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace1])),
    ?assertEqual({error, ?EACCES}, lfm_proxy:set_acl(W, SessId1, {uuid, FileUuid}, [])),

    Ace2 = ?allow_group(GroupId3, ?write_acl_mask),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, DirUuid}, [?acl_all(UserId2), Ace2])),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId1, {uuid, DirUuid}, [])),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId2, {uuid, FileUuid}, [?acl_all(UserId2), Ace2])),
    ?assertEqual(ok, lfm_proxy:set_acl(W, SessId1, {uuid, FileUuid}, [])).

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
    initializer:clean_test_users_and_spaces_no_validate(Config).
