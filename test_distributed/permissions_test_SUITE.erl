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
    posix_execute_dir_group_test/1
%%    acl_read_object_user_test/1,
%%    acl_read_object_group_test/1,
%%    acl_list_container_user_test/1,
%%    acl_list_container_group_test/1,
%%    acl_write_object_user_test/1,
%%    acl_write_object_group_test/1,
%%    acl_add_object_user_test/1,
%%    acl_add_object_group_test/1,
%%    acl_append_data_user_test/1,
%%    acl_append_data_group_test/1,
%%    acl_add_subcontainer_user_test/1,
%%    acl_add_subcontainer_group_test/1,
%%    acl_read_metadata_user_test/1,
%%    acl_read_metadata_group_test/1,
%%    acl_write_metadata_user_test/1,
%%    acl_write_metadata_group_test/1,
%%    acl_traverse_container_user_test/1,
%%    acl_traverse_container_group_test/1,
%%    acl_delete_object_user_test/1,
%%    acl_delete_object_group_test/1,
%%    acl_delete_subcontainer_user_test/1,
%%    acl_delete_subcontainer_group_test/1,
%%    acl_read_attributes_user_test/1,
%%    acl_read_attributes_group_test/1,
%%    acl_write_attributes_user_test/1,
%%    acl_write_attributes_group_test/1,
%%    acl_delete_user_test/1,
%%    acl_delete_group_test/1,
%%    acl_read_acl_user_test/1,
%%    acl_read_acl_group_test/1,
%%    acl_write_acl_user_test/1,
%%    acl_write_acl_group_test/1,
%%    acl_write_owner_user_test/1,
%%    acl_write_owner_group_test/1
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
        posix_execute_dir_group_test
%%        acl_read_object_user_test,
%%        acl_read_object_group_test,
%%        acl_list_container_user_test,
%%        acl_list_container_group_test,
%%        acl_write_object_user_test,
%%        acl_write_object_group_test,
%%        acl_add_object_user_test,
%%        acl_add_object_group_test,
%%        acl_append_data_user_test,
%%        acl_append_data_group_test,
%%        acl_add_subcontainer_user_test,
%%        acl_add_subcontainer_group_test,
%%        acl_read_metadata_user_test,
%%        acl_read_metadata_group_test,
%%        acl_write_metadata_user_test,
%%        acl_write_metadata_group_test,
%%        acl_traverse_container_user_test,
%%        acl_traverse_container_group_test,
%%        acl_delete_object_user_test,
%%        acl_delete_object_group_test,
%%        acl_delete_subcontainer_user_test,
%%        acl_delete_subcontainer_group_test,
%%        acl_read_attributes_user_test,
%%        acl_read_attributes_group_test,
%%        acl_write_attributes_user_test,
%%        acl_write_attributes_group_test,
%%        acl_delete_user_test,
%%        acl_delete_group_test,
%%        acl_read_acl_user_test,
%%        acl_read_acl_group_test,
%%        acl_write_acl_user_test,
%%        acl_write_acl_group_test,
%%        acl_write_owner_user_test,
%%        acl_write_owner_group_test
    ].


%%%===================================================================
%%% Test functions
%%%===================================================================

% Write some description
%%list_dir_test(Config) ->
%%    [W | _] = ?config(op_worker_nodes, Config),
%%    SessId1 = ?config({session_id, 1}, Config),
%%    UserId1 = ?config({user_id, 1}, Config),
%%    UserName1 = ?config({user_name, 1}, Config),
%%    SessId2 = ?config({session_id, 2}, Config),
%%    UserId2 = ?config({user_id, 2}, Config),
%%    UserName2 = ?config({user_name, 2}, Config),
%%    [{GroupId1, GroupName1}, {GroupId2, GroupName2}, {GroupId3, GroupName3}, {GroupId4, GroupName4}] =
%%        ?config({groups, 1}, Config),
%%
%%    tracer:start(W),
%%    tracer:trace_calls(logical_file_manager),
%%    tracer:trace_calls(lfm_proxy),
%%    tracer:stop().

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

%%    ok = lfm_proxy:set_perms(W, SessId2, {uuid, DirUuid}, 8#470),
    ok = lfm_proxy:set_perms(W, SessId2, {uuid, DirUuid}, 8#570),
    Ans2 = lfm_proxy:ls(W, SessId2, {uuid, DirUuid}, 5, 0),
    ?assertMatch({ok, [{FileUuid, _}]}, Ans2)
    % TODO: assert that file type can be checked?
    % TODO: assert that other file data cannot?
    .

posix_read_dir_group_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, 1}, Config),
    SessId2 = ?config({session_id, 2}, Config),
    {ok, DirUuid} = mkdir(W, SessId2, <<"/spaces/space_name2/dir">>, 8#730),
    {ok, FileUuid} =
        lfm_proxy:create(W, SessId2, <<"/spaces/space_name2/dir/file">>, 8#770),

    Ans1 = lfm_proxy:ls(W, SessId1, {uuid, DirUuid}, 5, 0),
    ?assertEqual({error, ?EACCES}, Ans1),

%%    ok = lfm_proxy:set_perms(W, SessId2, {uuid, DirUuid}, 8#740),
    ok = lfm_proxy:set_perms(W, SessId2, {uuid, DirUuid}, 8#750),
    Ans2 = lfm_proxy:ls(W, SessId1, {uuid, DirUuid}, 5, 0),
    ?assertMatch({ok, [{FileUuid, _}]}, Ans2)
    % TODO: assert that file type can be checked?
    % TODO: assert that other file data cannot?
    .

posix_write_dir_user_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId3 = ?config({session_id, 3}, Config),
    {ok, DirUuid} = mkdir(W, SessId3, <<"/dir">>, 8#770),
    {ok, File1Uuid} = lfm_proxy:create(W, SessId3, <<"/dir/file1">>, 8#770),
    ok = lfm_proxy:set_perms(W, SessId3, {uuid, DirUuid}, 8#570),

    Ans1 = lfm_proxy:create(W, SessId3, <<"/dir/file2">>, 8#770),
    ?assertEqual({error, ?EACCES}, Ans1),
    Ans2 = lfm_proxy:unlink(W, SessId3, {uuid, File1Uuid}),
    ?assertEqual({error, ?EACCES}, Ans2),
    % TODO: assert eacces on mv when implemented

    ok = lfm_proxy:set_perms(W, SessId3, {uuid, DirUuid}, 8#370),
    Ans3 = lfm_proxy:create(W, SessId3, <<"/dir/file2">>, 8#770),
    ?assertMatch({ok, _Uuid}, Ans3),
    Ans4 = lfm_proxy:unlink(W, SessId3, {uuid, File1Uuid}),
    ?assertEqual(ok, Ans4)
    % TODO: assert ok on mv when implemented
    .

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
    Ans2 = lfm_proxy:unlink(W, SessId1, {uuid, File1Uuid}),
    ?assertEqual({error, ?EACCES}, Ans2),
    % TODO: assert eacces on mv when implemented

    ok = lfm_proxy:set_perms(W, SessId3, {uuid, DirUuid}, 8#730),
    Ans3 =
        lfm_proxy:create(W, SessId1, <<"/spaces/space_name3/dir/file2">>, 8#770),
    ?assertMatch({ok, _Uuid}, Ans3),
    Ans4 = lfm_proxy:unlink(W, SessId1, {uuid, File1Uuid}),
    ?assertEqual(ok, Ans4)
    % TODO: assert ok on mv when implemented
    .

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

%%acl_read_object_user_test(Config) -> ok.
%%acl_read_object_group_test(Config) -> ok.
%%acl_list_container_user_test(Config) -> ok.
%%acl_list_container_group_test(Config) -> ok.
%%acl_write_object_user_test(Config) -> ok.
%%acl_write_object_group_test(Config) -> ok.
%%acl_add_object_user_test(Config) -> ok.
%%acl_add_object_group_test(Config) -> ok.
%%acl_append_data_user_test(Config) -> ok.
%%acl_append_data_group_test(Config) -> ok.
%%acl_add_subcontainer_user_test(Config) -> ok.
%%acl_add_subcontainer_group_test(Config) -> ok.
%%acl_read_metadata_user_test(Config) -> ok.
%%acl_read_metadata_group_test(Config) -> ok.
%%acl_write_metadata_user_test(Config) -> ok.
%%acl_write_metadata_group_test(Config) -> ok.
%%acl_traverse_container_user_test(Config) -> ok.
%%acl_traverse_container_group_test(Config) -> ok.
%%acl_delete_object_user_test(Config) -> ok.
%%acl_delete_object_group_test(Config) -> ok.
%%acl_delete_subcontainer_user_test(Config) -> ok.
%%acl_delete_subcontainer_group_test(Config) -> ok.
%%acl_read_attributes_user_test(Config) -> ok.
%%acl_read_attributes_group_test(Config) -> ok.
%%acl_write_attributes_user_test(Config) -> ok.
%%acl_write_attributes_group_test(Config) -> ok.
%%acl_delete_user_test(Config) -> ok.
%%acl_delete_group_test(Config) -> ok.
%%acl_read_acl_user_test(Config) -> ok.
%%acl_read_acl_group_test(Config) -> ok.
%%acl_write_acl_user_test(Config) -> ok.
%%acl_write_acl_group_test(Config) -> ok.
%%acl_write_owner_user_test(Config) -> ok.
%%acl_write_owner_group_test(Config) -> ok.


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
