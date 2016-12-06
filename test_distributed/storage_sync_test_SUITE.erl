%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests file deletion.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_test_SUITE).
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    simple_file_import_test/1,
    simple_file_export_test/1
]).

-define(TEST_CASES, [
    simple_file_import_test,
    simple_file_export_test
]).

all() -> ?ALL(?TEST_CASES).

-define(FILE_UUID, <<"file_uuid">>).
-define(SESSION_ID_1, <<"session_id_1">>).
-define(SESSION_ID_2, <<"session_id_2">>).

-define(req(W, P), rpc:call(W, worker_proxy, call, [file_deletion_worker, P])).


%%%===================================================================
%%% Test functions
%%%===================================================================

simple_file_import_test(Config) ->
    [W1, _W2] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),

    %% Create dir and file on storage
    ok = rpc:call(W1, file, make_dir, [<<W1MountPoint/binary, "/space1/", "test_dir">>]),
    TestFilePath = <<"/space_name1/test_dir/test_file">>,
    ok = rpc:call(W1, file, write_file, [<<W1MountPoint/binary, "/space1/", "test_dir/test_file">>, <<"test">>]),
    timer:sleep(timer:seconds(15)),

    %% Check if dir and file were imported
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, <<"/space_name1/test_dir">>})),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, TestFilePath})),
    OpenRes1 = lfm_proxy:open(W1, SessId, {path, TestFilePath}, rdwr),
    ?assertMatch({ok, _}, OpenRes1),
    {ok, Handle1} = OpenRes1,
    ?assertMatch({ok, <<"test">>}, lfm_proxy:read(W1, Handle1, 0, 4)).

simple_file_export_test(Config) ->
    [W1, _W2] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    TestData = <<"test_data">>,

    % Create dir and file in space
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, <<"/space_name1/exported_dir">>)),
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, <<"/space_name1/exported_dir/exported_file1">>, 8#777)),
    ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, <<"/space_name1/exported_file2">>, 8#777)),
    {ok, FileHandle} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, FileGuid}, write)),
    ?assertEqual({ok, 9}, lfm_proxy:write(W1, FileHandle, 0, TestData)),
    ?assertEqual(ok, lfm_proxy:fsync(W1, FileHandle)),

    % Check if dir and file were exported
    ?assert(rpc:call(W1, filelib, is_dir, [<<W1MountPoint/binary, "/space1/exported_dir">>])),
    ?assert(rpc:call(W1, filelib, is_file, [<<W1MountPoint/binary, "/space1/exported_dir/exported_file1">>])),
    ?assert(rpc:call(W1, filelib, is_file, [<<W1MountPoint/binary, "/space1/exported_file2">>])),
    ?assertEqual({ok, TestData}, rpc:call(W1, file, read_file, [<<W1MountPoint/binary, "/space1/exported_dir/exported_file1">>])).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]).

end_per_suite(Config) ->
    ?TEST_STOP(Config).

init_per_testcase(Case, Config) ->
    ?CASE_START(Case),
    ct:timetrap({minutes, 60}),
    application:start(etls),
    hackney:start(),
    initializer:disable_quota_limit(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    initializer:enable_grpca_based_communication(Config),
    ConfigWithProxy = lfm_proxy:init(ConfigWithSessionInfo),
    enable_storage_sync(ConfigWithProxy).

end_per_testcase(Case, Config) ->
    ?CASE_STOP(Case),
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:disable_grpca_based_communication(Config),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

enable_storage_sync(Config) ->
    case ?config(w1_mount_point, Config) of
        undefined ->
            [W1, W2] = ?config(op_worker_nodes, Config),
            SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
            {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/test1">>, 8#777),
            {ok, [W1Storage | _]} = rpc:call(W1, storage, list, []),

            %% Enable import
            #document{key = W1StorageId, value = #storage{helpers = [W1Helpers]}} = W1Storage,
            {ok, _} = rpc:call(W1, space_strategies, set_strategy, [
                <<"space1">>, W1StorageId, storage_import, bfs_scan, #{scan_interval => 10}]),
            #helper_init{args = #{<<"root_path">> := W1MountPoint}} = W1Helpers,
            [{w1_mount_point, W1MountPoint} | Config];
        _ ->
            Config
    end.
