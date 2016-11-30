%%%--------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests file deletion.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_test_SUITE).
-author("Michal Wrona").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    simple_file_import_test/1
]).

-define(TEST_CASES, [
    simple_file_import_test
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
    [W1, W2] = Workers = ?config(op_worker_nodes, Config),

    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),

    tracer:start(Workers),

    {ok, _} = lfm_proxy:create(W1, SessId, <<"/space_name1/test1">>, 8#777),

    {ok, [W1Storage | _]} = rpc:call(W1, storage, list, []),
    {ok, [W2Storage | _]} = rpc:call(W2, storage, list, []),

    %% Enable import
    #document{key = W1StorageId, value = #storage{helpers = [W1Helpers]}} = W1Storage,
    {ok, _} = rpc:call(W1, space_strategies, set_strategy, [
        <<"space1">>, W1StorageId, storage_import, bfs_scan, #{scan_interval => 10}]),

    #helper_init{args = #{<<"root_path">> := W1MountPoint}} = W1Helpers,

    %% Create dir on storage
    ok = rpc:call(W1, file, make_dir, [<<W1MountPoint/binary, "/space1/", "test_dir">>]),

    %% Create file on storage
    TestFilePath = <<"/space_name1/test_dir/test_file">>,
    ok = rpc:call(W1, file, write_file, [<<W1MountPoint/binary, "/space1/", "test_dir/test_file">>, <<"test">>]),

    timer:sleep(timer:seconds(15)),

    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, <<"/space_name1/test_dir">>})),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, TestFilePath})),
    OpenRes1 = lfm_proxy:open(W1, SessId, {path, TestFilePath}, rdwr),
    ?assertMatch({ok, _}, OpenRes1),
    {ok, Handle1} = OpenRes1,
    ?assertMatch({ok, <<"test">>}, lfm_proxy:read(W1, Handle1, 0, 4)),

    ok.


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
%%    initializer:enable_grpca_based_communication(Config),
    initializer:disable_quota_limit(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    initializer:enable_grpca_based_communication(Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(Case, Config) ->
    ?CASE_STOP(Case),
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
%%    initializer:disable_grpca_based_communication(Config),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

