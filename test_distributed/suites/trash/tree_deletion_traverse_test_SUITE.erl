%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of tree_deletion_traverse.
%%% @end
%%%-------------------------------------------------------------------
-module(tree_deletion_traverse_test_SUITE).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/traverse/storage_traverse.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/errors.hrl").


%% exported for CT
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    delete_empty_dir_test/1,
    delete_regular_files_test/1,
    delete_empty_dirs_test/1,
    delete_empty_dirs2_test/1,
    delete_tree_test/1
]).


all() -> ?ALL([
    delete_empty_dir_test,
    delete_regular_files_test,
    delete_empty_dirs_test,
    delete_empty_dirs2_test,
    delete_tree_test
]).

-define(SPACE_PLACEHOLDER, space1).
-define(SPACE_ID, oct_background:get_space_id(?SPACE_PLACEHOLDER)).
-define(SPACE_UUID, ?SPACE_UUID(?SPACE_ID)).
-define(SPACE_UUID(SpaceId), fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId)).
-define(SPACE_GUID, ?SPACE_GUID(?SPACE_ID)).
-define(SPACE_GUID(SpaceId), fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)).
-define(ATTEMPTS, 30).
-define(RAND_DIR_NAME, <<"dir_", (integer_to_binary(rand:uniform(1000)))/binary>>).


%%%===================================================================
%%% Test functions
%%%===================================================================

delete_empty_dir_test(_Config) ->
    delete_files_structure_test_base([]).

delete_regular_files_test(_Config) ->
    % TODO VFS-7101 increase number of file to 1000 after introducing offline access token
    delete_files_structure_test_base([{0, 100}]).

delete_empty_dirs_test(_Config) ->
    % TODO VFS-7101 increase number of file to 1000 after introducing offline access token
    delete_files_structure_test_base([{100, 0}]).

delete_empty_dirs2_test(_Config) ->
    % TODO VFS-7101 add one more level in the tree after introducing offline access token
    delete_files_structure_test_base([{10, 0}, {10, 0}]).

delete_tree_test(_Config) ->
    % TODO VFS-7101 add one more level in the tree after introducing offline access token
    delete_files_structure_test_base([{10, 10}, {10, 10}]).


%%%===================================================================
%%% Test bases
%%%===================================================================

delete_files_structure_test_base(FilesStructure) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),

    mock_traverse_finished(P1Node, self()),
    DirName = ?RAND_DIR_NAME,
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    {ok, RootGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ?SPACE_GUID, DirName, ?DEFAULT_DIR_PERMS),
    {DirGuids, FileGuids} = lfm_test_utils:create_files_tree(P1Node, UserSessIdP1, FilesStructure, RootGuid),
    RootDirCtx = file_ctx:new_by_guid(RootGuid),
    UserCtx = rpc:call(P1Node, user_ctx, new, [UserSessIdP1]),

    {ok, TaskId} = rpc:call(P1Node, tree_deletion_traverse, start, [RootDirCtx, UserCtx, false, ?SPACE_UUID]),
    await_traverse_finished(TaskId),

    ?assertMatch({ok, []}, lfm_proxy:get_children(P1Node, UserSessIdP1, {guid, ?SPACE_GUID}, 0, 10000)),
    lists:foreach(fun(Guid) ->
        ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(P1Node, UserSessIdP1, {guid, Guid}))
    end, [RootGuid | DirGuids] ++ FileGuids).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),
    oct_background:init_per_suite(Config, #onenv_test_config{onenv_scenario = "trash_tests"}).

end_per_suite(_Config) ->
    hackney:stop(),
    ssl:stop().

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    AllNodes = [P1Node, P2Node],
    lfm_test_utils:clean_space(P1Node, AllNodes, ?SPACE_ID, ?ATTEMPTS),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

mock_traverse_finished(Worker, TestProcess) ->
    ok = test_utils:mock_new(Worker, tree_deletion_traverse),
    ok = test_utils:mock_expect(Worker, tree_deletion_traverse, task_finished, fun(TaskId, Pool) ->
        Result = meck:passthrough([TaskId, Pool]),
        TestProcess ! {traverse_finished, TaskId},
        Result
    end).

await_traverse_finished(TaskId) ->
    receive {traverse_finished, TaskId} -> ok end.