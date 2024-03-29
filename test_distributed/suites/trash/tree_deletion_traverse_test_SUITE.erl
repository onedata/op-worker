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
-include("modules/logical_file_manager/lfm.hrl").
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
    delete_tree_test/1,
    backward_time_warp_test/1,
    forward_time_warp_smaller_than_7_days_test/1,
    forward_time_warp_greater_than_7_days_test/1
]).


all() -> ?ALL([
    delete_empty_dir_test,
    delete_regular_files_test,
    delete_empty_dirs_test,
    delete_empty_dirs2_test,
    delete_tree_test,
    backward_time_warp_test,
    forward_time_warp_smaller_than_7_days_test,
    forward_time_warp_greater_than_7_days_test
]).

-define(SPACE_PLACEHOLDER, space1).
-define(SPACE_ID, oct_background:get_space_id(?SPACE_PLACEHOLDER)).
-define(SPACE_UUID, ?SPACE_UUID(?SPACE_ID)).
-define(SPACE_UUID(SpaceId), fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId)).
-define(SPACE_GUID, ?SPACE_GUID(?SPACE_ID)).
-define(SPACE_GUID(SpaceId), fslogic_file_id:spaceid_to_space_dir_guid(SpaceId)).
-define(ATTEMPTS, 30).
-define(RAND_DIR_NAME, <<"dir_", (integer_to_binary(rand:uniform(1000)))/binary>>).


%%%===================================================================
%%% Test functions
%%%===================================================================

delete_empty_dir_test(Config) ->
    delete_files_structure_test_base(Config, []).

delete_regular_files_test(Config) ->
    delete_files_structure_test_base(Config, [{0, 1000}]).

delete_empty_dirs_test(Config) ->
    delete_files_structure_test_base(Config, [{1000, 0}]).

delete_empty_dirs2_test(Config) ->
    delete_files_structure_test_base(Config, [{10, 0}, {10, 0}, {10, 0}]).

delete_tree_test(Config) ->
    delete_files_structure_test_base(Config, [{10, 10}, {10, 10}, {10, 10}]).

backward_time_warp_test(Config) ->
    TimeWarp = - 3600 * 24 * 10, % -10 days
    delete_files_structure_test_base(Config, [{10, 10}, {10, 10}, {10, 10}], TimeWarp, success).

forward_time_warp_smaller_than_7_days_test(Config) ->
    [OZNode | _] = oct_background:get_zone_nodes(),
    {ok, MaxTemporaryTokenTTl} = test_utils:get_env(OZNode, oz_worker, max_temporary_token_ttl),
    TimeWarp = MaxTemporaryTokenTTl div 2,
    delete_files_structure_test_base(Config, [{10, 10}, {10, 10}, {10, 10}], TimeWarp, success).

forward_time_warp_greater_than_7_days_test(Config) ->
    [OZNode | _] = oct_background:get_zone_nodes(),
    {ok, MaxTemporaryTokenTTl} = test_utils:get_env(OZNode, oz_worker, max_temporary_token_ttl),
    TimeWarp = MaxTemporaryTokenTTl + 1,
    delete_files_structure_test_base(Config, [{10, 10}, {10, 10}, {10, 10}], TimeWarp, failure).

%%%===================================================================
%%% Test bases
%%%===================================================================

delete_files_structure_test_base(Config, FilesStructure) ->
    delete_files_structure_test_base(Config, FilesStructure, undefined, success).

delete_files_structure_test_base(Config, FilesStructure, TimeWarpSecs, ExpectedResult) ->
    % TimeWarpSecs arg allows to set period (and direction future/past as +/-)
    % of TimeWarp that occurs during traverse
    % if TimeWarpSecs is undefined, TimeWarp won't occur
    [P1Node] = oct_background:get_provider_nodes(krakow),

    mock_traverse_finished(P1Node, self()),
    DirName = ?RAND_DIR_NAME,
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    {ok, RootGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ?SPACE_GUID, DirName, ?DEFAULT_DIR_PERMS),
    {DirGuids, FileGuids} = lfm_test_utils:create_files_tree(P1Node, UserSessIdP1, FilesStructure, RootGuid),
    RootDirCtx = file_ctx:new_by_guid(RootGuid),
    UserCtx = rpc:call(P1Node, user_ctx, new, [UserSessIdP1]),

    {ok, TaskId} = rpc:call(P1Node, tree_deletion_traverse, start, [RootDirCtx, UserCtx, false, ?SPACE_UUID, DirName]),
    simulate_time_warp(Config, TimeWarpSecs),

    await_traverse_finished(TaskId),

    % below assertions are performed with ?ROOT_SESS_ID because user sessions may have expired
    case ExpectedResult of
        success ->
            % all files should have been deleted
            ?assertMatch({ok, []}, lfm_proxy:get_children(P1Node, ?ROOT_SESS_ID, ?FILE_REF(?SPACE_GUID), 0, 10000)),
            lists:foreach(fun(Guid) ->
                ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(P1Node, ?ROOT_SESS_ID, ?FILE_REF(Guid)))
            end, [RootGuid | DirGuids] ++ FileGuids);
        failure ->
            % failure was expected so there should be files which weren't deleted
            AllFilesNum = length([RootGuid | DirGuids] ++ FileGuids),
            DeletedFilesNum = lists:foldl(fun(Guid, Acc) ->
                case lfm_proxy:stat(P1Node, ?ROOT_SESS_ID, ?FILE_REF(Guid)) of
                    {ok, _} -> Acc;
                    {error, ?ENOENT} -> Acc + 1
                end
            end, 0, [RootGuid | DirGuids] ++ FileGuids),
            ?assertNotEqual(AllFilesNum, DeletedFilesNum)
    end.

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{onenv_scenario = "2op-manual-import"}).

end_per_suite(_Config) ->
    oct_background:end_per_suite().

init_per_testcase(_Case, Config) ->
    % update background config to update sessions
    Config2 = oct_background:update_background_config(Config),
    lfm_proxy:init(Config2).

end_per_testcase(_Case, Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    AllNodes = [P1Node, P2Node],
    time_test_utils:unfreeze_time(Config),
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
    end),
    ok = test_utils:mock_expect(Worker, tree_deletion_traverse, task_canceled, fun(TaskId, Pool) ->
        Result = meck:passthrough([TaskId, Pool]),
        TestProcess ! {traverse_finished, TaskId},
        Result
    end).

await_traverse_finished(TaskId) ->
    receive {traverse_finished, TaskId} -> ok end.


simulate_time_warp(_Config, undefined) ->
    ok;
simulate_time_warp(Config, Seconds) ->
    time_test_utils:freeze_time(Config),
    time_test_utils:simulate_seconds_passing(Seconds).