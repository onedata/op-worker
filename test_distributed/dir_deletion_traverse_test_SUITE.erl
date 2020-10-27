%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(dir_deletion_traverse_test_SUITE).
-author("Jakub Kudzia").

-include("lfm_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/traverse/storage_traverse.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% exported for CT
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    delete_regular_files_test/1,
    delete_empty_dirs_test/1,
    delete_empty_dirs2_test/1,
    delete_tree_test/1
]).


all() -> ?ALL([
    delete_regular_files_test
%%    ,
%%    delete_empty_dirs_test
%%    ,
%%    delete_empty_dirs2_test % todo debug bez 2 testu
%%    ,
%%    delete_tree_test
]).

-define(SPACE_ID, <<"space1">>).
-define(SPACE_GUID, ?SPACE_GUID(?SPACE_ID)).
-define(SPACE_GUID(SpaceId), fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)).
-define(USER1, <<"user1">>).
-define(SESS_ID(Worker, Config), ?SESS_ID(?USER1, Worker, Config)).
-define(ATTEMPTS, 30).
-define(RAND_DIR_NAME, <<"dir_", (integer_to_binary(rand:uniform(1000)))/binary>>).


%%%===================================================================
%%% Test functions
%%%===================================================================

% TODO ogarnac bledy w logach
% todo usuwwanie katalogw fchybamusi byc na sam koniec idac do gory
% jakie powinny tu byc testy?
% mechanizm do zabezpoieczenia itd itp

% TODO zabezpieczyc usuwania katalogu spejsa

delete_regular_files_test(Config) ->
%%   delete_files_structure_test_base(Config, [{0, 1}]).
%%   delete_files_structure_test_base(Config, [{0, 100}]).
   delete_files_structure_test_base(Config, [{0, 302}]).
%%   delete_files_structure_test_base(Config, [{0, 10000}]).

delete_empty_dirs_test(Config) ->
%%    delete_files_structure_test_base(Config, [{1, 0}]).
%%    delete_files_structure_test_base(Config, [{100, 0}]).
    delete_files_structure_test_base(Config, [{302, 0}]).
%%    delete_files_structure_test_base(Config, [{10000, 0}]).

delete_empty_dirs2_test(Config) ->
%%    delete_files_structure_test_base(Config, [{1, 0}, {1, 0}]).
%%    delete_files_structure_test_base(Config, [{10, 0}, {10, 0}]).
    delete_files_structure_test_base(Config, [{10, 0}, {10, 0}, {10, 0}]).
%%    delete_files_structure_test_base(Config, [{10, 0}, {10, 0}, {10, 0}, {10, 0}]).

delete_tree_test(Config) ->
    delete_files_structure_test_base(Config, [{10, 10}, {10, 10}]).
%%    delete_files_structure_test_base(Config, [{10, 10}, {10, 10}, {10, 10}]).
%%    delete_files_structure_test_base(Config, [{10, 10}, {10, 10}, {10, 10}, {10, 10}]).


%%%===================================================================
%%% Test bases
%%%===================================================================

delete_files_structure_test_base(Config, FilesStructure) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    mock_traverse_finished(W1, self()),
    DirName = ?RAND_DIR_NAME,
    ct:pal("DirName: ~p", [DirName]),
    {ok, RootGuid} = lfm_proxy:mkdir(W1, ?SESS_ID(W1, Config), ?SPACE_GUID, DirName, ?DEFAULT_DIR_PERMS),
    ct:pal("DirGuid = ~p.", [RootGuid]),
    {D, F} = lfm_test_utils:create_files_tree(W1, ?SESS_ID(W1, Config), FilesStructure, RootGuid),
    ct:pal("DirNum: ~p", [length(D)]),
    RootDirCtx = file_ctx:new_by_guid(RootGuid),
    UserCtx = rpc:call(W1, user_ctx, new, [?SESS_ID(W1, Config)]),

    {ok, C} = lfm_proxy:get_children(W1, ?SESS_ID(W1, Config), {guid, RootGuid}, 0, 1000),
    ct:pal("Children len: ~p", [length(C)]),


    {ok, TaskId} = rpc:call(W1, dir_deletion_traverse, start, [RootDirCtx, UserCtx]),
    await_traverse_finished(TaskId),


    try
        ?assertMatch({ok, []}, lfm_proxy:get_children(W1, ?SESS_ID(W1, Config), {guid, ?SPACE_GUID}, 0, 1000))
    catch
        E:R ->
            ct:pal("ASSERT FAILED: ~p", [{E, R}]),
            ct:timetrap({hours, 10}),
            ct:sleep({hours, 10})
    end,
    lists:foreach(fun(Guid) ->
        ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(W1, ?SESS_ID(W1, Config), {guid, Guid}))
    end, [RootGuid | D] ++ F),

    ct:pal("AFTER"),
    timer:sleep(timer:seconds(30)).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        initializer:mock_provider_ids(NewConfig),
        NewConfig2 = multi_provider_file_ops_test_base:init_env(NewConfig),
        sort_workers(NewConfig2)
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, ?MODULE]}
        | Config
    ].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    lfm_test_utils:clean_space(W1, ?SPACE_ID, ?ATTEMPTS),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).

mock_traverse_finished(Worker, TestProcess) ->
    ok = test_utils:mock_new(Worker, dir_deletion_traverse),
    ok = test_utils:mock_expect(Worker, dir_deletion_traverse, task_finished, fun(TaskId, Pool) ->
        Result = meck:passthrough([TaskId, Pool]),
        TestProcess ! {traverse_finished, TaskId},
        Result
    end).

await_traverse_finished(TaskId) ->
    receive {traverse_finished, TaskId} -> ok end.