%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of lfm symlink resolution API.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_symlinks_resolution_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/errors.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    rubbish_path_test/1,
    non_existent_path_test/1,
    path_with_file_in_the_middle_test/1,
    user_root_absolute_path_test/1,
    space_absolute_path_test/1,
    relative_path_test/1,
    path_with_dots_test/1,
    symlink_to_itself_test/1,
    symlink_loop_test/1,
    symlink_hops_limit_test/1
]).

all() -> [
    rubbish_path_test,
    non_existent_path_test,
    path_with_file_in_the_middle_test,
    user_root_absolute_path_test,
    space_absolute_path_test,
    relative_path_test,
    path_with_dots_test,
    symlink_to_itself_test,
    symlink_loop_test,
    symlink_hops_limit_test
].


-define(SPACE_ID, <<"space_id1">>).
-define(SPACE_NAME, <<"space_name1">>).
-define(SPACE_ID_PATH_PREFIX(__SPACE_ID),
    <<"<__onedata_space_id:", (__SPACE_ID)/binary, ">">>
).
-define(SPACE_ID_PATH_PREFIX, ?SPACE_ID_PATH_PREFIX(?SPACE_ID)).


%%%====================================================================
%%% Test function
%%%====================================================================


rubbish_path_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    SymlinkPath = filename:join(["/", ?SPACE_NAME, str_utils:rand_hex(10)]),
    SymlinkGuid = create_symlink(W, SessId, SymlinkPath, <<"rubbish<>!@#xd">>),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:resolve_symlink(W, SessId, {guid, SymlinkGuid})).


non_existent_path_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    SymlinkPath = filename:join(["/", ?SPACE_NAME, str_utils:rand_hex(10)]),
    SymlinkGuid = create_symlink(W, SessId, SymlinkPath, <<"a/b">>),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:resolve_symlink(W, SessId, {guid, SymlinkGuid})).


path_with_file_in_the_middle_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    DirName = str_utils:rand_hex(10),
    DirPath = filename:join(["/", ?SPACE_NAME, DirName]),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, DirPath)),
    FileName = str_utils:rand_hex(10),
    FilePath = filename:join([DirPath, FileName]),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, FilePath)),

    SymlinkPath = filename:join(["/", ?SPACE_NAME, str_utils:rand_hex(10)]),
    SymlinkGuid = create_symlink(W, SessId, SymlinkPath, filename:join([DirName, FileName, "file2"])),

    ?assertMatch({error, ?ENOTDIR}, lfm_proxy:resolve_symlink(W, SessId, {guid, SymlinkGuid})).


user_root_absolute_path_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    DirPath = filename:join(["/", ?SPACE_NAME, str_utils:rand_hex(10)]),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, DirPath)),
    FilePath = filename:join([DirPath, str_utils:rand_hex(10)]),
    ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, FilePath)),

    SymlinkPath = filename:join(["/", ?SPACE_NAME, str_utils:rand_hex(10)]),
    SymlinkGuid = create_symlink(W, SessId, SymlinkPath, FilePath),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:resolve_symlink(W, SessId, {guid, SymlinkGuid})).


space_absolute_path_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    DirName = str_utils:rand_hex(10),
    DirPath = filename:join(["/", ?SPACE_NAME, DirName]),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, DirPath)),
    FileName = str_utils:rand_hex(10),
    FilePath = filename:join([DirPath, FileName]),
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, FilePath)),

    SymlinkPath = filename:join([DirPath, str_utils:rand_hex(10)]),
    SymlinkTarget = filename:join([?SPACE_ID_PATH_PREFIX, DirName, FileName]),
    SymlinkGuid = create_symlink(W, SessId, SymlinkPath, SymlinkTarget),

    ?assertMatch({ok, FileGuid}, lfm_proxy:resolve_symlink(W, SessId, {guid, SymlinkGuid})).


relative_path_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    DirPath = filename:join(["/", ?SPACE_NAME, str_utils:rand_hex(10)]),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, DirPath)),
    FileName = str_utils:rand_hex(10),
    FilePath = filename:join([DirPath, FileName]),
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, FilePath)),

    SymlinkPath = filename:join([DirPath, str_utils:rand_hex(10)]),
    SymlinkGuid = create_symlink(W, SessId, SymlinkPath, FileName),

    ?assertMatch({ok, FileGuid}, lfm_proxy:resolve_symlink(W, SessId, {guid, SymlinkGuid})).


path_with_dots_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    DirName = str_utils:rand_hex(10),
    DirPath = filename:join(["/", ?SPACE_NAME, DirName]),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, DirPath)),
    FileName = str_utils:rand_hex(10),
    FilePath = filename:join([DirPath, FileName]),
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, FilePath)),

    Dir2Name = str_utils:rand_hex(10),
    Dir2Path = filename:join([DirPath, Dir2Name]),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, Dir2Path)),

    Symlink1Path = filename:join([Dir2Path, str_utils:rand_hex(10)]),
    Symlink1Target = filename:join(["..", FileName]),
    Symlink1Guid = create_symlink(W, SessId, Symlink1Path, Symlink1Target),

    ?assertMatch({ok, FileGuid}, lfm_proxy:resolve_symlink(W, SessId, {guid, Symlink1Guid})),

    Symlink2Path = filename:join([Dir2Path, str_utils:rand_hex(10)]),
    Symlink2Target = filename:join([".", "..", "..", ".", "..", "..", "..", DirName, FileName]),
    Symlink2Guid = create_symlink(W, SessId, Symlink2Path, Symlink2Target),

    ?assertMatch({ok, FileGuid}, lfm_proxy:resolve_symlink(W, SessId, {guid, Symlink2Guid})).


symlink_to_itself_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    SymlinkName = str_utils:rand_hex(10),
    SymlinkPath = filename:join(["/", ?SPACE_NAME, SymlinkName]),
    SymlinkGuid = create_symlink(W, SessId, SymlinkPath, SymlinkName),

    ?assertMatch({error, ?ELOOP}, lfm_proxy:resolve_symlink(W, SessId, {guid, SymlinkGuid})).


symlink_loop_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    Symlink1Name = str_utils:rand_hex(10),
    Symlink2Name = str_utils:rand_hex(10),
    Symlink3Name = str_utils:rand_hex(10),

    Symlink1Guid = create_symlink(W, SessId, filename:join(["/", ?SPACE_NAME, Symlink1Name]), Symlink2Name),
    Symlink2Guid = create_symlink(W, SessId, filename:join(["/", ?SPACE_NAME, Symlink2Name]), Symlink3Name),
    Symlink3Guid = create_symlink(W, SessId, filename:join(["/", ?SPACE_NAME, Symlink3Name]), Symlink1Name),

    ?assertMatch({error, ?ELOOP}, lfm_proxy:resolve_symlink(W, SessId, {guid, Symlink1Guid})),
    ?assertMatch({error, ?ELOOP}, lfm_proxy:resolve_symlink(W, SessId, {guid, Symlink2Guid})),
    ?assertMatch({error, ?ELOOP}, lfm_proxy:resolve_symlink(W, SessId, {guid, Symlink3Guid})).


symlink_hops_limit_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),

    DirName = str_utils:rand_hex(10),
    DirPath = filename:join(["/", ?SPACE_NAME, DirName]),
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, SessId, DirPath)),
    FileName = str_utils:rand_hex(10),
    FilePath = filename:join([DirPath, FileName]),
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, SessId, FilePath)),

    Symlink1Name = str_utils:rand_hex(10),
    Symlink1Path = filename:join(["/", ?SPACE_NAME, Symlink1Name]),
    Symlink1Target = filename:join([DirName, FileName]),
    Symlink1Guid = create_symlink(W, SessId, Symlink1Path, Symlink1Target),

    {_, [InvalidSymlink | ValidSymlinks]} = lists:foldl(fun(_, {PrevSymlinkName, Symlinks}) ->
        SymlinkName = str_utils:rand_hex(10),
        SymlinkPath = filename:join(["/", ?SPACE_NAME, SymlinkName]),
        SymlinkGuid = create_symlink(W, SessId, SymlinkPath, PrevSymlinkName),
        {SymlinkName, [SymlinkGuid | Symlinks]}
    end, {Symlink1Name, [Symlink1Guid]}, lists:seq(1, 40)),

    ?assertMatch({error, ?ELOOP}, lfm_proxy:resolve_symlink(W, SessId, {guid, InvalidSymlink})),

    lists:foreach(fun(SymlinkGuid) ->
        ?assertMatch({ok, FileGuid}, lfm_proxy:resolve_symlink(W, SessId, {guid, SymlinkGuid}))
    end, ValidSymlinks).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_symlink(node(), session:id(), file_meta:path(), binary()) ->
    file_id:file_guid().
create_symlink(Node, SessionId, SymlinkPath, SymlinkValue) ->
    {ok, #file_attr{guid = SymlinkGuid}} = ?assertMatch(
        {ok, #file_attr{type = ?SYMLINK_TYPE}},
        lfm_proxy:make_symlink(Node, SessionId, SymlinkPath, SymlinkValue)
    ),
    ?assert(fslogic_uuid:is_symlink_uuid(file_id:guid_to_uuid(SymlinkGuid))),

    SymlinkGuid.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        initializer:mock_auth_manager(NewConfig),
        initializer:setup_storage(NewConfig)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, pool_utils]} | Config].


end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    initializer:unmock_auth_manager(Config).


init_per_testcase(_Case, Config) ->
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config).
