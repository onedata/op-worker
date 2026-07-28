%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020-2026 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests which check whether files are created on
%%% storage with proper permissions and ownership, depending on the type
%%% of the supporting storage (posix/s3, plain/imported) and its LUMA DB
%%% feed (auto/external/local).
%%%
%%% Every test case runs against a matrix of spaces, each supported by
%%% a dedicated storage configured with one of the LUMA feed variants
%%% (see set_up_matrix_row/2). The external feed is served by a real HTTP
%%% service (luma_test_server) and the local feed is populated through the
%%% same code path as the onepanel LUMA REST API - LUMA DB is fed exactly
%%% the way it is in production.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_files_test_SUITE).
-author("Jakub Kudzia").

-include("space_setup_utils.hrl").
-include("storage_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    space_directory_mode_and_owner_test/1,
    regular_file_mode_and_owner_test/1,
    regular_file_custom_mode_and_owner_test/1,
    regular_file_unknown_owner_test/1,
    directory_mode_and_owner_test/1,
    directory_custom_mode_and_owner_test/1,
    directory_with_unknown_owner_test/1,
    rename_file_test/1,
    creating_file_should_result_in_eacces_when_mapping_is_not_found/1,
    remotely_updated_perms_should_be_updated_on_storage/1
]).

all() -> [
    space_directory_mode_and_owner_test,
    regular_file_mode_and_owner_test,
    regular_file_custom_mode_and_owner_test,
    regular_file_unknown_owner_test,
    directory_mode_and_owner_test,
    directory_custom_mode_and_owner_test,
    directory_with_unknown_owner_test,
    rename_file_test,
    creating_file_should_result_in_eacces_when_mapping_is_not_found,
    remotely_updated_perms_should_be_updated_on_storage
].

%% Matrix row names - they double as the names of the per-row spaces.
-define(POSIX_AUTO, posix_auto_feed).
-define(POSIX_EXTERNAL, posix_external_feed).
-define(POSIX_LOCAL, posix_local_feed).
-define(IMPORTED_POSIX_AUTO, imported_posix_auto_feed).
-define(IMPORTED_POSIX_EXTERNAL, imported_posix_external_feed).
-define(IMPORTED_POSIX_LOCAL, imported_posix_local_feed).
-define(S3_AUTO, s3_auto_feed).
-define(S3_EXTERNAL, s3_external_feed).
-define(S3_LOCAL, s3_local_feed).
-define(POSIX_EXTERNAL_NO_USER_MAPPINGS, posix_external_feed_without_user_mappings).

-spec matrix_rows() -> [row_name()].
matrix_rows() -> [
    ?POSIX_AUTO,
    ?POSIX_EXTERNAL,
    ?POSIX_LOCAL,
    ?IMPORTED_POSIX_AUTO,
    ?IMPORTED_POSIX_EXTERNAL,
    ?IMPORTED_POSIX_LOCAL,
    ?S3_AUTO,
    ?S3_EXTERNAL,
    ?S3_LOCAL,
    ?POSIX_EXTERNAL_NO_USER_MAPPINGS
].

-define(SUPPORT_SIZE, 10 * 1024 * 1024 * 1024).

%% Id under which files of a user that has never logged to Onezone may be chowned
%% (such files appear when synced from storage by a remote provider with reverse LUMA)
-define(UNKNOWN_USER_ID, <<"UNKNOWN OWNER ID">>).

%% Ranges used by the auto feed to generate posix identifiers (product defaults)
-define(UID_RANGE, {100000, 2000000}).
-define(GID_RANGE, {100000, 2000000}).

-define(OWNER(Uid, Gid), #{uid => Uid, gid => Gid}).
-define(ROOT_OWNER, ?OWNER(?ROOT_UID, ?ROOT_GID)).

-define(ASSERT_FILE_INFO(Expected, Node, FilePath),
    ?ASSERT_FILE_INFO(Expected, Node, FilePath, 0)
).
-define(ASSERT_FILE_INFO(Expected, Node, FilePath, Attempts),
    storage_test_utils:assert_file_info(Expected, Node, FilePath, ?LINE, Attempts)
).

-define(ATTEMPTS, 15).
-define(CLEAN_SPACE_ATTEMPTS, 30).
-define(SUB_RUN_TIMETRAP, {minutes, 5}).

%% Name of a matrix row - one of the macros above.
-type row_name() :: atom().
%% Everything the test cases need to know about a row's storage/space setup.
-type matrix_row() :: #{
    space_id := od_space:id(),
    storage_id := storage:id(),
    % set only for rows whose space is supported by paris as well
    paris_storage_id => storage:id(),
    feed := luma:feed(),
    posix := boolean()
}.
-type matrix() :: #{row_name() => matrix_row()}.
-type test_env() :: #{krk_node := node(), paris_node := node(), matrix := matrix()}.

%% ?ROOT_USER_ID is not an oct_background user, hence the extra 'root' selector.
-type user_selector() :: root | oct_background:entity_selector().
-type owner() :: #{uid := luma:uid(), gid := luma:gid()}.

%% Parameters and expectations of a single matrix sub-run.
-type setup() :: #{
    user => user_selector(),
    expected_owner => owner(),
    % owner expected after the deferred chown of a file of a user unknown so far
    expected_owner2 => owner(),
    expected_display_owner => owner(),
    file_perms => file_meta:mode(),
    dir_perms => file_meta:mode()
}.
-type setups_per_row() :: #{row_name() => [setup()]}.
-type test_fun() :: fun((TestName :: atom(), test_env(), row_name(), setup()) -> ok).


%%%===================================================================
%%% Test functions
%%%===================================================================


space_directory_mode_and_owner_test(Config) ->
    Env = test_env(Config),
    run_matrix(?FUNCTION_NAME, Env, fun space_directory_mode_and_owner_test_base/4,
        #{}, space_dir_ownership_setups(Env)).


regular_file_mode_and_owner_test(Config) ->
    Env = test_env(Config),
    run_matrix(?FUNCTION_NAME, Env, fun regular_file_mode_and_owner_test_base/4,
        #{file_perms => ?DEFAULT_FILE_PERMS}, file_ownership_setups(Env)).


regular_file_custom_mode_and_owner_test(Config) ->
    Env = test_env(Config),
    run_matrix(?FUNCTION_NAME, Env, fun regular_file_mode_and_owner_test_base/4,
        #{file_perms => 8#777}, file_ownership_setups(Env)).


regular_file_unknown_owner_test(Config) ->
    Env = test_env(Config),
    run_matrix(?FUNCTION_NAME, Env, fun regular_file_unknown_owner_test_base/4,
        #{file_perms => ?DEFAULT_FILE_PERMS}, unknown_owner_setups(Env)).


directory_mode_and_owner_test(Config) ->
    Env = test_env(Config),
    run_matrix(?FUNCTION_NAME, Env, fun directory_mode_and_owner_test_base/4,
        #{dir_perms => ?DEFAULT_DIR_PERMS}, file_ownership_setups(Env)).


directory_custom_mode_and_owner_test(Config) ->
    Env = test_env(Config),
    run_matrix(?FUNCTION_NAME, Env, fun directory_mode_and_owner_test_base/4,
        #{dir_perms => 8#777}, file_ownership_setups(Env)).


directory_with_unknown_owner_test(Config) ->
    Env = test_env(Config),
    run_matrix(?FUNCTION_NAME, Env, fun directory_with_unknown_owner_test_base/4,
        #{dir_perms => ?DEFAULT_DIR_PERMS}, unknown_owner_setups(Env)).


rename_file_test(Config) ->
    Env = test_env(Config),
    % not run as root - a path cannot be resolved in context of the special root session,
    % which mv requires
    SetupsWithoutRoot = maps:map(fun(_RowName, Setups) ->
        [Setup || Setup = #{user := User} <- Setups, User =/= root]
    end, file_ownership_setups(Env)),
    run_matrix(?FUNCTION_NAME, Env, fun rename_file_test_base/4,
        #{file_perms => ?DEFAULT_FILE_PERMS, dir_perms => ?DEFAULT_DIR_PERMS}, SetupsWithoutRoot).


creating_file_should_result_in_eacces_when_mapping_is_not_found(Config) ->
    Env = test_env(Config),
    run_matrix(?FUNCTION_NAME, Env, fun mapping_not_found_test_base/4, #{}, #{
        % user3 has no mapping defined in the feed, user4 has a malformed one
        ?POSIX_EXTERNAL => [#{user => user3}, #{user => user4}],
        ?POSIX_LOCAL => [#{user => user3}],
        ?IMPORTED_POSIX_EXTERNAL => [#{user => user3}, #{user => user4}],
        ?IMPORTED_POSIX_LOCAL => [#{user => user3}],
        ?S3_EXTERNAL => [#{user => user3}, #{user => user4}],
        ?S3_LOCAL => [#{user => user3}],
        ?POSIX_EXTERNAL_NO_USER_MAPPINGS => [#{user => user1}]
    }).


remotely_updated_perms_should_be_updated_on_storage(Config) ->
    Env = test_env(Config),
    run_matrix(?FUNCTION_NAME, Env, fun remotely_updated_perms_should_be_updated_on_storage_test_base/4,
        #{}, #{?POSIX_AUTO => [#{user => user1}]}).


%%%===================================================================
%%% Expected ownership matrices
%%%===================================================================


%% @private
%% @doc Per-row expected owners of the space directory. They do not depend on the
%% user performing the operations - each row is verified for all 3 user kinds only
%% to check that every session resolves the same view.
-spec space_dir_ownership_setups(test_env()) -> setups_per_row().
space_dir_ownership_setups(Env) ->
    RowExpectations = #{
        ?POSIX_AUTO => #{
            expected_owner => mount_dir_owner(Env, ?POSIX_AUTO),
            expected_display_owner => mount_dir_owner(Env, ?POSIX_AUTO)
        },
        ?POSIX_EXTERNAL => #{
            expected_owner => ?OWNER(2000, 2000),
            expected_display_owner => ?OWNER(2222, 2222)
        },
        ?POSIX_LOCAL => #{
            expected_owner => ?OWNER(3000, 3000),
            expected_display_owner => ?OWNER(3333, 3333)
        },
        % on imported storages the space dir is the storage mount dir itself - it is
        % not created by the provider, so it keeps the ownership and mode it was
        % set up with (the provider must not modify them)
        ?IMPORTED_POSIX_AUTO => #{
            expected_owner => mount_dir_owner(Env, ?IMPORTED_POSIX_AUTO),
            expected_mode => mount_dir_mode(Env, ?IMPORTED_POSIX_AUTO),
            expected_display_owner => mount_dir_owner(Env, ?IMPORTED_POSIX_AUTO)
        },
        ?IMPORTED_POSIX_EXTERNAL => #{
            expected_owner => mount_dir_owner(Env, ?IMPORTED_POSIX_EXTERNAL),
            expected_mode => mount_dir_mode(Env, ?IMPORTED_POSIX_EXTERNAL),
            expected_display_owner => ?OWNER(5555, 5555)
        },
        ?IMPORTED_POSIX_LOCAL => #{
            expected_owner => mount_dir_owner(Env, ?IMPORTED_POSIX_LOCAL),
            expected_mode => mount_dir_mode(Env, ?IMPORTED_POSIX_LOCAL),
            expected_display_owner => ?OWNER(6666, 6666)
        },
        ?S3_AUTO => #{
            expected_display_owner => generated_space_owner(Env, ?S3_AUTO)
        },
        ?S3_EXTERNAL => #{
            expected_display_owner => ?OWNER(8888, 8888)
        },
        ?S3_LOCAL => #{
            expected_display_owner => ?OWNER(9999, 9999)
        }
    },
    maps:map(fun(_RowName, Expectations) ->
        [Expectations#{user => User} || User <- [user1, user2, root]]
    end, RowExpectations).


%% @private
%% @doc Per-row, per-user expected owners of files and directories. Shared by the
%% regular file, directory and rename cases, which differ only in the perms used.
-spec file_ownership_setups(test_env()) -> setups_per_row().
file_ownership_setups(Env) ->
    RootSetup = #{
        user => root,
        expected_owner => ?ROOT_OWNER,
        expected_display_owner => ?ROOT_OWNER
    },
    RootDisplayOnlySetup = maps:remove(expected_owner, RootSetup),
    #{
        ?POSIX_AUTO => [
            #{user => user1,
                expected_owner => generated_owner(Env, ?POSIX_AUTO, user1),
                expected_display_owner => generated_owner(Env, ?POSIX_AUTO, user1)},
            #{user => user2,
                expected_owner => generated_owner(Env, ?POSIX_AUTO, user2),
                expected_display_owner => generated_owner(Env, ?POSIX_AUTO, user2)},
            RootSetup
        ],
        ?POSIX_EXTERNAL => [
            #{user => user1,
                expected_owner => ?OWNER(2001, 2000),
                expected_display_owner => ?OWNER(2221, 2222)},
            % no display uid defined in the feed - falls back to the storage uid
            #{user => user2,
                expected_owner => ?OWNER(2002, 2000),
                expected_display_owner => ?OWNER(2002, 2222)},
            RootSetup
        ],
        ?POSIX_LOCAL => [
            #{user => user1,
                expected_owner => ?OWNER(3001, 3000),
                expected_display_owner => ?OWNER(3331, 3333)},
            #{user => user2,
                expected_owner => ?OWNER(3002, 3000),
                expected_display_owner => ?OWNER(3002, 3333)},
            RootSetup
        ],
        ?IMPORTED_POSIX_AUTO => [
            #{user => user1,
                expected_owner => generated_owner(Env, ?IMPORTED_POSIX_AUTO, user1),
                expected_display_owner => generated_owner(Env, ?IMPORTED_POSIX_AUTO, user1)},
            #{user => user2,
                expected_owner => generated_owner(Env, ?IMPORTED_POSIX_AUTO, user2),
                expected_display_owner => generated_owner(Env, ?IMPORTED_POSIX_AUTO, user2)},
            RootSetup
        ],
        ?IMPORTED_POSIX_EXTERNAL => [
            % on imported storages the file gid stays the one of the mount dir
            #{user => user1,
                expected_owner => mount_dir_owner_with_uid(Env, ?IMPORTED_POSIX_EXTERNAL, 5001),
                expected_display_owner => ?OWNER(5551, 5555)},
            #{user => user2,
                expected_owner => mount_dir_owner_with_uid(Env, ?IMPORTED_POSIX_EXTERNAL, 5002),
                expected_display_owner => ?OWNER(5002, 5555)},
            RootSetup
        ],
        ?IMPORTED_POSIX_LOCAL => [
            #{user => user1,
                expected_owner => mount_dir_owner_with_uid(Env, ?IMPORTED_POSIX_LOCAL, 6001),
                expected_display_owner => ?OWNER(6661, 6666)},
            #{user => user2,
                expected_owner => mount_dir_owner_with_uid(Env, ?IMPORTED_POSIX_LOCAL, 6002),
                expected_display_owner => ?OWNER(6002, 6666)},
            RootSetup
        ],
        ?S3_AUTO => [
            #{user => user1, expected_display_owner => generated_display_owner(Env, ?S3_AUTO, user1)},
            #{user => user2, expected_display_owner => generated_display_owner(Env, ?S3_AUTO, user2)},
            RootDisplayOnlySetup
        ],
        ?S3_EXTERNAL => [
            #{user => user1, expected_display_owner => ?OWNER(8881, 8888)},
            % no display uid defined in the feed and no posix storage uid to fall
            % back to - the display uid is auto-generated
            #{user => user2, expected_display_owner => ?OWNER(generated_uid(user2), 8888)},
            RootDisplayOnlySetup
        ],
        ?S3_LOCAL => [
            #{user => user1, expected_display_owner => ?OWNER(9991, 9999)},
            #{user => user2, expected_display_owner => ?OWNER(generated_uid(user2), 9999)},
            RootDisplayOnlySetup
        ]
    }.


%% @private
%% @doc Rows with the auto feed are the only ones the unknown owner cases run on -
%% no mapping can be defined for the unknown user in a user defined feed.
-spec unknown_owner_setups(test_env()) -> setups_per_row().
unknown_owner_setups(Env) ->
    UnknownUserUid = generated_uid(?UNKNOWN_USER_ID),
    #{
        ?POSIX_AUTO => [#{
            expected_owner => mount_dir_owner(Env, ?POSIX_AUTO),
            expected_owner2 => mount_dir_owner_with_uid(Env, ?POSIX_AUTO, UnknownUserUid),
            expected_display_owner => mount_dir_owner_with_uid(Env, ?POSIX_AUTO, UnknownUserUid)
        }],
        ?IMPORTED_POSIX_AUTO => [#{
            expected_owner => mount_dir_owner(Env, ?IMPORTED_POSIX_AUTO),
            expected_owner2 => mount_dir_owner_with_uid(Env, ?IMPORTED_POSIX_AUTO, UnknownUserUid),
            expected_display_owner => mount_dir_owner_with_uid(Env, ?IMPORTED_POSIX_AUTO, UnknownUserUid)
        }],
        ?S3_AUTO => [#{
            expected_display_owner => ?OWNER(UnknownUserUid, generated_gid(space_id(Env, ?S3_AUTO)))
        }]
    }.


%%%===================================================================
%%% Test bases
%%%===================================================================


-spec space_directory_mode_and_owner_test_base(TestName :: atom(), test_env(), row_name(), setup()) ->
    ok | no_return().
space_directory_mode_and_owner_test_base(TestName, Env, RowName, Args) ->
    #{krk_node := KrkNode} = Env,
    SessId = session_id(maps:get(user, Args), krakow),
    SpaceId = space_id(Env, RowName),
    SpaceDirGuid = space_dir:guid(SpaceId),
    #{uid := ExpDisplayUid, gid := ExpDisplayGid} = maps:get(expected_display_owner, Args),

    % when
    {ok, _} = lfm_proxy:create_and_open(
        KrkNode, SessId, SpaceDirGuid, file_name(TestName), ?DEFAULT_FILE_PERMS
    ),

    % then
    % NOTE: unlike for regular directories, the mode of the space dir is reported
    % with the file type bits included
    ?assertMatch({ok, #file_attr{
        uid = ExpDisplayUid,
        gid = ExpDisplayGid,
        mode = ?DEFAULT_DIR_MODE
    }}, lfm_proxy:stat(KrkNode, SessId, ?FILE_REF(SpaceDirGuid))),

    exec_if_posix_row(Env, RowName, fun() ->
        SpacePath = storage_test_utils:space_path(KrkNode, SpaceId),
        ExpOwner = maps:get(expected_owner, Args),
        % all rows but the imported ones expect the mode the provider creates the
        % space dir with
        ExpMode = maps:get(expected_mode, Args, ?DIR_MODE(?DEFAULT_DIR_PERMS)),
        ?ASSERT_FILE_INFO(ExpOwner#{mode => ExpMode}, KrkNode, SpacePath)
    end).


-spec regular_file_mode_and_owner_test_base(TestName :: atom(), test_env(), row_name(), setup()) ->
    ok | no_return().
regular_file_mode_and_owner_test_base(TestName, Env, RowName, Args) ->
    #{krk_node := KrkNode} = Env,
    SessId = session_id(maps:get(user, Args), krakow),
    SpaceId = space_id(Env, RowName),
    FileName = file_name(TestName),
    FilePerms = maps:get(file_perms, Args),
    #{uid := ExpDisplayUid, gid := ExpDisplayGid} = maps:get(expected_display_owner, Args),

    % when
    {ok, {FileGuid, _}} = lfm_proxy:create_and_open(
        KrkNode, SessId, space_dir:guid(SpaceId), FileName, FilePerms
    ),

    % then
    ?assertMatch({ok, #file_attr{
        uid = ExpDisplayUid,
        gid = ExpDisplayGid,
        mode = FilePerms
    }}, lfm_proxy:stat(KrkNode, SessId, ?FILE_REF(FileGuid))),

    exec_if_posix_row(Env, RowName, fun() ->
        StorageFilePath = storage_test_utils:file_path(KrkNode, SpaceId, FileName),
        ExpOwner = maps:get(expected_owner, Args),
        ?ASSERT_FILE_INFO(ExpOwner#{mode => ?FILE_MODE(FilePerms)}, KrkNode, StorageFilePath)
    end).


-spec regular_file_unknown_owner_test_base(TestName :: atom(), test_env(), row_name(), setup()) ->
    ok | no_return().
regular_file_unknown_owner_test_base(TestName, Env, RowName, Args) ->
    #{krk_node := KrkNode} = Env,
    SessId = session_id(user1, krakow),
    SpaceId = space_id(Env, RowName),
    FileName = file_name(TestName),
    FilePerms = maps:get(file_perms, Args),
    #{uid := ExpDisplayUid, gid := ExpDisplayGid} = maps:get(expected_display_owner, Args),

    % when
    {ok, FileGuid} = lfm_proxy:create(KrkNode, SessId, space_dir:guid(SpaceId), FileName, FilePerms),
    {FileUuid, _} = file_id:unpack_guid(FileGuid),

    % pretend that the file belongs to an unknown user (not yet logged to Onezone) -
    % such file may occur when it was synced from storage in remote provider with
    % reverse LUMA
    {ok, _} = rpc:call(KrkNode, file_meta, update, [FileUuid, fun(FM) ->
        {ok, FM#file_meta{owner = ?UNKNOWN_USER_ID}}
    end]),

    {ok, _} = lfm_proxy:open(KrkNode, SessId, ?FILE_REF(FileGuid), read),

    % then
    ?assertMatch({ok, #file_attr{
        uid = ExpDisplayUid,
        gid = ExpDisplayGid,
        mode = FilePerms
    }}, lfm_proxy:stat(KrkNode, SessId, ?FILE_REF(FileGuid))),

    exec_if_posix_row(Env, RowName, fun() ->
        StorageFilePath = storage_test_utils:file_path(KrkNode, SpaceId, FileName),
        ExpOwner = maps:get(expected_owner, Args),
        ?ASSERT_FILE_INFO(ExpOwner#{mode => ?FILE_MODE(FilePerms)}, KrkNode, StorageFilePath),

        % pretend that the unknown user logged to Onezone
        ok = rpc:call(KrkNode, files_to_chown, chown_deferred_files, [?UNKNOWN_USER_ID]),
        ExpOwner2 = maps:get(expected_owner2, Args),
        ?ASSERT_FILE_INFO(ExpOwner2#{mode => ?FILE_MODE(FilePerms)}, KrkNode, StorageFilePath)
    end).


-spec directory_mode_and_owner_test_base(TestName :: atom(), test_env(), row_name(), setup()) ->
    ok | no_return().
directory_mode_and_owner_test_base(TestName, Env, RowName, Args) ->
    #{krk_node := KrkNode} = Env,
    SessId = session_id(maps:get(user, Args), krakow),
    SpaceId = space_id(Env, RowName),
    DirName = dir_name(TestName),
    DirPerms = maps:get(dir_perms, Args),
    #{uid := ExpDisplayUid, gid := ExpDisplayGid} = maps:get(expected_display_owner, Args),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(KrkNode, SessId, space_dir:guid(SpaceId), DirName, DirPerms),

    % directory is created on storage when its child is created on storage
    {ok, _} = lfm_proxy:create_and_open(
        KrkNode, SessId, DirGuid, file_name(TestName), ?DEFAULT_FILE_PERMS
    ),

    % then
    ?assertMatch({ok, #file_attr{
        uid = ExpDisplayUid,
        gid = ExpDisplayGid,
        mode = DirPerms
    }}, lfm_proxy:stat(KrkNode, SessId, ?FILE_REF(DirGuid))),

    exec_if_posix_row(Env, RowName, fun() ->
        StorageDirPath = storage_test_utils:file_path(KrkNode, SpaceId, DirName),
        ExpOwner = maps:get(expected_owner, Args),
        ?ASSERT_FILE_INFO(ExpOwner#{mode => ?DIR_MODE(DirPerms)}, KrkNode, StorageDirPath)
    end).


-spec directory_with_unknown_owner_test_base(TestName :: atom(), test_env(), row_name(), setup()) ->
    ok | no_return().
directory_with_unknown_owner_test_base(TestName, Env, RowName, Args) ->
    #{krk_node := KrkNode} = Env,
    SessId = session_id(user1, krakow),
    SpaceId = space_id(Env, RowName),
    DirName = dir_name(TestName),
    DirPerms = maps:get(dir_perms, Args),
    #{uid := ExpDisplayUid, gid := ExpDisplayGid} = maps:get(expected_display_owner, Args),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(KrkNode, SessId, space_dir:guid(SpaceId), DirName, DirPerms),
    {DirUuid, _} = file_id:unpack_guid(DirGuid),

    % pretend that the directory belongs to an unknown user (not yet logged to
    % Onezone) - such file may occur when it was synced from storage in remote
    % provider with reverse LUMA
    {ok, _} = rpc:call(KrkNode, file_meta, update, [DirUuid, fun(FM) ->
        {ok, FM#file_meta{owner = ?UNKNOWN_USER_ID}}
    end]),

    % directory is created on storage when its child is created on storage
    {ok, _} = lfm_proxy:create_and_open(
        KrkNode, SessId, DirGuid, file_name(TestName), ?DEFAULT_FILE_PERMS
    ),

    % then
    ?assertMatch({ok, #file_attr{
        uid = ExpDisplayUid,
        gid = ExpDisplayGid,
        mode = DirPerms
    }}, lfm_proxy:stat(KrkNode, SessId, ?FILE_REF(DirGuid))),

    exec_if_posix_row(Env, RowName, fun() ->
        StorageDirPath = storage_test_utils:file_path(KrkNode, SpaceId, DirName),
        ExpOwner = maps:get(expected_owner, Args),
        ?ASSERT_FILE_INFO(ExpOwner#{mode => ?DIR_MODE(DirPerms)}, KrkNode, StorageDirPath),

        % pretend that the unknown user logged to Onezone
        ok = rpc:call(KrkNode, files_to_chown, chown_deferred_files, [?UNKNOWN_USER_ID]),
        ExpOwner2 = maps:get(expected_owner2, Args),
        ?ASSERT_FILE_INFO(ExpOwner2#{mode => ?DIR_MODE(DirPerms)}, KrkNode, StorageDirPath)
    end).


-spec rename_file_test_base(TestName :: atom(), test_env(), row_name(), setup()) ->
    ok | no_return().
rename_file_test_base(TestName, Env, RowName, Args) ->
    #{krk_node := KrkNode} = Env,
    SessId = session_id(maps:get(user, Args), krakow),
    SpaceId = space_id(Env, RowName),
    DirName = dir_name(TestName),
    FileName = file_name(TestName),
    TargetFilePath = filename:join([<<"/">>, atom_to_binary(RowName), DirName, FileName]),
    FilePerms = maps:get(file_perms, Args),
    DirPerms = maps:get(dir_perms, Args),
    #{uid := ExpDisplayUid, gid := ExpDisplayGid} = maps:get(expected_display_owner, Args),

    % when
    SpaceDirGuid = space_dir:guid(SpaceId),
    {ok, DirGuid} = lfm_proxy:mkdir(KrkNode, SessId, SpaceDirGuid, DirName, DirPerms),
    {ok, {FileGuid, Handle}} = lfm_proxy:create_and_open(
        KrkNode, SessId, SpaceDirGuid, FileName, FilePerms
    ),
    ok = lfm_proxy:close(KrkNode, Handle),

    % and
    {ok, _} = lfm_proxy:mv(KrkNode, SessId, ?FILE_REF(FileGuid), TargetFilePath),

    % then
    ?assertMatch({ok, #file_attr{
        uid = ExpDisplayUid,
        gid = ExpDisplayGid,
        mode = FilePerms
    }}, lfm_proxy:stat(KrkNode, SessId, ?FILE_REF(FileGuid))),

    % and
    ?assertMatch({ok, #file_attr{
        uid = ExpDisplayUid,
        gid = ExpDisplayGid,
        mode = DirPerms
    }}, lfm_proxy:stat(KrkNode, SessId, ?FILE_REF(DirGuid))),

    % and
    exec_if_posix_row(Env, RowName, fun() ->
        TargetStorageDirPath = storage_test_utils:file_path(KrkNode, SpaceId, DirName),
        TargetStorageFilePath = filename:join(TargetStorageDirPath, FileName),
        ExpOwner = maps:get(expected_owner, Args),
        ?ASSERT_FILE_INFO(ExpOwner#{mode => ?DIR_MODE(DirPerms)}, KrkNode, TargetStorageDirPath),
        ?ASSERT_FILE_INFO(ExpOwner#{mode => ?FILE_MODE(FilePerms)}, KrkNode, TargetStorageFilePath)
    end).


-spec mapping_not_found_test_base(TestName :: atom(), test_env(), row_name(), setup()) ->
    ok | no_return().
mapping_not_found_test_base(TestName, Env, RowName, Args) ->
    #{krk_node := KrkNode} = Env,
    SessId = session_id(maps:get(user, Args), krakow),
    SpaceId = space_id(Env, RowName),

    ?assertMatch({error, ?EACCES}, lfm_proxy:create_and_open(
        KrkNode, SessId, space_dir:guid(SpaceId), file_name(TestName), ?DEFAULT_FILE_PERMS
    )),
    ok.


-spec remotely_updated_perms_should_be_updated_on_storage_test_base(
    TestName :: atom(), test_env(), row_name(), setup()
) ->
    ok | no_return().
remotely_updated_perms_should_be_updated_on_storage_test_base(TestName, Env, RowName, Args) ->
    #{krk_node := KrkNode, paris_node := ParisNode} = Env,
    User = maps:get(user, Args),
    KrkSessId = session_id(User, krakow),
    ParisSessId = session_id(User, paris),
    SpaceId = space_id(Env, RowName),
    FileName = file_name(TestName),
    InitialPerms = ?DEFAULT_FILE_PERMS,
    UpdatedPerms = 8#777,

    % when
    {ok, {FileGuid, Handle0}} = lfm_proxy:create_and_open(
        ParisNode, ParisSessId, space_dir:guid(SpaceId), FileName, InitialPerms
    ),
    ok = lfm_proxy:close(ParisNode, Handle0),

    % then
    ?assertMatch({ok, #file_attr{mode = InitialPerms}},
        lfm_proxy:stat(KrkNode, KrkSessId, ?FILE_REF(FileGuid)), ?ATTEMPTS),
    % open file to ensure that it's created on storage
    {ok, Handle} = ?assertMatch({ok, _},
        lfm_proxy:open(KrkNode, KrkSessId, ?FILE_REF(FileGuid), read), ?ATTEMPTS),
    ok = lfm_proxy:close(KrkNode, Handle),
    StorageFilePath = storage_test_utils:file_path(KrkNode, SpaceId, FileName),
    ?ASSERT_FILE_INFO(#{mode => ?FILE_MODE(InitialPerms)}, KrkNode, StorageFilePath),

    % and
    ok = lfm_proxy:set_perms(ParisNode, ParisSessId, ?FILE_REF(FileGuid), UpdatedPerms),
    ?assertMatch({ok, #file_attr{mode = UpdatedPerms}},
        lfm_proxy:stat(KrkNode, KrkSessId, ?FILE_REF(FileGuid)), ?ATTEMPTS),
    % ensure that mode has been updated on storage
    ?ASSERT_FILE_INFO(#{mode => ?FILE_MODE(UpdatedPerms)}, KrkNode, StorageFilePath, ?ATTEMPTS).


%%%===================================================================
%%% Matrix run machinery
%%%===================================================================


%% @private
-spec run_matrix(TestName :: atom(), test_env(), test_fun(), setup(), setups_per_row()) ->
    ok | no_return().
run_matrix(TestName, Env, TestFun, GenericArgs, SetupsPerRow) ->
    SubRuns = lists:flatmap(fun({RowName, Setups}) ->
        % TODO replace with lists:map(lists_utils:enumerate ??
        lists:zipwith(fun(Setup, SetupNo) ->
            {RowName, SetupNo, maps:merge(Setup, GenericArgs)}
        end, Setups, lists:seq(1, length(Setups)))
    end, lists:sort(maps:to_list(SetupsPerRow))),

    AllPassed = lists:foldl(fun({RowName, SetupNo, Args}, Acc) ->
        run_single(TestName, Env, TestFun, RowName, SetupNo, Args) andalso Acc
    end, true, SubRuns),

    AllPassed orelse ct:fail("Not all matrix sub-runs succeeded").


%% @private
-spec run_single(
    TestName :: atom(), test_env(), test_fun(), row_name(), SetupNo :: pos_integer(), setup()
) ->
    boolean().
run_single(TestName, Env, TestFun, RowName, SetupNo, Args) ->
    ct:timetrap(?SUB_RUN_TIMETRAP),
    try
        ct:pal("Starting testcase ~tp for matrix row ~tp (setup no. ~tp)",
            [TestName, RowName, SetupNo]),
        TestFun(TestName, Env, RowName, Args),
        ct:pal("Testcase ~tp for matrix row ~tp (setup no. ~tp) PASSED",
            [TestName, RowName, SetupNo]),
        true
    catch Class:Reason:Stacktrace ->
        ct:pal("Testcase ~tp for matrix row ~tp (setup no. ~tp) FAILED~n"
            "Error: ~tp~nStacktrace:~n~tp",
            [TestName, RowName, SetupNo, {Class, Reason}, Stacktrace]),
        false
    after
        clean_up_after_sub_run(Env, RowName)
    end.


%% @private
%% @doc Brings the sub-run's matrix row back to its initial state: no files in the
%% space, no space dir on storage (so that the next sub-run exercises its creation
%% and chown anew) and no LUMA DB mappings cached from the feed.
-spec clean_up_after_sub_run(test_env(), row_name()) -> ok.
clean_up_after_sub_run(Env, RowName) ->
    #{krk_node := KrkNode} = Env,
    Row = matrix_row(Env, RowName),
    SpaceId = maps:get(space_id, Row),
    NodesWithStorages = row_nodes_with_storages(Env, Row),
    Nodes = [Node || {Node, _} <- NodesWithStorages],

    lfm_test_utils:clean_space(KrkNode, Nodes, SpaceId, ?CLEAN_SPACE_ATTEMPTS),

    lists:foreach(fun({Node, StorageId}) ->
        case maps:get(posix, Row) of
            true -> reset_space_dir_on_storage(Node, SpaceId, StorageId);
            false -> ok
        end,
        case maps:get(feed, Row) of
            % never cleared for the local feed - its mappings live in the LUMA DB
            local -> ok;
            _ -> ok = rpc:call(Node, luma, clear_db, [StorageId])
        end
    end, NodesWithStorages).


%% @private
-spec reset_space_dir_on_storage(node(), od_space:id(), storage:id()) -> ok | no_return().
reset_space_dir_on_storage(Node, SpaceId, StorageId) ->
    ok = rpc:call(Node, dir_location, delete, [space_dir:uuid(SpaceId)]),
    SDHandle = sd_test_utils:new_handle(Node, SpaceId, <<"/">>, StorageId),
    sd_test_utils:recursive_rm(Node, SDHandle, true),
    ?assertMatch({ok, []}, sd_test_utils:ls(Node, SDHandle, 0, 1)).


%%%===================================================================
%%% Test environment setup
%%%===================================================================


%% @private
-spec test_env(test_config:config()) -> test_env().
test_env(Config) ->
    #{
        krk_node => oct_background:get_random_provider_node(krakow),
        paris_node => oct_background:get_random_provider_node(paris),
        matrix => ?config(matrix, Config)
    }.


%% @private
-spec set_up_matrix(LumaFeedUrl :: binary()) -> matrix().
set_up_matrix(LumaFeedUrl) ->
    maps:from_list(lists:map(fun(RowName) ->
        {RowName, set_up_matrix_row(RowName, LumaFeedUrl)}
    end, matrix_rows())).


%% @private
-spec set_up_matrix_row(row_name(), LumaFeedUrl :: binary()) -> matrix_row().
set_up_matrix_row(?POSIX_AUTO = RowName, _LumaFeedUrl) ->
    KrkStorageId = create_posix_storage(krakow, auto, false),
    ParisStorageId = create_posix_storage(paris, auto, false),
    SpaceId = create_space(RowName, [{krakow, KrkStorageId}, {paris, ParisStorageId}]),
    #{space_id => SpaceId, storage_id => KrkStorageId, paris_storage_id => ParisStorageId,
        feed => auto, posix => true};

set_up_matrix_row(?POSIX_EXTERNAL = RowName, LumaFeedUrl) ->
    StorageId = create_posix_storage(krakow, #external_feed_luma{url = LumaFeedUrl}, false),
    SpaceId = create_space(RowName, [{krakow, StorageId}]),
    populate_external_feed(StorageId, feed_data(SpaceId, #{
        user1 => #{<<"storageCredentials">> => #{<<"uid">> => 2001}, <<"displayUid">> => 2221},
        user2 => #{<<"storageCredentials">> => #{<<"uid">> => 2002}},
        % invalid entry - tests of failed mappings
        user4 => #{<<"storageCredentials">> => #{<<"uid">> => <<"forbidden type">>}}
    }, #{
        <<"posix">> => #{<<"uid">> => 2000, <<"gid">> => 2000},
        <<"display">> => #{<<"uid">> => 2222, <<"gid">> => 2222}
    })),
    #{space_id => SpaceId, storage_id => StorageId, feed => external, posix => true};

set_up_matrix_row(?POSIX_LOCAL = RowName, _LumaFeedUrl) ->
    StorageId = create_posix_storage(krakow, local, false),
    SpaceId = create_space(RowName, [{krakow, StorageId}]),
    onenv_luma_test_utils:populate_local_feed(krakow, StorageId, feed_data(SpaceId, #{
        user1 => #{<<"storageCredentials">> => #{<<"uid">> => 3001}, <<"displayUid">> => 3331},
        user2 => #{<<"storageCredentials">> => #{<<"uid">> => 3002}}
    }, #{
        <<"posix">> => #{<<"uid">> => 3000, <<"gid">> => 3000},
        <<"display">> => #{<<"uid">> => 3333, <<"gid">> => 3333}
    })),
    #{space_id => SpaceId, storage_id => StorageId, feed => local, posix => true};

set_up_matrix_row(?IMPORTED_POSIX_AUTO = RowName, _LumaFeedUrl) ->
    StorageId = create_posix_storage(krakow, auto, true),
    SpaceId = create_space(RowName, [{krakow, StorageId}]),
    #{space_id => SpaceId, storage_id => StorageId, feed => auto, posix => true};

set_up_matrix_row(?IMPORTED_POSIX_EXTERNAL = RowName, LumaFeedUrl) ->
    StorageId = create_posix_storage(krakow, #external_feed_luma{url = LumaFeedUrl}, true),
    SpaceId = create_space(RowName, [{krakow, StorageId}]),
    populate_external_feed(StorageId, feed_data(SpaceId, #{
        user1 => #{<<"storageCredentials">> => #{<<"uid">> => 5001}, <<"displayUid">> => 5551},
        user2 => #{<<"storageCredentials">> => #{<<"uid">> => 5002}},
        % invalid entry - tests of failed mappings
        user4 => #{<<"storageCredentials">> => #{<<"uid">> => 5004}, <<"displayUid">> => <<"forbidden type">>}
    }, #{
        <<"posix">> => #{<<"uid">> => 5000, <<"gid">> => 5000},
        <<"display">> => #{<<"uid">> => 5555, <<"gid">> => 5555}
    })),
    #{space_id => SpaceId, storage_id => StorageId, feed => external, posix => true};

set_up_matrix_row(?IMPORTED_POSIX_LOCAL = RowName, _LumaFeedUrl) ->
    StorageId = create_posix_storage(krakow, local, true),
    SpaceId = create_space(RowName, [{krakow, StorageId}]),
    % posix storage defaults can not be defined for an imported storage - the space
    % dir owner is always taken from the storage itself
    onenv_luma_test_utils:populate_local_feed(krakow, StorageId, feed_data(SpaceId, #{
        user1 => #{<<"storageCredentials">> => #{<<"uid">> => 6001}, <<"displayUid">> => 6661},
        user2 => #{<<"storageCredentials">> => #{<<"uid">> => 6002}}
    }, #{
        <<"display">> => #{<<"uid">> => 6666, <<"gid">> => 6666}
    })),
    #{space_id => SpaceId, storage_id => StorageId, feed => local, posix => true};

set_up_matrix_row(?S3_AUTO = RowName, _LumaFeedUrl) ->
    StorageId = create_s3_storage(auto),
    SpaceId = create_space(RowName, [{krakow, StorageId}]),
    #{space_id => SpaceId, storage_id => StorageId, feed => auto, posix => false};

set_up_matrix_row(?S3_EXTERNAL = RowName, LumaFeedUrl) ->
    StorageId = create_s3_storage(#external_feed_luma{url = LumaFeedUrl}),
    SpaceId = create_space(RowName, [{krakow, StorageId}]),
    populate_external_feed(StorageId, feed_data(SpaceId, #{
        user1 => #{
            <<"storageCredentials">> => #{<<"accessKey">> => <<"AccessKey">>, <<"secretKey">> => <<"SecretKey">>},
            <<"displayUid">> => 8881
        },
        user2 => #{
            <<"storageCredentials">> => #{<<"accessKey">> => <<"AccessKey">>, <<"secretKey">> => <<"SecretKey">>}
        },
        % invalid entry (no secret key) - tests of failed mappings
        user4 => #{<<"storageCredentials">> => #{<<"accessKey">> => <<"AccessKey">>}}
    }, #{
        <<"display">> => #{<<"uid">> => 8888, <<"gid">> => 8888}
    })),
    #{space_id => SpaceId, storage_id => StorageId, feed => external, posix => false};

set_up_matrix_row(?S3_LOCAL = RowName, _LumaFeedUrl) ->
    StorageId = create_s3_storage(local),
    SpaceId = create_space(RowName, [{krakow, StorageId}]),
    onenv_luma_test_utils:populate_local_feed(krakow, StorageId, feed_data(SpaceId, #{
        user1 => #{
            <<"storageCredentials">> => #{<<"accessKey">> => <<"AccessKey">>, <<"secretKey">> => <<"SecretKey">>},
            <<"displayUid">> => 9991
        },
        user2 => #{
            <<"storageCredentials">> => #{<<"accessKey">> => <<"AccessKey">>, <<"secretKey">> => <<"SecretKey">>}
        }
    }, #{
        <<"display">> => #{<<"uid">> => 9999, <<"gid">> => 9999}
    })),
    #{space_id => SpaceId, storage_id => StorageId, feed => local, posix => false};

set_up_matrix_row(?POSIX_EXTERNAL_NO_USER_MAPPINGS = RowName, LumaFeedUrl) ->
    StorageId = create_posix_storage(krakow, #external_feed_luma{url = LumaFeedUrl}, false),
    SpaceId = create_space(RowName, [{krakow, StorageId}]),
    % no user mappings at all and incomplete space defaults - no user can be
    % mapped to storage credentials on this storage
    populate_external_feed(StorageId, feed_data(SpaceId, #{}, #{
        <<"posix">> => #{<<"gid">> => 10000}
    })),
    #{space_id => SpaceId, storage_id => StorageId, feed => external, posix => true}.


%% @private
%% @doc Registers the storage's mappings in the external feed service and drops
%% whatever the provider has already cached in its LUMA DB. Setting up the space is
%% enough to make the provider acquire the space defaults, and until this call the
%% feed answers 404 to every query - which is not an error for space defaults, they
%% silently fall back to the credentials of the storage mount dir. Such a stale
%% entry would then be served until the first sub-run clears the LUMA DB, and any
%% session that resolved its storage credentials in the meantime would keep them
%% (LUMA DB changes do not invalidate already created helper handles).
-spec populate_external_feed(storage:id(), json_utils:json_map()) -> ok.
populate_external_feed(StorageId, FeedData) ->
    ok = luma_test_server:set_storage_feed_data(krakow, StorageId, FeedData),
    ok = opw_test_rpc:call(krakow, luma, clear_db, [StorageId]).


%% @private
-spec create_posix_storage(
    oct_background:entity_selector(), space_setup_utils:luma_feed_spec(), Imported :: boolean()
) ->
    storage:id().
create_posix_storage(Provider, LumaFeed, Imported) ->
    space_setup_utils:create_storage(Provider, #posix_storage_params{
        mount_point = <<"/mnt/st_", (generator:gen_name())/binary>>,
        imported_storage = Imported,
        luma_feed = LumaFeed
    }).


%% @private
-spec create_s3_storage(space_setup_utils:luma_feed_spec()) -> storage:id().
create_s3_storage(LumaFeed) ->
    space_setup_utils:create_storage(krakow, #s3_storage_params{
        storage_path_type = <<"flat">>,
        hostname = <<
            "volume-s3.dev-volume-s3-",
            (atom_to_binary(oct_background:to_entity_placeholder(krakow)))/binary,
            ".default:9000"
        >>,
        bucket_name = ?RAND_STR(15),
        luma_feed = LumaFeed
    }).


%% @private
-spec create_space(row_name(), [{oct_background:entity_selector(), storage:id()}]) -> od_space:id().
create_space(RowName, ProvidersWithStorages) ->
    space_setup_utils:set_up_space(#space_spec{
        name = RowName,
        owner = space_owner,
        users = [user1, user2, user3, user4],
        supports = lists:map(fun({Provider, StorageId}) ->
            StorageImport = case opw_test_rpc:call(Provider, storage, is_imported, [StorageId]) of
                true -> #{mode => <<"manual">>};
                false -> #{}
            end,
            #support_spec{
                provider = Provider,
                storage_spec = StorageId,
                size = ?SUPPORT_SIZE,
                storage_import = StorageImport
            }
        end, ProvidersWithStorages)
    }).


%% @private
%% @doc Builds feed data in the schema shared by luma_test_server and
%% onenv_luma_test_utils, resolving user placeholders to actual onedata user ids.
-spec feed_data(od_space:id(), #{user_selector() => json_utils:json_map()}, json_utils:json_map()) ->
    json_utils:json_map().
feed_data(SpaceId, StorageUsers, SpaceDefaults) ->
    #{
        <<"storageUsers">> => maps:fold(fun(UserSelector, Entry, Acc) ->
            Acc#{user_id(UserSelector) => Entry}
        end, #{}, StorageUsers),
        <<"spacesDefaults">> => #{SpaceId => SpaceDefaults}
    }.


%%%===================================================================
%%% Helper functions
%%%===================================================================


%% @private
-spec matrix_row(test_env(), row_name()) -> matrix_row().
matrix_row(#{matrix := Matrix}, RowName) ->
    maps:get(RowName, Matrix).


%% @private
-spec space_id(test_env(), row_name()) -> od_space:id().
space_id(Env, RowName) ->
    maps:get(space_id, matrix_row(Env, RowName)).


%% @private
-spec row_nodes_with_storages(test_env(), matrix_row()) -> [{node(), storage:id()}].
row_nodes_with_storages(#{krk_node := KrkNode, paris_node := ParisNode}, Row) ->
    NodesWithStorages = [{KrkNode, maps:get(storage_id, Row)}],
    case maps:find(paris_storage_id, Row) of
        {ok, ParisStorageId} -> NodesWithStorages ++ [{ParisNode, ParisStorageId}];
        error -> NodesWithStorages
    end.


%% @private
-spec exec_if_posix_row(test_env(), row_name(), fun(() -> ok | no_return())) -> ok | no_return().
exec_if_posix_row(Env, RowName, Fun) ->
    case maps:get(posix, matrix_row(Env, RowName)) of
        true -> Fun();
        false -> ok
    end.


%% @private
-spec user_id(user_selector()) -> od_user:id().
user_id(root) -> ?ROOT_USER_ID;
user_id(UserSelector) -> oct_background:get_user_id(UserSelector).


%% @private
-spec session_id(user_selector(), oct_background:entity_selector()) -> session:id().
session_id(root, _ProviderSelector) -> ?ROOT_SESS_ID;
session_id(UserSelector, ProviderSelector) ->
    oct_background:get_user_session_id(UserSelector, ProviderSelector).


%% @private
-spec file_name(TestName :: atom()) -> file_meta:name().
file_name(TestName) -> <<"file_", (atom_to_binary(TestName))/binary>>.


%% @private
-spec dir_name(TestName :: atom()) -> file_meta:name().
dir_name(TestName) -> <<"dir_", (atom_to_binary(TestName))/binary>>.


%% @private
-spec mount_dir_owner(test_env(), row_name()) -> owner().
mount_dir_owner(#{krk_node := KrkNode} = Env, RowName) ->
    StorageId = maps:get(storage_id, matrix_row(Env, RowName)),
    MountPoint = storage_test_utils:storage_mount_point(KrkNode, StorageId),
    {ok, FileInfo} = storage_test_utils:read_file_info(KrkNode, MountPoint),
    ?OWNER(FileInfo#file_info.uid, FileInfo#file_info.gid).


%% @private
-spec mount_dir_mode(test_env(), row_name()) -> file_meta:mode().
mount_dir_mode(#{krk_node := KrkNode} = Env, RowName) ->
    StorageId = maps:get(storage_id, matrix_row(Env, RowName)),
    MountPoint = storage_test_utils:storage_mount_point(KrkNode, StorageId),
    {ok, FileInfo} = storage_test_utils:read_file_info(KrkNode, MountPoint),
    FileInfo#file_info.mode.


%% @private
-spec mount_dir_owner_with_uid(test_env(), row_name(), luma:uid()) -> owner().
mount_dir_owner_with_uid(Env, RowName, Uid) ->
    (mount_dir_owner(Env, RowName))#{uid => Uid}.


%% @private
%% @doc Owner assigned by the auto feed on a posix compatible storage: generated
%% uid and the gid of the storage mount dir.
-spec generated_owner(test_env(), row_name(), user_selector()) -> owner().
generated_owner(Env, RowName, UserSelector) ->
    mount_dir_owner_with_uid(Env, RowName, generated_uid(user_id(UserSelector))).


%% @private
%% @doc Display owner assigned by the auto feed on a posix incompatible storage:
%% both uid and gid are generated.
-spec generated_display_owner(test_env(), row_name(), user_selector()) -> owner().
generated_display_owner(Env, RowName, UserSelector) ->
    ?OWNER(generated_uid(user_id(UserSelector)), generated_gid(space_id(Env, RowName))).


%% @private
-spec generated_space_owner(test_env(), row_name()) -> owner().
generated_space_owner(Env, RowName) ->
    SpaceId = space_id(Env, RowName),
    ?OWNER(generated_uid(?SPACE_OWNER_ID(SpaceId)), generated_gid(SpaceId)).


%% @private
-spec generated_uid(user_selector() | od_user:id()) -> luma:uid().
generated_uid(UserSelector) when is_atom(UserSelector) ->
    generated_uid(user_id(UserSelector));
generated_uid(UserId) when is_binary(UserId) ->
    luma_auto_feed:generate_posix_identifier(UserId, ?UID_RANGE).


%% @private
-spec generated_gid(od_space:id()) -> luma:gid().
generated_gid(SpaceId) ->
    luma_auto_feed:generate_posix_identifier(SpaceId, ?GID_RANGE).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    opt:init_per_suite([{?LOAD_MODULES, [?MODULE, luma_test_server]} | Config], #onenv_test_config{
        onenv_scenario = "2op_s3",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}],
        posthook = fun(NewConfig) ->
            space_setup_utils:clean_up_after_previous_run(matrix_rows(), [krakow, paris]),
            luma_test_server:stop_all(krakow),
            LumaServer = luma_test_server:start(krakow),
            Matrix = set_up_matrix(luma_test_server:endpoint(LumaServer)),
            [{luma_test_server, LumaServer}, {matrix, Matrix} | NewConfig]
        end
    }).


end_per_suite(Config) ->
    luma_test_server:stop(krakow, ?config(luma_test_server, Config)),
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
