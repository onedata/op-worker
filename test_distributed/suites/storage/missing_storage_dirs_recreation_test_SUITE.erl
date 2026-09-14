%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests recreation of directories (including the space directory) that were
%%% removed directly on storage, while their metadata still marks them as
%%% created on storage.
%%%
%%% In such a case recreation based on file_meta does nothing (all ancestors are
%%% marked as created), so the storage file creation falls back to recreating
%%% ancestors based on the file's storage file id
%%% (see sd_utils:generic_create_deferred/3).
%%%
%%% Each test case uses its own space supported by a newly created posix storage,
%%% so removing directories on storage does not affect other test cases.
%%% @end
%%%-------------------------------------------------------------------
-module(missing_storage_dirs_recreation_test_SUITE).
-author("Michal Stanisz").

-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include("space_setup_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    missing_space_dir_is_recreated_on_file_create_test/1,
    missing_space_dir_is_recreated_on_existing_file_open_test/1,
    missing_ancestor_dir_is_recreated_on_file_create_test/1,
    missing_ancestor_dir_is_recreated_on_file_create_on_imported_storage_test/1
]).

all() -> [
    missing_space_dir_is_recreated_on_file_create_test,
    missing_space_dir_is_recreated_on_existing_file_open_test,
    missing_ancestor_dir_is_recreated_on_file_create_test,
    missing_ancestor_dir_is_recreated_on_file_create_on_imported_storage_test
].

-record(test_space, {
    id :: od_space:id(),
    storage_mount_point :: binary()
}).

-record(nested_file, {
    top_dir_guid :: file_id:file_guid(),
    parent_dir_guid :: file_id:file_guid(),
    guid :: file_id:file_guid()
}).

-define(PROVIDER_PLACEHOLDER, krakow).
-define(USER_PLACEHOLDER, user1).
-define(SUPPORT_SIZE, 1000000000).
-define(FILE_CONTENT, <<"file_content">>).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% Test functions
%%%===================================================================


missing_space_dir_is_recreated_on_file_create_test(_Config) ->
    TestSpace = set_up_space_on_new_posix_storage(?FUNCTION_NAME, false),
    #nested_file{parent_dir_guid = ParentDirGuid} = create_nested_file(TestSpace),
    SpaceDirStoragePath = get_storage_path(TestSpace, space_dir:guid(TestSpace#test_space.id)),
    SpaceDirInfoBeforeRemoval = read_storage_file_info(SpaceDirStoragePath),

    remove_dir_on_storage(SpaceDirStoragePath),
    NewFileGuid = create_file_with_content(ParentDirGuid, <<"new_file">>),

    assert_content_on_storage(TestSpace, NewFileGuid),
    assert_owner_and_mode_on_storage(SpaceDirInfoBeforeRemoval, SpaceDirStoragePath).


missing_space_dir_is_recreated_on_existing_file_open_test(_Config) ->
    TestSpace = set_up_space_on_new_posix_storage(?FUNCTION_NAME, false),
    #nested_file{guid = FileGuid} = create_nested_file(TestSpace),
    SpaceDirStoragePath = get_storage_path(TestSpace, space_dir:guid(TestSpace#test_space.id)),
    SpaceDirInfoBeforeRemoval = read_storage_file_info(SpaceDirStoragePath),

    remove_dir_on_storage(SpaceDirStoragePath),
    % file is already marked as created on storage, so it is restored on open
    write_to_existing_file(FileGuid),

    assert_content_on_storage(TestSpace, FileGuid),
    assert_owner_and_mode_on_storage(SpaceDirInfoBeforeRemoval, SpaceDirStoragePath).


missing_ancestor_dir_is_recreated_on_file_create_test(_Config) ->
    TestSpace = set_up_space_on_new_posix_storage(?FUNCTION_NAME, false),
    missing_ancestor_dir_is_recreated_on_file_create_test_base(TestSpace).


missing_ancestor_dir_is_recreated_on_file_create_on_imported_storage_test(_Config) ->
    % space dir of an imported storage is the storage root, so only its descendants can go missing
    TestSpace = set_up_space_on_new_posix_storage(?FUNCTION_NAME, true),
    missing_ancestor_dir_is_recreated_on_file_create_test_base(TestSpace).


%% @private
missing_ancestor_dir_is_recreated_on_file_create_test_base(TestSpace) ->
    #nested_file{
        top_dir_guid = TopDirGuid,
        parent_dir_guid = ParentDirGuid
    } = create_nested_file(TestSpace),

    remove_dir_on_storage(get_storage_path(TestSpace, TopDirGuid)),
    NewFileGuid = create_file_with_content(ParentDirGuid, <<"new_file">>),

    assert_content_on_storage(TestSpace, NewFileGuid).


%%%===================================================================
%%% Helper functions
%%%===================================================================


%% @private
-spec set_up_space_on_new_posix_storage(atom(), boolean()) -> #test_space{}.
set_up_space_on_new_posix_storage(Name, IsImportedStorage) ->
    MountPoint = <<"/mnt/st_", (str_utils:rand_hex(8))/binary>>,
    StorageId = space_setup_utils:create_storage(?PROVIDER_PLACEHOLDER, #posix_storage_params{
        mount_point = MountPoint,
        imported_storage = IsImportedStorage
    }),
    SpaceId = space_setup_utils:set_up_space(#space_spec{
        name = Name,
        owner = ?USER_PLACEHOLDER,
        supports = [#support_spec{
            provider = ?PROVIDER_PLACEHOLDER,
            storage_spec = StorageId,
            size = ?SUPPORT_SIZE
        }]
    }),
    case IsImportedStorage of
        true -> await_initial_import_scan_finished(SpaceId);
        false -> ok
    end,
    #test_space{id = SpaceId, storage_mount_point = MountPoint}.


%% @private
-spec await_initial_import_scan_finished(od_space:id()) -> ok.
await_initial_import_scan_finished(SpaceId) ->
    ?assertEqual(
        true,
        catch opw_test_rpc:call(?PROVIDER_PLACEHOLDER, storage_import_monitoring, is_initial_scan_finished, [SpaceId]),
        ?ATTEMPTS
    ),
    ok.


%% @private
-spec create_nested_file(#test_space{}) -> #nested_file{}.
create_nested_file(#test_space{id = SpaceId}) ->
    % space support may not be visible to the user session right after it is set up
    {ok, TopDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
        get_node(), get_session_id(), space_dir:guid(SpaceId), <<"top_dir">>, ?DEFAULT_DIR_PERMS
    ), ?ATTEMPTS),
    {ok, ParentDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
        get_node(), get_session_id(), TopDirGuid, <<"parent_dir">>, ?DEFAULT_DIR_PERMS
    )),
    #nested_file{
        top_dir_guid = TopDirGuid,
        parent_dir_guid = ParentDirGuid,
        guid = create_file_with_content(ParentDirGuid, <<"file">>)
    }.


%% @private
-spec create_file_with_content(file_id:file_guid(), file_meta:name()) -> file_id:file_guid().
create_file_with_content(ParentGuid, Name) ->
    {ok, {FileGuid, Handle}} = ?assertMatch({ok, _}, lfm_proxy:create_and_open(
        get_node(), get_session_id(), ParentGuid, Name, ?DEFAULT_FILE_PERMS
    )),
    write_content_and_close(Handle),
    FileGuid.


%% @private
-spec write_to_existing_file(file_id:file_guid()) -> ok.
write_to_existing_file(FileGuid) ->
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(
        get_node(), get_session_id(), ?FILE_REF(FileGuid), write
    )),
    write_content_and_close(Handle).


%% @private
-spec write_content_and_close(lfm:handle()) -> ok.
write_content_and_close(Handle) ->
    ?assertEqual({ok, byte_size(?FILE_CONTENT)}, lfm_proxy:write(get_node(), Handle, 0, ?FILE_CONTENT)),
    ?assertEqual(ok, lfm_proxy:close(get_node(), Handle)).


%% @private
-spec get_storage_path(#test_space{}, file_id:file_guid()) -> binary().
get_storage_path(#test_space{storage_mount_point = MountPoint}, Guid) ->
    StorageFileId = opw_test_rpc:call(?PROVIDER_PLACEHOLDER, fun() ->
        {Id, _} = file_ctx:get_storage_file_id(file_ctx:new_by_guid(Guid)),
        Id
    end),
    % storage file id is absolute, so it cannot be joined using filename:join/1
    <<MountPoint/binary, StorageFileId/binary>>.


%% @private
-spec read_storage_file_info(binary()) -> #file_info{}.
read_storage_file_info(StoragePath) ->
    {ok, FileInfo} = ?assertMatch({ok, _}, opw_test_rpc:call(?PROVIDER_PLACEHOLDER, file, read_file_info, [StoragePath])),
    FileInfo.


%% @private
-spec remove_dir_on_storage(binary()) -> ok.
remove_dir_on_storage(StoragePath) ->
    ?assertEqual(ok, opw_test_rpc:call(?PROVIDER_PLACEHOLDER, file, del_dir_r, [StoragePath])),
    ?assertEqual({error, enoent}, opw_test_rpc:call(?PROVIDER_PLACEHOLDER, file, read_file_info, [StoragePath])).


%% @private
-spec assert_content_on_storage(#test_space{}, file_id:file_guid()) -> ok.
assert_content_on_storage(TestSpace, FileGuid) ->
    StoragePath = get_storage_path(TestSpace, FileGuid),
    ?assertEqual({ok, ?FILE_CONTENT}, opw_test_rpc:call(?PROVIDER_PLACEHOLDER, file, read_file, [StoragePath])).


%% @private
-spec assert_owner_and_mode_on_storage(#file_info{}, binary()) -> ok.
assert_owner_and_mode_on_storage(#file_info{uid = Uid, gid = Gid, mode = Mode}, StoragePath) ->
    #file_info{
        uid = ActualUid,
        gid = ActualGid,
        mode = ActualMode
    } = read_storage_file_info(StoragePath),
    ?assertEqual({Uid, Gid, Mode}, {ActualUid, ActualGid, ActualMode}).


%% @private
-spec get_node() -> node().
get_node() ->
    oct_background:get_random_provider_node(?PROVIDER_PLACEHOLDER).


%% @private
-spec get_session_id() -> session:id().
get_session_id() ->
    oct_background:get_user_session_id(?USER_PLACEHOLDER, ?PROVIDER_PLACEHOLDER).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    opt:init_per_suite([{?LOAD_MODULES, [?MODULE]} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}],
        posthook = fun(NewConfig) ->
            delete_spaces_and_storages_from_previous_run(),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).


%% @private
-spec delete_spaces_and_storages_from_previous_run() -> ok.
delete_spaces_and_storages_from_previous_run() ->
    TestCaseNames = [atom_to_binary(TestCase) || TestCase <- all()],
    lists:foreach(fun(SpaceId) ->
        #{<<"name">> := SpaceName} = ozw_test_rpc:get_space_protected_data(?ROOT, SpaceId),
        case lists:member(SpaceName, TestCaseNames) of
            true -> delete_space_with_local_storages(SpaceId);
            false -> ok
        end
    end, ozw_test_rpc:list_spaces()).


%% @private
-spec delete_space_with_local_storages(od_space:id()) -> ok.
delete_space_with_local_storages(SpaceId) ->
    % previous run could have been interrupted before the space was supported
    StorageIds = case opw_test_rpc:call(?PROVIDER_PLACEHOLDER, space_logic, get_local_storages, [SpaceId]) of
        {ok, Ids} -> Ids;
        {error, _} -> []
    end,
    ?assertEqual(ok, ozw_test_rpc:delete_space(SpaceId)),
    lists:foreach(fun(StorageId) ->
        ?assertEqual(ok, opw_test_rpc:call(?PROVIDER_PLACEHOLDER, storage, delete, [StorageId]), ?ATTEMPTS)
    end, StorageIds).
