%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of archives mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_test_SUITE).
-author("Jakub Kudzia").


-include("onenv_test_utils.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% exported for CT
-export([
    all/0, groups/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    % parallel tests
    create_archivisation_tree/1,
    archive_dataset_attached_to_dir/1,
    archive_dataset_attached_to_file/1,
    archive_dataset_attached_to_hardlink/1,
    archive_dataset_attached_to_symlink/1,
    archive_directory_with_number_of_files_exceeding_batch_size/1,
    archive_nested_datasets/1,

    % sequential tests
    archive_dataset_attached_to_space_dir/1,
    archive_big_tree/1
]).

groups() -> [
    {parallel_tests, [parallel], [
        create_archivisation_tree,
        archive_dataset_attached_to_dir,
        archive_dataset_attached_to_file,
        archive_dataset_attached_to_hardlink,
        archive_dataset_attached_to_symlink,
        archive_directory_with_number_of_files_exceeding_batch_size,
        archive_nested_datasets
    ]},
    {sequential_tests, [sequential], [
        archive_dataset_attached_to_space_dir,
        archive_big_tree
    ]}
].


all() -> [
    {group, parallel_tests},
    {group, sequential_tests}
].

-define(ATTEMPTS, 300).

-define(SPACE, space_krk_par_p).
-define(USER1, user1).


-define(TEST_ARCHIVE_CONFIG, #archive_config{
    incremental = false,
    include_dip = false,
    layout = ?ARCHIVE_PLAIN_LAYOUT
}).

-define(TEST_DESCRIPTION1, <<"TEST DESCRIPTION">>).
-define(TEST_DESCRIPTION2, <<"TEST DESCRIPTION2">>).
-define(TEST_ARCHIVE_PRESERVED_CALLBACK1, <<"https://preserved1.org">>).
-define(TEST_ARCHIVE_PRESERVED_CALLBACK2, <<"https://preserved1.org">>).
-define(TEST_ARCHIVE_PURGED_CALLBACK1, <<"https://purged1.org">>).
-define(TEST_ARCHIVE_PURGED_CALLBACK2, <<"https://purged2.org">>).
-define(TEST_ARCHIVE_PURGED_CALLBACK3, <<"https://purged3.org">>).

-define(TEST_TIMESTAMP, 1000000000).

-define(RAND_NAME(), str_utils:rand_hex(20)).
-define(RAND_SIZE(), rand:uniform(50)).
-define(RAND_CONTENT(), crypto:strong_rand_bytes(?RAND_SIZE())).
-define(RAND_CONTENT(Size), crypto:strong_rand_bytes(Size)).

-define(RAND_NAME(Prefix), ?NAME(Prefix, rand:uniform(?RAND_RANGE))).
-define(NAME(Prefix, Number), str_utils:join_binary([Prefix, integer_to_binary(Number)], <<"_">>)).
-define(RAND_RANGE, 1000000000).

-define(DATASET_ID(), ?RAND_NAME(<<"datasetId">>)).
-define(ARCHIVE_ID(), ?RAND_NAME(<<"archiveId">>)).
-define(USER_ID(), ?RAND_NAME(<<"userId">>)).

%===================================================================
% Parallel tests - tests which can be safely run in parallel
% as they do not interfere with any other test.
%===================================================================

create_archivisation_tree(_Config) ->
    Providers = [P1, P2] = oct_background:get_space_supporting_providers(?SPACE),
    SpaceId = oct_background:get_space_id(?SPACE),
    Count = 100,
    % Generate mock datasets, archives and users
    MockedData = [{?DATASET_ID(), ?ARCHIVE_ID(), ?USER_ID()} || _ <- lists:seq(1, Count)],

    P1Data = lists_utils:random_sublist(MockedData),
    P2Data = MockedData -- P1Data,

    % create archive directories for mock data
    lists_utils:pforeach(fun({Provider, Data}) ->
        Node = oct_background:get_random_provider_node(Provider),
        lists_utils:pforeach(fun({DatasetId, ArchiveId, UserId}) ->
            create_archive_dir(Node, ArchiveId, DatasetId, SpaceId, UserId)
        end, Data)
    end, [{P1, P1Data}, {P2, P2Data}]),

    % check whether archivisation tree is created correctly
    lists_utils:pforeach(fun(Provider) ->
        Node = oct_background:get_random_provider_node(Provider),
        SessionId = oct_background:get_user_session_id(?USER1, Provider),
        lists_utils:pforeach(fun({DatasetId, ArchiveId, UserId}) ->
            assert_archive_dir_structure_is_correct(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId)
        end, MockedData)
    end, Providers).

archive_dataset_attached_to_space_dir(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    #dataset_object{
        id = DatasetId,
        archives = [#archive_object{id = ArchiveId}]
    } = onenv_dataset_test_utils:set_up_and_sync_dataset(?USER1, SpaceGuid, #dataset_spec{archives = 1}),
    archive_simple_dataset_test_base(SpaceGuid, DatasetId, ArchiveId).

archive_dataset_attached_to_dir(_Config) ->
    #object{
        guid = DirGuid,
        dataset = #dataset_object{
            id = DatasetId,
            archives = [#archive_object{id = ArchiveId}]
    }} = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #dir_spec{dataset = #dataset_spec{archives = 1}}),
    archive_simple_dataset_test_base(DirGuid, DatasetId, ArchiveId).

archive_dataset_attached_to_file(_Config) ->
    Size = 20,
    #object{
        guid = FileGuid,
        dataset = #dataset_object{
            id = DatasetId,
            archives = [#archive_object{id = ArchiveId}]
    }} = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #file_spec{
        dataset = #dataset_spec{archives = 1},
        content = ?RAND_CONTENT(Size)
    }),
    archive_simple_dataset_test_base(FileGuid, DatasetId, ArchiveId).

archive_dataset_attached_to_hardlink(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(?USER1, krakow),
    SpaceId = oct_background:get_space_id(?SPACE),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    Size = 20,
    #object{guid = FileGuid} = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #file_spec{
        content = ?RAND_CONTENT(Size)
    }),
    {ok, #file_attr{guid = LinkGuid}} =
        lfm_proxy:make_link(P1Node, UserSessIdP1, ?FILE_REF(FileGuid), ?FILE_REF(SpaceGuid), ?RAND_NAME()),
    #dataset_object{
        id = DatasetId,
        archives = [#archive_object{id = ArchiveId}]
    } = onenv_dataset_test_utils:set_up_and_sync_dataset(?USER1, LinkGuid, #dataset_spec{archives = 1}),
    archive_simple_dataset_test_base(LinkGuid, DatasetId, ArchiveId).

archive_dataset_attached_to_symlink(_Config) ->
    #object{name = DirName} = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #dir_spec{}),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(oct_background:get_space_id(?SPACE)),
    LinkTarget = filename:join([SpaceIdPrefix, DirName]),
    #object{
        guid = LinkGuid,
        dataset = #dataset_object{
            id = DatasetId,
            archives = [#archive_object{id = ArchiveId}]
        }
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE,
        #symlink_spec{
            symlink_value = LinkTarget,
            dataset = #dataset_spec{archives = 1}
        }
    ),
    archive_simple_dataset_test_base(LinkGuid, DatasetId, ArchiveId).


archive_big_tree(_Config) ->
    archive_dataset_tree_test_base([{10, 10}, {10, 10}, {10, 10}]).


archive_directory_with_number_of_files_exceeding_batch_size(_Config) ->
    % default batch size is 1000
    archive_dataset_tree_test_base([{0, 2048}]).


archive_nested_datasets(_Config) ->
    #object{
        guid = Dir11Guid,
        dataset = #dataset_object{
            id = DatasetDir11Id,
            archives = [#archive_object{id = ArchiveDir11Id}]
        },
        children = [
            #object{
                guid = File21Guid,
                dataset = #dataset_object{id = DatasetFile21Id},
                content = File21Content
            },
            #object{
                children = [
                    #object{
                        guid = Dir31Guid,
                        dataset = #dataset_object{id = DatasetDir31Id},
                        children = [
                            #object{
                                guid = File41Guid,
                                dataset = #dataset_object{id = DatasetFile41Id},
                                content = File41Content
                            },
                            #object{
                                dataset = #dataset_object{id = DatasetFile42Id},
                                content = File42Content
                            }
                        ]
                    }
                ]
            },
            #object{
                guid = Dir22Guid,
                dataset = #dataset_object{id = DatasetDir22Id}
            }
        ]
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE,
        #dir_spec{ % dataset
            dataset = #dataset_spec{archives = 1},
            children = [
                #file_spec{ % dataset
                    dataset = #dataset_spec{},
                    content = ?RAND_CONTENT()
                },
                #dir_spec{
                    children = [
                        #dir_spec{
                            dataset = #dataset_spec{}, % dataset
                            children = [
                                #file_spec{  % dataset
                                    dataset = #dataset_spec{},
                                    content = ?RAND_CONTENT()
                                },
                                #file_spec{ % archive shouldn't be created for this file
                                    dataset = #dataset_spec{state = ?DETACHED_DATASET},
                                    content = ?RAND_CONTENT()
                                }
                            ]
                        }
                    ]
                },
                #dir_spec{dataset = #dataset_spec{}} % dataset
            ]
        }
    ),
    % archiving top dataset should result in creation of archives also for
    % nested datasets
    Node = oct_background:get_random_provider_node(krakow),
    SessionId = oct_background:get_user_session_id(?USER1, krakow),
    ListOpts = #{offset => 0, limit => 10},
    {ok, [{_, ArchiveFile21Id}], _} = ?assertMatch({ok, [_], true},
        lfm_proxy:list_archives(Node, SessionId, DatasetFile21Id, ListOpts), ?ATTEMPTS),
    {ok, [{_, ArchiveDir22Id}], _} = ?assertMatch({ok, [_], true},
        lfm_proxy:list_archives(Node, SessionId, DatasetDir22Id, ListOpts), ?ATTEMPTS),
    {ok, [{_, ArchiveDir31Id}], _} = ?assertMatch({ok, [_], true},
        lfm_proxy:list_archives(Node, SessionId, DatasetDir31Id, ListOpts), ?ATTEMPTS),
    {ok, [{_, ArchiveFile41Id}], _} = ?assertMatch({ok, [_], true},
        lfm_proxy:list_archives(Node, SessionId, DatasetFile41Id, ListOpts), ?ATTEMPTS),
    % DatasetFile4 is detached, therefore archive for this dataset shouldn't have been created
    ?assertMatch({ok, [], true},
        lfm_proxy:list_archives(Node, SessionId, DatasetFile42Id, ListOpts), ?ATTEMPTS),

    File21Size = byte_size(File21Content),
    File41Size = byte_size(File41Content),
    File42Size = byte_size(File42Content),
    ArchiveDir11Bytes = File21Size + File41Size + File42Size,
    ArchiveDir31Bytes = File41Size + File42Size,

    assert_archive_is_preserved(Node, SessionId, ArchiveDir11Id, DatasetDir11Id, Dir11Guid, 3, ArchiveDir11Bytes),
    assert_archive_is_preserved(Node, SessionId, ArchiveFile21Id, DatasetFile21Id, File21Guid, 1, File21Size),
    assert_archive_is_preserved(Node, SessionId, ArchiveDir22Id,  DatasetDir22Id, Dir22Guid, 0, 0),
    assert_archive_is_preserved(Node, SessionId, ArchiveDir31Id, DatasetDir31Id, Dir31Guid, 2, ArchiveDir31Bytes),
    assert_archive_is_preserved(Node, SessionId, ArchiveFile41Id, DatasetFile41Id, File41Guid, 1, File41Size).


%===================================================================
% Test bases
%===================================================================

archive_simple_dataset_test_base(Guid, DatasetId, ArchiveId) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    lists:foreach(fun(Provider) ->
        Node = oct_background:get_random_provider_node(Provider),
        SessionId = oct_background:get_user_session_id(?USER1, Provider),
        UserId = oct_background:get_user_id(?USER1),
        assert_archive_dir_structure_is_correct(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId),
        {ok, #file_attr{type = Type, size = Size}} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(Guid)),
        {FileCount, ExpSize} = case Type of
            ?DIRECTORY_TYPE -> {0, 0};
            ?SYMLINK_TYPE -> {1, 0};
            _ -> {1, Size}
        end,
        assert_archive_is_preserved(Node, SessionId, ArchiveId, DatasetId, Guid, FileCount, ExpSize)
    end, oct_background:get_space_supporting_providers(?SPACE)).

archive_dataset_tree_test_base(FileStructure) ->
    Provider = lists_utils:random_element(oct_background:get_space_supporting_providers(?SPACE)),
    Node = oct_background:get_random_provider_node(Provider),
    SessId = oct_background:get_user_session_id(?USER1, Provider),
    #object{
        guid = RootGuid,
        dataset = #dataset_object{id = DatasetId}
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #dir_spec{dataset = #dataset_spec{}}),

    {_, FileGuids} = lfm_test_utils:create_files_tree(Node, SessId, FileStructure, RootGuid),

    {ok, ArchiveId} =
        lfm_proxy:archive_dataset(Node, SessId, DatasetId, #archive_config{layout = ?ARCHIVE_PLAIN_LAYOUT}, <<>>),

    % created files are empty therefore expected size is 0
    assert_archive_is_preserved(Node, SessId, ArchiveId, DatasetId, RootGuid, length(FileGuids), 0).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op-archive",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).

end_per_suite(_Config) ->
    oct_background:end_per_suite().

init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).

end_per_group(_Group, Config) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    Workers = oct_background:get_all_providers_nodes(),
    lfm_test_utils:clean_space(Workers, SpaceId, ?ATTEMPTS),
    onenv_dataset_test_utils:cleanup_all_datasets(krakow, ?SPACE),
    lfm_proxy:teardown(Config).

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%===================================================================
% Internal functions
%===================================================================

create_archive_dir(Node, ArchiveId, DatasetId, SpaceId, UserId) ->
    rpc:call(Node, archivisation_tree, create_archive_dir, [ArchiveId, DatasetId, SpaceId, UserId]).

assert_archive_dir_structure_is_correct(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId) ->
    assert_archives_root_dir_exists(Node, SessionId, SpaceId),
    assert_dataset_archives_dir_exists(Node, SessionId, SpaceId, DatasetId),
    assert_archive_dir_exists(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId).

assert_archives_root_dir_exists(Node, SessionId, SpaceId) ->
    ArchivesRootUuid = ?ARCHIVES_ROOT_DIR_UUID(SpaceId),
    ArchivesRootGuid = file_id:pack_guid(ArchivesRootUuid, SpaceId),
    ArchivesRootDirName = ?ARCHIVES_ROOT_DIR_NAME,
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    ?assertMatch({ok, #file_attr{
        guid = ArchivesRootGuid,
        name = ArchivesRootDirName,
        mode = ?ARCHIVES_ROOT_DIR_PERMS,
        owner_id = ?SPACE_OWNER_ID(SpaceId),
        parent_guid = SpaceGuid
    }}, lfm_proxy:stat(Node, SessionId, ?FILE_REF(ArchivesRootGuid)), ?ATTEMPTS).


assert_dataset_archives_dir_exists(Node, SessionId, SpaceId, DatasetId) ->
    ArchivesRootUuid = ?ARCHIVES_ROOT_DIR_UUID(SpaceId),
    ArchivesRootGuid = file_id:pack_guid(ArchivesRootUuid, SpaceId),
    DatasetArchivesDirUuid = ?DATASET_ARCHIVES_DIR_UUID(DatasetId),
    DatasetArchivesDirGuid = file_id:pack_guid(DatasetArchivesDirUuid, SpaceId),

    ?assertMatch({ok, #file_attr{
        guid = DatasetArchivesDirGuid,
        name = DatasetArchivesDirUuid,
        mode = ?DEFAULT_DIR_PERMS,
        owner_id = ?SPACE_OWNER_ID(SpaceId),
        parent_guid = ArchivesRootGuid
    }}, lfm_proxy:stat(Node, SessionId, ?FILE_REF(DatasetArchivesDirGuid)), ?ATTEMPTS).


assert_archive_dir_exists(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId) ->
    ArchiveDirUuid = ?ARCHIVE_DIR_UUID(ArchiveId),
    ArchiveDirGuid = file_id:pack_guid(ArchiveDirUuid, SpaceId),
    DatasetArchivesDirUuid = ?DATASET_ARCHIVES_DIR_UUID(DatasetId),
    DatasetArchivesDirGuid = file_id:pack_guid(DatasetArchivesDirUuid, SpaceId),

    ?assertMatch({ok, #file_attr{
        guid = ArchiveDirGuid,
        name = ArchiveDirUuid,
        mode = ?DEFAULT_DIR_PERMS,
        owner_id = UserId,
        parent_guid = DatasetArchivesDirGuid
    }}, lfm_proxy:stat(Node, SessionId, ?FILE_REF(ArchiveDirGuid)), ?ATTEMPTS).


assert_archive_is_preserved(Node, SessionId, ArchiveId, DatasetId, RootGuid, FileCount, ExpSize) ->
    {ok, #archive_info{root_file_guid = CopyRootGuid}} = ?assertMatch({ok, #archive_info{
        state = ?ARCHIVE_PRESERVED,
        stats = #archive_stats{
            files_archived = FileCount,
            files_failed = 0,
            bytes_archived = ExpSize
        }
    }}, lfm_proxy:get_archive_info(Node, SessionId, ArchiveId), ?ATTEMPTS),

    GetDatasetArchives = fun() ->
        case lfm_proxy:list_archives(Node, SessionId, DatasetId, #{offset => 0, limit => 10000}) of
            {ok, ArchiveIdsAndIndices, _} ->
                [AID || {_, AID} <- ArchiveIdsAndIndices];
            _ ->
                error
        end
    end,
    ?assertEqual(true, lists:member(ArchiveId, GetDatasetArchives()), ?ATTEMPTS),

    assert_copied(Node, SessionId, RootGuid, CopyRootGuid).


    assert_copied(Node, SessionId, SourceGuid, TargetGuid) ->
    assert_attrs_copied(Node, SessionId, SourceGuid, TargetGuid),
    assert_metadata_copied(Node, SessionId, SourceGuid, TargetGuid),
    {ok, SourceAttr} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(SourceGuid)),
    case SourceAttr#file_attr.type of
        ?DIRECTORY_TYPE ->
            assert_children_copied(Node, SessionId, SourceGuid, TargetGuid);
        ?REGULAR_FILE_TYPE ->
            assert_content_copied(Node, SessionId, SourceGuid, TargetGuid);
        ?SYMLINK_TYPE ->
            assert_symlink_values_copied(Node, SessionId, SourceGuid, TargetGuid)
    end.


assert_attrs_copied(Node, SessionId, SourceGuid, TargetGuid) ->
    Stat = fun(Guid) ->
        lfm_proxy:stat(Node, SessionId, ?FILE_REF(Guid))
    end,
    {ok, SourceAttr} = Stat(SourceGuid),
    ?assertEqual(true, try
        {ok, TargetAttr} = ?assertMatch({ok, #file_attr{}}, Stat(TargetGuid), ?ATTEMPTS),
        ?assertEqual(SourceAttr#file_attr.name, TargetAttr#file_attr.name),
        ?assertEqual(SourceAttr#file_attr.mode, TargetAttr#file_attr.mode),
        ?assertEqual(SourceAttr#file_attr.type, TargetAttr#file_attr.type),
        ?assertEqual(SourceAttr#file_attr.size, TargetAttr#file_attr.size),
        true
    catch
        _:_ ->
            false
    end, ?ATTEMPTS).

assert_metadata_copied(Node, SessionId, SourceGuid, TargetGuid) ->
    GetXattrs = fun(Guid) ->
        lfm_proxy:list_xattr(Node, SessionId, ?FILE_REF(Guid), false, false)
    end,
    ?assertEqual(GetXattrs(SourceGuid), GetXattrs(TargetGuid)).


assert_children_copied(Node, SessionId, SourceGuid, TargetGuid) ->
    assert_children_copied(Node, SessionId, SourceGuid, TargetGuid, #{offset => 0, size => 1000}).

assert_children_copied(Node, SessionId, SourceGuid, TargetGuid, ListOpts = #{offset := Offset}) ->
    {ok, SourceChildren, #{is_last := SourceIsLast}} =
        lfm_proxy:get_children(Node, SessionId, ?FILE_REF(SourceGuid), ListOpts),
    {ok, TargetChildren, _} = ?assertMatch({ok, _, #{is_last := SourceIsLast}},
        lfm_proxy:get_children(Node, SessionId, ?FILE_REF(TargetGuid), ListOpts), ?ATTEMPTS),
    SourceNames = [N || {_, N} <- SourceChildren],
    TargetNames = [N || {_, N} <- TargetChildren],
    ?assertEqual(SourceNames, TargetNames),
    lists:foreach(fun({{SourceChildGuid, _}, {TargetChildGuid, _}}) ->
        assert_copied(Node, SessionId, SourceChildGuid, TargetChildGuid)
    end, lists:zip(SourceChildren, TargetChildren)),

    case SourceIsLast of
        true ->
            ok;
        false ->
            assert_children_copied(Node, SessionId, SourceGuid, TargetGuid, ListOpts#{offset => Offset + length(SourceChildren)})
    end.


assert_content_copied(Node, SessionId, SourceGuid, TargetGuid) ->
    {ok, SourceHandle} = lfm_proxy:open(Node, SessionId, ?FILE_REF(SourceGuid), read),
    {ok, SourceContent} = lfm_proxy:read(Node, SourceHandle, 0, 10000),
    {ok, TargetHandle} = ?assertMatch({ok, _},
        lfm_proxy:open(Node, SessionId, ?FILE_REF(TargetGuid), read), ?ATTEMPTS),
    ?assertEqual({ok, SourceContent},
        lfm_proxy:read(Node, SourceHandle, 0, 10000), ?ATTEMPTS),
    lfm_proxy:close(Node, SourceHandle),
    lfm_proxy:close(Node, TargetHandle),
    assert_file_is_flushed_from_buffer(Node, SessionId, SourceGuid, TargetGuid).


assert_file_is_flushed_from_buffer(Node, SessionId, SourceGuid, TargetGuid) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    {ok, #file_attr{size = SourceSize}} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(SourceGuid)),
    TargetSDHandle = sd_test_utils:new_handle(Node, SpaceId, get_storage_file_id(Node, TargetGuid)),
    GetStorageSize = fun(SDHandle) ->
        case sd_test_utils:stat(Node, SDHandle) of
            {ok, #statbuf{st_size = SourceSize}} -> SourceSize;
            _ -> error
        end
    end,
    ?assertEqual(SourceSize, GetStorageSize(TargetSDHandle), ?ATTEMPTS).

assert_symlink_values_copied(Node, SessionId, SourceGuid, TargetGuid) ->
    ReadSymlink = fun(Guid) ->
        lfm_proxy:read_symlink(Node, SessionId, ?FILE_REF(Guid))
    end,
    ?assertEqual(ReadSymlink(SourceGuid), ReadSymlink(TargetGuid), ?ATTEMPTS).

get_storage_file_id(Node, Guid) ->
    FileCtx = rpc:call(Node, file_ctx, new_by_guid, [Guid]),
    {StorageFileId, _} = rpc:call(Node, file_ctx, get_storage_file_id, [FileCtx]),
    StorageFileId.