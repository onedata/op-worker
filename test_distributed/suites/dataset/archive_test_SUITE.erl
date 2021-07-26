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
-include_lib("ctool/include/test/assertions.hrl").
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
    archive_dataset_attached_to_dir_plain_layout/1,
    archive_dataset_attached_to_file_plain_layout/1,
    archive_dataset_attached_to_hardlink_plain_layout/1,
    archive_dataset_attached_to_symlink_plain_layout/1,
    archive_nested_datasets_plain_layout/1,
    archive_dataset_attached_to_dir_bagit_layout/1,
    archive_dataset_attached_to_file_bagit_layout/1,
    archive_dataset_attached_to_hardlink_bagit_layout/1,
    archive_dataset_attached_to_symlink_bagit_layout/1,
    archive_nested_datasets_bagit_layout/1,
    
    incremental_archive_plain_layout/1,
    incremental_archive_bagit_layout/1,
    incremental_archive_modified_content/1, 
    incremental_archive_modified_metadata/1,
    incremental_archive_new_file/1,
    incremental_nested_archive_plain_layout/1,
    incremental_nested_archive_bagit_layout/1,
    
    dip_archive_dataset_attached_to_dir_plain_layout/1,
    dip_archive_dataset_attached_to_file_plain_layout/1,
    dip_archive_dataset_attached_to_hardlink_plain_layout/1,
    dip_archive_dataset_attached_to_symlink_plain_layout/1,
    dip_archive_nested_datasets_plain_layout/1,
    dip_archive_dataset_attached_to_dir_bagit_layout/1,
    dip_archive_dataset_attached_to_file_bagit_layout/1,
    dip_archive_dataset_attached_to_hardlink_bagit_layout/1,
    dip_archive_dataset_attached_to_symlink_bagit_layout/1,
    dip_archive_nested_datasets_bagit_layout/1
]).

groups() -> [
    {parallel_tests, [parallel], [
        create_archivisation_tree,
        archive_dataset_attached_to_dir_plain_layout,
        archive_dataset_attached_to_file_plain_layout,
        archive_dataset_attached_to_hardlink_plain_layout,
        % archive_dataset_attached_to_symlink_plain_layout, TODO VFS-7664
        archive_nested_datasets_plain_layout,
        archive_dataset_attached_to_dir_bagit_layout,
        archive_dataset_attached_to_file_bagit_layout,
        archive_dataset_attached_to_hardlink_bagit_layout,
        % archive_dataset_attached_to_symlink_bagit_layout TODO VFS-7664
        archive_nested_datasets_bagit_layout,

        incremental_archive_plain_layout,
        incremental_archive_bagit_layout,
        incremental_archive_modified_content,
        incremental_archive_modified_metadata,
        incremental_archive_new_file,
        incremental_nested_archive_plain_layout,
        incremental_nested_archive_bagit_layout,

        dip_archive_dataset_attached_to_dir_plain_layout,
        dip_archive_dataset_attached_to_file_plain_layout,
        dip_archive_dataset_attached_to_hardlink_plain_layout,
        % dip_archive_dataset_attached_to_symlink_plain_layout, TODO VFS-7664
        dip_archive_nested_datasets_plain_layout,
        dip_archive_dataset_attached_to_dir_bagit_layout,
        dip_archive_dataset_attached_to_file_bagit_layout,
        dip_archive_dataset_attached_to_hardlink_bagit_layout,
        % dip_archive_dataset_attached_to_symlink_bagit_layout, TODO VFS-7664
        dip_archive_nested_datasets_bagit_layout
    ]}
].


all() -> [
    {group, parallel_tests}
].

-define(ATTEMPTS, 60).

-define(SPACE, space_krk_par_p).
-define(USER1, user1).

-define(TEST_DESCRIPTION1, <<"TEST DESCRIPTION">>).
-define(TEST_DESCRIPTION2, <<"TEST DESCRIPTION2">>).
-define(TEST_ARCHIVE_PRESERVED_CALLBACK1, <<"https://preserved1.org">>).
-define(TEST_ARCHIVE_PRESERVED_CALLBACK2, <<"https://preserved1.org">>).
-define(TEST_ARCHIVE_PURGED_CALLBACK1, <<"https://purged1.org">>).
-define(TEST_ARCHIVE_PURGED_CALLBACK2, <<"https://purged2.org">>).
-define(TEST_ARCHIVE_PURGED_CALLBACK3, <<"https://purged3.org">>).

-define(RAND_NAME(), str_utils:rand_hex(20)).
-define(RAND_SIZE(), rand:uniform(50)).
-define(RAND_CONTENT(), crypto:strong_rand_bytes(?RAND_SIZE())).
-define(RAND_CONTENT(Size), crypto:strong_rand_bytes(Size)).
-define(FILE_IN_BASE_ARCHIVE_NAME, <<"file_in_base_archive">>).

-define(RAND_NAME(Prefix), ?NAME(Prefix, rand:uniform(?RAND_RANGE))).
-define(NAME(Prefix, Number), str_utils:join_binary([Prefix, integer_to_binary(Number)], <<"_">>)).
-define(RAND_RANGE, 1000000000).

-define(DATASET_ID(), ?RAND_NAME(<<"datasetId">>)).
-define(ARCHIVE_ID(), ?RAND_NAME(<<"archiveId">>)).
-define(USER_ID(), ?RAND_NAME(<<"userId">>)).

-define(RAND_JSON_METADATA(), begin
    lists:foldl(fun(_, AccIn) ->
        AccIn#{?RAND_NAME() => ?RAND_NAME()}
    end, #{}, lists:seq(1, rand:uniform(10)))
end).

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
            archive_tests_utils:create_archive_dir(Node, ArchiveId, DatasetId, SpaceId, UserId)
        end, Data)
    end, [{P1, P1Data}, {P2, P2Data}]),

    % check whether archivisation tree is created correctly
    lists_utils:pforeach(fun(Provider) ->
        Node = oct_background:get_random_provider_node(Provider),
        SessionId = oct_background:get_user_session_id(?USER1, Provider),
        lists_utils:pforeach(fun({DatasetId, ArchiveId, UserId}) ->
            archive_tests_utils:assert_archive_dir_structure_is_correct(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId)
        end, MockedData)
    end, Providers).


archive_dataset_attached_to_dir_plain_layout(_Config) ->
    archive_dataset_attached_to_dir_test_base(?ARCHIVE_PLAIN_LAYOUT, false).

archive_dataset_attached_to_file_plain_layout(_Config) ->
    archive_dataset_attached_to_file_test_base(?ARCHIVE_PLAIN_LAYOUT, false).

archive_dataset_attached_to_hardlink_plain_layout(_Config) ->
    archive_dataset_attached_to_hardlink_test_base(?ARCHIVE_PLAIN_LAYOUT, false).

archive_dataset_attached_to_symlink_plain_layout(_Config) ->
    archive_dataset_attached_to_symlink_test_base(?ARCHIVE_PLAIN_LAYOUT, false).

archive_nested_datasets_plain_layout(_Config) ->
    archive_nested_datasets_test_base(?ARCHIVE_PLAIN_LAYOUT, false).

archive_dataset_attached_to_dir_bagit_layout(_Config) ->
    archive_dataset_attached_to_dir_test_base(?ARCHIVE_BAGIT_LAYOUT, false).

archive_dataset_attached_to_file_bagit_layout(_Config) ->
    archive_dataset_attached_to_file_test_base(?ARCHIVE_BAGIT_LAYOUT, false).

archive_dataset_attached_to_hardlink_bagit_layout(_Config) ->
    archive_dataset_attached_to_hardlink_test_base(?ARCHIVE_BAGIT_LAYOUT, false).

archive_dataset_attached_to_symlink_bagit_layout(_Config) ->
    archive_dataset_attached_to_symlink_test_base(?ARCHIVE_BAGIT_LAYOUT, false).

archive_nested_datasets_bagit_layout(_Config) ->
    archive_nested_datasets_test_base(?ARCHIVE_BAGIT_LAYOUT, false).


dip_archive_dataset_attached_to_dir_plain_layout(_Config) ->
    archive_dataset_attached_to_dir_test_base(?ARCHIVE_PLAIN_LAYOUT, true).

dip_archive_dataset_attached_to_file_plain_layout(_Config) ->
    archive_dataset_attached_to_file_test_base(?ARCHIVE_PLAIN_LAYOUT, true).

dip_archive_dataset_attached_to_hardlink_plain_layout(_Config) ->
    archive_dataset_attached_to_hardlink_test_base(?ARCHIVE_PLAIN_LAYOUT, true).

dip_archive_dataset_attached_to_symlink_plain_layout(_Config) ->
    archive_dataset_attached_to_symlink_test_base(?ARCHIVE_PLAIN_LAYOUT, true).

dip_archive_nested_datasets_plain_layout(_Config) ->
    archive_nested_datasets_test_base(?ARCHIVE_PLAIN_LAYOUT, true).

dip_archive_dataset_attached_to_dir_bagit_layout(_Config) ->
    archive_dataset_attached_to_dir_test_base(?ARCHIVE_BAGIT_LAYOUT, true).

dip_archive_dataset_attached_to_file_bagit_layout(_Config) ->
    archive_dataset_attached_to_file_test_base(?ARCHIVE_BAGIT_LAYOUT, true).

dip_archive_dataset_attached_to_hardlink_bagit_layout(_Config) ->
    archive_dataset_attached_to_hardlink_test_base(?ARCHIVE_BAGIT_LAYOUT, true).

dip_archive_dataset_attached_to_symlink_bagit_layout(_Config) ->
    archive_dataset_attached_to_symlink_test_base(?ARCHIVE_BAGIT_LAYOUT, true).

dip_archive_nested_datasets_bagit_layout(_Config) ->
    archive_nested_datasets_test_base(?ARCHIVE_BAGIT_LAYOUT, true).

incremental_archive_plain_layout(_Config) ->
    simple_incremental_archive_test_base(?ARCHIVE_PLAIN_LAYOUT, []).

incremental_archive_bagit_layout(_Config) ->
    simple_incremental_archive_test_base(?ARCHIVE_BAGIT_LAYOUT, []).

incremental_archive_modified_content(_Config) ->
    simple_incremental_archive_test_base(?ARCHIVE_PLAIN_LAYOUT, [content]).

incremental_archive_modified_metadata(_Config) ->
    simple_incremental_archive_test_base(?ARCHIVE_BAGIT_LAYOUT, [metadata]).

incremental_archive_new_file(_Config) ->
    simple_incremental_archive_test_base(?ARCHIVE_BAGIT_LAYOUT, [new_file]).

incremental_nested_archive_plain_layout(_Config) ->
    nested_incremental_archive_test_base(?ARCHIVE_PLAIN_LAYOUT).

incremental_nested_archive_bagit_layout(_Config) ->
    nested_incremental_archive_test_base(?ARCHIVE_BAGIT_LAYOUT).


%===================================================================
% Test bases
%===================================================================

archive_dataset_attached_to_dir_test_base(Layout, IncludeDip) ->
    #object{
        guid = DirGuid,
        dataset = #dataset_object{
            id = DatasetId,
            archives = [#archive_object{id = ArchiveId}]
        }} = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #dir_spec{dataset = #dataset_spec{archives = [#archive_spec{
        config = #archive_config{layout = Layout, include_dip = IncludeDip}
    }]}}),
    archive_simple_dataset_test_base(DirGuid, DatasetId, ArchiveId).

archive_dataset_attached_to_file_test_base(Layout, IncludeDip) ->
    Size = 20,
    #object{
        guid = FileGuid,
        dataset = #dataset_object{
            id = DatasetId,
            archives = [#archive_object{id = ArchiveId}]
        }} = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #file_spec{
        dataset = #dataset_spec{archives = [#archive_spec{
            config = #archive_config{layout = Layout, include_dip = IncludeDip}
        }]},
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
        content = ?RAND_CONTENT(Size)
    }),
    archive_simple_dataset_test_base(FileGuid, DatasetId, ArchiveId).

archive_dataset_attached_to_hardlink_test_base(Layout, IncludeDip) ->
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
    Json = ?RAND_JSON_METADATA(),
    ok = lfm_proxy:set_metadata(P1Node, UserSessIdP1, ?FILE_REF(LinkGuid), json, Json, []),
    UserId = oct_background:get_user_id(?USER1),
    onenv_file_test_utils:await_file_metadata_sync(oct_background:get_space_supporting_providers(?SPACE), UserId, #object{
        guid = LinkGuid,
        metadata = #metadata_object{json = Json}
    }),
    #dataset_object{
        id = DatasetId,
        archives = [#archive_object{id = ArchiveId}]
    } = onenv_dataset_test_utils:set_up_and_sync_dataset(?USER1, LinkGuid, #dataset_spec{archives = [#archive_spec{
        config = #archive_config{layout = Layout, include_dip = IncludeDip}
    }]}),

    archive_simple_dataset_test_base(LinkGuid, DatasetId, ArchiveId).

archive_dataset_attached_to_symlink_test_base(Layout, IncludeDip) ->
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
            dataset = #dataset_spec{archives = [#archive_spec{
                config = #archive_config{layout = Layout, include_dip = IncludeDip}
            }]}
        }
    ),
    archive_simple_dataset_test_base(LinkGuid, DatasetId, ArchiveId).

archive_simple_dataset_test_base(Guid, DatasetId, ArchiveId) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    lists:foreach(fun(Provider) ->
        Node = oct_background:get_random_provider_node(Provider),
        SessionId = oct_background:get_user_session_id(?USER1, Provider),
        UserId = oct_background:get_user_id(?USER1),
        archive_tests_utils:assert_archive_dir_structure_is_correct(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId),
        {ok, #file_attr{type = Type, size = Size}} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(Guid)),
        {FileCount, ExpSize} = case Type of
            ?DIRECTORY_TYPE -> {0, 0};
            ?SYMLINK_TYPE -> {1, 0};
            _ -> {1, Size}
        end,
        archive_tests_utils:assert_archive_is_preserved(Node, SessionId, ArchiveId, DatasetId, Guid, FileCount, ExpSize)
    end, oct_background:get_space_supporting_providers(?SPACE)).

archive_nested_datasets_test_base(ArchiveLayout, IncludeDip) ->
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
            dataset = #dataset_spec{archives = [#archive_spec{
                config = #archive_config{
                    layout = ArchiveLayout, 
                    create_nested_archives = true,
                    include_dip = IncludeDip
                }
            }]},
            children = [
                #file_spec{ % dataset
                    dataset = #dataset_spec{},
                    content = ?RAND_CONTENT(),
                    metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}
                },
                #dir_spec{
                    children = [
                        #dir_spec{
                            dataset = #dataset_spec{}, % dataset
                            children = [
                                #file_spec{  % dataset
                                    dataset = #dataset_spec{},
                                    content = ?RAND_CONTENT(),
                                    metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}
                                },
                                #file_spec{ % archive shouldn't be created for this file
                                    dataset = #dataset_spec{state = ?DETACHED_DATASET},
                                    content = ?RAND_CONTENT(),
                                    metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}
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

    archive_tests_utils:assert_archive_is_preserved(Node, SessionId, ArchiveDir11Id, DatasetDir11Id, Dir11Guid, 3, ArchiveDir11Bytes),
    archive_tests_utils:assert_archive_is_preserved(Node, SessionId, ArchiveFile21Id, DatasetFile21Id, File21Guid, 1, File21Size),
    archive_tests_utils:assert_archive_is_preserved(Node, SessionId, ArchiveDir22Id,  DatasetDir22Id, Dir22Guid, 0, 0),
    archive_tests_utils:assert_archive_is_preserved(Node, SessionId, ArchiveDir31Id, DatasetDir31Id, Dir31Guid, 2, ArchiveDir31Bytes),
    archive_tests_utils:assert_archive_is_preserved(Node, SessionId, ArchiveFile41Id, DatasetFile41Id, File41Guid, 1, File41Size).


simple_incremental_archive_test_base(Layout, Modifications) ->
    #object{
        guid = DirGuid,
        children = [#object{guid = ChildGuid}],
        dataset = #dataset_object{
            id = DatasetId,
            archives = [#archive_object{id = BaseArchiveId}]
        }} = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, 
            #dir_spec{
                dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout}}]},
                children = [#file_spec{
                    name = ?FILE_IN_BASE_ARCHIVE_NAME,
                    content = ?RAND_CONTENT(), 
                    metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}
                }]
            }, paris),
    archive_tests_utils:assert_archive_state(BaseArchiveId, ?ARCHIVE_PRESERVED),
    Node = oct_background:get_random_provider_node(krakow),
    SessionId = oct_background:get_user_session_id(?USER1, krakow),
    ModifiedFiles = lists:usort(lists:map(fun
        (content) ->
            {ok, H} = lfm_proxy:open(Node, SessionId, #file_ref{guid = ChildGuid}, write),
            ?assertMatch({ok, _}, lfm_proxy:write(Node, H, 100, ?RAND_CONTENT())),
            ok = lfm_proxy:close(Node, H),
            ?FILE_IN_BASE_ARCHIVE_NAME;
        (metadata) ->
            ok = lfm_proxy:set_metadata(Node, SessionId, #file_ref{guid = ChildGuid}, json, ?RAND_JSON_METADATA(), []),
            ?FILE_IN_BASE_ARCHIVE_NAME;
        (new_file) ->
            Name = ?RAND_NAME(),
            {ok, {_, H}} = lfm_proxy:create_and_open(Node, SessionId, DirGuid, Name, ?DEFAULT_FILE_MODE),
            ?assertMatch({ok, _}, lfm_proxy:write(Node, H, 0, ?RAND_CONTENT())),
            ok = lfm_proxy:close(Node, H),
            Name
    end, Modifications)),
        
    {ok, ArchiveId} = lfm_proxy:archive_dataset(Node, SessionId, DatasetId, #archive_config{
        incremental = #{<<"enabled">> => true, <<"basedOn">> => BaseArchiveId},
        layout = Layout
    }, <<>>),
    archive_tests_utils:assert_archive_state(ArchiveId, ?ARCHIVE_PRESERVED),
    archive_tests_utils:assert_incremental_archive_links(BaseArchiveId, ArchiveId, ModifiedFiles),
    {ok, Children} = lfm_proxy:get_children(Node, SessionId, ?FILE_REF(DirGuid), 0, 10),
    {FilesNum, TotalSize} = lists:foldl(fun({Guid, _}, {AccNum, AccSize}) ->
        {ok, #file_attr{size = Size}} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(Guid)),
        {AccNum + 1, AccSize + Size}
    end, {0, 0}, Children),
    archive_tests_utils:assert_archive_is_preserved(Node, SessionId, ArchiveId, DatasetId, DirGuid, FilesNum, TotalSize).


nested_incremental_archive_test_base(Layout) ->
    #object{
        guid = TopDirGuid,
        dataset = #dataset_object{
            id = TopDatasetId,
            archives = [#archive_object{id = TopBaseArchiveId}]
        },
        children = [
            #object{guid = FileGuid1},
            #object{
                guid = NestedDirGuid,
                dataset = #dataset_object{
                    id = NestedDatasetId
                },
                children = [#object{guid = FileGuid2}]
            }
        ]
        } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE,
        #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, create_nested_archives = true}}]},
            children = [
                #file_spec{content = ?RAND_CONTENT(), metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}},
                #dir_spec{
                    dataset = #dataset_spec{},
                    children = [
                        #file_spec{
                            content = ?RAND_CONTENT(), 
                            metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}
                        }
                    ]
                }
            ]
        }, paris),
    archive_tests_utils:assert_archive_state(TopBaseArchiveId, ?ARCHIVE_PRESERVED),
    Node = oct_background:get_random_provider_node(krakow),
    SessionId = oct_background:get_user_session_id(?USER1, krakow),
    
    ListOpts = #{offset => 0, limit => 10},
    {ok, [{_, NestedBaseArchiveId}], _} = ?assertMatch({ok, [_], true},
        lfm_proxy:list_archives(Node, SessionId, NestedDatasetId, ListOpts), ?ATTEMPTS),
    
    {ok, TopArchiveId} = lfm_proxy:archive_dataset(Node, SessionId, TopDatasetId, #archive_config{
        create_nested_archives = true,
        incremental = #{<<"enabled">> => true, <<"basedOn">> => TopBaseArchiveId},
        layout = Layout
    }, <<>>),
    
    {ok, NestedArchives, _} = ?assertMatch({ok, [_, _], true},
        lfm_proxy:list_archives(Node, SessionId, NestedDatasetId, ListOpts), ?ATTEMPTS),
    NestedArchivesIds = lists:map(fun({_, ArchiveId}) ->
        ArchiveId
    end, NestedArchives),
    [NestedArchiveId] = NestedArchivesIds -- [NestedBaseArchiveId],
    
    {ok, #file_attr{size = Size1}} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(FileGuid1)),
    {ok, #file_attr{size = Size2}} = lfm_proxy:stat(Node, SessionId, ?FILE_REF(FileGuid2)),
    
    archive_tests_utils:assert_archive_state(TopArchiveId, ?ARCHIVE_PRESERVED),
    archive_tests_utils:assert_incremental_archive_links(TopBaseArchiveId, TopArchiveId, []),
    archive_tests_utils:assert_archive_is_preserved(Node, SessionId, TopArchiveId, TopDatasetId, TopDirGuid, 2, Size1 + Size2),
    
    archive_tests_utils:assert_archive_state(NestedArchiveId, ?ARCHIVE_PRESERVED),
    archive_tests_utils:assert_incremental_archive_links(NestedBaseArchiveId, NestedArchiveId, []),
    archive_tests_utils:assert_archive_is_preserved(Node, SessionId, NestedArchiveId, NestedDatasetId, NestedDirGuid, 1, Size2).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op-archive",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {provider_token_ttl_sec, 24 * 60 * 60}
        ]}]
    }).

end_per_suite(_Config) ->
    oct_background:end_per_suite().

init_per_group(_Group, Config) ->
    Config2 = oct_background:update_background_config(Config),
    lfm_proxy:init(Config2, false).

end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.
