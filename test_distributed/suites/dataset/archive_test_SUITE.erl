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
-include_lib("ctool/include/test/test_utils.hrl").
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
    archive_dataset_attached_to_symlink_to_reg_file_plain_layout_follow/1,
    archive_dataset_containing_symlink_to_reg_file_plain_layout_follow/1,
    archive_dataset_containing_symlink_to_directory_plain_layout_follow/1,
    archive_dataset_attached_to_symlink_to_reg_file_plain_layout_not_follow/1,
    archive_dataset_containing_symlink_to_reg_file_plain_layout_not_follow/1,
    archive_dataset_containing_symlink_to_directory_plain_layout_not_follow/1,
    archive_nested_datasets_plain_layout/1,
    archive_dataset_attached_to_dir_bagit_layout/1,
    archive_dataset_attached_to_file_bagit_layout/1,
    archive_dataset_attached_to_hardlink_bagit_layout/1,
    archive_dataset_attached_to_symlink_to_reg_file_bagit_layout_follow/1,
    archive_dataset_containing_symlink_to_reg_file_bagit_layout_follow/1,
    archive_dataset_containing_symlink_to_directory_bagit_layout_follow/1,
    archive_dataset_attached_to_symlink_to_reg_file_bagit_layout_not_follow/1,
    archive_dataset_containing_symlink_to_reg_file_bagit_layout_not_follow/1,
    archive_dataset_containing_symlink_to_directory_bagit_layout_not_follow/1,
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
    dip_archive_dataset_attached_to_symlink_to_reg_file_plain_layout_follow/1,
    dip_archive_dataset_containing_symlink_to_reg_file_plain_layout_follow/1,
    dip_archive_dataset_containing_symlink_to_directory_plain_layout_follow/1,
    dip_archive_dataset_attached_to_symlink_to_reg_file_plain_layout_not_follow/1,
    dip_archive_dataset_containing_symlink_to_reg_file_plain_layout_not_follow/1,
    dip_archive_dataset_containing_symlink_to_directory_plain_layout_not_follow/1,
    dip_archive_nested_datasets_plain_layout/1,
    dip_archive_dataset_attached_to_dir_bagit_layout/1,
    dip_archive_dataset_attached_to_file_bagit_layout/1,
    dip_archive_dataset_attached_to_hardlink_bagit_layout/1,
    dip_archive_dataset_attached_to_symlink_to_reg_file_bagit_layout_follow/1,
    dip_archive_dataset_containing_symlink_to_reg_file_bagit_layout_follow/1,
    dip_archive_dataset_containing_symlink_to_directory_bagit_layout_follow/1,
    dip_archive_dataset_attached_to_symlink_to_reg_file_bagit_layout_not_follow/1,
    dip_archive_dataset_containing_symlink_to_reg_file_bagit_layout_not_follow/1,
    dip_archive_dataset_containing_symlink_to_directory_bagit_layout_not_follow/1,
    dip_archive_nested_datasets_bagit_layout/1,
    modify_preserved_plain_archive_test/1,
    modify_preserved_bagit_archive_test/1,
    
    verification_plain_modify_file/1,
    verification_plain_modify_file_metadata/1,
    verification_plain_create_file/1,
    verification_plain_remove_file/1,
    verification_plain_recreate_file/1,
    dip_verification_plain/1,
    nested_verification_plain/1,
    verification_bagit_modify_file/1,
    verification_bagit_modify_file_metadata/1,
    verification_bagit_create_file/1,
    verification_bagit_remove_file/1,
    verification_bagit_recreate_file/1,
    dip_verification_bagit/1,
    nested_verification_bagit/1
]).

groups() -> [
    {parallel_tests, [ % fixme parallel
        create_archivisation_tree,
        archive_dataset_attached_to_dir_plain_layout,
        archive_dataset_attached_to_file_plain_layout,
        archive_dataset_attached_to_hardlink_plain_layout,
        archive_dataset_attached_to_symlink_to_reg_file_plain_layout_follow,
        archive_dataset_containing_symlink_to_reg_file_plain_layout_follow,
        archive_dataset_containing_symlink_to_directory_plain_layout_follow,
        archive_dataset_attached_to_symlink_to_reg_file_plain_layout_not_follow,
        archive_dataset_containing_symlink_to_reg_file_plain_layout_not_follow,
        archive_dataset_containing_symlink_to_directory_plain_layout_not_follow,
        archive_nested_datasets_plain_layout,
        archive_dataset_attached_to_dir_bagit_layout,
        archive_dataset_attached_to_file_bagit_layout,
        archive_dataset_attached_to_hardlink_bagit_layout,
        archive_dataset_attached_to_symlink_to_reg_file_bagit_layout_follow,
        archive_dataset_containing_symlink_to_reg_file_bagit_layout_follow,
        archive_dataset_containing_symlink_to_directory_bagit_layout_follow,
        archive_dataset_attached_to_symlink_to_reg_file_bagit_layout_not_follow,
        archive_dataset_containing_symlink_to_reg_file_bagit_layout_not_follow,
        archive_dataset_containing_symlink_to_directory_bagit_layout_not_follow,
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
        dip_archive_dataset_attached_to_symlink_to_reg_file_plain_layout_follow,
        dip_archive_dataset_containing_symlink_to_reg_file_plain_layout_follow,
        dip_archive_dataset_containing_symlink_to_directory_plain_layout_follow,
        dip_archive_dataset_attached_to_symlink_to_reg_file_plain_layout_not_follow,
        dip_archive_dataset_containing_symlink_to_reg_file_plain_layout_not_follow,
        dip_archive_dataset_containing_symlink_to_directory_plain_layout_not_follow,
        dip_archive_nested_datasets_plain_layout,
        dip_archive_dataset_attached_to_dir_bagit_layout,
        dip_archive_dataset_attached_to_file_bagit_layout,
        dip_archive_dataset_attached_to_hardlink_bagit_layout,
        dip_archive_dataset_attached_to_symlink_to_reg_file_bagit_layout_follow,
        dip_archive_dataset_containing_symlink_to_reg_file_bagit_layout_follow,
        dip_archive_dataset_containing_symlink_to_directory_bagit_layout_follow,
        dip_archive_dataset_attached_to_symlink_to_reg_file_bagit_layout_not_follow,
        dip_archive_dataset_containing_symlink_to_reg_file_bagit_layout_not_follow,
        dip_archive_dataset_containing_symlink_to_directory_bagit_layout_not_follow,
        dip_archive_nested_datasets_bagit_layout,
    
        modify_preserved_plain_archive_test,
        modify_preserved_bagit_archive_test
    ]},
    {verification_tests, [
        verification_plain_modify_file, 
        verification_plain_modify_file_metadata,
        verification_plain_create_file,
        verification_plain_remove_file,
        verification_plain_recreate_file,
        dip_verification_plain,
        nested_verification_plain,
        verification_bagit_modify_file,
        verification_bagit_modify_file_metadata,
        verification_bagit_create_file,
        verification_bagit_remove_file,
        verification_bagit_recreate_file,
        dip_verification_bagit,
        nested_verification_bagit
    ]}
].


all() -> [
    {group, parallel_tests},
    {group, verification_tests}
].

-define(ATTEMPTS, 18). % fixme

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

archive_dataset_attached_to_symlink_to_reg_file_plain_layout_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_PLAIN_LAYOUT, false, true, direct).

archive_dataset_containing_symlink_to_reg_file_plain_layout_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_PLAIN_LAYOUT, false, true, nested).

archive_dataset_containing_symlink_to_directory_plain_layout_follow(_Config) ->
    archive_dataset_containing_symlink_to_directory_test_base(?ARCHIVE_PLAIN_LAYOUT, false, true, nested).

archive_dataset_attached_to_symlink_to_reg_file_plain_layout_not_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_PLAIN_LAYOUT, false, false, direct).

archive_dataset_containing_symlink_to_reg_file_plain_layout_not_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_PLAIN_LAYOUT, false, false, nested).

archive_dataset_containing_symlink_to_directory_plain_layout_not_follow(_Config) ->
    archive_dataset_containing_symlink_to_directory_test_base(?ARCHIVE_PLAIN_LAYOUT, false, false, nested).

archive_nested_datasets_plain_layout(_Config) ->
    archive_nested_datasets_test_base(?ARCHIVE_PLAIN_LAYOUT, false).

archive_dataset_attached_to_dir_bagit_layout(_Config) ->
    archive_dataset_attached_to_dir_test_base(?ARCHIVE_BAGIT_LAYOUT, false).

archive_dataset_attached_to_file_bagit_layout(_Config) ->
    archive_dataset_attached_to_file_test_base(?ARCHIVE_BAGIT_LAYOUT, false).

archive_dataset_attached_to_hardlink_bagit_layout(_Config) ->
    archive_dataset_attached_to_hardlink_test_base(?ARCHIVE_BAGIT_LAYOUT, false).

archive_dataset_attached_to_symlink_to_reg_file_bagit_layout_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_BAGIT_LAYOUT, false, true, direct).

archive_dataset_containing_symlink_to_reg_file_bagit_layout_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_BAGIT_LAYOUT, false, true, nested).

archive_dataset_containing_symlink_to_directory_bagit_layout_follow(_Config) ->
    archive_dataset_containing_symlink_to_directory_test_base(?ARCHIVE_BAGIT_LAYOUT, false, true, nested).

archive_dataset_attached_to_symlink_to_reg_file_bagit_layout_not_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_BAGIT_LAYOUT, false, false, direct).

archive_dataset_containing_symlink_to_reg_file_bagit_layout_not_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_BAGIT_LAYOUT, false, false, nested).

archive_dataset_containing_symlink_to_directory_bagit_layout_not_follow(_Config) ->
    archive_dataset_containing_symlink_to_directory_test_base(?ARCHIVE_BAGIT_LAYOUT, false, false, nested).

archive_nested_datasets_bagit_layout(_Config) ->
    archive_nested_datasets_test_base(?ARCHIVE_BAGIT_LAYOUT, false).


dip_archive_dataset_attached_to_dir_plain_layout(_Config) ->
    archive_dataset_attached_to_dir_test_base(?ARCHIVE_PLAIN_LAYOUT, true).

dip_archive_dataset_attached_to_file_plain_layout(_Config) ->
    archive_dataset_attached_to_file_test_base(?ARCHIVE_PLAIN_LAYOUT, true).

dip_archive_dataset_attached_to_hardlink_plain_layout(_Config) ->
    archive_dataset_attached_to_hardlink_test_base(?ARCHIVE_PLAIN_LAYOUT, true).

dip_archive_dataset_attached_to_symlink_to_reg_file_plain_layout_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_PLAIN_LAYOUT, true, true, direct).

dip_archive_dataset_containing_symlink_to_reg_file_plain_layout_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_PLAIN_LAYOUT, true, true, nested).

dip_archive_dataset_containing_symlink_to_directory_plain_layout_follow(_Config) ->
    archive_dataset_containing_symlink_to_directory_test_base(?ARCHIVE_PLAIN_LAYOUT, true, true, nested).

dip_archive_dataset_attached_to_symlink_to_reg_file_plain_layout_not_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_PLAIN_LAYOUT, true, false, direct).

dip_archive_dataset_containing_symlink_to_reg_file_plain_layout_not_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_PLAIN_LAYOUT, true, false, nested).

dip_archive_dataset_containing_symlink_to_directory_plain_layout_not_follow(_Config) ->
    archive_dataset_containing_symlink_to_directory_test_base(?ARCHIVE_PLAIN_LAYOUT, true, false, nested).

dip_archive_nested_datasets_plain_layout(_Config) ->
    archive_nested_datasets_test_base(?ARCHIVE_PLAIN_LAYOUT, true).

dip_archive_dataset_attached_to_dir_bagit_layout(_Config) ->
    archive_dataset_attached_to_dir_test_base(?ARCHIVE_BAGIT_LAYOUT, true).

dip_archive_dataset_attached_to_file_bagit_layout(_Config) ->
    archive_dataset_attached_to_file_test_base(?ARCHIVE_BAGIT_LAYOUT, true).

dip_archive_dataset_attached_to_hardlink_bagit_layout(_Config) ->
    archive_dataset_attached_to_hardlink_test_base(?ARCHIVE_BAGIT_LAYOUT, true).

dip_archive_dataset_attached_to_symlink_to_reg_file_bagit_layout_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_BAGIT_LAYOUT, true, true, direct).

dip_archive_dataset_containing_symlink_to_reg_file_bagit_layout_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_BAGIT_LAYOUT, true, true, nested).

dip_archive_dataset_containing_symlink_to_directory_bagit_layout_follow(_Config) ->
    archive_dataset_containing_symlink_to_directory_test_base(?ARCHIVE_BAGIT_LAYOUT, true, true, nested).

dip_archive_dataset_attached_to_symlink_to_reg_file_bagit_layout_not_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_BAGIT_LAYOUT, true, false, direct).

dip_archive_dataset_containing_symlink_to_reg_file_bagit_layout_not_follow(_Config) ->
    archive_dataset_containing_symlink_to_reg_file_test_base(?ARCHIVE_BAGIT_LAYOUT, true, false, nested).

dip_archive_dataset_containing_symlink_to_directory_bagit_layout_not_follow(_Config) ->
    archive_dataset_containing_symlink_to_directory_test_base(?ARCHIVE_BAGIT_LAYOUT, true, false, nested).

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

modify_preserved_plain_archive_test(_Config) ->
    modify_preserved_archive_test_base(?ARCHIVE_PLAIN_LAYOUT).

modify_preserved_bagit_archive_test(_Config) ->
    modify_preserved_archive_test_base(?ARCHIVE_BAGIT_LAYOUT).


verification_plain_modify_file(_Config) ->
    verification_modify_file_base(?ARCHIVE_PLAIN_LAYOUT).

verification_plain_modify_file_metadata(_Config) ->
    verification_modify_file_metadata_base(?ARCHIVE_PLAIN_LAYOUT).

verification_plain_create_file(_Config) ->
    verification_create_file_base(?ARCHIVE_PLAIN_LAYOUT).

verification_plain_remove_file(_Config) ->
    verification_remove_file_base(?ARCHIVE_PLAIN_LAYOUT).

verification_plain_recreate_file(_Config) ->
    verification_recreate_file_base(?ARCHIVE_PLAIN_LAYOUT).

dip_verification_plain(_Config) ->
    dip_verification_test_base(?ARCHIVE_PLAIN_LAYOUT).

nested_verification_plain(_Config) ->
    nested_verification_test_base(?ARCHIVE_PLAIN_LAYOUT).

verification_bagit_modify_file(_Config) ->
    verification_modify_file_base(?ARCHIVE_BAGIT_LAYOUT).

verification_bagit_modify_file_metadata(_Config) ->
    verification_modify_file_metadata_base(?ARCHIVE_BAGIT_LAYOUT).

verification_bagit_create_file(_Config) ->
    verification_create_file_base(?ARCHIVE_BAGIT_LAYOUT).

verification_bagit_remove_file(_Config) ->
    verification_remove_file_base(?ARCHIVE_BAGIT_LAYOUT).

verification_bagit_recreate_file(_Config) ->
    verification_recreate_file_base(?ARCHIVE_BAGIT_LAYOUT).

dip_verification_bagit(_Config) ->
    dip_verification_test_base(?ARCHIVE_BAGIT_LAYOUT).

nested_verification_bagit(_Config) ->
    nested_verification_test_base(?ARCHIVE_BAGIT_LAYOUT).


%===================================================================
% Test bases
%===================================================================

archive_dataset_attached_to_dir_test_base(Layout, IncludeDip) ->
    #object{
        guid = DirGuid,
        dataset = #dataset_object{
            id = DatasetId,
            archives = [#archive_object{id = ArchiveId}]
        }} = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip}}]},
            metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}
    }),
    archive_simple_dataset_test_base(DirGuid, DatasetId, ArchiveId, 0, 0).

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
    archive_simple_dataset_test_base(FileGuid, DatasetId, ArchiveId, 1, Size).

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

    archive_simple_dataset_test_base(LinkGuid, DatasetId, ArchiveId, 1, Size).

archive_dataset_containing_symlink_to_reg_file_test_base(Layout, IncludeDip, FollowSymlinks, Strategy) ->
    TargetSpec = #file_spec{
        content = ?RAND_CONTENT(),
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
        mode = 8#770
    },
    archive_dataset_containing_symlink_test_base(Layout, IncludeDip, FollowSymlinks, TargetSpec, Strategy).


archive_dataset_containing_symlink_to_directory_test_base(Layout, IncludeDip, FollowSymlinks, Strategy) ->
    TargetSpec = #dir_spec{
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
        mode = 8#770
    },
    archive_dataset_containing_symlink_test_base(Layout, IncludeDip, FollowSymlinks, TargetSpec, Strategy).

archive_dataset_containing_symlink_test_base(Layout, IncludeDip, FollowSymlinks, TargetSpec, Strategy) ->
    #object{name = TargetName, content = Content} = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, TargetSpec),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(oct_background:get_space_id(?SPACE)),
    LinkTarget = filename:join([SpaceIdPrefix, TargetName]),
    DatasetSpec = #dataset_spec{archives = [#archive_spec{
        config = #archive_config{layout = Layout, include_dip = IncludeDip, follow_symlinks = FollowSymlinks}
    }]},
    SymlinkSpec = #symlink_spec{
        symlink_value = LinkTarget
    },
    Spec = case Strategy of
        direct -> 
            SymlinkSpec#symlink_spec{dataset = DatasetSpec};
        nested ->
            #dir_spec{
                dataset = DatasetSpec,
                children = [SymlinkSpec]
            }
    end,
    #object{
        guid = DirGuid,
        dataset = #dataset_object{
            id = DatasetId,
            archives = [#archive_object{id = ArchiveId}]
        }
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, Spec),
    {FileCount, ExpSize} = case {Content, FollowSymlinks} of
        {_, false} -> {1, 0};
        {undefined, _} -> {0, 0};
        {_, true} -> {1, byte_size(Content)}
    end,
    archive_simple_dataset_test_base(DirGuid, DatasetId, ArchiveId, FileCount, ExpSize).

archive_simple_dataset_test_base(Guid, DatasetId, ArchiveId, FileCount, ExpSize) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    lists:foreach(fun(Provider) ->
        Node = oct_background:get_random_provider_node(Provider),
        SessionId = oct_background:get_user_session_id(?USER1, Provider),
        UserId = oct_background:get_user_id(?USER1),
        archive_tests_utils:assert_archive_dir_structure_is_correct(Node, SessionId, SpaceId, DatasetId, ArchiveId, UserId),
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


modify_preserved_archive_test_base(Layout) ->
    #object{
        dataset = #dataset_object{
            archives = [#archive_object{id = ArchiveId}]
        }
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE,
        #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout}}]},
            children = [#file_spec{content = ?RAND_CONTENT(), metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}}]
        }),
    [Provider | _] = oct_background:get_space_supporting_providers(?SPACE),
    Node = oct_background:get_random_provider_node(Provider),
    ?assertMatch({ok, #document{value = #archive{state = ?ARCHIVE_PRESERVED}}}, rpc:call(Node, archive, get, [ArchiveId]), ?ATTEMPTS),
    
    {ok, ArchiveDataDirGuid} = rpc:call(Node, archive, get_data_dir_guid, [ArchiveId]),
    {ok, [{DirGuid, _}]} = lfm_proxy:get_children(Node, ?ROOT_SESS_ID, #file_ref{guid = ArchiveDataDirGuid}, 0, 1),
    {ok, [{FileGuid, FileName}]} = lfm_proxy:get_children(Node, ?ROOT_SESS_ID, #file_ref{guid = DirGuid}, 0, 1),
    
    ?assertEqual({error, eperm}, lfm_proxy:unlink(Node, ?ROOT_SESS_ID, #file_ref{guid = FileGuid})),
    ?assertEqual({error, eperm},lfm_proxy:create(Node, ?ROOT_SESS_ID, DirGuid, FileName, ?DEFAULT_FILE_MODE)),
    ?assertEqual({error, eperm}, lfm_proxy:open(Node, ?ROOT_SESS_ID, #file_ref{guid = FileGuid}, write)),
    ?assertEqual({error, eperm}, lfm_proxy:set_metadata(Node, ?ROOT_SESS_ID, #file_ref{guid = FileGuid}, json, ?RAND_JSON_METADATA(), [])),
    ?assertEqual({error, eperm}, lfm_proxy:rm_recursive(Node, ?ROOT_SESS_ID, #file_ref{guid = DirGuid})).


verification_modify_file_base(Layout) ->
    ModificationFun = fun(Node, _DirGuid, FileGuid, _FileName, _Content, _Metadata) ->
        {ok, H} = lfm_proxy:open(Node, ?ROOT_SESS_ID, #file_ref{guid = FileGuid}, write),
        {ok, _} = lfm_proxy:write(Node, H, 0, ?RAND_CONTENT()),
        ok = lfm_proxy:close(Node, H)
    end,
    simple_verification_test_base(Layout, ModificationFun).


verification_modify_file_metadata_base(Layout) ->
    ModificationFun = fun(Node, _DirGuid, FileGuid, _FileName, _Content, _Metadata) ->
        ok = lfm_proxy:set_metadata(Node, ?ROOT_SESS_ID, #file_ref{guid = FileGuid}, json, ?RAND_JSON_METADATA(), [])
    end,
    simple_verification_test_base(Layout, ModificationFun).


verification_remove_file_base(Layout) ->
    ModificationFun = fun(Node, _DirGuid, FileGuid, _FileName, _Content, _Metadata) ->
        ok = lfm_proxy:unlink(Node, ?ROOT_SESS_ID, #file_ref{guid = FileGuid})
    end,
    simple_verification_test_base(Layout, ModificationFun).


verification_create_file_base(Layout) ->
    ModificationFun = fun(Node, DirGuid, _FileGuid, _FileName, _Content, _Metadata) ->
        {ok, _} = lfm_proxy:create(Node, ?ROOT_SESS_ID, DirGuid, generator:gen_name(), ?DEFAULT_FILE_MODE)
    end,
    simple_verification_test_base(Layout, ModificationFun).


verification_recreate_file_base(Layout) ->
    ModificationFun = fun(Node, DirGuid, FileGuid, FileName, Content, Metadata) ->
        ok = lfm_proxy:unlink(Node, ?ROOT_SESS_ID, #file_ref{guid = FileGuid}),
        {ok, NewFileGuid} = lfm_proxy:create(Node, ?ROOT_SESS_ID, DirGuid, FileName, ?DEFAULT_FILE_MODE),
        {ok, H} = lfm_proxy:open(Node, ?ROOT_SESS_ID, #file_ref{guid = NewFileGuid}, write),
        {ok, _} = lfm_proxy:write(Node, H, 0, Content),
        ok = lfm_proxy:close(Node, H),
        ok = lfm_proxy:set_metadata(Node, ?ROOT_SESS_ID, #file_ref{guid = NewFileGuid}, json, Metadata, [])
    end,
    simple_verification_test_base(Layout, ModificationFun).


simple_verification_test_base(Layout, ModificationFun) ->
    OriginalContent = ?RAND_CONTENT(),
    OriginalMetadata = ?RAND_JSON_METADATA(),
    archive_tests_utils:mock_archive_verification(),
    #object{
        dataset = #dataset_object{
            archives = [#archive_object{id = ArchiveId}]
        }
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, 
        #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout}}]},
            children = [#file_spec{content = OriginalContent, metadata = #metadata_spec{json = OriginalMetadata}}]
    }),
    {ok, Pid} = archive_tests_utils:wait_for_archive_verification_traverse(ArchiveId),
    
    [Provider | _] = oct_background:get_space_supporting_providers(?SPACE),
    Node = oct_background:get_random_provider_node(Provider),
    SessionId = oct_background:get_user_session_id(?USER1, Provider),
    ?assertMatch({ok, #document{value = #archive{state = ?ARCHIVE_VERIFYING}}}, rpc:call(Node, archive, get, [ArchiveId]), ?ATTEMPTS),
    
    {ok, ArchiveDataDirGuid} = rpc:call(Node, archive, get_data_dir_guid, [ArchiveId]),
    {ok, [{ArchivedDirGuid, _}]} = lfm_proxy:get_children(Node, SessionId, #file_ref{guid = ArchiveDataDirGuid}, 0, 1),
    {ok, [{ArchivedFileGuid, ArchivedFileName}]} = lfm_proxy:get_children(Node, SessionId, #file_ref{guid = ArchivedDirGuid}, 0, 1),
    
    ModificationFun(Node, ArchivedDirGuid, ArchivedFileGuid, ArchivedFileName, OriginalContent, OriginalMetadata),
    
    archive_tests_utils:start_verification_traverse(Pid, ArchiveId),
    
    ?assertMatch({ok, #document{value = #archive{state = ?ARCHIVE_VERIFICATION_FAILED}}}, rpc:call(Node, archive, get, [ArchiveId]), ?ATTEMPTS).


dip_verification_test_base(Layout) ->
    archive_tests_utils:mock_archive_verification(),
    #object{
        dataset = #dataset_object{
            archives = [#archive_object{id = AipArchiveId}]
        }
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE,
        #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = true}}]},
            children = [#file_spec{content = ?RAND_CONTENT(), metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}}]
        }),
    
    [Provider | _] = oct_background:get_space_supporting_providers(?SPACE),
    Node = oct_background:get_random_provider_node(Provider),
    
    {ok, #document{value = #archive{related_dip = DipArchiveId}}} = rpc:call(Node, archive, get, [AipArchiveId]),
    
    lists:foreach(fun(ArchiveId) ->
        {ok, Pid} = archive_tests_utils:wait_for_archive_verification_traverse(ArchiveId),
        archive_tests_utils:start_verification_traverse(Pid, ArchiveId),
        ?assertMatch({ok, #document{value = #archive{state = ?ARCHIVE_PRESERVED}}}, rpc:call(Node, archive, get, [ArchiveId]), ?ATTEMPTS)
    end, [AipArchiveId, DipArchiveId]).
    

nested_verification_test_base(Layout) ->
    archive_tests_utils:mock_archive_verification(),
    #object{
        dataset = #dataset_object{
            archives = [#archive_object{id = ArchiveId}]
        },
        children = [
            #object{dataset = #dataset_object{id = NestedDatasetId1}},
            #object{dataset = #dataset_object{id = NestedDatasetId2}}
        ]
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE,
        #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, create_nested_archives = true}}]},
            children = [
                #file_spec{dataset = #dataset_spec{}},
                #dir_spec{dataset = #dataset_spec{}}
            ]
        }),
    
    [Provider | _] = oct_background:get_space_supporting_providers(?SPACE),
    Node = oct_background:get_random_provider_node(Provider),
    
    {ok, [{_, NestedArchiveId1} | _], _} = lfm_proxy:list_archives(Node, ?ROOT_SESS_ID, NestedDatasetId1, #{offset => 0, limit => 1}),
    {ok, [{_, NestedArchiveId2} | _], _} = lfm_proxy:list_archives(Node, ?ROOT_SESS_ID, NestedDatasetId2, #{offset => 0, limit => 1}),
    
    lists:foreach(fun(Id) ->
        {ok, Pid} = archive_tests_utils:wait_for_archive_verification_traverse(Id),
        archive_tests_utils:start_verification_traverse(Pid, Id),
        ?assertMatch({ok, #document{value = #archive{state = ?ARCHIVE_PRESERVED}}}, rpc:call(Node, archive, get, [Id]), ?ATTEMPTS)
    end, [NestedArchiveId1, NestedArchiveId2, ArchiveId]).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [?MODULE, archive_tests_utils]} | Config], #onenv_test_config{
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
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_unload(Nodes),
    ok.
