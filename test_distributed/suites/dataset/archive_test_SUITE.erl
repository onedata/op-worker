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
-include("modules/archive/archive.hrl").
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
    archive_dataset_attached_to_space_dir/1,
    archive_dataset_attached_to_dir/1,
    archive_dataset_attached_to_file/1,
    archive_dataset_attached_to_hardlink/1,
    archive_dataset_attached_to_symlink/1,
    archivisation_of_detached_dataset_should_be_impossible/1,
    archive_of_detached_dataset_should_be_accessible/1,
    archive_of_dataset_associated_with_deleted_file_should_be_accessible/1,
    archive_reattached_dataset/1,
    removal_of_not_empty_dataset_should_fail/1,
    iterate_over_1000_archives_using_offset_and_limit_1/1,
    iterate_over_1000_archives_using_offset_and_limit_10/1,
    iterate_over_1000_archives_using_offset_and_limit_100/1,
    iterate_over_1000_archives_using_offset_and_limit_1000/1,
    iterate_over_1000_archives_using_offset_and_limit_10000/1,
    iterate_over_1000_archives_using_start_index_and_limit_1/1,
    iterate_over_1000_archives_using_start_index_and_limit_10/1,
    iterate_over_1000_archives_using_start_index_and_limit_100/1,
    iterate_over_1000_archives_using_start_index_and_limit_1000/1,
    iterate_over_1000_archives_using_start_index_and_limit_10000/1,

    % sequential tests
    archive_dataset_many_times/1,
    time_warp_test/1, 
    create_archive_privileges_test/1, 
    view_archive_privileges_test/1, 
    remove_archive_privileges_test/1
]).

groups() -> [
    {parallel_tests, [parallel], [
        archive_dataset_attached_to_space_dir,
        archive_dataset_attached_to_dir,
        archive_dataset_attached_to_file,
        archive_dataset_attached_to_hardlink,
        archive_dataset_attached_to_symlink,
        archivisation_of_detached_dataset_should_be_impossible,
        archive_of_detached_dataset_should_be_accessible,
        archive_of_dataset_associated_with_deleted_file_should_be_accessible,
        archive_reattached_dataset,
        removal_of_not_empty_dataset_should_fail,
        iterate_over_1000_archives_using_offset_and_limit_1,
        iterate_over_1000_archives_using_offset_and_limit_10,
        iterate_over_1000_archives_using_offset_and_limit_100,
        iterate_over_1000_archives_using_offset_and_limit_1000,
        iterate_over_1000_archives_using_offset_and_limit_10000,
        iterate_over_1000_archives_using_start_index_and_limit_1,
        iterate_over_1000_archives_using_start_index_and_limit_10,
        iterate_over_1000_archives_using_start_index_and_limit_100,
        iterate_over_1000_archives_using_start_index_and_limit_1000,
        iterate_over_1000_archives_using_start_index_and_limit_10000
    ]},
    {sequential_tests, [sequential], [
        archive_dataset_many_times,
        time_warp_test,
        create_archive_privileges_test,
        view_archive_privileges_test,
        remove_archive_privileges_test
    ]}
].


all() -> [
    {group, parallel_tests},
    {group, sequential_tests}
].

-define(ATTEMPTS, 30).

-define(SPACE, space1).


-define(TEST_ARCHIVE_PARAMS, #{
    type => ?FULL_ARCHIVE,
    character => ?DIP,
    data_structure => ?BAGIT,
    metadata_structure => ?BUILT_IN
}).

-define(TEST_DESCRIPTION, <<"TEST DESCRIPTION">>).
-define(TEST_ARCHIVE_ATTRS, #{
    description => ?TEST_DESCRIPTION
}).

-define(TEST_TIMESTAMP, 1000000000).

-define(RAND_NAME, str_utils:rand_hex(20)).

%===================================================================
% Parallel tests - tests which can be safely run in parallel
% as they do not interfere with any other test.
%===================================================================

archive_dataset_attached_to_space_dir(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    simple_archive_crud_test_base(SpaceGuid).

archive_dataset_attached_to_dir(_Config) ->
    #object{guid = DirGuid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #dir_spec{}),
    simple_archive_crud_test_base(DirGuid).

archive_dataset_attached_to_file(_Config) ->
    #object{guid = FileGuid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{}),
    simple_archive_crud_test_base(FileGuid).

archive_dataset_attached_to_hardlink(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(?SPACE),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    #object{guid = FileGuid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{}),
    {ok, #file_attr{guid = LinkGuid}} =
        lfm_proxy:make_link(P1Node, UserSessIdP1, ?FILE_REF(FileGuid), ?FILE_REF(SpaceGuid), ?RAND_NAME),
    simple_archive_crud_test_base(LinkGuid).

archive_dataset_attached_to_symlink(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    #object{guid = DirGuid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #dir_spec{}),
    {ok, DirPath} = lfm_proxy:get_file_path(P1Node, UserSessIdP1, DirGuid),
    #object{guid = LinkGuid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #symlink_spec{symlink_value = DirPath}),
    simple_archive_crud_test_base(LinkGuid).

archivisation_of_detached_dataset_should_be_impossible(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    #object{guid = Guid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{}),

    {ok, DatasetId} = ?assertMatch({ok, _},
        lfm_proxy:establish_dataset(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?no_flags_mask)),
    ok = lfm_proxy:detach_dataset(P1Node, UserSessIdP1, DatasetId),

    ?assertMatch({error, ?EINVAL},
        lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS)).

archive_of_detached_dataset_should_be_accessible(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    #object{guid = Guid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{}),

    {ok, DatasetId} = ?assertMatch({ok, _},
        lfm_proxy:establish_dataset(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?no_flags_mask)),
    {ok, ArchiveId} = ?assertMatch({ok, _},
        lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS)),
    ?assertMatch({ok, #archive_info{}},
        lfm_proxy:get_archive_info(P1Node, UserSessIdP1, ArchiveId)),
    Index = archives_list:index(ArchiveId, ?TEST_TIMESTAMP),
    ?assertEqual({ok, [{Index, ArchiveId}], true},
        lfm_proxy:list_archives(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})),

    ok = lfm_proxy:detach_dataset(P1Node, UserSessIdP1, DatasetId),

    ?assertMatch({ok, #archive_info{}},
        lfm_proxy:get_archive_info(P1Node, UserSessIdP1, ArchiveId)),
    ?assertEqual({ok, [{Index, ArchiveId}], true},
        lfm_proxy:list_archives(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})).

archive_of_dataset_associated_with_deleted_file_should_be_accessible(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    #object{guid = Guid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{}),

    {ok, DatasetId} = ?assertMatch({ok, _},
        lfm_proxy:establish_dataset(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?no_flags_mask)),
    {ok, ArchiveId} = ?assertMatch({ok, _},
        lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS)),
    ?assertMatch({ok, #archive_info{}},
        lfm_proxy:get_archive_info(P1Node, UserSessIdP1, ArchiveId)),
    Index = archives_list:index(ArchiveId, ?TEST_TIMESTAMP),
    ?assertEqual({ok, [{Index, ArchiveId}], true},
        lfm_proxy:list_archives(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})),

    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(Guid)),

    ?assertMatch({ok, #archive_info{}},
        lfm_proxy:get_archive_info(P1Node, UserSessIdP1, ArchiveId)),
    ?assertEqual({ok, [{Index, ArchiveId}], true},
        lfm_proxy:list_archives(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})).

archive_reattached_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    #object{guid = Guid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{}),

    {ok, DatasetId} = ?assertMatch({ok, _},
        lfm_proxy:establish_dataset(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?no_flags_mask)),
    {ok, ArchiveId} = ?assertMatch({ok, _},
        lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS)),
    ?assertMatch({ok, #archive_info{}},
        lfm_proxy:get_archive_info(P1Node, UserSessIdP1, ArchiveId)),
    Index = archives_list:index(ArchiveId, ?TEST_TIMESTAMP),
    ?assertEqual({ok, [{Index, ArchiveId}], true},
        lfm_proxy:list_archives(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})),

    ok = lfm_proxy:detach_dataset(P1Node, UserSessIdP1, DatasetId),
    ok = lfm_proxy:reattach_dataset(P1Node, UserSessIdP1, DatasetId),

    {ok, ArchiveId2} = ?assertMatch({ok, _},
        lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS)),

    ?assertMatch({ok, #archive_info{}}, lfm_proxy:get_archive_info(P1Node, UserSessIdP1, ArchiveId2)),
    ?assertMatch({ok, [_, _], true},
        lfm_proxy:list_archives(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})).

removal_of_not_empty_dataset_should_fail(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    #object{guid = Guid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{}),

    {ok, DatasetId} = ?assertMatch({ok, _},
        lfm_proxy:establish_dataset(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?no_flags_mask)),
    {ok, ArchiveId} = ?assertMatch({ok, _},
        lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS)),

    ?assertMatch({ok, #archive_info{}},
        lfm_proxy:get_archive_info(P1Node, UserSessIdP1, ArchiveId)),
    Index = archives_list:index(ArchiveId, ?TEST_TIMESTAMP),
    ?assertEqual({ok, [{Index, ArchiveId}], true},
        lfm_proxy:list_archives(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})),

    ?assertEqual({error, ?ENOTEMPTY},
        lfm_proxy:remove_dataset(P1Node, UserSessIdP1, DatasetId)),

    ?assertEqual(ok, lfm_proxy:remove_archive(P1Node, UserSessIdP1, ArchiveId)),
    ?assertEqual(ok, lfm_proxy:remove_dataset(P1Node, UserSessIdP1, DatasetId)).

iterate_over_1000_archives_using_offset_and_limit_1(_Config) ->
    iterate_over_archives_test_base(1000, offset, 1).

iterate_over_1000_archives_using_offset_and_limit_10(_Config) ->
    iterate_over_archives_test_base(1000, offset, 10).

iterate_over_1000_archives_using_offset_and_limit_100(_Config) ->
    iterate_over_archives_test_base(1000, offset, 100).

iterate_over_1000_archives_using_offset_and_limit_1000(_Config) ->
    iterate_over_archives_test_base(1000, offset, 1000).

iterate_over_1000_archives_using_offset_and_limit_10000(_Config) ->
    iterate_over_archives_test_base(1000, offset, 10000).

iterate_over_1000_archives_using_start_index_and_limit_1(_Config) ->
    iterate_over_archives_test_base(1000, start_index, 1).

iterate_over_1000_archives_using_start_index_and_limit_10(_Config) ->
    iterate_over_archives_test_base(1000, start_index, 10).

iterate_over_1000_archives_using_start_index_and_limit_100(_Config) ->
    iterate_over_archives_test_base(1000, start_index, 100).

iterate_over_1000_archives_using_start_index_and_limit_1000(_Config) ->
    iterate_over_archives_test_base(1000, start_index, 1000).

iterate_over_1000_archives_using_start_index_and_limit_10000(_Config) ->
    iterate_over_archives_test_base(1000, start_index, 10000).

%===================================================================
% Sequential tests - tests which must be performed one after another
% to ensure that they do not interfere with each other (e. g. by
% modifying mocked global_clock or changing user's privileges)
%===================================================================

archive_dataset_many_times(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user1, paris),

    #object{guid = Guid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{}),
    Count = 1000,
    {ok, DatasetId} = ?assertMatch({ok, _},
        lfm_proxy:establish_dataset(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?no_flags_mask)),

    Timestamp = global_clock_timestamp(P1Node),

    ExpArchiveIdsReversed = lists:map(fun(I) ->
        {ok, ArchiveId} = ?assertMatch({ok, _},
            lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS)),
        % mock time lapse to ensure that archives will have different creation timestamps
        time_test_utils:simulate_seconds_passing(1),
        Index = archives_list:index(ArchiveId, Timestamp + I - 1),
        {Index, ArchiveId}
    end, lists:seq(1, Count)),

    lists:foreach(fun({_Index, ArchiveId}) ->
        ?assertMatch({ok, #archive_info{}},
            lfm_proxy:get_archive_info(P1Node, UserSessIdP1, ArchiveId))
    end, ExpArchiveIdsReversed),

    ?assertMatch({ok, #dataset_info{archives_count = Count}},
        lfm_proxy:get_dataset_info(P1Node, UserSessIdP1, DatasetId)),
    ?assertMatch({ok, #dataset_info{archives_count = Count}},
        lfm_proxy:get_dataset_info(P2Node, UserSessIdP2, DatasetId), ?ATTEMPTS),

    ?assertEqual({ok, lists:reverse(ExpArchiveIdsReversed), false},
        lfm_proxy:list_archives(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => Count})),
    ?assertEqual({ok, lists:reverse(ExpArchiveIdsReversed), false},
        lfm_proxy:list_archives(P2Node, UserSessIdP2, DatasetId, #{offset => 0, limit => Count}), ?ATTEMPTS).

time_warp_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    #object{guid = Guid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{}),
    {ok, DatasetId} = ?assertMatch({ok, _},
        lfm_proxy:establish_dataset(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?no_flags_mask)),

    {ok, ArchiveId} = ?assertMatch({ok, _},
        lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS)),

    time_test_utils:simulate_seconds_passing(-1),

    {ok, ArchiveId2} = ?assertMatch({ok, _},
        lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS)),

    ?assertMatch({ok, [{_, ArchiveId}, {_, ArchiveId2}], true},
        lfm_proxy:list_archives(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})).

create_archive_privileges_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    User2SessIdP1 = oct_background:get_user_session_id(user2, krakow),
    UserId2 = oct_background:get_user_id(user2),
    SpaceId = oct_background:get_space_id(?SPACE),

    #object{guid = Guid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{}),
    {ok, DatasetId} = ?assertMatch({ok, _},
        lfm_proxy:establish_dataset(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?no_flags_mask)),
    {ok, ArchiveId} = lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS),

    RequiredPrivileges = privileges:from_list([?SPACE_MANAGE_DATASETS, ?SPACE_CREATE_ARCHIVES]),
    AllPrivileges = privileges:from_list(RequiredPrivileges ++ privileges:space_member()),

    lists:foreach(fun(Privilege) ->

        ensure_privilege_revoked(P1Node, SpaceId, UserId2, Privilege, AllPrivileges),
        % user2 cannot create archive
        ?assertEqual({error, ?EPERM},
            lfm_proxy:archive_dataset(P1Node, User2SessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS)),
        % user2 cannot modify an existing archive either
        ?assertEqual({error, ?EPERM},
            lfm_proxy:update_archive(P1Node, User2SessIdP1, ArchiveId, #{description => ?TEST_DESCRIPTION})),

        ensure_privilege_assigned(P1Node, SpaceId, UserId2, Privilege, AllPrivileges),
        % user2 can now create archive
        ?assertMatch({ok, _},
            lfm_proxy:archive_dataset(P1Node, User2SessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS)),
        % as well as modify an existing one
        ?assertMatch(ok,
            lfm_proxy:update_archive(P1Node, User2SessIdP1, ArchiveId, #{description => ?TEST_DESCRIPTION}))
    end, RequiredPrivileges).


view_archive_privileges_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    User2SessIdP1 = oct_background:get_user_session_id(user2, krakow),
    UserId2 = oct_background:get_user_id(user2),
    SpaceId = oct_background:get_space_id(?SPACE),

    #object{guid = Guid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{}),
    {ok, DatasetId} = lfm_proxy:establish_dataset(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?no_flags_mask),
    {ok, ArchiveId} = lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS),

    AllPrivileges = privileges:from_list([?SPACE_VIEW_ARCHIVES | privileges:space_member()]),

    % assign user only space_member privileges
    ensure_privilege_revoked(P1Node, SpaceId, UserId2, ?SPACE_VIEW_ARCHIVES, AllPrivileges),

    % user2 cannot fetch archive info
    ?assertEqual({error, ?EPERM},
        lfm_proxy:get_archive_info(P1Node, User2SessIdP1, ArchiveId), ?ATTEMPTS),
    % neither can he list the archives
    ?assertEqual({error, ?EPERM},
        lfm_proxy:list_archives(P1Node, User2SessIdP1, DatasetId, #{offset => 0, limit => 10})),

    % assign user2 privilege to view archives
    ensure_privilege_assigned(P1Node, SpaceId, UserId2, ?SPACE_VIEW_ARCHIVES, AllPrivileges),

    % now user2 should be able to fetch archive info
    ?assertMatch({ok, _},
        lfm_proxy:get_archive_info(P1Node, User2SessIdP1, ArchiveId)),
    % as well as list the archives
    ?assertMatch({ok, [{_, ArchiveId}], _},
        lfm_proxy:list_archives(P1Node, User2SessIdP1, DatasetId, #{offset => 0, limit => 10})).

remove_archive_privileges_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    User2SessIdP1 = oct_background:get_user_session_id(user2, krakow),
    UserId2 = oct_background:get_user_id(user2),
    SpaceId = oct_background:get_space_id(?SPACE),

    #object{guid = Guid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{}),
    {ok, DatasetId} = ?assertMatch({ok, _},
        lfm_proxy:establish_dataset(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?no_flags_mask)),
    {ok, ArchiveId1} = lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS),
    {ok, ArchiveId2} = lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS),

    RequiredPrivileges = privileges:from_list([?SPACE_MANAGE_DATASETS, ?SPACE_REMOVE_ARCHIVES]),
    AllPrivileges = privileges:from_list(RequiredPrivileges ++ privileges:space_member()),

    lists:foreach(fun({Privilege, ArchiveId}) ->

        ensure_privilege_revoked(P1Node, SpaceId, UserId2, Privilege, AllPrivileges),
        % user2 cannot remove the archive
        ?assertEqual({error, ?EPERM}, lfm_proxy:remove_archive(P1Node, User2SessIdP1, ArchiveId)),

        ensure_privilege_assigned(P1Node, SpaceId, UserId2, Privilege, AllPrivileges),
        % user2 can now remove archive
        ?assertEqual(ok, lfm_proxy:remove_archive(P1Node, User2SessIdP1, ArchiveId))
    
    end, lists:zip(RequiredPrivileges, [ArchiveId1, ArchiveId2])).


%===================================================================
% Test bases
%===================================================================

simple_archive_crud_test_base(Guid) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user1, paris),
    {ok, DatasetId} = ?assertMatch({ok, _},
        lfm_proxy:establish_dataset(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?no_flags_mask)),

    % create archive
    {ok, ArchiveId} = lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS),

    Index = archives_list:index(ArchiveId, ?TEST_TIMESTAMP),
    ExpArchiveInfo = #archive_info{
        id = ArchiveId,
        dataset_id = DatasetId,
        root_dir = undefined,
        creation_timestamp = ?TEST_TIMESTAMP,
        type = ?FULL_ARCHIVE,
        character = ?DIP,
        data_structure = ?BAGIT,
        metadata_structure = ?BUILT_IN,
        index = Index,
        description = ?TEST_DESCRIPTION
    },

    % verify whether Archive is visible in the local provider
    ?assertEqual({ok, ExpArchiveInfo},
        lfm_proxy:get_archive_info(P1Node, UserSessIdP1, ArchiveId)),
    ?assertEqual({ok, [{Index, ArchiveId}], true},
        lfm_proxy:list_archives(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10}, ?BASIC_INFO)),
    ?assertEqual({ok, [ExpArchiveInfo], true},
        lfm_proxy:list_archives(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10}, ?EXTENDED_INFO)),

    % verify whether Archive is visible in the remote provider
    ?assertEqual({ok, ExpArchiveInfo},
        lfm_proxy:get_archive_info(P2Node, UserSessIdP2, ArchiveId), ?ATTEMPTS),
    ?assertEqual({ok, [{Index, ArchiveId}], true},
        lfm_proxy:list_archives(P2Node, UserSessIdP2, DatasetId, #{offset => 0, limit => 10}, ?BASIC_INFO), ?ATTEMPTS),
    ?assertEqual({ok, [ExpArchiveInfo], true},
        lfm_proxy:list_archives(P2Node, UserSessIdP2, DatasetId, #{offset => 0, limit => 10}, ?EXTENDED_INFO), ?ATTEMPTS),

    % update archive
    UpdateDescription = <<"NEW DESCRIPTION">>,
    ExpArchiveInfo2 = ExpArchiveInfo#archive_info{description = UpdateDescription},
    ?assertEqual(ok,
        lfm_proxy:update_archive(P2Node, UserSessIdP2, ArchiveId, #{description => UpdateDescription})),
    ?assertEqual({ok, ExpArchiveInfo2},
        lfm_proxy:get_archive_info(P2Node, UserSessIdP2, ArchiveId)),
    ?assertEqual({ok, ExpArchiveInfo2},
        lfm_proxy:get_archive_info(P1Node, UserSessIdP1, ArchiveId), ?ATTEMPTS),

    % remove archive
    ok = lfm_proxy:remove_archive(P1Node, UserSessIdP1, ArchiveId),

    % verify whether Archive has been removed in the local provider
    ?assertEqual({error, ?ENOENT},
        lfm_proxy:get_archive_info(P1Node, UserSessIdP1, ArchiveId)),
    ?assertEqual({ok, [], true},
        lfm_proxy:list_archives(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})),

    % verify whether Archive has been removed in the remote provider
    ?assertEqual({error, ?ENOENT},
        lfm_proxy:get_archive_info(P2Node, UserSessIdP2, ArchiveId), ?ATTEMPTS),
    ?assertEqual({ok, [], true},
        lfm_proxy:list_archives(P2Node, UserSessIdP2, DatasetId, #{offset => 0, limit => 10}), ?ATTEMPTS).


iterate_over_archives_test_base(ArchivesCount, ListingMethod, Limit) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),

    #object{guid = Guid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{}),
    {ok, DatasetId} = ?assertMatch({ok, _},
        lfm_proxy:establish_dataset(P1Node, UserSessIdP1, ?FILE_REF(Guid), ?no_flags_mask)),

    UnsortedArchiveIds = lists:map(fun(_) ->
        {ok, ArchiveId} = lfm_proxy:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_PARAMS, ?TEST_ARCHIVE_ATTRS),
        ArchiveId
    end, lists:seq(1, ArchivesCount)),

    % all archives will have the same timestamp so they will be sorted in ascending order by they ids
    ExpArchiveIds = lists:sort(UnsortedArchiveIds),

    ListingOpts = case ListingMethod of
        offset -> #{offset => 0, limit => Limit};
        start_index -> #{start_index => <<>>, limit => Limit}
    end,

    check_if_all_archives_listed(ExpArchiveIds, P1Node, UserSessIdP1, DatasetId, ListingOpts).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).

end_per_suite(_Config) ->
    oct_background:end_per_suite().

init_per_group(_Group, Config) ->
    time_test_utils:freeze_time(Config),
    time_test_utils:set_current_time_seconds(?TEST_TIMESTAMP),
    lfm_proxy:init(Config, false).

end_per_group(parallel_tests, Config) ->
    time_test_utils:unfreeze_time(Config),
    end_per_group(default, Config);
end_per_group(_Group, Config) ->
    onenv_dataset_test_utils:cleanup_all_datasets(krakow, ?SPACE),
    lfm_proxy:teardown(Config).

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%===================================================================
% Internal functions
%===================================================================

global_clock_timestamp(Node) ->
    rpc:call(Node, global_clock, timestamp_seconds, []).

check_if_all_archives_listed([], _Node, _SessId, _DatasetId, _Opts) ->
    true;
check_if_all_archives_listed(ExpArchiveIds, Node, SessId, DatasetId, Opts) ->
    {ok, ListedArchives, IsLast} = lfm_proxy:list_archives(Node, SessId, DatasetId, Opts),
    Limit = maps:get(limit, Opts),
    ListedArchiveIds = [AId || {_, AId} <- ListedArchives],
    ?assertEqual(lists:sublist(ExpArchiveIds, 1, Limit), ListedArchiveIds),
    RestExpArchiveIds = ExpArchiveIds -- ListedArchiveIds,
    case {IsLast, RestExpArchiveIds == []} of
        {true, true} ->
            ok;
        {true, false} ->
            ct:fail("Not all expected archive were listed.~nExpected: ~p", [ExpArchiveIds]);
        {false, _} ->
            NewOpts = update_opts(Opts, ListedArchives),
            check_if_all_archives_listed(RestExpArchiveIds, Node, SessId, DatasetId, NewOpts)
    end.

update_opts(Opts = #{offset := Offset}, ListedArchives) ->
    Opts#{offset => Offset + length(ListedArchives)};
update_opts(Opts = #{start_index := _}, ListedArchives) ->
    Opts#{start_index => element(1, lists:last(ListedArchives)), offset => 1}.

has_eff_privilege(Node, SpaceId, UserId, Privilege) ->
    rpc:call(Node, space_logic, has_eff_privilege, [SpaceId, UserId, Privilege]).

ensure_privilege_revoked(Node, SpaceId, UserId, Privilege, AllPrivileges) ->
    % assign AllPrivileges with Privilege missing
    Privileges = privileges:from_list(AllPrivileges -- [Privilege]),
    ozw_test_rpc:space_set_user_privileges(SpaceId, UserId, Privileges),
    % wait till information is synced from onezone
    ?assertMatch(false, has_eff_privilege(Node, SpaceId, UserId, Privilege), ?ATTEMPTS).

ensure_privilege_assigned(Node, SpaceId, UserId, Privilege, AllPrivileges) ->
    % assign user AllPrivileges
    ozw_test_rpc:space_set_user_privileges(SpaceId, UserId, AllPrivileges),
    % wait till information is synced from onezone
    ?assertMatch(true, has_eff_privilege(Node, SpaceId, UserId, Privilege), ?ATTEMPTS).