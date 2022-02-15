%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of archives model.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_model_test_SUITE).
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
-include_lib("ctool/include/test/test_utils.hrl").


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
    archive_dataset_attached_to_dir/1,
    archive_dataset_attached_to_file/1,
    archive_dataset_attached_to_hardlink/1,
    archive_dataset_attached_to_symlink/1,
    archivisation_of_detached_dataset_should_be_impossible/1,
    archive_of_detached_dataset_should_be_accessible/1,
    archive_of_dataset_associated_with_deleted_file_should_be_accessible/1,
    archive_reattached_dataset/1,
    removal_of_not_empty_dataset_should_fail/1,
    iterate_over_100_archives_using_offset_and_limit_1/1,
    iterate_over_100_archives_using_offset_and_limit_10/1,
    iterate_over_100_archives_using_offset_and_limit_100/1,
    iterate_over_100_archives_using_offset_and_limit_1000/1,
    iterate_over_100_archives_using_offset_and_limit_10000/1,
    iterate_over_100_archives_using_start_index_and_limit_1/1,
    iterate_over_100_archives_using_start_index_and_limit_10/1,
    iterate_over_100_archives_using_start_index_and_limit_100/1,
    iterate_over_100_archives_using_start_index_and_limit_1000/1,
    iterate_over_100_archives_using_start_index_and_limit_10000/1,

    % sequential tests
    archive_dataset_attached_to_space_dir/1,
    archive_dataset_many_times/1,
    time_warp_test/1,
    create_archive_privileges_test/1,
    view_archive_privileges_test/1,
    remove_archive_privileges_test/1
]).

groups() -> [
    {time_mock_parallel_tests, [parallel], [
        % these tests has been moved to separate group so that
        % mocking time does not interfere with other tests
        archive_dataset_attached_to_dir,
        archive_dataset_attached_to_file,
        archive_dataset_attached_to_hardlink,
        archive_dataset_attached_to_symlink
    ]},
    {parallel_tests, [parallel], [
        archivisation_of_detached_dataset_should_be_impossible,
        archive_of_detached_dataset_should_be_accessible,
        archive_of_dataset_associated_with_deleted_file_should_be_accessible,
        archive_reattached_dataset,
        removal_of_not_empty_dataset_should_fail,
        iterate_over_100_archives_using_offset_and_limit_1,
        iterate_over_100_archives_using_offset_and_limit_10,
        iterate_over_100_archives_using_offset_and_limit_100,
        iterate_over_100_archives_using_offset_and_limit_1000,
        iterate_over_100_archives_using_offset_and_limit_10000,
        iterate_over_100_archives_using_start_index_and_limit_1,
        iterate_over_100_archives_using_start_index_and_limit_10,
        iterate_over_100_archives_using_start_index_and_limit_100,
        iterate_over_100_archives_using_start_index_and_limit_1000,
        iterate_over_100_archives_using_start_index_and_limit_10000
    ]},
    {sequential_tests, [sequential], [
        archive_dataset_attached_to_space_dir,
        time_warp_test,
        create_archive_privileges_test,
        view_archive_privileges_test,
        remove_archive_privileges_test,
        archive_dataset_many_times
    ]}
].


all() -> [
    {group, time_mock_parallel_tests},
    {group, parallel_tests},
    {group, sequential_tests}
].

-define(ATTEMPTS, 300).

-define(SPACE, space_krk_par_p).


-define(TEST_ARCHIVE_CONFIG, #archive_config{
    incremental = #{<<"enabled">> => false},
    include_dip = false,
    layout = ?ARCHIVE_PLAIN_LAYOUT,
    follow_symlinks = false
}).

-define(TEST_DESCRIPTION1, <<"TEST DESCRIPTION">>).
-define(TEST_DESCRIPTION2, <<"TEST DESCRIPTION2">>).
-define(TEST_ARCHIVE_PRESERVED_CALLBACK1, <<"https://preserved1.org">>).
-define(TEST_ARCHIVE_PRESERVED_CALLBACK2, <<"https://preserved1.org">>).
-define(TEST_ARCHIVE_PURGED_CALLBACK1, <<"https://purged1.org">>).
-define(TEST_ARCHIVE_PURGED_CALLBACK2, <<"https://purged2.org">>).
-define(TEST_ARCHIVE_PURGED_CALLBACK3, <<"https://purged3.org">>).

-define(RAND_NAME, str_utils:rand_hex(20)).
-define(RAND_CONTENT(Size), crypto:strong_rand_bytes(Size)).

%===================================================================
% Parallel tests that use clock freezer mock.
%===================================================================

archive_dataset_attached_to_dir(_Config) ->
    #object{dataset = #dataset_object{id = DatasetId}} =
        onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #dir_spec{dataset = #dataset_spec{}}),
    simple_archive_crud_test_base(DatasetId, ?DIRECTORY_TYPE).

archive_dataset_attached_to_file(_Config) ->
    Size = 20,
    #object{dataset = #dataset_object{id = DatasetId}} =
        onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{
            dataset = #dataset_spec{},
            content = ?RAND_CONTENT(Size)
        }),
    simple_archive_crud_test_base(DatasetId, ?REGULAR_FILE_TYPE, Size).

archive_dataset_attached_to_hardlink(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(?SPACE),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    Size = 20,
    #object{guid = FileGuid} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{
        content = ?RAND_CONTENT(Size)
    }),
    {ok, #file_attr{guid = LinkGuid}} =
        lfm_proxy:make_link(P1Node, UserSessIdP1, ?FILE_REF(FileGuid), ?FILE_REF(SpaceGuid), ?RAND_NAME),
    #dataset_object{id = DatasetId} = onenv_dataset_test_utils:set_up_and_sync_dataset(user1, LinkGuid),
    simple_archive_crud_test_base(DatasetId, ?LINK_TYPE, Size).

archive_dataset_attached_to_symlink(_Config) ->
    #object{name = DirName} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #dir_spec{}),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(oct_background:get_space_id(?SPACE)),
    LinkTarget = filename:join([SpaceIdPrefix, DirName]),
    #object{dataset = #dataset_object{id = DatasetId}} = onenv_file_test_utils:create_and_sync_file_tree(
        user1, ?SPACE, #symlink_spec{symlink_value = LinkTarget, dataset = #dataset_spec{}}
    ),
    simple_archive_crud_test_base(DatasetId, ?SYMLINK_TYPE).

%===================================================================
% Parallel tests - tests which can be safely run in parallel
% as they do not interfere with any other test.
%===================================================================

archivisation_of_detached_dataset_should_be_impossible(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    #object{dataset = #dataset_object{id = DatasetId}} =
        onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{dataset = #dataset_spec{state = ?DETACHED_DATASET}}),

    ?assertMatch(
        ?ERROR_BAD_DATA(<<"datasetId">>, <<"Detached dataset cannot be modified.">>),
        opt_archives:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_CONFIG, ?TEST_DESCRIPTION1)
    ).

archive_of_detached_dataset_should_be_accessible(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    #object{dataset = #dataset_object{id = DatasetId}} =
        onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{dataset = #dataset_spec{}}),

    {ok, ArchiveId} = ?assertMatch({ok, _},
        opt_archives:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_CONFIG, ?TEST_DESCRIPTION1)),
    {ok, #archive_info{index = Index}} = ?assertMatch({ok, #archive_info{state = ?ARCHIVE_PRESERVED}},
        opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId), ?ATTEMPTS),
    ?assertEqual({ok, {[{Index, ArchiveId}], true}},
        opt_archives:list(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})),

    ok = opt_datasets:detach_dataset(P1Node, UserSessIdP1, DatasetId),

    ?assertMatch({ok, #archive_info{}},
        opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId)),
    ?assertEqual({ok, {[{Index, ArchiveId}], true}},
        opt_archives:list(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})).

archive_of_dataset_associated_with_deleted_file_should_be_accessible(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    #object{guid = Guid, dataset = #dataset_object{id = DatasetId}} =
        onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{dataset = #dataset_spec{}}),

    {ok, ArchiveId} = ?assertMatch({ok, _},
        opt_archives:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_CONFIG, ?TEST_DESCRIPTION1)),
    {ok, #archive_info{index = Index}} = ?assertMatch({ok, #archive_info{state = ?ARCHIVE_PRESERVED}},
        opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId), ?ATTEMPTS),
    ?assertEqual({ok, {[{Index, ArchiveId}], true}},
        opt_archives:list(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})),

    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, ?FILE_REF(Guid)),

    ?assertMatch({ok, #archive_info{}},
        opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId)),
    ?assertEqual({ok, {[{Index, ArchiveId}], true}},
        opt_archives:list(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})).

archive_reattached_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    #object{dataset = #dataset_object{id = DatasetId}} =
        onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{dataset = #dataset_spec{}}),

    {ok, ArchiveId} = ?assertMatch({ok, _},
        opt_archives:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_CONFIG, ?TEST_DESCRIPTION1)),
    {ok, #archive_info{index = Index}} = ?assertMatch({ok, #archive_info{state = ?ARCHIVE_PRESERVED}},
        opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId), ?ATTEMPTS),
    ?assertEqual({ok, {[{Index, ArchiveId}], true}},
        opt_archives:list(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})),

    ok = opt_datasets:detach_dataset(P1Node, UserSessIdP1, DatasetId),
    ok = opt_datasets:reattach_dataset(P1Node, UserSessIdP1, DatasetId),

    {ok, ArchiveId2} = ?assertMatch({ok, _},
        opt_archives:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_CONFIG, ?TEST_DESCRIPTION1)),

    ?assertMatch({ok, #archive_info{state = ?ARCHIVE_PRESERVED}},
        opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId2), ?ATTEMPTS),
    ?assertMatch({ok, {[_, _], true}},
        opt_archives:list(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})).

removal_of_not_empty_dataset_should_fail(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    #object{dataset = #dataset_object{id = DatasetId}} =
        onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{dataset = #dataset_spec{}}),

    {ok, ArchiveId} = ?assertMatch({ok, _},
        opt_archives:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_CONFIG, ?TEST_DESCRIPTION1)),

    {ok, #archive_info{index = Index}} = ?assertMatch({ok, #archive_info{state = ?ARCHIVE_PRESERVED}},
        opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId), ?ATTEMPTS),
    ?assertEqual({ok, {[{Index, ArchiveId}], true}},
        opt_archives:list(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})),

    ?assertEqual(?ERROR_POSIX(?ENOTEMPTY), opt_datasets:remove(P1Node, UserSessIdP1, DatasetId)),

    ?assertEqual(ok, opt_archives:purge(P1Node, UserSessIdP1, ArchiveId)),
    % wait till archive is purged
    ?assertMatch({ok, {[], true}},
        opt_archives:list(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10}), ?ATTEMPTS),
    ?assertEqual(ok, opt_datasets:remove(P1Node, UserSessIdP1, DatasetId)).

iterate_over_100_archives_using_offset_and_limit_1(_Config) ->
    iterate_over_archives_test_base(100, offset, 1).

iterate_over_100_archives_using_offset_and_limit_10(_Config) ->
    iterate_over_archives_test_base(100, offset, 10).

iterate_over_100_archives_using_offset_and_limit_100(_Config) ->
    iterate_over_archives_test_base(100, offset, 100).

iterate_over_100_archives_using_offset_and_limit_1000(_Config) ->
    iterate_over_archives_test_base(100, offset, 1000).

iterate_over_100_archives_using_offset_and_limit_10000(_Config) ->
    iterate_over_archives_test_base(100, offset, 10000).

iterate_over_100_archives_using_start_index_and_limit_1(_Config) ->
    iterate_over_archives_test_base(100, start_index, 1).

iterate_over_100_archives_using_start_index_and_limit_10(_Config) ->
    iterate_over_archives_test_base(100, start_index, 10).

iterate_over_100_archives_using_start_index_and_limit_100(_Config) ->
    iterate_over_archives_test_base(100, start_index, 100).

iterate_over_100_archives_using_start_index_and_limit_1000(_Config) ->
    iterate_over_archives_test_base(100, start_index, 1000).

iterate_over_100_archives_using_start_index_and_limit_10000(_Config) ->
    iterate_over_archives_test_base(100, start_index, 10000).

%===================================================================
% Sequential tests - tests which must be performed one after another
% to ensure that they do not interfere with each other (e. g. by
% modifying mocked global_clock or changing user's privileges)
%===================================================================

archive_dataset_attached_to_space_dir(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    #dataset_object{id = DatasetId} = onenv_dataset_test_utils:set_up_and_sync_dataset(user1, SpaceGuid),
    simple_archive_crud_test_base(DatasetId, ?DIRECTORY_TYPE).


archive_dataset_many_times(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user1, paris),

    Count = 1000,
    #object{dataset = #dataset_object{id = DatasetId}} =
        onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{dataset = #dataset_spec{}}),

    ExpArchiveIdsReversed = lists:map(fun(_) ->
        {ok, ArchiveId} = ?assertMatch({ok, _},
            opt_archives:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_CONFIG, ?TEST_DESCRIPTION1)),
        % mock time lapse to ensure that archives will have different creation timestamps
        time_test_utils:simulate_seconds_passing(2),
        ArchiveId
    end, lists:seq(1, Count)),

    ExpArchiveIdsAndIndicesReversed = lists:map(fun(ArchiveId) ->
        {ok, #archive_info{index = Index}} = ?assertMatch({ok, #archive_info{state = ?ARCHIVE_PRESERVED}},
            opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId), ?ATTEMPTS),
        {Index, ArchiveId}
    end, ExpArchiveIdsReversed),

    ?assertMatch({ok, #dataset_info{archive_count = Count}},
        opt_datasets:get_info(P1Node, UserSessIdP1, DatasetId)),
    ?assertMatch({ok, #dataset_info{archive_count = Count}},
        opt_datasets:get_info(P2Node, UserSessIdP2, DatasetId), ?ATTEMPTS),

    ExpArchiveIdsAndIndices = lists:reverse(ExpArchiveIdsAndIndicesReversed),

    ?assertEqual({ok, {ExpArchiveIdsAndIndices, false}},
        opt_archives:list(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => Count})),
    ?assertEqual({ok, {ExpArchiveIdsAndIndices, false}},
        opt_archives:list(P2Node, UserSessIdP2, DatasetId, #{offset => 0, limit => Count}), ?ATTEMPTS).

time_warp_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),

    #object{
        dataset = #dataset_object{
            id = DatasetId,
            archives = [#archive_object{id = ArchiveId}]
        }
    } = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{dataset = #dataset_spec{archives = 1}}),

    time_test_utils:simulate_seconds_passing(-1),

    {ok, ArchiveId2} = ?assertMatch({ok, _},
        opt_archives:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_CONFIG, ?TEST_DESCRIPTION1)),

    ?assertMatch({ok, {[{_, ArchiveId}, {_, ArchiveId2}], true}},
        opt_archives:list(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10})).

create_archive_privileges_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    User2SessIdP1 = oct_background:get_user_session_id(user2, krakow),
    UserId2 = oct_background:get_user_id(user2),
    SpaceId = oct_background:get_space_id(?SPACE),

    #object{
        dataset = #dataset_object{
            id = DatasetId,
            archives = [#archive_object{id = ArchiveId, config = ArchiveConfig}]
        }
    } = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{dataset = #dataset_spec{archives = 1}}),

    RequiredPrivileges = privileges:from_list([?SPACE_MANAGE_DATASETS, ?SPACE_CREATE_ARCHIVES]),
    AllPrivileges = privileges:from_list(RequiredPrivileges ++ privileges:space_member()),

    ?assertMatch({ok, #archive_info{state = ?ARCHIVE_PRESERVED, config = ArchiveConfig}},
        opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId), ?ATTEMPTS),

    lists:foreach(fun(Privilege) ->

        ensure_privilege_revoked(P1Node, SpaceId, UserId2, Privilege, AllPrivileges),
        % user2 cannot create archive
        ?assertEqual(?ERROR_POSIX(?EPERM),
            opt_archives:archive_dataset(P1Node, User2SessIdP1, DatasetId, ?TEST_ARCHIVE_CONFIG, ?TEST_DESCRIPTION1)),
        % user2 cannot modify an existing archive either
        ?assertEqual(?ERROR_POSIX(?EPERM),
            opt_archives:update(P1Node, User2SessIdP1, ArchiveId, #{<<"description">> => ?TEST_DESCRIPTION2})),

        ensure_privilege_assigned(P1Node, SpaceId, UserId2, Privilege, AllPrivileges),
        % user2 can now create archive

        {ok, ArchiveId2} = ?assertMatch({ok, _},
            opt_archives:archive_dataset(P1Node, User2SessIdP1, DatasetId, ?TEST_ARCHIVE_CONFIG, ?TEST_DESCRIPTION1)),

        ?assertMatch({ok, #archive_info{state = ?ARCHIVE_PRESERVED}},
            opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId2), ?ATTEMPTS),

        % as well as modify an existing one
        ?assertMatch(ok,
            opt_archives:update(P1Node, User2SessIdP1, ArchiveId, #{<<"description">> => ?TEST_DESCRIPTION2}))
    end, RequiredPrivileges).


view_archive_privileges_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    User2SessIdP1 = oct_background:get_user_session_id(user2, krakow),
    UserId2 = oct_background:get_user_id(user2),
    SpaceId = oct_background:get_space_id(?SPACE),

    #object{
        dataset = #dataset_object{
            id = DatasetId,
            archives = [#archive_object{id = ArchiveId, config = ArchiveConfig}]
        }
    } = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{dataset = #dataset_spec{archives = 1}}),

    ?assertMatch({ok, #archive_info{state = ?ARCHIVE_PRESERVED, config = ArchiveConfig}},
        opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId), ?ATTEMPTS),

    AllPrivileges = privileges:from_list([?SPACE_VIEW_ARCHIVES | privileges:space_member()]),

    % assign user only space_member privileges
    ensure_privilege_revoked(P1Node, SpaceId, UserId2, ?SPACE_VIEW_ARCHIVES, AllPrivileges),

    % user2 cannot fetch archive info
    ?assertEqual(?ERROR_POSIX(?EPERM),
        opt_archives:get_info(P1Node, User2SessIdP1, ArchiveId), ?ATTEMPTS),
    % neither can he list the archives
    ?assertEqual(?ERROR_POSIX(?EPERM),
        opt_archives:list(P1Node, User2SessIdP1, DatasetId, #{offset => 0, limit => 10})),

    % assign user2 privilege to view archives
    ensure_privilege_assigned(P1Node, SpaceId, UserId2, ?SPACE_VIEW_ARCHIVES, AllPrivileges),

    % now user2 should be able to fetch archive info
    ?assertMatch({ok, _},
        opt_archives:get_info(P1Node, User2SessIdP1, ArchiveId)),
    % as well as list the archives
    ?assertMatch({ok, {[{_, ArchiveId}], _}},
        opt_archives:list(P1Node, User2SessIdP1, DatasetId, #{offset => 0, limit => 10})).

remove_archive_privileges_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    User2SessIdP1 = oct_background:get_user_session_id(user2, krakow),
    UserId2 = oct_background:get_user_id(user2),
    SpaceId = oct_background:get_space_id(?SPACE),

    #object{
        dataset = #dataset_object{
            archives = [
                #archive_object{id = ArchiveId1, config = ArchiveConfig1},
                #archive_object{id = ArchiveId2, config = ArchiveConfig2}
            ]
        }
    } = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE, #file_spec{dataset = #dataset_spec{archives = 2}}),

    ?assertMatch({ok, #archive_info{state = ?ARCHIVE_PRESERVED, config = ArchiveConfig1}},
        opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId1), ?ATTEMPTS),
    ?assertMatch({ok, #archive_info{state = ?ARCHIVE_PRESERVED, config = ArchiveConfig2}},
        opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId2), ?ATTEMPTS),

    RequiredPrivileges = privileges:from_list([?SPACE_MANAGE_DATASETS, ?SPACE_REMOVE_ARCHIVES]),
    AllPrivileges = privileges:from_list(RequiredPrivileges ++ privileges:space_member()),

    lists:foreach(fun({Privilege, ArchiveId}) ->

        ensure_privilege_revoked(P1Node, SpaceId, UserId2, Privilege, AllPrivileges),
        % user2 cannot remove the archive
        ?assertEqual(?ERROR_POSIX(?EPERM), opt_archives:purge(P1Node, User2SessIdP1, ArchiveId)),

        ensure_privilege_assigned(P1Node, SpaceId, UserId2, Privilege, AllPrivileges),
        % user2 can now remove archive
        ?assertEqual(ok, opt_archives:purge(P1Node, User2SessIdP1, ArchiveId))

    end, lists:zip(RequiredPrivileges, [ArchiveId1, ArchiveId2])).


%===================================================================
% Test bases
%===================================================================

simple_archive_crud_test_base(DatasetId, RootFileType) ->
    simple_archive_crud_test_base(DatasetId, RootFileType, 0).

simple_archive_crud_test_base(DatasetId, RootFileType, ExpSize) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user1, paris),
    SpaceId = oct_background:get_space_id(?SPACE),

    % create archive
    {ok, ArchiveId} = opt_archives:archive_dataset(P1Node, UserSessIdP1, DatasetId, ?TEST_ARCHIVE_CONFIG,
        ?TEST_ARCHIVE_PRESERVED_CALLBACK1, ?TEST_ARCHIVE_PURGED_CALLBACK1, ?TEST_DESCRIPTION1),

    ArchiveRootDirUuid = ?ARCHIVE_DIR_UUID(ArchiveId),
    ArchiveRootDirGuid = file_id:pack_guid(ArchiveRootDirUuid, SpaceId),

    ExpectedFilesArchived = case RootFileType of
        ?DIRECTORY_TYPE -> 0;
        _ -> 1
    end,

    Timestamp = global_clock_timestamp(P1Node),
    Index = archives_list:index(ArchiveId, Timestamp),

    ExpArchiveInfo = #archive_info{
        id = ArchiveId,
        dataset_id = DatasetId,
        state = ?ARCHIVE_PRESERVED,
        root_dir_guid = ArchiveRootDirGuid,
        creation_time = Timestamp,
        index = Index,
        config = #archive_config{
            incremental = #{<<"enabled">> => false},
            include_dip = false,
            layout = ?ARCHIVE_PLAIN_LAYOUT,
            follow_symlinks = false
        },
        preserved_callback = ?TEST_ARCHIVE_PRESERVED_CALLBACK1,
        purged_callback = ?TEST_ARCHIVE_PURGED_CALLBACK1,
        description = ?TEST_DESCRIPTION1,
        stats = archive_stats:new(ExpectedFilesArchived, 0, ExpSize)
    },

    % verify whether Archive is visible in the local provider
    ?assertMatch({ok, ExpArchiveInfo},
        opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId), ?ATTEMPTS),
    ?assertEqual({ok, {[{Index, ArchiveId}], true}},
        opt_archives:list(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10}, ?BASIC_INFO), ?ATTEMPTS),
    ?assertMatch({ok, {[ExpArchiveInfo], true}},
        opt_archives:list(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10}, ?EXTENDED_INFO), ?ATTEMPTS),

    % verify whether Archive is visible in the remote provider
    ?assertMatch({ok, ExpArchiveInfo},
        opt_archives:get_info(P2Node, UserSessIdP2, ArchiveId), ?ATTEMPTS),
    ?assertEqual({ok, {[{Index, ArchiveId}], true}},
        opt_archives:list(P2Node, UserSessIdP2, DatasetId, #{offset => 0, limit => 10}, ?BASIC_INFO), ?ATTEMPTS),
    ?assertMatch({ok, {[ExpArchiveInfo], true}},
        opt_archives:list(P2Node, UserSessIdP2, DatasetId, #{offset => 0, limit => 10}, ?EXTENDED_INFO), ?ATTEMPTS),

    % update archive
    ExpArchiveInfo2 = ExpArchiveInfo#archive_info{
        preserved_callback = ?TEST_ARCHIVE_PRESERVED_CALLBACK2,
        purged_callback = ?TEST_ARCHIVE_PURGED_CALLBACK2,
        description = ?TEST_DESCRIPTION2
    },
    ?assertEqual(ok,
        opt_archives:update(P2Node, UserSessIdP2, ArchiveId, #{
            <<"description">> => ?TEST_DESCRIPTION2,
            <<"preservedCallback">> => ?TEST_ARCHIVE_PRESERVED_CALLBACK2,
            <<"purgedCallback">> => ?TEST_ARCHIVE_PURGED_CALLBACK2
        })),
    ?assertMatch({ok, ExpArchiveInfo2},
        opt_archives:get_info(P2Node, UserSessIdP2, ArchiveId)),
    ?assertMatch({ok, ExpArchiveInfo2},
        opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId), ?ATTEMPTS),

    % remove archive
    ok = opt_archives:purge(P1Node, UserSessIdP1, ArchiveId, ?TEST_ARCHIVE_PURGED_CALLBACK3),

    % verify whether Archive has been removed in the local provider
    ?assertEqual(?ERROR_NOT_FOUND,
        opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId), ?ATTEMPTS),
    ?assertEqual({ok, {[], true}},
        opt_archives:list(P1Node, UserSessIdP1, DatasetId, #{offset => 0, limit => 10}), ?ATTEMPTS),

    % verify whether Archive has been removed in the remote provider
    ?assertEqual(?ERROR_NOT_FOUND,
        opt_archives:get_info(P2Node, UserSessIdP2, ArchiveId), ?ATTEMPTS),
    ?assertEqual({ok, {[], true}},
        opt_archives:list(P2Node, UserSessIdP2, DatasetId, #{offset => 0, limit => 10}), ?ATTEMPTS).


iterate_over_archives_test_base(ArchiveCount, ListingMethod, Limit) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),

    #object{dataset = #dataset_object{
        id = DatasetId,
        archives = ArchiveObjects
    }} = onenv_file_test_utils:create_and_sync_file_tree(user1, ?SPACE,
        #file_spec{dataset = #dataset_spec{archives = ArchiveCount}}),

    lists_utils:pforeach(fun(#archive_object{id = ArchiveId, config = Config}) ->
        ?assertMatch({ok, #archive_info{
            state = ?ARCHIVE_PRESERVED,
            config = Config
        }}, opt_archives:get_info(P1Node, UserSessIdP1, ArchiveId), ?ATTEMPTS)
    end, ArchiveObjects),

    % sort archives by their indices
    ExpArchiveIdsAndIndices = lists:sort(fun(A1, A2) ->
        A1#archive_object.index =< A2#archive_object.index
    end, ArchiveObjects),
    ExpArchiveIds = [Id || {Id, _} <- ExpArchiveIdsAndIndices],

    ListingOpts = case ListingMethod of
        offset -> #{offset => 0, limit => Limit};
        start_index -> #{start_index => <<>>, limit => Limit}
    end,

    check_if_all_archives_listed(ExpArchiveIds, P1Node, UserSessIdP1, DatasetId, ListingOpts).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [dir_stats_test_utils]} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {provider_token_ttl_sec, 24 * 60 * 60}
        ]}],
        posthook = fun dir_stats_test_utils:disable_stats_counting_ct_posthook/1
    }).

end_per_suite(Config) ->
    oct_background:end_per_suite(),
    dir_stats_test_utils:enable_stats_counting(Config).

init_per_group(parallel_tests, Config) ->
    Config2 = oct_background:update_background_config(Config),
    lfm_proxy:init(Config2, false);
init_per_group(_Group, Config) ->
    ok = time_test_utils:freeze_time(Config),
    Config2 = oct_background:update_background_config(Config),
    lfm_proxy:init(Config2, false).

end_per_group(_Group, Config) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    Workers = oct_background:get_all_providers_nodes(),
    CleaningWorker = oct_background:get_random_provider_node(krakow),
    onenv_dataset_test_utils:cleanup_all_datasets(krakow, ?SPACE),
    lfm_test_utils:clean_space(CleaningWorker, Workers, SpaceId, ?ATTEMPTS),
    lfm_proxy:teardown(Config),
    time_test_utils:unfreeze_time(Config).

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
    {ok, {ListedArchives, IsLast}} = opt_archives:list(Node, SessId, DatasetId, Opts),
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
