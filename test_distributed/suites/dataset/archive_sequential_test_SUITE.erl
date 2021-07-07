%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Sequential tests of archives mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_sequential_test_SUITE).
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
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    archive_dataset_attached_to_space_dir/1,
    archive_big_tree_plain_layout/1,
    archive_big_tree_bagit_layout/1,
    archive_directory_with_number_of_files_exceeding_batch_size_plain_layout/1,
    archive_directory_with_number_of_files_exceeding_batch_size_bagit_layout/1
]).


all() -> [
    archive_dataset_attached_to_space_dir,
    archive_big_tree_plain_layout,
    archive_big_tree_bagit_layout,
    archive_directory_with_number_of_files_exceeding_batch_size_plain_layout,
    archive_directory_with_number_of_files_exceeding_batch_size_bagit_layout
].

-define(ATTEMPTS, 60).

-define(SPACE, space_krk_par_p).
-define(USER1, user1).

%===================================================================
% Sequential tests - tests which must be performed one after another
% to ensure that they do not interfere with each other
%===================================================================

archive_dataset_attached_to_space_dir(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    #dataset_object{
        id = DatasetId,
        archives = [#archive_object{id = ArchiveId}]
    } = onenv_dataset_test_utils:set_up_and_sync_dataset(?USER1, SpaceGuid, #dataset_spec{archives = 1}),
    archive_simple_dataset_test_base(SpaceGuid, DatasetId, ArchiveId).

archive_big_tree_plain_layout(_Config) ->
    archive_big_tree_test_base(?ARCHIVE_PLAIN_LAYOUT).

archive_big_tree_bagit_layout(_Config) ->
    archive_big_tree_test_base(?ARCHIVE_BAGIT_LAYOUT).

archive_directory_with_number_of_files_exceeding_batch_size_plain_layout(_Config) ->
    archive_directory_with_number_of_files_exceeding_batch_size_test_base(?ARCHIVE_PLAIN_LAYOUT).

archive_directory_with_number_of_files_exceeding_batch_size_bagit_layout(_Config) ->
    archive_directory_with_number_of_files_exceeding_batch_size_test_base(?ARCHIVE_BAGIT_LAYOUT).

%===================================================================
% Test bases
%===================================================================


archive_big_tree_test_base(Layout) ->
    archive_dataset_tree_test_base([{10, 10}, {10, 10}, {10, 10}], Layout).


archive_directory_with_number_of_files_exceeding_batch_size_test_base(Layout) ->
    % default batch size is 1000
    archive_dataset_tree_test_base([{0, 2048}], Layout).

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

archive_dataset_tree_test_base(FileStructure, ArchiveLayout) ->
    Provider = lists_utils:random_element(oct_background:get_space_supporting_providers(?SPACE)),
    Node = oct_background:get_random_provider_node(Provider),
    SessId = oct_background:get_user_session_id(?USER1, Provider),
    #object{
        guid = RootGuid,
        dataset = #dataset_object{id = DatasetId}
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #dir_spec{dataset = #dataset_spec{}}),

    {_, FileGuids} = lfm_test_utils:create_files_tree(Node, SessId, FileStructure, RootGuid),

    {ok, ArchiveId} =
        lfm_proxy:archive_dataset(Node, SessId, DatasetId, #archive_config{layout = ArchiveLayout}, <<>>),

    % created files are empty therefore expected size is 0
    archive_tests_utils:assert_archive_is_preserved(Node, SessId, ArchiveId, DatasetId, RootGuid, length(FileGuids), 0).


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

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config, false).

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
