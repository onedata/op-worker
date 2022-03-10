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
    archive_directory_with_number_of_files_exceeding_batch_size_plain_layout/1
]).


all() -> [
    archive_dataset_attached_to_space_dir,
    archive_big_tree_plain_layout,
    archive_directory_with_number_of_files_exceeding_batch_size_plain_layout
].

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
    archive_sequential_test_base:archive_simple_dataset_test(SpaceGuid, DatasetId, ArchiveId).

archive_big_tree_plain_layout(_Config) ->
    archive_sequential_test_base:archive_big_tree_test(?ARCHIVE_PLAIN_LAYOUT).

archive_directory_with_number_of_files_exceeding_batch_size_plain_layout(_Config) ->
    archive_sequential_test_base:archive_directory_with_number_of_files_exceeding_batch_size_test(?ARCHIVE_PLAIN_LAYOUT).


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
