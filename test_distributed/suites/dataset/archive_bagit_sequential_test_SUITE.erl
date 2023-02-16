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
-module(archive_bagit_sequential_test_SUITE).
-author("Jakub Kudzia").


-include("onenv_test_utils.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
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
    archive_big_tree_bagit_layout/1,
    archive_directory_with_number_of_files_exceeding_batch_size_bagit_layout/1,
    
    verification_bagit_modify_file/1,
    verification_bagit_modify_file_metadata/1,
    verification_bagit_modify_dir_metadata/1,
    verification_bagit_create_file/1,
    verification_bagit_remove_file/1,
    verification_bagit_recreate_file/1,
    dip_verification_bagit/1,
    nested_verification_bagit/1
]).


groups() -> [
    {archivisation_tests, [
        archive_big_tree_bagit_layout,
        archive_directory_with_number_of_files_exceeding_batch_size_bagit_layout
    ]},
    {verification_tests, [
        verification_bagit_modify_file,
        verification_bagit_modify_file_metadata,
        verification_bagit_modify_dir_metadata,
        verification_bagit_create_file,
        verification_bagit_remove_file,
        verification_bagit_recreate_file,
        dip_verification_bagit,
        nested_verification_bagit
    ]}
].


all() -> [
    {group, archivisation_tests},
    {group, verification_tests}
].


%===================================================================
% Sequential tests - tests which must be performed one after another
% to ensure that they do not interfere with each other
%===================================================================

archive_big_tree_bagit_layout(_Config) ->
    archive_sequential_test_base:archive_big_tree_test(?ARCHIVE_BAGIT_LAYOUT).

archive_directory_with_number_of_files_exceeding_batch_size_bagit_layout(_Config) ->
    archive_sequential_test_base:archive_directory_with_number_of_files_exceeding_batch_size_test(?ARCHIVE_BAGIT_LAYOUT).


%===================================================================
% Verification tests - can not be run in parallel as they use mocks.
%===================================================================

verification_bagit_modify_file(_Config) ->
    archive_sequential_test_base:verification_modify_file_base(?ARCHIVE_BAGIT_LAYOUT).

verification_bagit_modify_file_metadata(_Config) ->
    archive_sequential_test_base:verification_modify_file_metadata_base(?ARCHIVE_BAGIT_LAYOUT).

verification_bagit_modify_dir_metadata(_Config) ->
    archive_sequential_test_base:verification_modify_dir_metadata_base(?ARCHIVE_BAGIT_LAYOUT).

verification_bagit_create_file(_Config) ->
    archive_sequential_test_base:verification_create_file_base(?ARCHIVE_BAGIT_LAYOUT).

verification_bagit_remove_file(_Config) ->
    archive_sequential_test_base:verification_remove_file_base(?ARCHIVE_BAGIT_LAYOUT).

verification_bagit_recreate_file(_Config) ->
    archive_sequential_test_base:verification_recreate_file_base(?ARCHIVE_BAGIT_LAYOUT).

dip_verification_bagit(_Config) ->
    archive_sequential_test_base:dip_verification_test_base(?ARCHIVE_BAGIT_LAYOUT).

nested_verification_bagit(_Config) ->
    archive_sequential_test_base:nested_verification_test_base(?ARCHIVE_BAGIT_LAYOUT).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(
        [{?LOAD_MODULES, [?MODULE, archive_tests_utils, dir_stats_test_utils, archive_sequential_test_base]} | Config],
        #onenv_test_config{
            onenv_scenario = "2op-archive",
            envs = [{op_worker, op_worker, [
                {fuse_session_grace_period_seconds, 24 * 60 * 60},
                {provider_token_ttl_sec, 24 * 60 * 60}
            ]}]
        }
    ).

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
    test_utils:mock_unload(oct_background:get_all_providers_nodes(), archivisation_traverse),
    test_utils:mock_unload(oct_background:get_all_providers_nodes(), archive_verification_traverse),
    ok.
