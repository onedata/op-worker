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
    archive_big_tree_bagit_layout/1,
    archive_directory_with_number_of_files_exceeding_batch_size_bagit_layout/1
]).


all() -> [
    archive_big_tree_bagit_layout,
    archive_directory_with_number_of_files_exceeding_batch_size_bagit_layout
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
