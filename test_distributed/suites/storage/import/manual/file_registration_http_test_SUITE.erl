%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests the file registration mechanism (POST data/register)
%%% with the registering provider backed by an imported, read-only HTTP storage
%%% (the other provider uses a regular POSIX storage). The files to register are
%%% served by a test HTTP server running on the registering provider node (see
%%% {@link http_storage_test_server}). The actual test logic lives in
%%% {@link file_registration_test_base}.
%%% @end
%%%--------------------------------------------------------------------
-module(file_registration_http_test_SUITE).
-author("Bartosz Walkowicz").

-include("file_registration_test.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    register_file_test/1,
    register_file_with_conflict_test/1,
    register_file_and_create_parents_test/1,
    update_registered_file_test/1,
    update_registered_file_with_not_matching_destination_test/1,
    stat_on_storage_should_not_be_performed_if_automatic_detection_of_attributes_is_disabled/1,
    registration_should_fail_if_size_is_not_passed_and_automatic_detection_of_attributes_is_disabled/1,
    registration_should_fail_if_file_is_missing/1,
    registration_should_succeed_if_size_is_passed/1,
    interrupted_registration_test/1,
    interrupted_registration_nested_file_test/1,
    register_many_files_test/1,
    register_many_nested_files_test/1,
    register_file_with_size_smaller_than_real_test/1,
    register_file_with_size_larger_than_real_test/1,
    registration_should_succeed_if_file_is_missing_and_existence_verification_is_disabled/1,
    registration_should_fail_if_file_is_missing_and_existence_verification_is_enabled/1,
    registration_should_verify_existence_without_detecting_attributes/1,
    read_registered_file_after_source_removed_from_storage_test/1,
    read_registered_file_after_source_modified_on_storage_test/1,
    read_registered_file_when_storage_returns_error_test/1,
    large_registered_file_should_be_correctly_replicated_to_other_provider_test/1,
    register_shared_file_via_public_url_test/1
]).

all() -> [
    register_file_test,
    register_file_with_conflict_test,
    register_file_and_create_parents_test,
    update_registered_file_test,
    update_registered_file_with_not_matching_destination_test,
    stat_on_storage_should_not_be_performed_if_automatic_detection_of_attributes_is_disabled,
    registration_should_fail_if_size_is_not_passed_and_automatic_detection_of_attributes_is_disabled,
    registration_should_fail_if_file_is_missing,
    registration_should_succeed_if_size_is_passed,
    interrupted_registration_test,
    interrupted_registration_nested_file_test,
    register_many_files_test,
    register_many_nested_files_test,
    register_file_with_size_smaller_than_real_test,
    register_file_with_size_larger_than_real_test,
    registration_should_succeed_if_file_is_missing_and_existence_verification_is_disabled,
    registration_should_fail_if_file_is_missing_and_existence_verification_is_enabled,
    registration_should_verify_existence_without_detecting_attributes,
    read_registered_file_after_source_removed_from_storage_test,
    read_registered_file_after_source_modified_on_storage_test,
    read_registered_file_when_storage_returns_error_test,
    large_registered_file_should_be_correctly_replicated_to_other_provider_test,
    register_shared_file_via_public_url_test
].

-define(SUITE_CTX, #file_registration_test_suite_ctx{
    registering_storage_type = http,
    registering_provider_selector = krakow,
    other_provider_selector = paris,
    test_user_selector = user1
}).
-define(run_test(), file_registration_test_base:?FUNCTION_NAME(?SUITE_CTX)).


%%%==================================================================
%%% Test functions
%%%===================================================================


register_file_test(_Config) -> ?run_test().
register_file_with_conflict_test(_Config) -> ?run_test().
register_file_and_create_parents_test(_Config) -> ?run_test().
update_registered_file_test(_Config) -> ?run_test().
update_registered_file_with_not_matching_destination_test(_Config) -> ?run_test().
stat_on_storage_should_not_be_performed_if_automatic_detection_of_attributes_is_disabled(_Config) -> ?run_test().
registration_should_fail_if_size_is_not_passed_and_automatic_detection_of_attributes_is_disabled(_Config) -> ?run_test().
registration_should_fail_if_file_is_missing(_Config) -> ?run_test().
registration_should_succeed_if_size_is_passed(_Config) -> ?run_test().
interrupted_registration_test(_Config) -> ?run_test().
interrupted_registration_nested_file_test(_Config) -> ?run_test().
register_many_files_test(_Config) -> ?run_test().
register_many_nested_files_test(_Config) -> ?run_test().
register_file_with_size_smaller_than_real_test(_Config) -> ?run_test().
register_file_with_size_larger_than_real_test(_Config) -> ?run_test().
registration_should_succeed_if_file_is_missing_and_existence_verification_is_disabled(_Config) -> ?run_test().
registration_should_fail_if_file_is_missing_and_existence_verification_is_enabled(_Config) -> ?run_test().
registration_should_verify_existence_without_detecting_attributes(_Config) -> ?run_test().
read_registered_file_after_source_removed_from_storage_test(_Config) -> ?run_test().
read_registered_file_after_source_modified_on_storage_test(_Config) -> ?run_test().
read_registered_file_when_storage_returns_error_test(_Config) -> ?run_test().
large_registered_file_should_be_correctly_replicated_to_other_provider_test(_Config) -> ?run_test().
register_shared_file_via_public_url_test(_Config) -> ?run_test().


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, file_registration_test_base, http_storage_test_server, storage_import_test_utils],
    opt:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {dbsync_changes_broadcast_interval, timer:seconds(1)}
        ]}],
        posthook = fun(NewConfig) ->
            file_registration_test_base:clean_up_after_previous_run(all(), ?SUITE_CTX),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    file_registration_test_base:init_per_testcase(Config).


end_per_testcase(Case, Config) ->
    file_registration_test_base:end_per_testcase(Case, ?SUITE_CTX, Config).
