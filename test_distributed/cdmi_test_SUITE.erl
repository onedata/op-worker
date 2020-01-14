%%%-------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------
%%% @doc
%%% CDMI tests
%%% @end
%%%-------------------------------------
-module(cdmi_test_SUITE).
-author("Tomasz Lichon").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    list_dir_test/1,
    get_file_test/1,
    metadata_test/1,
    delete_file_test/1,
    delete_dir_test/1,
    create_file_test/1,
    update_file_test/1,
    create_dir_test/1,
    capabilities_test/1,
    use_supported_cdmi_version_test/1,
    use_unsupported_cdmi_version_test/1,
    moved_permanently_test/1,
    objectid_test/1,
    request_format_check_test/1,
    mimetype_and_encoding_test/1,
    out_of_range_test/1,
    partial_upload_test/1,
    acl_test/1,
    errors_test/1,
    accept_header_test/1,
    move_copy_conflict_test/1,
    move_test/1,
    copy_test/1,
    create_raw_file_with_cdmi_version_header_should_succeed_test/1,
    create_raw_dir_with_cdmi_version_header_should_succeed_test/1,
    create_cdmi_file_without_cdmi_version_header_should_fail_test/1,
    create_cdmi_dir_without_cdmi_version_header_should_fail_test/1
]).

all() ->
    ?ALL([
        list_dir_test,
        get_file_test,
        metadata_test,
        delete_file_test,
        delete_dir_test,
        create_file_test,
        update_file_test,
        create_dir_test,
        capabilities_test,
        use_supported_cdmi_version_test,
        use_unsupported_cdmi_version_test,
        moved_permanently_test,
        objectid_test,
        request_format_check_test,
        mimetype_and_encoding_test,
        out_of_range_test,
        partial_upload_test,
        acl_test,
        errors_test,
        accept_header_test,
        move_copy_conflict_test,
        move_test,
        copy_test,
        create_raw_file_with_cdmi_version_header_should_succeed_test,
        create_raw_dir_with_cdmi_version_header_should_succeed_test,
        create_cdmi_file_without_cdmi_version_header_should_fail_test,
        create_cdmi_dir_without_cdmi_version_header_should_fail_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

list_dir_test(Config) ->
    cdmi_test_base:list_dir(Config).

get_file_test(Config) ->
    cdmi_test_base:get_file(Config).

metadata_test(Config) ->
    cdmi_test_base:metadata(Config).

delete_file_test(Config) ->
    cdmi_test_base:delete_file(Config).

delete_dir_test(Config) ->
    cdmi_test_base:delete_dir(Config).

create_file_test(Config) ->
    cdmi_test_base:create_file(Config).

update_file_test(Config) ->
    cdmi_test_base:update_file(Config).

create_dir_test(Config) ->
    cdmi_test_base:create_dir(Config).

objectid_test(Config) ->
    cdmi_test_base:objectid(Config).

capabilities_test(Config) ->
    cdmi_test_base:capabilities(Config).

use_supported_cdmi_version_test(Config) ->
    cdmi_test_base:use_supported_cdmi_version(Config).

use_unsupported_cdmi_version_test(Config) ->
    cdmi_test_base:use_unsupported_cdmi_version(Config).

moved_permanently_test(Config) ->
    cdmi_test_base:moved_permanently(Config).

request_format_check_test(Config) ->
    cdmi_test_base:request_format_check(Config).

mimetype_and_encoding_test(Config) ->
    cdmi_test_base:mimetype_and_encoding(Config).

out_of_range_test(Config) ->
    cdmi_test_base:out_of_range(Config).

move_copy_conflict_test(Config) ->
    cdmi_test_base:move_copy_conflict(Config).

move_test(Config) ->
    cdmi_test_base:move(Config).

copy_test(Config) ->
    cdmi_test_base:copy(Config).

partial_upload_test(Config) ->
    cdmi_test_base:partial_upload(Config).

acl_test(Config) ->
    cdmi_test_base:acl(Config).

errors_test(Config) ->
    cdmi_test_base:errors(Config).

accept_header_test(Config) ->
    cdmi_test_base:accept_header(Config).

create_raw_file_with_cdmi_version_header_should_succeed_test(Config) ->
    cdmi_test_base:create_raw_file_with_cdmi_version_header_should_succeed(Config).

create_raw_dir_with_cdmi_version_header_should_succeed_test(Config) ->
    cdmi_test_base:create_raw_dir_with_cdmi_version_header_should_succeed(Config).

create_cdmi_file_without_cdmi_version_header_should_fail_test(Config) ->
    cdmi_test_base:create_cdmi_file_without_cdmi_version_header_should_fail(Config).

create_cdmi_dir_without_cdmi_version_header_should_fail_test(Config) ->
    cdmi_test_base:create_cdmi_dir_without_cdmi_version_header_should_fail(Config).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer]} | Config].

end_per_suite(_) ->
    ok.

init_per_testcase(choose_adequate_handler_test = Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [cdmi_object_handler, cdmi_container_handler], [passthrough]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(_Case, Config) ->
    ssl:start(),
    hackney:start(),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    initializer:mock_auth_manager(ConfigWithSessionInfo),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(choose_adequate_handler_test = Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, [cdmi_object_handler, cdmi_container_handler]),
    rpc:multicall(Workers, code, ensure_loaded, [cdmi_object_handler]),
    rpc:multicall(Workers, code, ensure_loaded, [cdmi_container_handler]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
     %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    ssl:stop().

%%%===================================================================
%%% Internal functions
%%%===================================================================
