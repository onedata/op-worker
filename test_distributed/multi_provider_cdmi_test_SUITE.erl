%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% CDMI tests
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_cdmi_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include("http/rest/cdmi/cdmi_capabilities.hrl").
-include("http/rest/http_status.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

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
    choose_adequate_handler_test/1,
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
    copy_test/1
]).

all() ->
    ?ALL([
        delete_dir_test,
        list_dir_test,
        get_file_test,
        metadata_test,
        delete_file_test,
        create_file_test,
        update_file_test,
        create_dir_test,
        capabilities_test,
        choose_adequate_handler_test,
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
        move_test
%%        copy_test %todo implement copy
]).

-define(TIMEOUT, timer:seconds(5)).


-define(CDMI_VERSION_HEADER, {<<"X-CDMI-Specification-Version">>, <<"1.1.1">>}).
-define(CONTAINER_CONTENT_TYPE_HEADER, {<<"content-type">>, <<"application/cdmi-container">>}).
-define(OBJECT_CONTENT_TYPE_HEADER, {<<"content-type">>, <<"application/cdmi-object">>}).

-define(DEFAULT_FILE_MODE, 8#664).
-define(FILE_BEGINNING, 0).
-define(INFINITY, 9999).


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

choose_adequate_handler_test(Config) ->
    cdmi_test_base:choose_adequate_handler(Config).

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


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig2 = initializer:setup_storage(NewConfig),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_flush_queue_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
%%        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(2)),
            % TODO - change to 2 seconds
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, datastore_pool_queue_flush_delay, 1000)
        end, ?config(op_worker_nodes, NewConfig2)),

        application:start(etls),
        hackney:start(),
        initializer:enable_grpca_based_communication(NewConfig2),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig2, "env_desc.json"), NewConfig2)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:disable_grpca_based_communication(Config),
    hackney:stop(),
    application:stop(etls),
    initializer:teardown_storage(Config).

init_per_testcase(choose_adequate_handler_test = Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Workers, [cdmi_object_handler, cdmi_container_handler], [passthrough]),
    init_per_testcase(?DEFAULT_CASE(Case), Config);
init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).

end_per_testcase(choose_adequate_handler_test = Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [cdmi_object_handler, cdmi_container_handler]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

