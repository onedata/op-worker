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
-module(cdmi_multi_provider_test_SUITE).
-author("Tomasz Lichon").

-include_lib("ctool/include/http/headers.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include("onenv_test_utils.hrl").
-include("cdmi_test.hrl").

%% API
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/1
]).

-export([
    list_basic_dir/1,
    list_root_space_dir/1,
    list_nonexisting_dir/1,
    selective_params_list/1,
    childrenrange_list/1,
    objectid_root/1,
    objectid_dir/1,
    objectid_file/1,
    unauthorized_access_by_object_id/1,
    unauthorized_access_error/1,
    wrong_create_path_error/1,
    wrong_base_error/1,
    non_existing_file_error/1,
    open_binary_file_without_permission/1,
    open_cdmi_file_without_permission/1,

    basic_read/1,
    get_file_cdmi/1,
    get_file_non_cdmi/1,
    create_file_with_metadata/1,
    selective_metadata_read/1,
    update_user_metadata_file/1,
    create_and_update_dir_with_user_metadata/1,
    write_acl_metadata/1,
    delete_file/1,
    delete_dir/1,
    basic_create_file/1,
    base64_create_file/1,
    create_empty_file/1,
    create_noncdmi_file/1,
    basic_create_dir/1,
    create_noncdmi_dir_and_update/1,
    missing_parent_create_dir/1,

    update_file_cdmi/1,
    update_file_http/1,
    system_capabilities/1,
    container_capabilities/1,
    dataobject_capabilities/1,
    use_supported_cdmi_version/1,
    use_unsupported_cdmi_version/1,

    copy_file/1,
    copy_dir/1,
    move_file/1,
    move_dir/1,
    moved_file_permanently/1,
    moved_dir_permanently/1,
    moved_dir_with_QS_permanently/1,
    move_copy_conflict/1,
    request_format_check/1,
    mimetype_and_encoding_non_cdmi_file/1,
    update_mimetype_and_encoding/1,
    mimetype_and_encoding_create_file/1,
    mimetype_and_encoding_create_file_non_cdmi_request/1,
    out_of_range/1,
    partial_upload_cdmi/1,
    partial_upload_non_cdmi/1,
    acl_read_file/1,
    acl_write_file/1,
    acl_delete_file/1,
    acl_read_write_dir/1,
    accept_header/1
]).

groups() -> [
    {sequential_tests, [sequential], [
        %% list_root_space_dir needs to start first as it lists the main directory
        list_root_space_dir,
        list_basic_dir,
        list_nonexisting_dir,
        selective_params_list,
        childrenrange_list,
        objectid_root,
        objectid_dir,
        objectid_file,
        unauthorized_access_by_object_id,
        unauthorized_access_error,
        wrong_create_path_error,
        wrong_base_error,
        non_existing_file_error,
        open_binary_file_without_permission,
        open_cdmi_file_without_permission
    ]},
    {parallel_tests, [parallel], [
        basic_read,
        get_file_cdmi,
        get_file_non_cdmi,
        create_file_with_metadata,
        selective_metadata_read,
        update_user_metadata_file,
        create_and_update_dir_with_user_metadata,
        write_acl_metadata,
        delete_file,
        delete_dir,
        basic_create_file,
        base64_create_file,
        create_empty_file,
        create_noncdmi_file,
        basic_create_dir,
        create_noncdmi_dir_and_update,
        missing_parent_create_dir,
        update_file_cdmi,
        update_file_http,
        system_capabilities,
        container_capabilities,
        dataobject_capabilities,
        use_supported_cdmi_version,
        use_unsupported_cdmi_version,
        copy_file,
        copy_dir,
        move_file,
        move_dir,
        moved_file_permanently,
        moved_dir_permanently,
        moved_dir_with_QS_permanently,
        move_copy_conflict,
        request_format_check,
        mimetype_and_encoding_non_cdmi_file,
        update_mimetype_and_encoding,
        mimetype_and_encoding_create_file,
        mimetype_and_encoding_create_file_non_cdmi_request,
        out_of_range,
        partial_upload_cdmi,
        partial_upload_non_cdmi,
        acl_read_file,
        acl_write_file,
        acl_delete_file,
        acl_read_write_dir,
        accept_header
    ]}

].

all() -> [
    {group, sequential_tests},
    {group, parallel_tests}
].

-define(RUN_TEST(__TEST_BASE_MODULE),
    try
        __TEST_BASE_MODULE:?FUNCTION_NAME(#cdmi_test_config{
            p1_selector = krakow,
            p2_selector = paris,
            space_selector = space_krk_par_p})
    catch __TYPE:__REASON:__STACKTRACE ->
        ct:pal("Test failed due to ~p:~p.~nStacktrace: ~p", [__TYPE, __REASON, __STACKTRACE]),
        error(test_failed)
    end
).

-define(RUN_BASE_TEST(), ?RUN_TEST(cdmi_test_base)).
-define(RUN_CREATE_TEST(), ?RUN_TEST(cdmi_create_test_base)).
-define(RUN_MOVE_COPY_TEST(), ?RUN_TEST(cdmi_move_copy_test_base)).

%%%===================================================================
%%% Test functions
%%%===================================================================

%%%===================================================================
%%% Sequential tests
%%%===================================================================

list_basic_dir(_Config) ->
    ?RUN_BASE_TEST().

list_root_space_dir(_Config) ->
    ?RUN_BASE_TEST().

list_nonexisting_dir(_Config) ->
    ?RUN_BASE_TEST().

selective_params_list(_Config) ->
    ?RUN_BASE_TEST().

childrenrange_list(_Config) ->
    ?RUN_BASE_TEST().

objectid_root(_Config) ->
    ?RUN_BASE_TEST().

objectid_dir(_Config) ->
    ?RUN_BASE_TEST().

objectid_file(_Config) ->
    ?RUN_BASE_TEST().

unauthorized_access_by_object_id(_Config) ->
    ?RUN_BASE_TEST().

unauthorized_access_error(_Config) ->
    ?RUN_BASE_TEST().

wrong_create_path_error(_Config) ->
    ?RUN_BASE_TEST().

wrong_base_error(_Config) ->
    ?RUN_BASE_TEST().

non_existing_file_error(_Config) ->
    ?RUN_BASE_TEST().

open_binary_file_without_permission(_Config) ->
    ?RUN_BASE_TEST().

open_cdmi_file_without_permission(_Config) ->
    ?RUN_BASE_TEST().

%%%===================================================================
%%% Parallel tests
%%%===================================================================

basic_read(_Config) ->
    ?RUN_BASE_TEST().

get_file_cdmi(_Config) ->
    ?RUN_BASE_TEST().

get_file_non_cdmi(_Config) ->
    ?RUN_BASE_TEST().

create_file_with_metadata(_Config) ->
    ?RUN_BASE_TEST().

selective_metadata_read(_Config) ->
    ?RUN_BASE_TEST().

update_user_metadata_file(_Config) ->
    ?RUN_BASE_TEST().

create_and_update_dir_with_user_metadata(_Config) ->
    ?RUN_BASE_TEST().

write_acl_metadata(_Config) ->
    ?RUN_BASE_TEST().

delete_file(_Config) ->
    ?RUN_BASE_TEST().

delete_dir(_Config) ->
    ?RUN_BASE_TEST().

basic_create_file(_Config) ->
    ?RUN_CREATE_TEST().

base64_create_file(_Config) ->
    ?RUN_CREATE_TEST().

create_empty_file(_Config) ->
    ?RUN_CREATE_TEST().

create_noncdmi_file(_Config) ->
    ?RUN_CREATE_TEST().

basic_create_dir(_Config) ->
    ?RUN_CREATE_TEST().

create_noncdmi_dir_and_update(_Config) ->
    ?RUN_CREATE_TEST().

missing_parent_create_dir(_Config) ->
    ?RUN_CREATE_TEST().

update_file_cdmi(_Config) ->
    ?RUN_BASE_TEST().

update_file_http(_Config) ->
    ?RUN_BASE_TEST().

system_capabilities(_Config) ->
    ?RUN_BASE_TEST().

container_capabilities(_Config) ->
    ?RUN_BASE_TEST().

dataobject_capabilities(_Config) ->
    ?RUN_BASE_TEST().

use_supported_cdmi_version(_Config) ->
    ?RUN_BASE_TEST().

use_unsupported_cdmi_version(_Config) ->
    ?RUN_BASE_TEST().

copy_file(_Config) ->
    ?RUN_MOVE_COPY_TEST().

copy_dir(_Config) ->
    ?RUN_MOVE_COPY_TEST().

move_file(_Config) ->
    ?RUN_MOVE_COPY_TEST().

move_dir(_Config) ->
    ?RUN_MOVE_COPY_TEST().

moved_file_permanently(_Config) ->
    ?RUN_MOVE_COPY_TEST().

moved_dir_permanently(_Config) ->
    ?RUN_MOVE_COPY_TEST().

moved_dir_with_QS_permanently(_Config) ->
    ?RUN_MOVE_COPY_TEST().

move_copy_conflict(_Config) ->
    ?RUN_MOVE_COPY_TEST().

request_format_check(_Config) ->
    ?RUN_BASE_TEST().

mimetype_and_encoding_non_cdmi_file(_Config) ->
    ?RUN_BASE_TEST().

update_mimetype_and_encoding(_Config) ->
    ?RUN_BASE_TEST().

mimetype_and_encoding_create_file(_Config) ->
    ?RUN_BASE_TEST().

mimetype_and_encoding_create_file_non_cdmi_request(_Config) ->
    ?RUN_BASE_TEST().

out_of_range(_Config) ->
    ?RUN_BASE_TEST().

partial_upload_cdmi(_Config) ->
    ?RUN_BASE_TEST().

partial_upload_non_cdmi(_Config) ->
    ?RUN_BASE_TEST().

acl_read_file(_Config) ->
    ?RUN_BASE_TEST().

acl_write_file(_Config) ->
    ?RUN_BASE_TEST().

acl_delete_file(_Config) ->
    ?RUN_BASE_TEST().

acl_read_write_dir(_Config) ->
    ?RUN_BASE_TEST().

accept_header(_Config) ->
    ?RUN_BASE_TEST().


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    {{YY, MM, DD}, {Hour, Min, Sec}} = time:seconds_to_datetime(global_clock:timestamp_seconds()),
    DateString = str_utils:format_bin(
        "~4..0w-~2..0w-~2..0w_~2..0w~2..0w~2..0w",
        [YY, MM, DD, Hour, Min, Sec]
    ),
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
        posthook = fun(NewConfig) ->
            #object{guid = DirGuid} = onenv_file_test_utils:create_and_sync_file_tree(user2, space_krk_par_p,
                #dir_spec{
                    name = DateString
                }, krakow
            ),
            node_cache:put(root_dir_guid, DirGuid),
            node_cache:put(root_dir_name, binary:bin_to_list(DateString)),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case) ->
    ok.

