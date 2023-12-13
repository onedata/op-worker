%%%-------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------
%%% @doc
%%% CDMI tests using single provider with two nodes
%%% @end
%%%-------------------------------------
-module(cdmi_single_provider_test_SUITE).
-author("Tomasz Lichon").

-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
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
    list_basic_dir_test/1,
    list_root_space_dir_test/1,
    list_nonexisting_dir_test/1,
    selective_params_list_test/1,
    childrenrange_list_test/1,
    get_root_with_objectid_endpoint_test/1,
    get_dir_with_objectid_endpoint_test/1,
    get_file_with_objectid_endpoint_test/1,
    open_binary_file_without_permission_test/1,
    open_cdmi_file_without_permission_test/1,
    download_file_in_blocks_test/1,

    basic_read_test/1,
    get_file_cdmi_test/1,
    get_file_noncdmi_test/1,
    create_file_with_metadata_test/1,
    selective_metadata_read_test/1,
    update_user_metadata_file_test/1,
    create_and_update_dir_with_user_metadata_test/1,
    write_acl_metadata_test/1,
    delete_file_test/1,
    delete_dir_test/1,
    basic_create_file_test/1,
    base64_create_file_test/1,
    create_empty_file_test/1,
    create_noncdmi_file_test/1,
    create_cdmi_file_version_header_test/1,
    create_noncdmi_file_version_header_failure_test/1,
    basic_create_dir_test/1,
    create_noncdmi_dir_and_update_test/1,
    missing_parent_create_dir_test/1,
    create_cdmi_dir_version_header_test/1,
    create_noncdmi_dir_version_header_failure_test/1,

    update_file_cdmi_test/1,
    update_file_http_test/1,
    get_system_capabilities_test/1,
    get_container_capabilities_test/1,
    get_dataobject_capabilities_test/1,
    use_supported_cdmi_version_test/1,
    use_unsupported_cdmi_version_test/1,
    copy_file_test/1,
    copy_dir_test/1,
    move_file_test/1,
    move_dir_test/1,
    moved_file_permanently_test/1,
    moved_dir_permanently_test/1,
    moved_dir_with_QS_permanently_test/1,
    move_copy_conflict_test/1,
    request_format_check_test/1,
    mimetype_and_encoding_noncdmi_file_test/1,
    update_mimetype_and_encoding_test/1,
    mimetype_and_encoding_create_file_noncdmi_request_test/1,
    mimetype_and_encoding_create_file_test/1,
    out_of_range_test/1,
    partial_upload_cdmi_test/1,
    partial_upload_noncdmi_test/1,
    acl_read_file_test/1,
    acl_write_file_test/1,
    acl_delete_file_test/1,
    acl_read_write_dir_test/1,
    accept_header_test/1,
    download_empty_file_test/1,
    unauthorized_access_by_object_id_test/1,
    unauthorized_access_error_test/1,
    wrong_create_path_error_test/1,
    wrong_base_error_test/1,
    non_existing_file_error_test/1
]).


groups() -> [
    {sequential_tests, [sequential], [
%%         list_root_space_dir_test needs to start first as it lists the main directory
        list_root_space_dir_test,
        list_basic_dir_test,
        list_nonexisting_dir_test,
        selective_params_list_test,
        childrenrange_list_test,
        get_root_with_objectid_endpoint_test,
        get_dir_with_objectid_endpoint_test,
        get_file_with_objectid_endpoint_test,
        open_binary_file_without_permission_test,
        open_cdmi_file_without_permission_test,
        download_file_in_blocks_test
    ]},
    {parallel_tests, [parallel], [
        basic_read_test,
        get_file_cdmi_test,
        get_file_noncdmi_test,
        create_file_with_metadata_test,
        selective_metadata_read_test,
        update_user_metadata_file_test,
        create_and_update_dir_with_user_metadata_test,
        write_acl_metadata_test,
        delete_file_test,
        delete_dir_test,
        basic_create_file_test,
        base64_create_file_test,
        create_empty_file_test,
        create_noncdmi_file_test,
        create_cdmi_file_version_header_test,
        create_noncdmi_file_version_header_failure_test,
        basic_create_dir_test,
        create_noncdmi_dir_and_update_test,
        missing_parent_create_dir_test,
        create_cdmi_dir_version_header_test,
        create_noncdmi_dir_version_header_failure_test,
        update_file_cdmi_test,
        update_file_http_test,
        get_system_capabilities_test,
        get_container_capabilities_test,
        get_dataobject_capabilities_test,
        use_supported_cdmi_version_test,
        use_unsupported_cdmi_version_test,
        copy_file_test,
        copy_dir_test,
        move_file_test,
        move_dir_test,
        moved_file_permanently_test,
        moved_dir_permanently_test,
        moved_dir_with_QS_permanently_test,
        move_copy_conflict_test,
        request_format_check_test,
        mimetype_and_encoding_noncdmi_file_test,
        update_mimetype_and_encoding_test,
        mimetype_and_encoding_create_file_noncdmi_request_test,
        mimetype_and_encoding_create_file_test,
        out_of_range_test,
        partial_upload_cdmi_test,
        partial_upload_noncdmi_test,
        acl_read_file_test,
        acl_write_file_test,
        acl_delete_file_test,
        acl_read_write_dir_test,
        accept_header_test,
        download_empty_file_test,
        unauthorized_access_by_object_id_test,
        unauthorized_access_error_test,
        wrong_create_path_error_test,
        wrong_base_error_test,
        non_existing_file_error_test
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
            p2_selector = krakow,
            space_selector = space_krk})
    catch __TYPE:__REASON:__STACKTRACE ->
        ct:pal("Test failed due to ~p:~p.~nStacktrace: ~p", [__TYPE, __REASON, __STACKTRACE]),
        error(test_failed)
    end
).

-define(RUN_BASE_TEST(), ?RUN_TEST(cdmi_test_base)).
-define(RUN_CREATE_TEST(), ?RUN_TEST(cdmi_create_test_base)).
-define(RUN_MOVE_COPY_TEST(), ?RUN_TEST(cdmi_move_copy_test_base)).
-define(RUN_GET_TEST(), ?RUN_TEST(cdmi_get_test_base)).
-define(RUN_ACL_TEST(), ?RUN_TEST(cdmi_acl_test_base)).


%%%===================================================================
%%% Test functions
%%%===================================================================

%%%===================================================================
%%% Sequential tests
%%%===================================================================

list_basic_dir_test(_Config) ->
    ?RUN_GET_TEST().

list_root_space_dir_test(_Config) ->
    ?RUN_GET_TEST().

list_nonexisting_dir_test(_Config) ->
    ?RUN_GET_TEST().

selective_params_list_test(_Config) ->
    ?RUN_GET_TEST().

childrenrange_list_test(_Config) ->
    ?RUN_GET_TEST().

get_root_with_objectid_endpoint_test(_Config) ->
    ?RUN_GET_TEST().

get_dir_with_objectid_endpoint_test(_Config) ->
    ?RUN_GET_TEST().

get_file_with_objectid_endpoint_test(_Config) ->
    ?RUN_GET_TEST().

open_binary_file_without_permission_test(_Config) ->
    ?RUN_BASE_TEST().

open_cdmi_file_without_permission_test(_Config) ->
    ?RUN_BASE_TEST().

%% TODO VFS-11357 add download_file_in_blocks_test test to cdmi_multi_provider
download_file_in_blocks_test(_Config) ->
    [_WorkerP1, WorkerP2] = Workers = oct_background:get_provider_nodes(krakow),
    SpaceName = binary_to_list(oct_background:get_space_name(space_krk)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = filename:join(SpaceName, RootName) ++ "/",

    AuthHeaders = [rest_test_utils:user_token_header(oct_background:get_user_access_token(user2))],

    % Create file
    FilePath = filename:join([RootPath, <<"upload_file_in_blocks">>]),
    Data = crypto:strong_rand_bytes(200),

    onenv_file_test_utils:create_and_sync_file_tree(
        user2,
        node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"upload_file_in_blocks">>,
            content = Data
        },
        krakow
    ),

    % Reading file with ?DEFAULT_STORAGE_BLOCK_SIZE should result in 2 full reads
    {ok, _, _, Response1} = ?assertMatch(
        {ok, 200, _Headers, _Response},
        cdmi_test_utils:do_request_base(WorkerP2, FilePath, get, AuthHeaders, <<>>)
    ),
    ?assertEqual(Data, Response1),
    ?assertEqual(
        [
            #chunk{offset = 0, size = 100},
            #chunk{offset = 100, size = 100}
        ],
        get_read_chunks()
    ),

    % When streaming starting from offset not being multiple of block size
    % only bytes up to next smallest multiple of block size should be read
    % in first block. Next blocks read should be of equal size to storage
    % block size. The exception to this is last block as it only returns
    % remaining bytes.
    set_storage_block_size(Workers, 50),
    DataPart = binary:part(Data, {33, 100}),
    RangeHeader = {?HDR_RANGE, <<"bytes=33-132">>},    % 33-132 inclusive
    {ok, _, _, Response2} = ?assertMatch(
        {ok, 206, _Headers, _Response},
        cdmi_test_utils:do_request_base(WorkerP2, FilePath, get, [RangeHeader | AuthHeaders], <<>>)
    ),
    ?assertEqual(DataPart, Response2),

    ?assertEqual(
        [
            #chunk{offset = 33, size = 17},
            #chunk{offset = 50, size = 50},
            #chunk{offset = 100, size = 33}
        ],
        get_read_chunks()
    ).

%%%===================================================================
%%% Parallel tests
%%%===================================================================

basic_read_test(_Config) ->
    ?RUN_GET_TEST().

get_file_cdmi_test(_Config) ->
    ?RUN_GET_TEST().

get_file_noncdmi_test(_Config) ->
    ?RUN_GET_TEST().

create_file_with_metadata_test(_Config) ->
    ?RUN_CREATE_TEST().

selective_metadata_read_test(_Config) ->
    ?RUN_BASE_TEST().

update_user_metadata_file_test(_Config) ->
    ?RUN_BASE_TEST().

create_and_update_dir_with_user_metadata_test(_Config) ->
    ?RUN_CREATE_TEST().

write_acl_metadata_test(_Config) ->
    ?RUN_ACL_TEST().

delete_file_test(_Config) ->
    ?RUN_BASE_TEST().

delete_dir_test(_Config) ->
    ?RUN_BASE_TEST().

basic_create_file_test(_Config) ->
    ?RUN_CREATE_TEST().

base64_create_file_test(_Config) ->
    ?RUN_CREATE_TEST().

create_empty_file_test(_Config) ->
    ?RUN_CREATE_TEST().

create_noncdmi_file_test(_Config) ->
    ?RUN_CREATE_TEST().

create_cdmi_file_version_header_test(_Config) ->
    ?RUN_CREATE_TEST().

create_noncdmi_file_version_header_failure_test(_Config) ->
    ?RUN_CREATE_TEST().

basic_create_dir_test(_Config) ->
    ?RUN_CREATE_TEST().

create_noncdmi_dir_and_update_test(_Config) ->
    ?RUN_CREATE_TEST().

missing_parent_create_dir_test(_Config) ->
    ?RUN_CREATE_TEST().

create_cdmi_dir_version_header_test(_Config) ->
    ?RUN_CREATE_TEST().

create_noncdmi_dir_version_header_failure_test(_Config) ->
    ?RUN_CREATE_TEST().

update_file_cdmi_test(_Config) ->
    ?RUN_BASE_TEST().

update_file_http_test(_Config) ->
    ?RUN_BASE_TEST().

get_system_capabilities_test(_Config) ->
    ?RUN_GET_TEST().

get_container_capabilities_test(_Config) ->
    ?RUN_GET_TEST().

get_dataobject_capabilities_test(_Config) ->
    ?RUN_GET_TEST().

use_supported_cdmi_version_test(_Config) ->
    ?RUN_BASE_TEST().

use_unsupported_cdmi_version_test(_Config) ->
    ?RUN_BASE_TEST().

copy_file_test(_Config) ->
    ?RUN_MOVE_COPY_TEST().

copy_dir_test(_Config) ->
    ?RUN_MOVE_COPY_TEST().

move_file_test(_Config) ->
    ?RUN_MOVE_COPY_TEST().

move_dir_test(_Config) ->
    ?RUN_MOVE_COPY_TEST().

moved_file_permanently_test(_Config) ->
    ?RUN_MOVE_COPY_TEST().

moved_dir_permanently_test(_Config) ->
    ?RUN_MOVE_COPY_TEST().

moved_dir_with_QS_permanently_test(_Config) ->
    ?RUN_MOVE_COPY_TEST().

move_copy_conflict_test(_Config) ->
    ?RUN_MOVE_COPY_TEST().

request_format_check_test(_Config) ->
    ?RUN_BASE_TEST().

mimetype_and_encoding_noncdmi_file_test(_Config) ->
    ?RUN_BASE_TEST().

update_mimetype_and_encoding_test(_Config) ->
    ?RUN_BASE_TEST().

mimetype_and_encoding_create_file_noncdmi_request_test(_Config) ->
    ?RUN_CREATE_TEST().

mimetype_and_encoding_create_file_test(_Config) ->
    ?RUN_CREATE_TEST().

out_of_range_test(_Config) ->
    ?RUN_BASE_TEST().

partial_upload_cdmi_test(_Config) ->
    ?RUN_BASE_TEST().

partial_upload_noncdmi_test(_Config) ->
    ?RUN_BASE_TEST().

acl_read_file_test(_Config) ->
    ?RUN_ACL_TEST().

acl_write_file_test(_Config) ->
    ?RUN_ACL_TEST().

acl_delete_file_test(_Config) ->
    ?RUN_ACL_TEST().

acl_read_write_dir_test(_Config) ->
    ?RUN_ACL_TEST().

accept_header_test(_Config) ->
    ?RUN_BASE_TEST().

download_empty_file_test(_Config) ->
    ?RUN_BASE_TEST().

unauthorized_access_by_object_id_test(_Config) ->
    ?RUN_GET_TEST().

unauthorized_access_error_test(_Config) ->
    ?RUN_BASE_TEST().

wrong_create_path_error_test(_Config) ->
    ?RUN_CREATE_TEST().

wrong_base_error_test(_Config) ->
    ?RUN_BASE_TEST().

non_existing_file_error_test(_Config) ->
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
        onenv_scenario = "1op-2nodes",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
        posthook = fun(NewConfig) ->
            #object{guid = DirGuid} = onenv_file_test_utils:create_and_sync_file_tree(user2, space_krk,
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


init_per_testcase(download_file_in_blocks_test = Case, Config) ->
    Self = self(),
    Workers = oct_background:get_provider_nodes(krakow),

    test_utils:mock_new(Workers, [lfm], [passthrough]),
    test_utils:mock_expect(Workers, lfm, check_size_and_read, fun(FileHandle, Offset, ToRead) ->
        {ok, _, Data} = Res = meck:passthrough([FileHandle, Offset, ToRead]),
        Self ! {read, #chunk{offset = Offset, size = byte_size(Data)}},
        Res
    end),
    mock_storage_get_block_size(Workers),
    set_storage_block_size(Workers, ?CDMI_DEFAULT_STORAGE_BLOCK_SIZE),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(download_file_in_blocks_test = Case) ->
    Workers = oct_background:get_provider_nodes(krakow),
    unmock_storage_get_block_size(Workers),
    ok = test_utils:mock_unload(Workers, [lfm]),
    end_per_testcase(?DEFAULT_CASE(Case));

end_per_testcase(_Case) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

get_read_chunks() ->
    get_read_chunks([]).


get_read_chunks(Chunks) ->
    receive
        {read, Chunk} ->
            get_read_chunks([Chunk | Chunks])
    after 10 ->
        lists:reverse(Chunks)
    end.


set_storage_block_size(Workers, BlockSize) ->
    ?assertMatch(
        {_, []},
        utils:rpc_multicall(Workers, node_cache, put, [storage_block_size, BlockSize])
    ).


mock_storage_get_block_size(Workers) ->
    test_utils:mock_new(Workers, [storage], [passthrough]),
    test_utils:mock_expect(Workers, storage, get_block_size, fun(_) ->
        node_cache:get(storage_block_size, ?CDMI_DEFAULT_STORAGE_BLOCK_SIZE)
    end).


unmock_storage_get_block_size(Workers) ->
    ok = test_utils:mock_unload(Workers, [storage]).
