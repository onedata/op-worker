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
-module(cdmi_misc_test_base).
-author("Tomasz Lichon").

-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include("onenv_test_utils.hrl").
-include("cdmi_test.hrl").

-export([
    selective_metadata_read_test/1,
    update_user_metadata_file_test/1,
    unauthorized_access_error_test/1,
    wrong_base_error_test/1,
    non_existing_file_error_test/1,
    open_binary_file_without_permission_test/1,
    open_cdmi_file_without_permission_test/1,
    delete_file_test/1,
    delete_dir_test/1,
    update_file_cdmi_test/1,
    update_file_http_test/1,
    use_supported_cdmi_version_test/1,
    use_unsupported_cdmi_version_test/1,
    request_format_check_test/1,
    mimetype_and_encoding_noncdmi_file_test/1,
    update_mimetype_and_encoding_test/1,
    out_of_range_test/1,
    partial_upload_cdmi_test/1,
    partial_upload_noncdmi_test/1,
    accept_header_test/1,
    download_empty_file_test/1
]).


%%%===================================================================
%%% Test functions
%%%===================================================================

unauthorized_access_error_test(Config) ->
    TestDirName = ?build_test_root_path(Config),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{name = atom_to_binary(?FUNCTION_NAME)},
    Config#cdmi_test_config.p1_selector),
    {ok, Code, _Headers, Response} =
        cdmi_test_utils:do_request(?WORKERS, TestDirName, get, [], []),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_UNAUTHORIZED),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


wrong_base_error_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    RequestHeaders = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_OBJECT_CONTENT_TYPE_HEADER
    ],
    RequestBody = json_utils:encode(#{
        <<"valuetransferencoding">> => <<"base64">>,
        <<"value">> => <<"#$%">>
    }),
    {ok, Code, _Headers, Response} = cdmi_test_utils:do_request(
        ?WORKERS, filename:join(RootPath,  "some_file_b64"), put, RequestHeaders, RequestBody
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(<<"base64">>)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


non_existing_file_error_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    %%-- reading non-existing file --
    RequestHeaders = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_OBJECT_CONTENT_TYPE_HEADER
    ],
    {ok, _Code, _Headers, _Response} = ?assertMatch(
        {ok, ?HTTP_404_NOT_FOUND, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS, filename:join(RootPath, "nonexistent_file"), get, RequestHeaders
    )),

    %%--- listing non-existing dir -----
    RequestHeaders2 = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, _Code2, _Headers2, _Response2} = ?assertMatch(
        {ok, ?HTTP_404_NOT_FOUND, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS, filename:join(RootPath, "/nonexisting_dir") ++ "/", get, RequestHeaders2
    )).


open_binary_file_without_permission_test(Config) ->
    [WorkerP1, _WorkerP2] = ?WORKERS,
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    FilePath = filename:join([RootPath, "file8"]),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"file8">>,
            content = ?FILE_CONTENT
        }, Config#cdmi_test_config.p1_selector
    ),
    ?assertEqual(cdmi_test_utils:object_exists(FilePath, Config), true),

    cdmi_test_utils:write_to_file(FilePath, ?FILE_CONTENT, ?FILE_OFFSET_START, Config),
    ?assertEqual(cdmi_test_utils:get_file_content(FilePath, Config), ?FILE_CONTENT, ?ATTEMPTS),
    RequestHeaders = [cdmi_test_utils:user_2_token_header()],

    ?assertMatch(ok, cdmi_test_utils:mock_opening_file_without_perms(Config), ?ATTEMPTS),

    {ok, Code, _Headers, Response} = ?assertMatch(
        {ok, 400, _, _},
        cdmi_test_utils:do_request(WorkerP1, FilePath, get, RequestHeaders),
        ?ATTEMPTS
    ),
    ?assertMatch(ok, cdmi_test_utils:unmock_opening_file_without_perms(Config), ?ATTEMPTS),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}, ?ATTEMPTS).


open_cdmi_file_without_permission_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    FilePath = filename:join([RootPath, "file9"]),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"file9">>,
            content = ?FILE_CONTENT
        }, Config#cdmi_test_config.p1_selector
    ),
    ?assertEqual(cdmi_test_utils:object_exists(FilePath, Config), true),

    cdmi_test_utils:write_to_file(FilePath, ?FILE_CONTENT, ?FILE_OFFSET_START, Config),
    ?assertEqual(cdmi_test_utils:get_file_content(FilePath, Config), ?FILE_CONTENT, ?ATTEMPTS),
    RequestHeaders = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_OBJECT_CONTENT_TYPE_HEADER
    ],

    ?assertMatch(ok, cdmi_test_utils:mock_opening_file_without_perms(Config), ?ATTEMPTS),
    {ok, Code, _Headers, Response} = ?assertMatch(
        {ok, 400, _, _},
        cdmi_test_utils:do_request(
            ?WORKERS, FilePath, get, RequestHeaders
        ),
        ?ATTEMPTS
    ),
    ?assertMatch(ok, cdmi_test_utils:unmock_opening_file_without_perms(Config), ?ATTEMPTS),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}, ?ATTEMPTS).


selective_metadata_read_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    UserId = oct_background:get_user_id(user2),
    FilePath = filename:join([RootPath, "metadataTest2.txt"]),
    RequestHeaders = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody = #{
        <<"value">> => ?FILE_CONTENT,
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"text/plain">>,
        <<"metadata">> => #{<<"my_metadata">> => <<"my_value">>,
            <<"cdmi_not_allowed">> => <<"my_value">>}},

    RawRequestBody = json_utils:encode(RequestBody),
    {ok, _, _Headers, _} = cdmi_test_utils:do_request(
        ?WORKERS, FilePath, put, RequestHeaders, RawRequestBody
    ),

    %%-- selective metadata read -----
    {ok, ?HTTP_200_OK, _Headers2, Response2} = ?assertMatch(
        {ok, 200, _, _}, cdmi_test_utils:do_request(
            ?WORKERS, FilePath ++ "?metadata", get, RequestHeaders, []), ?ATTEMPTS),
    CdmiResponse2 = json_utils:decode(Response2),
    Metadata2 = maps:get(<<"metadata">>, CdmiResponse2),
    ?assertEqual(1, maps:size(CdmiResponse2)),
    ?assertEqual(6, maps:size(Metadata2)),

    %%-- selective metadata read with prefix -----
    {ok, ?HTTP_200_OK, _Headers3, Response3} = cdmi_test_utils:do_request(
        ?WORKERS, FilePath ++ "?metadata:cdmi_", get, RequestHeaders, []
    ),
    CdmiResponse3 = json_utils:decode(Response3),
    Metadata3 = maps:get(<<"metadata">>, CdmiResponse3),
    ?assertEqual(1, maps:size(CdmiResponse3)),
    ?assertEqual(5, maps:size(Metadata3)),

    {ok, ?HTTP_200_OK, _Headers4, Response4} = cdmi_test_utils:do_request(
        ?WORKERS, FilePath ++ "?metadata:cdmi_o", get, RequestHeaders, []
    ),
    CdmiResponse4 = json_utils:decode(Response4),
    Metadata4 = maps:get(<<"metadata">>, CdmiResponse4),
    ?assertEqual(1, maps:size(CdmiResponse4)),
    ?assertMatch(UserId, maps:get(<<"cdmi_owner">>, Metadata4)),
    ?assertEqual(1, maps:size(Metadata4)),

    {ok, ?HTTP_200_OK, _Headers5, Response5} = cdmi_test_utils:do_request(
        ?WORKERS, FilePath ++ "?metadata:cdmi_size", get, RequestHeaders, []
    ),
    CdmiResponse5 = json_utils:decode(Response5),
    Metadata5 = maps:get(<<"metadata">>, CdmiResponse5),
    ?assertEqual(1, maps:size(CdmiResponse5)),
    ?assertMatch(#{<<"cdmi_size">> := <<"13">>}, Metadata5),
    ?assertEqual(1, maps:size(Metadata5)),

    {ok, ?HTTP_200_OK, _Headers6, Response6} = cdmi_test_utils:do_request(
        ?WORKERS, FilePath ++ "?metadata:cdmi_no_such_metadata", get, RequestHeaders, []
    ),
    CdmiResponse6 = json_utils:decode(Response6),
    ?assertEqual(1, maps:size(CdmiResponse6)),
    ?assertMatch(#{<<"metadata">> := #{}}, CdmiResponse6).


update_user_metadata_file_test(Config) ->
    [WorkerP1, WorkerP2] = ?WORKERS,
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    FilePath = filename:join([RootPath, "metadataTest3.txt"]),
    RequestHeaders = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody = #{
        <<"value">> => ?FILE_CONTENT,
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"text/plain">>,
        <<"metadata">> => #{<<"my_metadata">> => <<"my_value">>,
            <<"cdmi_not_allowed">> => <<"my_value">>}},

    RawRequestBody = json_utils:encode(RequestBody),
    {ok, _, _Headers, _} = ?assertMatch(
        {ok, _, _, _},
        cdmi_test_utils:do_request(?WORKERS, FilePath, put, RequestHeaders, RawRequestBody),
        ?ATTEMPTS
    ),
    RequestBody2 = #{<<"metadata">> => #{<<"my_new_metadata">> => <<"my_new_value">>}},
    RawRequestBody2 = json_utils:encode(RequestBody2),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(
            WorkerP1, FilePath, put, RequestHeaders, RawRequestBody2
        ),
        ?ATTEMPTS
    ),
    ?assertEqual(1, maps:size(get_metadata_from_request(FilePath ++ "?metadata:my",
        WorkerP2, RequestHeaders)), ?ATTEMPTS),
    ?assertMatch(
        #{<<"my_new_metadata">> := <<"my_new_value">>},
        get_metadata_from_request(FilePath ++ "?metadata:my", ?WORKERS, RequestHeaders),
        ?ATTEMPTS
    ),

    {ok, ?HTTP_200_OK, _Headers2, Response2} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_test_utils:do_request(
            ?WORKERS, FilePath ++ "?metadata:my", get, RequestHeaders, []
        ),
        ?ATTEMPTS
    ),
    CdmiResponse2 = json_utils:decode(Response2),
    Metadata2 = maps:get(<<"metadata">>, CdmiResponse2),
    ?assertEqual(1, maps:size(CdmiResponse2)),
    ?assertMatch(#{<<"my_new_metadata">> := <<"my_new_value">>}, Metadata2),
    ?assertEqual(1, maps:size(Metadata2)),

    RequestBody3 = #{<<"metadata">> =>
    #{<<"my_new_metadata_add">> => <<"my_new_value_add">>,
        <<"my_new_metadata">> => <<"my_new_value_update">>,
        <<"cdmi_not_allowed">> => <<"my_value">>}},
    RawRequestBody3 = json_utils:encode(RequestBody3),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(
            WorkerP1,
            FilePath ++ "?metadata:my_new_metadata_add;metadata:my_new_metadata;metadata:cdmi_not_allowed",
            put,
            RequestHeaders,
            RawRequestBody3
        ),
        ?ATTEMPTS
    ),

    {ok, ?HTTP_200_OK, _Headers3, Response3} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_test_utils:do_request(WorkerP1, FilePath ++ "?metadata:my", get, RequestHeaders, []),
        ?ATTEMPTS
    ),
    CdmiResponse3 = json_utils:decode(Response3),
    Metadata3 = maps:get(<<"metadata">>, CdmiResponse3),
    ?assertEqual(1, maps:size(CdmiResponse3)),
    ?assertMatch(#{<<"my_new_metadata_add">> := <<"my_new_value_add">>}, Metadata3),
    ?assertMatch(#{<<"my_new_metadata">> := <<"my_new_value_update">>}, Metadata3),
    ?assertEqual(2, maps:size(Metadata3)),

    {ok, ?HTTP_200_OK, _Headers4, Response4} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_test_utils:do_request(WorkerP1, FilePath ++ "?metadata:cdmi_", get, RequestHeaders, []),
        ?ATTEMPTS
    ),
    CdmiResponse4 = json_utils:decode(Response4),
    Metadata4 = maps:get(<<"metadata">>, CdmiResponse4),
    ?assertEqual(1, maps:size(CdmiResponse4)),
    ?assertEqual(5, maps:size(Metadata4)),

    RequestBody5 = #{<<"metadata">> => #{<<"my_new_metadata">> => <<"my_new_value_ignore">>}},
    RawRequestBody5 = json_utils:encode(RequestBody5),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(
            ?WORKERS, FilePath ++ "?metadata:my_new_metadata_add", put, RequestHeaders, RawRequestBody5
        ),
        ?ATTEMPTS
    ),

    {ok, ?HTTP_200_OK, _Headers6, Response6} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_test_utils:do_request(
            ?WORKERS, FilePath ++ "?metadata:my", get, RequestHeaders, []
        ),
        ?ATTEMPTS
    ),
    CdmiResponse6 = json_utils:decode(Response6),
    Metadata6 = maps:get(<<"metadata">>, CdmiResponse6),

    ?assertEqual(1, maps:size(CdmiResponse6)),
    ?assertMatch(#{<<"my_new_metadata">> := <<"my_new_value_update">>}, Metadata6).


% Tests cdmi object DELETE requests
delete_file_test(Config) ->
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = filename:join(SpaceName, RootName) ++ "/",

    FilePath = filename:join([RootPath, "toDelete.txt"]),
    GroupFilePath =
        filename:join([RootPath, "groupFile"]),

    %%----- basic delete -----------
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{name = <<"toDelete.txt">>},
    Config#cdmi_test_config.p1_selector),

    RequestHeaders = [?CDMI_VERSION_HEADER],
    {ok, _Code1, _Headers1, _Response1} = ?assertMatch(
        {ok, ?HTTP_204_NO_CONTENT, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS, FilePath, delete, [cdmi_test_utils:user_2_token_header() | RequestHeaders]
    )),
    ?assertNot(cdmi_test_utils:object_exists(FilePath, Config), ?ATTEMPTS),

    %%----- delete group file ------
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{name = <<"groupFile">>},
    Config#cdmi_test_config.p1_selector),

    RequestHeaders2 = [?CDMI_VERSION_HEADER],
    {ok, _Code2, _Headers2, _Response2} = ?assertMatch(
        {ok, ?HTTP_204_NO_CONTENT, _, _},
        cdmi_test_utils:do_request(?WORKERS, GroupFilePath, delete,
            [cdmi_test_utils:user_2_token_header() | RequestHeaders2])
    ),
    ?assertNot(cdmi_test_utils:object_exists(GroupFilePath, Config), ?ATTEMPTS).


% Tests cdmi container DELETE requests
delete_dir_test(Config) ->
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = filename:join(SpaceName, RootName) ++ "/",

    DirPath = filename:join([RootPath, "toDelete"]) ++ "/",
    ChildDirPath = filename:join([RootPath, "toDelete", "child"]) ++ "/",

    %%----- basic delete -----------
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{name = <<"toDelete">>},
    Config#cdmi_test_config.p1_selector),

    RequestHeaders = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, _Code1, _Headers1, _Response1} = ?assertMatch(
        {ok, ?HTTP_204_NO_CONTENT, _, _},
        cdmi_test_utils:do_request(?WORKERS, DirPath, delete, RequestHeaders, [])
    ),
    ?assertNot(cdmi_test_utils:object_exists(DirPath, Config), ?ATTEMPTS),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = <<"toDelete">>,
            children = [
                #dir_spec{name = <<"toDeleteChild">>}
        ]},
        Config#cdmi_test_config.p1_selector
    ),

    RequestHeaders2 = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, _Code2, _Headers2, _Response2} = ?assertMatch(
        {ok, ?HTTP_204_NO_CONTENT, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS, DirPath, delete, RequestHeaders2, []
    )),
    ?assertNot(cdmi_test_utils:object_exists(DirPath, Config), ?ATTEMPTS),
    ?assertNot(cdmi_test_utils:object_exists(ChildDirPath, Config)),

    %%----- delete root dir -------
    RequestHeaders3 = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER],
    ?assert(cdmi_test_utils:object_exists("/", Config)),

    {ok, Code3, _Headers3, Response3} = cdmi_test_utils:do_request(
        ?WORKERS, "/", delete, RequestHeaders3, []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EPERM)),
    ?assertMatch(ExpRestError, {Code3, json_utils:decode(Response3)}),
    ?assert(cdmi_test_utils:object_exists("/", Config)).


% Tests cdmi object PUT requests (updating content)
update_file_cdmi_test(Config) ->
    [WorkerP1, _WorkerP2] = ?WORKERS,
    FilePath = ?build_test_root_specified_path(Config, filename:join(?FUNCTION_NAME, "1")),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            children = [
                #file_spec{
                    name = <<"1">>,
                    content = ?FILE_CONTENT
                }
            ]
        }, Config#cdmi_test_config.p1_selector
    ),

    NewValue = <<"New Value!">>,
    UpdatedValue = <<"123 Value!">>,

    %%--- value replace, cdmi ------
    ?assert(cdmi_test_utils:object_exists(FilePath, Config)),
    ?assertEqual(?FILE_CONTENT, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    RequestHeaders = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody = #{<<"value">> => NewValue},
    RawRequestBody = json_utils:encode(RequestBody),

    {ok, _Code1, _Headers1, _Response1} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(
            WorkerP1, FilePath, put, RequestHeaders, RawRequestBody
        ),
        ?ATTEMPTS
    ),
    ?assert(cdmi_test_utils:object_exists(FilePath, Config)),
    ?assertEqual(NewValue, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    %%---- value update, cdmi ------
    UpdateValue = <<"123">>,
    RequestHeaders2 = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody2 = #{<<"value">> => base64:encode(UpdateValue)},
    RawRequestBody2 = json_utils:encode(RequestBody2),
    {ok, _Code2, _Headers2, _Response2} = ?assertMatch(
        {ok, ?HTTP_204_NO_CONTENT, _, _},
        cdmi_test_utils:do_request(
        WorkerP1, FilePath ++ "?value:0-2", put, RequestHeaders2, RawRequestBody2
    )),
    ?assert(cdmi_test_utils:object_exists(FilePath, Config)),
    ?assertEqual(UpdatedValue, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS).


update_file_http_test(Config) ->
    [WorkerP1, _WorkerP2] = ?WORKERS,
    FilePath = ?build_test_root_specified_path(Config, filename:join(?FUNCTION_NAME, "1")),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            children = [
                #file_spec{
                    name = <<"1">>,
                    content = ?FILE_CONTENT
                }
            ]
        }, Config#cdmi_test_config.p1_selector
    ),
    %%--- value replace, http ------
    RequestBody = ?FILE_CONTENT,
    {ok, ?HTTP_204_NO_CONTENT, _Headers0, _Response0} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(WorkerP1, FilePath, put, [cdmi_test_utils:user_2_token_header()], RequestBody),
        ?ATTEMPTS
    ),
    ?assert(cdmi_test_utils:object_exists(FilePath, Config)),
    ?assertEqual(?FILE_CONTENT,
        cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    %%---- value update, http ------
    UpdateValue = <<"123">>,
    RequestHeaders = [{?HDR_CONTENT_RANGE, <<"bytes 0-2/3">>}],
    {ok, ?HTTP_204_NO_CONTENT, _Headers1, _Response1} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(WorkerP1, FilePath,
            put, [cdmi_test_utils:user_2_token_header() | RequestHeaders], UpdateValue),
        ?ATTEMPTS
    ),
    ?assert(cdmi_test_utils:object_exists(FilePath, Config)),
    ?assertEqual(<<"123e content!">>,
        cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    %%---- value update2, http -----
    UpdateValue2 = <<"00">>,
    RequestHeaders2 = [{?HDR_CONTENT_RANGE, <<"bytes 3-4/*">>}],
    {ok, ?HTTP_204_NO_CONTENT, _Headers2, _Response2} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(WorkerP1, FilePath,
            put, [cdmi_test_utils:user_2_token_header() | RequestHeaders2], UpdateValue2),
        ?ATTEMPTS
    ),
    ?assert(cdmi_test_utils:object_exists(FilePath, Config)),
    ?assertEqual(<<"12300content!">>,
        cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    %%---- value update, http error ------
    RequestHeaders3 = [{?HDR_CONTENT_RANGE, <<"bytes 0-2,3-4/*">>}],
    {ok, Code3, _Headers3, Response3} =
        cdmi_test_utils:do_request(WorkerP1, FilePath, put, [cdmi_test_utils:user_2_token_header() | RequestHeaders3],
            UpdateValue),

    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(?HDR_CONTENT_RANGE)),
    ?assertMatch(ExpRestError, {Code3, json_utils:decode(Response3)}),
    ?assert(cdmi_test_utils:object_exists(FilePath, Config)),
    ?assertEqual(<<"12300content!">>,
        cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS).


use_supported_cdmi_version_test(Config) ->
    RequestHeaders = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],

    {ok, _Code, _ResponseHeaders, _Response} = ?assertMatch(
        {ok, ?HTTP_404_NOT_FOUND, _, _},
        cdmi_test_utils:do_request(?WORKERS, "/random", get, RequestHeaders)
    ).


use_unsupported_cdmi_version_test(Config) ->
    RequestHeaders = [{<<"X-CDMI-Specification-Version">>, <<"1.0.2">>}],
    {ok, Code, _ResponseHeaders, Response} =
        cdmi_test_utils:do_request(?WORKERS, "/random", get, RequestHeaders),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>])),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


% tests req format checking
request_format_check_test(Config) ->
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = filename:join(SpaceName, RootName) ++ "/",

    FilePath = filename:join([RootPath, "file.txt"]),
    DirPath = filename:join([RootPath, "dir"]) ++ "/",

    %%-- obj missing content-type --
    RequestHeaders = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody = #{<<"value">> => ?FILE_CONTENT},
    RawRequestBody = json_utils:encode(RequestBody),
    {ok, _Code1, _Headers1, _Response1} = ?assertMatch(
        {ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS, FilePath, put, RequestHeaders, RawRequestBody
    )),

    %%-- dir missing content-type --
    RequestHeaders2 = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody2 = #{<<"metadata">> => <<"">>},
    RawRequestBody2 = json_utils:encode(RequestBody2),
    {ok, _Code2, _Headers2, _Response2} = ?assertMatch(
        {ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS, DirPath, put, RequestHeaders2, RawRequestBody2
    )).


% tests mimetype and valuetransferencoding properties, they are part of cdmi-object and cdmi-container
% and should be changeble
mimetype_and_encoding_noncdmi_file_test(Config) ->
    FilePath = ?build_test_root_specified_path(
        Config, filename:join(?FUNCTION_NAME, "1")),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            children = [
                #file_spec{
                    name = <<"1">>,
                    content = ?FILE_CONTENT
                }
            ]
        }, Config#cdmi_test_config.p1_selector
    ),
    %% get mimetype and valuetransferencoding of non-cdmi file
    RequestHeaders = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, _Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request( ?WORKERS, FilePath ++ "?mimetype;valuetransferencoding",
        get, RequestHeaders, [])
    ),
    CdmiResponse = json_utils:decode(Response),
    ?assertMatch(#{<<"mimetype">> := <<"application/octet-stream">>}, CdmiResponse),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"base64">>}, CdmiResponse).


update_mimetype_and_encoding_test(Config) ->
    [WorkerP1, _WorkerP2] = ?WORKERS,
    FilePath = ?build_test_root_specified_path(
        Config, filename:join(?FUNCTION_NAME, "2")),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            children = [
                #file_spec{
                    name = <<"2">>,
                    content = ?FILE_CONTENT
                }
            ]
        }, Config#cdmi_test_config.p1_selector
    ),
    %%-- update mime and encoding --
    RequestHeaders = [?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER, cdmi_test_utils:user_2_token_header()],
    RawBody = json_utils:encode(#{
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"application/binary">>
    }),
    {ok, _Code, _Headers, _Response} = ?assertMatch(
        {ok, ?HTTP_204_NO_CONTENT, _, _},
        cdmi_test_utils:do_request(
        WorkerP1, FilePath, put, RequestHeaders, RawBody
    )),

    {ok, _Code2, _Headers2, Response2} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
        WorkerP1, FilePath ++ "?mimetype;valuetransferencoding", get, RequestHeaders, []
    )),
    CdmiResponse = json_utils:decode(Response2),
    ?assertMatch(#{<<"mimetype">> := <<"application/binary">>}, CdmiResponse),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>}, CdmiResponse).


% tests reading&writing file at random ranges
out_of_range_test(Config) ->
    Workers = oct_background:get_provider_nodes(Config#cdmi_test_config.p1_selector),
    SpaceName = binary_to_list(oct_background:get_space_name(
        Config#cdmi_test_config.space_selector)
    ),
    DirPath = ?build_test_root_path(Config),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{name = atom_to_binary(?FUNCTION_NAME)},
    Config#cdmi_test_config.p1_selector),

    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = filename:join(SpaceName, RootName) ++ "/",

    FilePath = filename:join([RootPath, "random_range_file.txt"]),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"random_range_file.txt">>
        }, Config#cdmi_test_config.p1_selector
    ),

    %%---- reading out of range ---- (should return empty binary)
    ?assertEqual(<<>>, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),
    RequestHeaders = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER],
    RequestBody = json_utils:encode(#{<<"value">> => <<"data">>}),

    {ok, _Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
        Workers, FilePath ++ "?value:0-3", get, RequestHeaders, RequestBody
    )),
    CdmiResponse = json_utils:decode(Response),
    ?assertMatch(#{<<"value">> := <<>>}, CdmiResponse),

    %%------ writing at end -------- (should extend file)
    ?assertEqual(<<>>, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    RequestHeaders2 = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER],
    RequestBody2 = json_utils:encode(#{<<"value">> => base64:encode(<<"data">>)}),
    {ok, _Code2, _Headers2, _Response2} = ?assertMatch(
        {ok, ?HTTP_204_NO_CONTENT, _, _},
        cdmi_test_utils:do_request(
        Workers, FilePath ++ "?value:0-3", put, RequestHeaders2, RequestBody2
    )),
    ?assertEqual(<<"data">>, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    %%------ writing at random -------- (should return zero bytes in any gaps)
     RequestBody3 = json_utils:encode(#{<<"value">> => base64:encode(<<"data">>)}),
     {ok, _Code3, _Headers3, _Response3} = ?assertMatch(
         {ok, ?HTTP_204_NO_CONTENT, _, _},
         cdmi_test_utils:do_request(
         Workers, FilePath ++ "?value:10-13", put, RequestHeaders2, RequestBody3
     )),

    % "data(6x<0_byte>)data"
     ?assertEqual(
         <<100, 97, 116, 97, 0, 0, 0, 0, 0, 0, 100, 97, 116, 97>>,
         cdmi_test_utils:get_file_content(FilePath, Config),
         ?ATTEMPTS
     ),

    %%----- random childrange ------ (shuld fail)
    {ok, Code4, _Headers4, Response4} = cdmi_test_utils:do_request(
        Workers, filename:join(DirPath, "?children:100-132"), get, RequestHeaders2, []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(<<"childrenrange">>)),

    ?assertMatch(ExpRestError, {Code4, json_utils:decode(Response4)}).


% tests cdmi and non-cdmi partial upload feature (requests with x-cdmi-partial flag set to true)
partial_upload_cdmi_test(Config) ->
    [WorkerP1, WorkerP2] = ?WORKERS,
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = filename:join(SpaceName, RootName) ++ "/",

    FilePath = filename:join([RootPath, "partial.txt"]),
    Chunk1 = <<"some">>,
    Chunk2 = <<"_">>,
    Chunk3 = <<"value">>,

    %%------ cdmi request partial upload ------
    ?assertNot(cdmi_test_utils:object_exists(FilePath, Config)),

    % upload first chunk of file
    RequestHeaders = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_OBJECT_CONTENT_TYPE_HEADER,
        {"X-CDMI-Partial", "true"}
    ],
    RequestBody = json_utils:encode(#{<<"value">> => Chunk1}),
    {ok, ?HTTP_201_CREATED, _Headers1, Response1} = ?assertMatch(
        {ok, 201, _, _},
        cdmi_test_utils:do_request(
            WorkerP2, FilePath, put, RequestHeaders, RequestBody
        ),
        ?ATTEMPTS
    ),
    CdmiResponse = json_utils:decode(Response1),
    ?assertMatch(#{<<"completionStatus">> := <<"Processing">>}, CdmiResponse),

    % upload second chunk of file
    RequestBody2 = json_utils:encode(#{<<"value">> => base64:encode(Chunk2)}),
    {ok, ?HTTP_204_NO_CONTENT, _Headers2, _Response2} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(
            WorkerP2, FilePath ++ "?value:4-4", put, RequestHeaders, RequestBody2
        ),
        ?ATTEMPTS
    ),
    % upload third chunk of file
    RequestHeaders3 = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER],
    RequestBody3 = json_utils:encode(#{<<"value">> => base64:encode(Chunk3)}),
    {ok, ?HTTP_204_NO_CONTENT, _Headers3, _Response3} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(
            WorkerP2, FilePath ++ "?value:5-9", put, RequestHeaders3, RequestBody3
        ),
        ?ATTEMPTS
    ),
    timer:sleep(2000),
    % get created file and check its consistency
    RequestHeaders4 = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER],
    % TODO Verify once after VFS-2023
    CheckAllChunks = fun() ->
        ?assertMatch(#{<<"completionStatus">> := <<"Complete">>},
            get_cdmi_response_from_request(FilePath, WorkerP1, RequestHeaders4),
            ?ATTEMPTS
        ),
        ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>},
            get_cdmi_response_from_request(FilePath, WorkerP1, RequestHeaders4),
            ?ATTEMPTS
        ),
        {ok, ?HTTP_200_OK, _Headers4, Response4} = ?assertMatch(
            {ok, 200, _, _},
            cdmi_test_utils:do_request(WorkerP2, FilePath, get, RequestHeaders4, []),
            ?ATTEMPTS
        ),
        CdmiResponse4 = json_utils:decode(Response4),
        maps:get(<<"value">>, CdmiResponse4)
    end,
    % File size event change is async
    Chunks123 = <<Chunk1/binary, Chunk2/binary, Chunk3/binary>>,
    ?assertMatch(Chunks123, CheckAllChunks(), ?ATTEMPTS).


partial_upload_noncdmi_test(Config) ->
    [WorkerP1, WorkerP2] = ?WORKERS,
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = filename:join(SpaceName, RootName) ++ "/",

    FilePath = filename:join([RootPath, "partial2.txt"]),
    Chunk1 = <<"some">>,
    Chunk2 = <<"_">>,
    Chunk3 = <<"value">>,
    %%----- non-cdmi request partial upload -------
    ?assertNot(cdmi_test_utils:object_exists(FilePath, Config)),

    % upload first chunk of file
    RequestHeaders = [cdmi_test_utils:user_2_token_header(), {<<"X-CDMI-Partial">>, <<"true">>}],
    {ok, ?HTTP_201_CREATED, _Headers, _Response} = ?assertMatch(
        {ok, 201, _, _},
        cdmi_test_utils:do_request(
            WorkerP2, FilePath, put, RequestHeaders, Chunk1
        ),
        ?ATTEMPTS
    ),
    RequestHeaders2 = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER],
    % check "completionStatus", should be set to "Processing"
    {ok, ?HTTP_200_OK, _Headers2, Response2} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_test_utils:do_request(
            WorkerP1, FilePath ++ "?completionStatus", get, RequestHeaders2, Chunk1
        ),
        ?ATTEMPTS
    ),
    CdmiResponse2 = json_utils:decode(Response2),
    ?assertMatch(#{<<"completionStatus">> := <<"Processing">>}, CdmiResponse2),

    % upload second chunk of file
    RequestHeaders3 = [
        cdmi_test_utils:user_2_token_header(),
        {?HDR_CONTENT_RANGE, <<"bytes 4-4/10">>}, {<<"X-CDMI-Partial">>, <<"true">>}
    ],
    {ok, ?HTTP_204_NO_CONTENT, _Headers3, _Response3} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(
            WorkerP2, FilePath, put, RequestHeaders3, Chunk2
        ),
        ?ATTEMPTS
    ),

    % upload third chunk of file
    RequestHeaders4 = [
        cdmi_test_utils:user_2_token_header(),
        {?HDR_CONTENT_RANGE, <<"bytes 5-9/10">>},
        {<<"X-CDMI-Partial">>, <<"false">>}
    ],
    {ok, ?HTTP_204_NO_CONTENT, _Headers4, _Response4} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(
            WorkerP2, FilePath, put, RequestHeaders4, Chunk3
        ),
        ?ATTEMPTS
    ),
    timer:sleep(5000),
    % get created file and check its consistency
    RequestHeaders5 = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER],
    % TODO Verify once after VFS-2023
    CheckAllChunks2 = fun() ->
        ?assertMatch(#{<<"completionStatus">> := <<"Complete">>},
            get_cdmi_response_from_request(FilePath, WorkerP2, RequestHeaders5), ?ATTEMPTS),
        {ok, ?HTTP_200_OK, _Headers5, Response5} = ?assertMatch(
            {ok, 200, _, _},
            cdmi_test_utils:do_request(WorkerP1, FilePath, get, RequestHeaders5, []),
            ?ATTEMPTS
        ),
        CdmiResponse5 = json_utils:decode(Response5),
        base64:decode(maps:get(<<"value">>, CdmiResponse5))
    end,
    % File size event change is async
    Chunks123 = <<Chunk1/binary, Chunk2/binary, Chunk3/binary>>,
    ?assertMatch(Chunks123, CheckAllChunks2(), ?ATTEMPTS).


accept_header_test(Config) ->
    AcceptHeader = {?HDR_ACCEPT, <<"*/*">>},
    % when
    {ok, _Code, _Headers, _Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS, [], get,
            [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, AcceptHeader], [])
    ).


download_empty_file_test(Config) ->
    [_WorkerP1, WorkerP2] = ?WORKERS,
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = filename:join(SpaceName, RootName) ++ "/",

    AuthHeaders = [rest_test_utils:user_token_header(oct_background:get_user_access_token(user2))],
    SessionId = oct_background:get_user_session_id(user2, Config#cdmi_test_config.p2_selector),
    UserId = oct_background:get_user_id(user2),

    % Create file
    FileName = <<"download_empty_file_test">>,
    FilePath = filename:join(["/", RootPath, FileName]),
    {ok, FileGuid} = cdmi_test_utils:create_new_file(binary_to_list(FilePath), Config),
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),

    ?assertMatch(ok, lfm_proxy:truncate(WorkerP2, SessionId, ?FILE_REF(FileGuid), 0), ?ATTEMPTS),

    {ok, _, _, Response} = ?assertMatch(
        {ok, 200, _Headers, _Response},
        cdmi_test_utils:do_request_base(
            WorkerP2, FilePath, get, [?CDMI_VERSION_HEADER | AuthHeaders], <<>>
        ),
        ?ATTEMPTS
    ),
    ?assertMatch(
        #{
            <<"completionStatus">> := <<"Complete">>,
            <<"metadata">> := #{
                <<"cdmi_owner">> := UserId,
                <<"cdmi_size">> := <<"0">>
            },
            <<"objectID">> := ObjectId,
            <<"objectName">> := FileName,
            <<"objectType">> := <<"application/cdmi-object">>,
            <<"value">> := <<>>,
            <<"valuerange">> := <<"0--1">>,
            <<"valuetransferencoding">> := <<"base64">>
        },
        json_utils:decode(Response)
    ).


get_metadata_from_request(Metadata, Workers, RequestHeaders1) ->
    {ok, ?HTTP_200_OK, _Headers, Response} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_test_utils:do_request(
            Workers, Metadata, get, RequestHeaders1, []
        ),
        ?ATTEMPTS
    ),
    CdmiResponse = json_utils:decode(Response),
    maps:get(<<"metadata">>, CdmiResponse).


get_cdmi_response_from_request(FileName, Workers, RequestHeaders1) ->
    {ok, ?HTTP_200_OK, _Headers, Response} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_test_utils:do_request(
            Workers, FileName, get, RequestHeaders1, []
        ),
        ?ATTEMPTS
    ),
    json_utils:decode(Response).
