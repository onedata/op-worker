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

-include("cdmi_test.hrl").
-include("onenv_test_utils.hrl").

-include_lib("ctool/include/test/test_utils.hrl").

%% Tests
-export([
    selective_metadata_read_test/1,
    update_user_metadata_file_test/1,
    unauthorized_access_error_test/1,
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
    accept_header_test/1
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
        cdmi_test_utils:do_request(?WORKERS(Config), TestDirName, get, [], []),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_UNAUTHORIZED),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


open_binary_file_without_permission_test(Config) ->
    [WorkerP1, _WorkerP2] = ?WORKERS(Config),
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
        {ok, ?HTTP_400_BAD_REQUEST, _, _},
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
        {ok, ?HTTP_400_BAD_REQUEST, _, _},
        cdmi_test_utils:do_request(
            ?WORKERS(Config), FilePath, get, RequestHeaders
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
        ?WORKERS(Config), FilePath, put, RequestHeaders, RawRequestBody
    ),

    %%-- selective metadata read -----
    {ok, ?HTTP_200_OK, _Headers2, Response2} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _}, cdmi_test_utils:do_request(
            ?WORKERS(Config), FilePath ++ "?metadata", get, RequestHeaders, []
    ), ?ATTEMPTS),
    CdmiResponse2 = json_utils:decode(Response2),
    Metadata2 = maps:get(<<"metadata">>, CdmiResponse2),
    ?assertEqual(1, maps:size(CdmiResponse2)),
    ?assertEqual(6, maps:size(Metadata2)),

    %%-- selective metadata read with prefix -----
    {ok, ?HTTP_200_OK, _Headers3, Response3} = cdmi_test_utils:do_request(
        ?WORKERS(Config), FilePath ++ "?metadata:cdmi_", get, RequestHeaders, []
    ),
    CdmiResponse3 = json_utils:decode(Response3),
    Metadata3 = maps:get(<<"metadata">>, CdmiResponse3),
    ?assertEqual(1, maps:size(CdmiResponse3)),
    ?assertEqual(5, maps:size(Metadata3)),

    {ok, ?HTTP_200_OK, _Headers4, Response4} = cdmi_test_utils:do_request(
        ?WORKERS(Config), FilePath ++ "?metadata:cdmi_o", get, RequestHeaders, []
    ),
    CdmiResponse4 = json_utils:decode(Response4),
    Metadata4 = maps:get(<<"metadata">>, CdmiResponse4),
    ?assertEqual(1, maps:size(CdmiResponse4)),
    ?assertMatch(UserId, maps:get(<<"cdmi_owner">>, Metadata4)),
    ?assertEqual(1, maps:size(Metadata4)),

    {ok, ?HTTP_200_OK, _Headers5, Response5} = cdmi_test_utils:do_request(
        ?WORKERS(Config), FilePath ++ "?metadata:cdmi_size", get, RequestHeaders, []
    ),
    CdmiResponse5 = json_utils:decode(Response5),
    Metadata5 = maps:get(<<"metadata">>, CdmiResponse5),
    ?assertEqual(1, maps:size(CdmiResponse5)),
    ?assertMatch(#{<<"cdmi_size">> := <<"13">>}, Metadata5),
    ?assertEqual(1, maps:size(Metadata5)),

    {ok, ?HTTP_200_OK, _Headers6, Response6} = cdmi_test_utils:do_request(
        ?WORKERS(Config), FilePath ++ "?metadata:cdmi_no_such_metadata", get, RequestHeaders, []
    ),
    CdmiResponse6 = json_utils:decode(Response6),
    ?assertEqual(1, maps:size(CdmiResponse6)),
    ?assertMatch(#{<<"metadata">> := #{}}, CdmiResponse6).


update_user_metadata_file_test(Config) ->
    [WorkerP1, WorkerP2] = ?WORKERS(Config),
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
        cdmi_test_utils:do_request(?WORKERS(Config), FilePath, put, RequestHeaders, RawRequestBody),
        ?ATTEMPTS
    ),
    RequestBody2 = #{<<"metadata">> => #{<<"my_new_metadata">> => <<"my_new_value">>}},
    RawRequestBody2 = json_utils:encode(RequestBody2),
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP1, FilePath, put, RequestHeaders, RawRequestBody2
    ), ?ATTEMPTS),
    ?assertEqual(1, maps:size(get_metadata_from_request(FilePath ++ "?metadata:my",
        WorkerP2, RequestHeaders)), ?ATTEMPTS),
    ?assertMatch(
        #{<<"my_new_metadata">> := <<"my_new_value">>},
        get_metadata_from_request(FilePath ++ "?metadata:my", ?WORKERS(Config), RequestHeaders),
        ?ATTEMPTS
    ),

    {ok, ?HTTP_200_OK, _Headers2, Response2} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
            ?WORKERS(Config), FilePath ++ "?metadata:my", get, RequestHeaders, []
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
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP1,
        FilePath ++ "?metadata:my_new_metadata_add;metadata:my_new_metadata;metadata:cdmi_not_allowed",
        put,
        RequestHeaders,
        RawRequestBody3
    ), ?ATTEMPTS),

    {ok, ?HTTP_200_OK, _Headers3, Response3} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
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
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(WorkerP1, FilePath ++ "?metadata:cdmi_", get, RequestHeaders, []),
        ?ATTEMPTS
    ),
    CdmiResponse4 = json_utils:decode(Response4),
    Metadata4 = maps:get(<<"metadata">>, CdmiResponse4),
    ?assertEqual(1, maps:size(CdmiResponse4)),
    ?assertEqual(5, maps:size(Metadata4)),

    RequestBody5 = #{<<"metadata">> => #{<<"my_new_metadata">> => <<"my_new_value_ignore">>}},
    RawRequestBody5 = json_utils:encode(RequestBody5),
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        ?WORKERS(Config), FilePath ++ "?metadata:my_new_metadata_add", put, RequestHeaders, RawRequestBody5
    ),?ATTEMPTS),

    {ok, ?HTTP_200_OK, _Headers6, Response6} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
            ?WORKERS(Config), FilePath ++ "?metadata:my", get, RequestHeaders, []
        ),
        ?ATTEMPTS
    ),
    CdmiResponse6 = json_utils:decode(Response6),
    Metadata6 = maps:get(<<"metadata">>, CdmiResponse6),

    ?assertEqual(1, maps:size(CdmiResponse6)),
    ?assertMatch(#{<<"my_new_metadata">> := <<"my_new_value_update">>}, Metadata6).


% Tests cdmi object DELETE requests
delete_file_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),

    FilePath = filename:join([RootPath, "toDelete.txt"]),
    GroupFilePath =
        filename:join([RootPath, "groupFile"]),

    %%----- basic delete -----------
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{name = <<"toDelete.txt">>},
    Config#cdmi_test_config.p1_selector),

    RequestHeaders = [?CDMI_VERSION_HEADER],
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        ?WORKERS(Config), FilePath, delete, [cdmi_test_utils:user_2_token_header() | RequestHeaders]
    )),
    ?assertNot(cdmi_test_utils:object_exists(FilePath, Config), ?ATTEMPTS),

    %%----- delete group file ------
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{name = <<"groupFile">>},
    Config#cdmi_test_config.p1_selector),

    RequestHeaders2 = [?CDMI_VERSION_HEADER],
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        ?WORKERS(Config), GroupFilePath, delete, [cdmi_test_utils:user_2_token_header() | RequestHeaders2])
    ),
    ?assertNot(cdmi_test_utils:object_exists(GroupFilePath, Config), ?ATTEMPTS).


% Tests cdmi container DELETE requests
delete_dir_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),

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
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        ?WORKERS(Config), DirPath, delete, RequestHeaders, [])
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
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        ?WORKERS(Config), DirPath, delete, RequestHeaders2, []
    )),
    ?assertNot(cdmi_test_utils:object_exists(DirPath, Config), ?ATTEMPTS),
    ?assertNot(cdmi_test_utils:object_exists(ChildDirPath, Config)),

    %%----- delete root dir -------
    RequestHeaders3 = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER],
    ?assert(cdmi_test_utils:object_exists("/", Config)),

    {ok, Code3, _Headers3, Response3} = cdmi_test_utils:do_request(
        ?WORKERS(Config), "/", delete, RequestHeaders3, []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EPERM)),
    ?assertMatch(ExpRestError, {Code3, json_utils:decode(Response3)}),
    ?assert(cdmi_test_utils:object_exists("/", Config)).


% Tests cdmi object PUT requests (updating content)
update_file_cdmi_test(Config) ->
    [WorkerP1, _WorkerP2] = ?WORKERS(Config),
    FilePath = cdmi_test_utils:build_test_root_path(Config, filename:join(?FUNCTION_NAME, "1")),

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

    ?assertMatch( {ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP1, FilePath, put, RequestHeaders, RawRequestBody
    ),?ATTEMPTS),
    ?assert(cdmi_test_utils:object_exists(FilePath, Config)),
    ?assertEqual(NewValue, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    %%---- value update, cdmi ------
    UpdateValue = <<"123">>,
    RequestHeaders2 = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody2 = #{<<"value">> => base64:encode(UpdateValue)},
    RawRequestBody2 = json_utils:encode(RequestBody2),
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP1, FilePath ++ "?value:0-2", put, RequestHeaders2, RawRequestBody2
    )),
    ?assert(cdmi_test_utils:object_exists(FilePath, Config)),
    ?assertEqual(UpdatedValue, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS).


update_file_http_test(Config) ->
    [WorkerP1, _WorkerP2] = ?WORKERS(Config),
    FilePath = cdmi_test_utils:build_test_root_path(Config, filename:join(?FUNCTION_NAME, "1")),

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
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP1, FilePath, put, [cdmi_test_utils:user_2_token_header()], RequestBody
    ),?ATTEMPTS),
    ?assert(cdmi_test_utils:object_exists(FilePath, Config)),
    ?assertEqual(?FILE_CONTENT,
        cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    %%---- value update, http ------
    UpdateValue = <<"123">>,
    RequestHeaders = [{?HDR_CONTENT_RANGE, <<"bytes 0-2/3">>}],
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP1, FilePath, put, [cdmi_test_utils:user_2_token_header() | RequestHeaders], UpdateValue
    ), ?ATTEMPTS),
    ?assert(cdmi_test_utils:object_exists(FilePath, Config)),
    ?assertEqual(<<"123e content!">>,
        cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    %%---- value update2, http -----
    UpdateValue2 = <<"00">>,
    RequestHeaders2 = [{?HDR_CONTENT_RANGE, <<"bytes 3-4/*">>}],
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP1, FilePath, put, [cdmi_test_utils:user_2_token_header() | RequestHeaders2], UpdateValue2
    ),?ATTEMPTS),
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
        cdmi_test_utils:do_request(?WORKERS(Config), "/random", get, RequestHeaders)
    ).


use_unsupported_cdmi_version_test(Config) ->
    RequestHeaders = [{<<"X-CDMI-Specification-Version">>, <<"1.0.2">>}],
    {ok, Code, _ResponseHeaders, Response} =
        cdmi_test_utils:do_request(?WORKERS(Config), "/random", get, RequestHeaders),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>])),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


% tests req format checking
request_format_check_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),

    FilePath = filename:join([RootPath, "file.txt"]),
    DirPath = filename:join([RootPath, "dir"]) ++ "/",

    %%-- obj missing content-type --
    RequestHeaders = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody = #{<<"value">> => ?FILE_CONTENT},
    RawRequestBody = json_utils:encode(RequestBody),
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS(Config), FilePath, put, RequestHeaders, RawRequestBody
    )),

    %%-- dir missing content-type --
    RequestHeaders2 = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody2 = #{<<"metadata">> => <<"">>},
    RawRequestBody2 = json_utils:encode(RequestBody2),
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, cdmi_test_utils:do_request(
        ?WORKERS(Config), DirPath, put, RequestHeaders2, RawRequestBody2
    )).


% tests mimetype and valuetransferencoding properties, they are part of cdmi-object and cdmi-container
% and should be changeble
mimetype_and_encoding_noncdmi_file_test(Config) ->
    FilePath = cdmi_test_utils:build_test_root_path(
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
        cdmi_test_utils:do_request( ?WORKERS(Config), FilePath ++ "?mimetype;valuetransferencoding",
        get, RequestHeaders, [])
    ),
    CdmiResponse = json_utils:decode(Response),
    ?assertMatch(#{<<"mimetype">> := <<"application/octet-stream">>}, CdmiResponse),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"base64">>}, CdmiResponse).


update_mimetype_and_encoding_test(Config) ->
    [WorkerP1, _WorkerP2] = ?WORKERS(Config),
    FilePath = cdmi_test_utils:build_test_root_path(
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
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
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
    DirPath = ?build_test_root_path(Config),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{name = atom_to_binary(?FUNCTION_NAME)},
    Config#cdmi_test_config.p1_selector),

    RootPath = cdmi_test_utils:get_tests_root_path(Config),

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
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        Workers, FilePath ++ "?value:0-3", put, RequestHeaders2, RequestBody2
    )),
    ?assertEqual(<<"data">>, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    %%------ writing at random -------- (should return zero bytes in any gaps)
     RequestBody3 = json_utils:encode(#{<<"value">> => base64:encode(<<"data">>)}),
     ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
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


accept_header_test(Config) ->
    AcceptHeader = {?HDR_ACCEPT, <<"*/*">>},
    % when
    ?assertMatch({ok, ?HTTP_200_OK, _, _}, cdmi_test_utils:do_request(?WORKERS(Config), [], get,
        [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, AcceptHeader], [])
    ).


get_metadata_from_request(Metadata, Workers, RequestHeaders1) ->
    {ok, ?HTTP_200_OK, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
            Workers, Metadata, get, RequestHeaders1, []
        ),
        ?ATTEMPTS
    ),
    CdmiResponse = json_utils:decode(Response),
    maps:get(<<"metadata">>, CdmiResponse).
