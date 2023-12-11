%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% CDMI create tests
%%% @end
%%%-------------------------------------------------------------------
-module(cdmi_create_test_base).
-author("Katarzyna Such").

-include_lib("ctool/include/test/test_utils.hrl").
-include("cdmi_test.hrl").

%% API
-export([
    basic_create_file_test/1,
    base64_create_file_test/1,
    create_empty_file_test/1,
    create_noncdmi_file_test/1,
    create_raw_file_with_cdmi_version_header_should_succeed_test/1,
    create_cdmi_file_without_cdmi_version_header_should_fail_test/1,
    basic_create_dir_test/1,
    create_noncdmi_dir_and_update_test/1,
    missing_parent_create_dir_test/1,
    create_raw_dir_with_cdmi_version_header_should_succeed_test/1,
    create_cdmi_dir_without_cdmi_version_header_should_fail_test/1,

    create_file_with_metadata_test/1,
    create_and_update_dir_with_user_metadata_test/1,
    mimetype_and_encoding_create_file_test/1,
    mimetype_and_encoding_create_file_non_cdmi_request_test/1,
    wrong_create_path_error_test/1
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

% Tests file creation (cdmi object PUT), It can be done with cdmi header (when file data is provided as cdmi-object
% json string), or without (when we treat request body as new file content)
basic_create_file_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    ToCreate = ?build_test_root_path(Config),
    RootDirPath = list_to_binary("/" ++ RootPath),

    %%-------- basic create --------
    ?assert(not cdmi_internal:object_exists(ToCreate, Config)),

    RequestHeaders1 = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody1 = #{<<"value">> => ?FILE_CONTENT},
    RawRequestBody1 = json_utils:encode((RequestBody1)),
    {ok, Code1, _Headers1, Response1} = cdmi_internal:do_request(
        ?WORKERS, ToCreate, put, RequestHeaders1, RawRequestBody1
    ),

    ?assertEqual(?HTTP_201_CREATED, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    Metadata1 = maps:get(<<"metadata">>, CdmiResponse1),
    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-object">>}, CdmiResponse1),
    ?assertMatch(#{<<"objectName">> := <<"basic_create_file_test">>}, CdmiResponse1),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse1),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse1),
    ?assertNotEqual([], Metadata1),

    ?assert(cdmi_internal:object_exists(ToCreate, Config), ?ATTEMPTS),
    ?assertEqual(?FILE_CONTENT, cdmi_internal:get_file_content(ToCreate, Config), ?ATTEMPTS).
    %%------------------------------


base64_create_file_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    ToCreate2 = ?build_test_root_path(Config),
    RootDirPath = list_to_binary("/" ++ RootPath),

    %%------ base64 create ---------
    ?assert(not cdmi_internal:object_exists(ToCreate2, Config)),

    RequestHeaders2 = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody2 = #{<<"valuetransferencoding">> => <<"base64">>,
        <<"value">> => base64:encode(?FILE_CONTENT)},
    RawRequestBody2 = json_utils:encode((RequestBody2)),
    {ok, Code2, _Headers2, Response2} =
        cdmi_internal:do_request(?WORKERS, ToCreate2, put, RequestHeaders2, RawRequestBody2),
    ?assertEqual(?HTTP_201_CREATED, Code2),
    CdmiResponse2 = (json_utils:decode(Response2)),

    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-object">>}, CdmiResponse2),
    ?assertMatch(#{<<"objectName">> := <<"base64_create_file_test">>}, CdmiResponse2),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse2),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse2),
    ?assert(maps:get(<<"metadata">>, CdmiResponse2) =/= <<>>),

    ?assert(cdmi_internal:object_exists(ToCreate2, Config), ?ATTEMPTS),
    ?assertEqual(?FILE_CONTENT, cdmi_internal:get_file_content(ToCreate2, Config), ?ATTEMPTS).
    %%------------------------------


create_empty_file_test(Config) ->
    ToCreate3 = ?build_test_root_path(Config),
    %%------- create empty ---------
    ?assert(not cdmi_internal:object_exists(ToCreate3, Config), ?ATTEMPTS),

    RequestHeaders4 = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, Code4, _Headers4, _Response4} = cdmi_internal:do_request(
        ?WORKERS, ToCreate3, put, RequestHeaders4, []
    ),
    ?assertEqual(?HTTP_201_CREATED, Code4),

    ?assert(cdmi_internal:object_exists(ToCreate3, Config), ?ATTEMPTS),
    ?assertEqual(<<>>, cdmi_internal:get_file_content(ToCreate3, Config), ?ATTEMPTS).


create_noncdmi_file_test(Config) ->
    ToCreate4 = ?build_test_root_path(Config),
    %%------ create noncdmi --------
    ?assert(not cdmi_internal:object_exists(ToCreate4, Config)),

    RequestHeaders5 = [{?HDR_CONTENT_TYPE, <<"application/binary">>}],
    {ok, Code5, _Headers5, _Response5} =cdmi_internal:do_request(
        ?WORKERS, ToCreate4, put, [cdmi_test_utils:user_2_token_header() | RequestHeaders5], ?FILE_CONTENT
    ),

    ?assertEqual(?HTTP_201_CREATED, Code5),

    ?assert(cdmi_internal:object_exists(ToCreate4, Config), ?ATTEMPTS),
    ?assertEqual(?FILE_CONTENT, cdmi_internal:get_file_content(ToCreate4, Config), ?ATTEMPTS).


create_raw_file_with_cdmi_version_header_should_succeed_test(Config) ->
    % when
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders, _Response},
        cdmi_internal:do_request(
            ?WORKERS,
            ?build_test_root_path(Config),
            put,
            [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()], <<"data">>
        )
    ),
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders2, _Response2},
        cdmi_internal:do_request(
            ?WORKERS,
            ?build_test_root_specified_path(Config, atom_to_list(?FUNCTION_NAME) ++"1"),
            put,
            [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header(), {?HDR_CONTENT_TYPE, <<"text/plain">>}],
            <<"data2">>
        )
    ).


create_cdmi_file_without_cdmi_version_header_should_fail_test(Config) ->
    % when
    {ok, Code, _ResponseHeaders, Response} = cdmi_internal:do_request(
        ?WORKERS, ?build_test_root_path(Config), put,
        [cdmi_test_utils:user_2_token_header(), ?CDMI_OBJECT_CONTENT_TYPE_HEADER], <<"{}">>
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_MISSING_REQUIRED_VALUE(<<"version">>)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


basic_create_dir_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    DirName2 = ?build_test_root_path(Config) ++ "/",
    RootDirPath = list_to_binary("/" ++ RootPath),
    %%------ basic create ----------
    ?assert(not cdmi_internal:object_exists(DirName2, Config)),

    RequestHeaders2 = [
        cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code2, _Headers2, Response2} = cdmi_internal:do_request(
        ?WORKERS, DirName2, put, RequestHeaders2, []
    ),

    ?assertEqual(?HTTP_201_CREATED, Code2),
    CdmiResponse2 = json_utils:decode(Response2),

    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-container">>}, CdmiResponse2),
    ?assertMatch(#{<<"objectName">> := <<"basic_create_dir_test/">>}, CdmiResponse2),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse2),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse2),
    ?assertMatch(#{<<"children">> := []}, CdmiResponse2),
    ?assert(maps:get(<<"metadata">>, CdmiResponse2) =/= <<>>),

    ?assert(cdmi_internal:object_exists(DirName2, Config), ?ATTEMPTS).


create_noncdmi_dir_and_update_test(Config) ->
    DirName = ?build_test_root_path(Config) ++ "/",
    %%------ non-cdmi create -------
    ?assert(not cdmi_internal:object_exists(DirName, Config)),

    {ok, Code1, _Headers1, _Response1} =
        cdmi_internal:do_request(?WORKERS, DirName, put, [cdmi_test_utils:user_2_token_header()]),
    ?assertEqual(?HTTP_201_CREATED, Code1),

    ?assert(cdmi_internal:object_exists(DirName, Config), ?ATTEMPTS),

    %%---------- update ------------

    RequestHeaders3 = [
        cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, ?HTTP_204_NO_CONTENT, _Headers3, _Response3} =  ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            ?WORKERS, DirName, put, RequestHeaders3, []
        ),
        ?ATTEMPTS
    ),

    ?assert(cdmi_internal:object_exists(DirName, Config)).


missing_parent_create_dir_test(Config) ->
    MissingParentName = ?build_test_root_specified_path(
        Config, atom_to_list(?FUNCTION_NAME) ++"unknown"
    ) ++ "/",
    DirWithoutParentName = filename:join(MissingParentName, "dir") ++ "/",
    %%----- missing parent ---------
    ?assert(not cdmi_internal:object_exists(MissingParentName, Config)),

    RequestHeaders4 = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code4, _Headers4, Response4} = cdmi_internal:do_request(
        ?WORKERS, DirWithoutParentName, put, RequestHeaders4, []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?ENOENT)),
    ?assertMatch(ExpRestError, {Code4, json_utils:decode(Response4)}).


create_raw_dir_with_cdmi_version_header_should_succeed_test(Config) ->
    % when
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders, _Response},
        cdmi_internal:do_request(
            ?WORKERS,
            ?build_test_root_path(Config) ++ "/",
            put,
            [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()]
        )),
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders2, _Response2},
        cdmi_internal:do_request(
            ?WORKERS,
            ?build_test_root_specified_path(Config, atom_to_list(?FUNCTION_NAME) ++"1"),
            put,
            [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header(), {?HDR_CONTENT_TYPE, <<"application/json">>}],
            <<"{}">>
        )).


create_cdmi_dir_without_cdmi_version_header_should_fail_test(Config) ->
    % when
    {ok, Code, _ResponseHeaders, Response} = cdmi_internal:do_request(
        ?WORKERS, ?build_test_root_path(Config) ++ "/", put,
        [cdmi_test_utils:user_2_token_header(), ?CDMI_CONTAINER_CONTENT_TYPE_HEADER]
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_MISSING_REQUIRED_VALUE(<<"version">>)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


create_file_with_metadata_test(Config) ->
    UserId2 = oct_background:get_user_id(user2),
    FileName = ?build_test_root_path(Config),
    %%-------- create file with user metadata --------
    ?assert(not cdmi_internal:object_exists(FileName, Config)),
    RequestHeaders1 = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody1 = #{
        <<"value">> => ?FILE_CONTENT,
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"text/plain">>,
        <<"metadata">> => #{<<"my_metadata">> => <<"my_value">>,
            <<"cdmi_not_allowed">> => <<"my_value">>}},

    RawRequestBody1 = json_utils:encode(RequestBody1),
    Before = time:seconds_to_datetime(global_clock:timestamp_seconds()),
    {ok, Code1, _Headers1, Response1} = cdmi_internal:do_request(
        ?WORKERS, FileName, put, RequestHeaders1, RawRequestBody1
    ),
    After = time:seconds_to_datetime(global_clock:timestamp_seconds()),

    CdmiResponse1 = (json_utils:decode(Response1)),
    Metadata = maps:get(<<"metadata">>, CdmiResponse1),
    Metadata1 = Metadata,
    CTime1 = time:iso8601_to_datetime(maps:get(<<"cdmi_ctime">>, Metadata1)),
    ATime1 = time:iso8601_to_datetime(maps:get(<<"cdmi_atime">>, Metadata1)),
    MTime1 = time:iso8601_to_datetime(maps:get(<<"cdmi_mtime">>, Metadata1)),

    ?assertEqual(?HTTP_201_CREATED, Code1),
    ?assertMatch(#{<<"cdmi_size">> := <<"13">>}, Metadata1),

    ?assert(Before =< ATime1),
    ?assert(Before =< MTime1),
    ?assert(Before =< CTime1),
    ?assert(ATime1 =< After),
    ?assert(MTime1 =< After),
    ?assert(CTime1 =< After),
    ?assertMatch(UserId2, maps:get(<<"cdmi_owner">>, Metadata1)),
    ?assertMatch(#{<<"my_metadata">> := <<"my_value">>}, Metadata1),
    ?assertEqual(6, maps:size(Metadata1)).


create_and_update_dir_with_user_metadata_test(Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    RequestHeaders1 = [?CDMI_OBJECT_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    DirName = ?build_test_root_path(Config) ++ "/",

    %%------ create directory with user metadata  ----------
    RequestHeaders2 = [?CDMI_CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody11 = #{<<"metadata">> => #{<<"my_metadata">> => <<"my_dir_value">>}},
    RawRequestBody11 = json_utils:encode(RequestBody11),
    {ok, ?HTTP_201_CREATED, _Headers11, Response11} = cdmi_internal:do_request(
        WorkerP1, DirName, put, RequestHeaders2, RawRequestBody11
    ),
    CdmiResponse11 = (json_utils:decode(Response11)),
    Metadata11 = maps:get(<<"metadata">>, CdmiResponse11),
    ?assertMatch(#{<<"my_metadata">> := <<"my_dir_value">>}, Metadata11),

    %%------ update user metadata of a directory ----------
    RequestBody12 = #{<<"metadata">> => #{<<"my_metadata">> => <<"my_dir_value_update">>}},
    RawRequestBody12 = json_utils:encode(RequestBody12),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(WorkerP1, DirName, put, RequestHeaders2, RawRequestBody12),
        ?ATTEMPTS
    ),
    {ok, ?HTTP_200_OK, _Headers13, Response13} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_internal:do_request(WorkerP1, DirName ++ "?metadata:my", get, RequestHeaders1, []),
        ?ATTEMPTS
    ),
    CdmiResponse13 = (json_utils:decode(Response13)),
    Metadata13 = maps:get(<<"metadata">>, CdmiResponse13),

    ?assertEqual(1, maps:size(CdmiResponse13)),
    ?assertMatch(#{<<"my_metadata">> := <<"my_dir_value_update">>}, Metadata13),
    ?assertEqual(1, maps:size(Metadata13)).


mimetype_and_encoding_create_file_test(Config) ->
    [WorkerP1, WorkerP2] = ?WORKERS,
    FileName4 = ?build_test_root_path(Config),
    RequestHeaders4 = [?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER, cdmi_test_utils:user_2_token_header()],
    RawBody4 = json_utils:encode(#{
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"text/plain">>,
        <<"value">> => ?FILE_CONTENT
    }),
    {ok, Code4, _Headers4, Response4} = cdmi_internal:do_request(
        WorkerP1, FileName4, put, RequestHeaders4, RawBody4
    ),
    ?assertEqual(?HTTP_201_CREATED, Code4),
    CdmiResponse4 = (json_utils:decode(Response4)),
    ?assertMatch(#{<<"mimetype">> := <<"text/plain">>}, CdmiResponse4),

    RequestHeaders5 = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, ?HTTP_200_OK, _Headers5, Response5} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_internal:do_request(
            WorkerP2, FileName4 ++ "?value;mimetype;valuetransferencoding", get, RequestHeaders5, []
        ),
        ?ATTEMPTS
    ),
    CdmiResponse5 = (json_utils:decode(Response5)),
    ?assertMatch(#{<<"mimetype">> := <<"text/plain">>}, CdmiResponse5),

    %TODO VFS-7376 what do we return here if file contains valid utf-8 string and we read byte range?
    ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>}, CdmiResponse5),
    ?assertMatch(#{<<"value">> := ?FILE_CONTENT}, CdmiResponse5).


mimetype_and_encoding_create_file_non_cdmi_request_test(Config) ->
    %% create file with given mime and encoding using non-cdmi request
    FileName6 = ?build_test_root_path(Config),
    RequestHeaders6 = [{?HDR_CONTENT_TYPE, <<"text/plain; charset=utf-8">>}, cdmi_test_utils:user_2_token_header()],
    {ok, ?HTTP_201_CREATED, _Headers6, _Response6} = ?assertMatch(
        {ok, 201, _, _},
        cdmi_internal:do_request(
            ?WORKERS, FileName6, put, RequestHeaders6, ?FILE_CONTENT
        ),
        ?ATTEMPTS
    ),

    RequestHeaders7 = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, ?HTTP_200_OK, _Headers7, Response7} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_internal:do_request(
            ?WORKERS, FileName6 ++ "?value;mimetype;valuetransferencoding", get, RequestHeaders7, []
        ),
        ?ATTEMPTS
    ),
    CdmiResponse7 = (json_utils:decode(Response7)),
    ?assertMatch(#{<<"mimetype">> := <<"text/plain">>}, CdmiResponse7),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>}, CdmiResponse7),
    ?assertMatch(#{<<"value">> := ?FILE_CONTENT}, CdmiResponse7).


wrong_create_path_error_test(Config) ->
    %%----- wrong create path ------
    RequestHeaders2 = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code2, _Headers2, Response2} =
        cdmi_internal:do_request(?WORKERS, ?build_test_root_path(Config), put, RequestHeaders2, []),
    ExpRestError2 = rest_test_utils:get_rest_error(?ERROR_BAD_VALUE_IDENTIFIER(<<"path">>)),
    ?assertMatch(ExpRestError2, {Code2, json_utils:decode(Response2)}),
    %%------------------------------

    %%---- wrong create path 2 -----
    RequestHeaders3 = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_OBJECT_CONTENT_TYPE_HEADER
    ],
    {ok, Code3, _Headers3, Response3} =
        cdmi_internal:do_request(?WORKERS, ?build_test_root_path(Config) ++ "/", put, RequestHeaders3, []),
    ExpRestError3 = rest_test_utils:get_rest_error(?ERROR_BAD_VALUE_IDENTIFIER(<<"path">>)),
    ?assertMatch(ExpRestError3, {Code3, json_utils:decode(Response3)}).