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

-include("cdmi_test.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% Tests
-export([
    basic_create_file_test/1,
    base64_create_file_test/1,
    create_empty_file_test/1,
    create_noncdmi_file_test/1,
    create_noncdmi_file_version_header_test/1,
    create_cdmi_file_version_header_test/1,
    basic_create_dir_test/1,
    create_noncdmi_dir_and_update_test/1,
    missing_parent_create_dir_test/1,
    create_noncdmi_dir_version_header_test/1,
    create_cdmi_dir_version_header_failure_test/1,

    create_file_with_metadata_test/1,
    create_and_update_dir_with_user_metadata_test/1,
    mimetype_and_encoding_create_file_test/1,
    mimetype_and_encoding_create_file_noncdmi_request_test/1,
    wrong_create_path_error_test/1,
    wrong_base_error_test/1,

    partial_upload_cdmi_test/1,
    partial_upload_noncdmi_test/1
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

    ?assertNot(cdmi_test_utils:object_exists(ToCreate, Config)),

    RequestHeaders = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RawRequestBody = json_utils:encode(#{<<"value">> => ?FILE_CONTENT}),
    {ok, _Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS(Config), ToCreate, put, RequestHeaders, RawRequestBody
    )),

    CdmiResponse = json_utils:decode(Response),
    Metadata = maps:get(<<"metadata">>, CdmiResponse),
    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-object">>}, CdmiResponse),
    ?assertMatch(#{<<"objectName">> := <<"basic_create_file_test">>}, CdmiResponse),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse),
    ?assertNotEqual([], Metadata),

    ?assert(cdmi_test_utils:object_exists(ToCreate, Config), ?ATTEMPTS),
    ?assertEqual(?FILE_CONTENT, cdmi_test_utils:get_file_content(ToCreate, Config), ?ATTEMPTS).


base64_create_file_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    ToCreate = ?build_test_root_path(Config),
    RootDirPath = list_to_binary("/" ++ RootPath),

    ?assertNot(cdmi_test_utils:object_exists(ToCreate, Config)),

    RequestHeaders = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody = #{<<"valuetransferencoding">> => <<"base64">>,
        <<"value">> => base64:encode(?FILE_CONTENT)},
    RawRequestBody2 = json_utils:encode((RequestBody)),
    {ok, _Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), ToCreate, put, RequestHeaders, RawRequestBody2)
    ),
    CdmiResponse = json_utils:decode(Response),

    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-object">>}, CdmiResponse),
    ?assertMatch(#{<<"objectName">> := <<"base64_create_file_test">>}, CdmiResponse),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse),
    ?assert(maps:get(<<"metadata">>, CdmiResponse) =/= <<>>),

    ?assert(cdmi_test_utils:object_exists(ToCreate, Config), ?ATTEMPTS),
    ?assertEqual(?FILE_CONTENT, cdmi_test_utils:get_file_content(ToCreate, Config), ?ATTEMPTS).


create_empty_file_test(Config) ->
    ToCreate = ?build_test_root_path(Config),
    ?assertNot(cdmi_test_utils:object_exists(ToCreate, Config), ?ATTEMPTS),

    RequestHeaders = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), ToCreate, put, RequestHeaders, []
    )),

    ?assert(cdmi_test_utils:object_exists(ToCreate, Config), ?ATTEMPTS),
    ?assertEqual(<<>>, cdmi_test_utils:get_file_content(ToCreate, Config), ?ATTEMPTS).


create_noncdmi_file_test(Config) ->
    ToCreate = ?build_test_root_path(Config),
    ?assertNot(cdmi_test_utils:object_exists(ToCreate, Config)),

    RequestHeaders = [cdmi_test_utils:user_2_token_header(), {?HDR_CONTENT_TYPE, <<"application/binary">>}],
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, cdmi_test_utils:do_request(
        ?WORKERS(Config), ToCreate, put, RequestHeaders, ?FILE_CONTENT
    )),

    ?assert(cdmi_test_utils:object_exists(ToCreate, Config), ?ATTEMPTS),
    ?assertEqual(?FILE_CONTENT, cdmi_test_utils:get_file_content(ToCreate, Config), ?ATTEMPTS).


create_noncdmi_file_version_header_test(Config) ->
    % when
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders, _Response},
        cdmi_test_utils:do_request(
            ?WORKERS(Config),
            ?build_test_root_path(Config),
            put,
            [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()], <<"data">>
        )
    ),
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders2, _Response2},
        cdmi_test_utils:do_request(
            ?WORKERS(Config),
            cdmi_test_utils:build_test_root_path(Config, atom_to_list(?FUNCTION_NAME) ++"1"),
            put,
            [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header(), {?HDR_CONTENT_TYPE, <<"text/plain">>}],
            <<"data2">>
        )
    ).


create_cdmi_file_version_header_test(Config) ->
    % when
    {ok, Code, _ResponseHeaders, Response} = cdmi_test_utils:do_request(
        ?WORKERS(Config), ?build_test_root_path(Config), put,
        [cdmi_test_utils:user_2_token_header(), ?CDMI_OBJECT_CONTENT_TYPE_HEADER], <<"{}">>
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_MISSING_REQUIRED_VALUE(<<"version">>)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


basic_create_dir_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    DirPath = ?build_test_root_path(Config) ++ "/",
    RootDirPath = list_to_binary("/" ++ RootPath),
    ?assertNot(cdmi_test_utils:object_exists(DirPath, Config)),

    RequestHeaders = [
        cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, _Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), DirPath, put, RequestHeaders, [])
    ),

    CdmiResponse = json_utils:decode(Response),

    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-container">>}, CdmiResponse),
    ?assertMatch(#{<<"objectName">> := <<"basic_create_dir_test/">>}, CdmiResponse),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse),
    ?assertMatch(#{<<"children">> := []}, CdmiResponse),
    ?assert(maps:get(<<"metadata">>, CdmiResponse) =/= <<>>),

    ?assert(cdmi_test_utils:object_exists(DirPath, Config), ?ATTEMPTS).


create_noncdmi_dir_and_update_test(Config) ->
    DirPath = ?build_test_root_path(Config) ++ "/",
    ?assertNot(cdmi_test_utils:object_exists(DirPath, Config)),

    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), DirPath, put, [cdmi_test_utils:user_2_token_header()])
    ),

    ?assert(cdmi_test_utils:object_exists(DirPath, Config), ?ATTEMPTS),

    RequestHeaders2 = [
        cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        ?WORKERS(Config), DirPath, put, RequestHeaders2, []
    ), ?ATTEMPTS),

    ?assert(cdmi_test_utils:object_exists(DirPath, Config)).


missing_parent_create_dir_test(Config) ->
    MissingParentName = cdmi_test_utils:build_test_root_path(
        Config, atom_to_list(?FUNCTION_NAME) ++"unknown"
    ) ++ "/",
    DirWithoutParentName = filename:join(MissingParentName, "dir") ++ "/",
    ?assertNot(cdmi_test_utils:object_exists(MissingParentName, Config)),

    RequestHeaders = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code, _Headers, Response} = cdmi_test_utils:do_request(
        ?WORKERS(Config), DirWithoutParentName, put, RequestHeaders, []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?ENOENT)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


create_noncdmi_dir_version_header_test(Config) ->
    % when
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders, _Response},
        cdmi_test_utils:do_request(
            ?WORKERS(Config),
            ?build_test_root_path(Config) ++ "/",
            put,
            [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()]
        )),
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders2, _Response2},
        cdmi_test_utils:do_request(
            ?WORKERS(Config),
            cdmi_test_utils:build_test_root_path(Config, atom_to_list(?FUNCTION_NAME) ++"1"),
            put,
            [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header(), {?HDR_CONTENT_TYPE, <<"application/json">>}],
            <<"{}">>
        )).


create_cdmi_dir_version_header_failure_test(Config) ->
    % when
    {ok, Code, _ResponseHeaders, Response} = cdmi_test_utils:do_request(
        ?WORKERS(Config), ?build_test_root_path(Config) ++ "/", put,
        [cdmi_test_utils:user_2_token_header(), ?CDMI_CONTAINER_CONTENT_TYPE_HEADER]
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_MISSING_REQUIRED_VALUE(<<"version">>)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


create_file_with_metadata_test(Config) ->
    UserId = oct_background:get_user_id(user2),
    FilePath = ?build_test_root_path(Config),

    ?assertNot(cdmi_test_utils:object_exists(FilePath, Config)),
    RequestHeaders = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody = #{
        <<"value">> => ?FILE_CONTENT,
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"text/plain">>,
        <<"metadata">> => #{<<"my_metadata">> => <<"my_value">>,
            <<"cdmi_not_allowed">> => <<"my_value">>}},

    RawRequestBody = json_utils:encode(RequestBody),
    Before = time:seconds_to_datetime(global_clock:timestamp_seconds()),
    {ok, _Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS(Config), FilePath, put, RequestHeaders, RawRequestBody
    )),
    After = time:seconds_to_datetime(global_clock:timestamp_seconds()),

    CdmiResponse = json_utils:decode(Response),
    Metadata = maps:get(<<"metadata">>, CdmiResponse),
    CTime = time:iso8601_to_datetime(maps:get(<<"cdmi_ctime">>, Metadata)),
    ATime = time:iso8601_to_datetime(maps:get(<<"cdmi_atime">>, Metadata)),
    MTime = time:iso8601_to_datetime(maps:get(<<"cdmi_mtime">>, Metadata)),

    ?assertMatch(#{<<"cdmi_size">> := <<"13">>}, Metadata),

    ?assert(Before =< ATime),
    ?assert(Before =< MTime),
    ?assert(Before =< CTime),
    ?assert(ATime =< After),
    ?assert(MTime =< After),
    ?assert(CTime =< After),
    ?assertMatch(UserId, maps:get(<<"cdmi_owner">>, Metadata)),
    ?assertMatch(#{<<"my_metadata">> := <<"my_value">>}, Metadata),
    ?assertEqual(6, maps:size(Metadata)).


create_and_update_dir_with_user_metadata_test(Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    RequestHeaders1 = [?CDMI_OBJECT_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    DirPath = ?build_test_root_path(Config) ++ "/",

    RequestHeaders2 = [?CDMI_CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody = #{<<"metadata">> => #{<<"my_metadata">> => <<"my_dir_value">>}},
    RawRequestBody = json_utils:encode(RequestBody),
    {ok, ?HTTP_201_CREATED, _Headers, Response} = cdmi_test_utils:do_request(
        WorkerP1, DirPath, put, RequestHeaders2, RawRequestBody
    ),
    CdmiResponse = json_utils:decode(Response),
    ?assertMatch(#{<<"my_metadata">> := <<"my_dir_value">>},
        maps:get(<<"metadata">>, CdmiResponse)),

    %%------ update user metadata of a directory ----------
    NewMetadata2 = #{<<"my_metadata">> => <<"my_dir_value_update">>},
    RequestBody2 = #{<<"metadata">> => NewMetadata2},
    RawRequestBody2 = json_utils:encode(RequestBody2),
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP1, DirPath, put, RequestHeaders2, RawRequestBody2
    ),?ATTEMPTS),
    {ok, ?HTTP_200_OK, _Headers2, Response2} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(WorkerP1, DirPath ++ "?metadata:my", get, RequestHeaders1, []),
        ?ATTEMPTS
    ),
    CdmiResponse2 = json_utils:decode(Response2),
    ActualMetadata = maps:get(<<"metadata">>, CdmiResponse2),
    ?assertEqual(1, maps:size(CdmiResponse2)),
    ?assertMatch(NewMetadata2, ActualMetadata),
    ?assertEqual(1, maps:size(ActualMetadata)).


mimetype_and_encoding_create_file_test(Config) ->
    [WorkerP1, WorkerP2] = ?WORKERS(Config),
    FilePath = ?build_test_root_path(Config),
    RequestHeaders = [?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER, cdmi_test_utils:user_2_token_header()],
    RawBody = json_utils:encode(#{
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"text/plain">>,
        <<"value">> => ?FILE_CONTENT
    }),
    {ok, _Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(
        WorkerP1, FilePath, put, RequestHeaders, RawBody
    )),

    CdmiResponse = json_utils:decode(Response),
    ?assertMatch(#{<<"mimetype">> := <<"text/plain">>}, CdmiResponse),

    RequestHeaders2 = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    GetCdmiMetadataFun =  fun() ->
        {ok, ?HTTP_200_OK, _Headers2, Response2} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
            WorkerP2, FilePath ++ "?value;mimetype;valuetransferencoding", get, RequestHeaders2, []
        ),?ATTEMPTS),
        json_utils:decode(Response2)
    end,

    %TODO VFS-7376 what do we return here if file contains valid utf-8 string and we read byte range?
    ?assertMatch(#{
        <<"mimetype">> := <<"text/plain">>,
        <<"valuetransferencoding">> := <<"utf-8">>,
        <<"value">> := ?FILE_CONTENT
    }, GetCdmiMetadataFun(), ?ATTEMPTS).


mimetype_and_encoding_create_file_noncdmi_request_test(Config) ->
    %% create file with given mime and encoding using non-cdmi request
    FilePath = ?build_test_root_path(Config),
    RequestHeaders = [{?HDR_CONTENT_TYPE, <<"text/plain; charset=utf-8">>}, cdmi_test_utils:user_2_token_header()],
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(
            ?WORKERS(Config), FilePath, put, RequestHeaders, ?FILE_CONTENT
        ),
        ?ATTEMPTS
    ),

    RequestHeaders2 = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    GetCdmiMetadataFun = fun() ->
        {ok, ?HTTP_200_OK, _Headers2, Response2} = ?assertMatch(
            {ok, ?HTTP_200_OK, _, _},
            cdmi_test_utils:do_request(
                ?WORKERS(Config), FilePath ++ "?value;mimetype;valuetransferencoding", get, RequestHeaders2, []
        ), ?ATTEMPTS),
        json_utils:decode(Response2)
    end,
    ?assertMatch(#{
        <<"mimetype">> := <<"text/plain">>,
        <<"valuetransferencoding">> := <<"utf-8">>,
        <<"value">> := ?FILE_CONTENT
    }, GetCdmiMetadataFun(), ?ATTEMPTS).


wrong_create_path_error_test(Config) ->
    RequestHeaders = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code, _Headers, Response} =
        cdmi_test_utils:do_request(?WORKERS(Config), ?build_test_root_path(Config), put, RequestHeaders, []),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_VALUE_IDENTIFIER(<<"path">>)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}),

    %%---- wrong create path 2 -----
    RequestHeaders2 = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_OBJECT_CONTENT_TYPE_HEADER
    ],
    {ok, Code, _Headers2, Response2} =
        cdmi_test_utils:do_request(?WORKERS(Config), ?build_test_root_path(Config) ++ "/", put, RequestHeaders2, []),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_VALUE_IDENTIFIER(<<"path">>)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response2)}).


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
        ?WORKERS(Config), filename:join(RootPath,  "some_file_b64"), put, RequestHeaders, RequestBody
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(<<"base64">>)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


% tests cdmi and non-cdmi partial upload feature (requests with x-cdmi-partial flag set to true)
partial_upload_cdmi_test(Config) ->
    [WorkerP1, WorkerP2] = ?WORKERS(Config),
    RootPath = cdmi_test_utils:get_tests_root_path(Config),

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
    GetResponseFun = fun() ->
        {ok, ?HTTP_201_CREATED, _Headers1, Response1} = ?assertMatch(
            {ok, ?HTTP_201_CREATED, _, _},
            cdmi_test_utils:do_request(WorkerP2, FilePath, put, RequestHeaders, RequestBody
            ), ?ATTEMPTS),
        json_utils:decode(Response1)
    end,
    ?assertMatch(#{<<"completionStatus">> := <<"Processing">>}, GetResponseFun(), ?ATTEMPTS),

    % upload second chunk of file
    RequestBody2 = json_utils:encode(#{<<"value">> => base64:encode(Chunk2)}),
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP2, FilePath ++ "?value:4-4", put, RequestHeaders, RequestBody2
    ), ?ATTEMPTS),
    % upload third chunk of file
    RequestHeaders3 = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER],
    RequestBody3 = json_utils:encode(#{<<"value">> => base64:encode(Chunk3)}),
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP2, FilePath ++ "?value:5-9", put, RequestHeaders3, RequestBody3
    ),?ATTEMPTS),
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
            {ok, ?HTTP_200_OK, _, _},
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
    [WorkerP1, WorkerP2] = ?WORKERS(Config),
    RootPath = cdmi_test_utils:get_tests_root_path(Config),

    FilePath = filename:join([RootPath, "partial2.txt"]),
    Chunk1 = <<"some">>,
    Chunk2 = <<"_">>,
    Chunk3 = <<"value">>,
    %%----- non-cdmi request partial upload -------
    ?assertNot(cdmi_test_utils:object_exists(FilePath, Config)),

    % upload first chunk of file
    RequestHeaders = [cdmi_test_utils:user_2_token_header(), {<<"X-CDMI-Partial">>, <<"true">>}],
    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, cdmi_test_utils:do_request(
        WorkerP2, FilePath, put, RequestHeaders, Chunk1
    ),?ATTEMPTS),
    RequestHeaders2 = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER],
    % check "completionStatus", should be set to "Processing"
    GetResponseFun2 = fun() ->
        {ok, ?HTTP_200_OK, _Headers2, Response2} = ?assertMatch(
            {ok, ?HTTP_200_OK, _, _},
            cdmi_test_utils:do_request(
                WorkerP1, FilePath ++ "?completionStatus", get, RequestHeaders2, Chunk1
            ),
            ?ATTEMPTS
        ),
        json_utils:decode(Response2)
    end,
    ?assertMatch(#{<<"completionStatus">> := <<"Processing">>}, GetResponseFun2(), ?ATTEMPTS),

    % upload second chunk of file
    RequestHeaders3 = [
        cdmi_test_utils:user_2_token_header(),
        {?HDR_CONTENT_RANGE, <<"bytes 4-4/10">>}, {<<"X-CDMI-Partial">>, <<"true">>}
    ],
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP2, FilePath, put, RequestHeaders3, Chunk2
    ), ?ATTEMPTS),

    % upload third chunk of file
    RequestHeaders4 = [
        cdmi_test_utils:user_2_token_header(),
        {?HDR_CONTENT_RANGE, <<"bytes 5-9/10">>},
        {<<"X-CDMI-Partial">>, <<"false">>}
    ],
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP2, FilePath, put, RequestHeaders4, Chunk3
    ), ?ATTEMPTS),
    timer:sleep(5000),
    % get created file and check its consistency
    RequestHeaders5 = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER],
    % TODO Verify once after VFS-2023
    CheckAllChunks2 = fun() ->
        ?assertMatch(#{<<"completionStatus">> := <<"Complete">>},
            get_cdmi_response_from_request(FilePath, WorkerP2, RequestHeaders5), ?ATTEMPTS),
        {ok, ?HTTP_200_OK, _Headers5, Response5} = ?assertMatch(
            {ok, ?HTTP_200_OK, _, _},
            cdmi_test_utils:do_request(WorkerP1, FilePath, get, RequestHeaders5, []),
            ?ATTEMPTS
        ),
        CdmiResponse5 = json_utils:decode(Response5),
        base64:decode(maps:get(<<"value">>, CdmiResponse5))
    end,
    % File size event change is async
    Chunks123 = <<Chunk1/binary, Chunk2/binary, Chunk3/binary>>,
    ?assertMatch(Chunks123, CheckAllChunks2(), ?ATTEMPTS).


get_cdmi_response_from_request(FileName, Workers, RequestHeaders1) ->
    {ok, ?HTTP_200_OK, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
            Workers, FileName, get, RequestHeaders1, []
        ),
        ?ATTEMPTS
    ),
    json_utils:decode(Response).