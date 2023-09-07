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
    basic_create_file/1,
    base64_create_file/1,
    create_empty_file/1,
    create_noncdmi_file/1,
    create_raw_file_with_cdmi_version_header_should_succeed/1,
    create_cdmi_file_without_cdmi_version_header_should_fail/1,
    basic_create_dir/1,
    create_noncdmi_dir_and_update/1,
    missing_parent_create_dir/1,
    create_raw_dir_with_cdmi_version_header_should_succeed/1,
    create_cdmi_dir_without_cdmi_version_header_should_fail/1
]).

user_2_token_header() ->
    rest_test_utils:user_token_header(oct_background:get_user_access_token(user2)).


% Tests file creation (cdmi object PUT), It can be done with cdmi header (when file data is provided as cdmi-object
% json string), or without (when we treat request body as new file content)
%% @private
create_file_base(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,
    RootDirPath = list_to_binary("/" ++ RootPath),

    FileContent = <<"File content!">>,
    {Workers, RootPath, RootDirPath, FileContent}.


basic_create_file(Config) ->
    {Workers, RootPath, RootDirPath, FileContent} = create_file_base(Config),
    ToCreate = filename:join([RootPath, "file1.txt"]),
    %%-------- basic create --------
    ?assert(not cdmi_internal:object_exists(ToCreate, Config)),

    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody1 = #{<<"value">> => FileContent},
    RawRequestBody1 = json_utils:encode((RequestBody1)),
    {ok, Code1, _Headers1, Response1} = cdmi_internal:do_request(
        Workers, ToCreate, put, RequestHeaders1, RawRequestBody1
    ),

    ?assertEqual(?HTTP_201_CREATED, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    Metadata1 = maps:get(<<"metadata">>, CdmiResponse1),
    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-object">>}, CdmiResponse1),
    ?assertMatch(#{<<"objectName">> := <<"file1.txt">>}, CdmiResponse1),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse1),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse1),
    ?assertNotEqual([], Metadata1),

    ?assert(cdmi_internal:object_exists(ToCreate, Config)),
    ?assertEqual(FileContent, cdmi_internal:get_file_content(ToCreate, Config), ?ATTEMPTS).
    %%------------------------------


base64_create_file(Config) ->
    {Workers, RootPath, RootDirPath, FileContent} = create_file_base(Config),
    ToCreate2 = filename:join([RootPath, "file2.txt"]),

    %%------ base64 create ---------
    ?assert(not cdmi_internal:object_exists(ToCreate2, Config)),

    RequestHeaders2 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody2 = #{<<"valuetransferencoding">> => <<"base64">>,
        <<"value">> => base64:encode(FileContent)},
    RawRequestBody2 = json_utils:encode((RequestBody2)),
    {ok, Code2, _Headers2, Response2} =
        cdmi_internal:do_request(Workers, ToCreate2, put, RequestHeaders2, RawRequestBody2),
    ?assertEqual(?HTTP_201_CREATED, Code2),
    CdmiResponse2 = (json_utils:decode(Response2)),

    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-object">>}, CdmiResponse2),
    ?assertMatch(#{<<"objectName">> := <<"file2.txt">>}, CdmiResponse2),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse2),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse2),
    ?assert(maps:get(<<"metadata">>, CdmiResponse2) =/= <<>>),

    ?assert(cdmi_internal:object_exists(ToCreate2, Config)),
    ?assertEqual(FileContent, cdmi_internal:get_file_content(ToCreate2, Config), ?ATTEMPTS).
    %%------------------------------


create_empty_file(Config) ->
    {Workers, RootPath, _, _} = create_file_base(Config),
    ToCreate3 = filename:join([RootPath, "file3.txt"]),
    %%------- create empty ---------
    ?assert(not cdmi_internal:object_exists(ToCreate3, Config)),

    RequestHeaders4 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code4, _Headers4, _Response4} = cdmi_internal:do_request(
        Workers, ToCreate3, put, RequestHeaders4, []
    ),
    ?assertEqual(?HTTP_201_CREATED, Code4),

    ?assert(cdmi_internal:object_exists(ToCreate3, Config)),
    ?assertEqual(<<>>, cdmi_internal:get_file_content(ToCreate3, Config), ?ATTEMPTS).
    %%------------------------------


create_noncdmi_file(Config) ->
    {Workers, RootPath, _, FileContent} = create_file_base(Config),
    ToCreate4 = filename:join([RootPath, "file4.txt"]),
    %%------ create noncdmi --------
    ?assert(not cdmi_internal:object_exists(ToCreate4, Config)),

    RequestHeaders5 = [{?HDR_CONTENT_TYPE, <<"application/binary">>}],
    {ok, Code5, _Headers5, _Response5} =cdmi_internal:do_request(
        Workers, ToCreate4, put, [user_2_token_header() | RequestHeaders5], FileContent
    ),

    ?assertEqual(?HTTP_201_CREATED, Code5),

    ?assert(cdmi_internal:object_exists(ToCreate4, Config)),
    ?assertEqual(FileContent, cdmi_internal:get_file_content(ToCreate4, Config), ?ATTEMPTS).
%%------------------------------


create_raw_file_with_cdmi_version_header_should_succeed(Config) ->
    % given
    {Workers, RootPath, _, _} = create_file_base(Config),

    % when
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders, _Response},
        cdmi_internal:do_request(Workers, RootPath ++ "/file1", put,
            [?CDMI_VERSION_HEADER, user_2_token_header()], <<"data">>
        )),
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders2, _Response2},
        cdmi_internal:do_request(Workers, RootPath ++ "/file2", put,
            [?CDMI_VERSION_HEADER, user_2_token_header(), {?HDR_CONTENT_TYPE, <<"text/plain">>}],
            <<"data2">>
        )).


create_cdmi_file_without_cdmi_version_header_should_fail(Config) ->
    % given
    {Workers, RootPath, _, _} = create_file_base(Config),

    % when
    {ok, Code, _ResponseHeaders, Response} = cdmi_internal:do_request(
        Workers, RootPath ++ "/file1", put,
        [user_2_token_header(), ?OBJECT_CONTENT_TYPE_HEADER], <<"{}">>
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_MISSING_REQUIRED_VALUE(<<"version">>)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


% Tests dir creation (cdmi container PUT), remember that every container URI ends
% with '/'
create_dir_base(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,
    RootDirPath = list_to_binary("/" ++ RootPath),

    {Workers, RootPath, RootDirPath}.


basic_create_dir(Config) ->
    {Workers, RootPath, RootDirPath} = create_dir_base(Config),
    DirName2 = filename:join([RootPath, "toCreate2"]) ++ "/",
    %%------ basic create ----------
    ?assert(not cdmi_internal:object_exists(DirName2, Config)),

    RequestHeaders2 = [
        user_2_token_header(), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code2, _Headers2, Response2} = cdmi_internal:do_request(
        Workers, DirName2, put, RequestHeaders2, []
    ),

    ?assertEqual(?HTTP_201_CREATED, Code2),
    CdmiResponse2 = json_utils:decode(Response2),

    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-container">>}, CdmiResponse2),
    ?assertMatch(#{<<"objectName">> := <<"toCreate2/">>}, CdmiResponse2),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse2),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse2),
    ?assertMatch(#{<<"children">> := []}, CdmiResponse2),
    ?assert(maps:get(<<"metadata">>, CdmiResponse2) =/= <<>>),

    ?assert(cdmi_internal:object_exists(DirName2, Config)).
%%------------------------------


create_noncdmi_dir_and_update(Config) ->
    {Workers, RootPath, _} = create_dir_base(Config),
    DirName = filename:join([RootPath, "toCreate1"]) ++ "/",
    %%------ non-cdmi create -------
    ?assert(not cdmi_internal:object_exists(DirName, Config)),

    {ok, Code1, _Headers1, _Response1} =
        cdmi_internal:do_request(Workers, DirName, put, [user_2_token_header()]),
    ?assertEqual(?HTTP_201_CREATED, Code1),

    ?assert(cdmi_internal:object_exists(DirName, Config)),

    %%---------- update ------------

    RequestHeaders3 = [
        user_2_token_header(), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code3, _Headers3, _Response3} = cdmi_internal:do_request(
        Workers, DirName, put, RequestHeaders3, []
    ),

    ?assertEqual(?HTTP_204_NO_CONTENT, Code3),
    ?assert(cdmi_internal:object_exists(DirName, Config)).
    %%------------------------------


missing_parent_create_dir(Config) ->
    {Workers, RootPath, _} = create_dir_base(Config),
    MissingParentName = filename:join([RootPath, "unknown"]) ++ "/",
    DirWithoutParentName = filename:join(MissingParentName, "dir") ++ "/",
    %%----- missing parent ---------
    ?assert(not cdmi_internal:object_exists(MissingParentName, Config)),

    RequestHeaders4 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code4, _Headers4, Response4} = cdmi_internal:do_request(
        Workers, DirWithoutParentName, put, RequestHeaders4, []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?ENOENT)),
    ?assertMatch(ExpRestError, {Code4, json_utils:decode(Response4)}).
%%------------------------------


create_raw_dir_with_cdmi_version_header_should_succeed(Config) ->
    % given
    {Workers, RootPath, _} = create_dir_base(Config),

    % when
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders, _Response},
        cdmi_internal:do_request(Workers, RootPath ++ "/dir1/", put,
            [?CDMI_VERSION_HEADER, user_2_token_header()]
        )),
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders2, _Response2},
        cdmi_internal:do_request(Workers, RootPath ++ "/dir2/", put,
            [?CDMI_VERSION_HEADER, user_2_token_header(), {?HDR_CONTENT_TYPE, <<"application/json">>}],
            <<"{}">>
        )).


create_cdmi_dir_without_cdmi_version_header_should_fail(Config) ->
    % given
    {Workers, RootPath, _} = create_dir_base(Config),

    % when
    {ok, Code, _ResponseHeaders, Response} = cdmi_internal:do_request(
        Workers, RootPath ++ "/dir1/", put,
        [user_2_token_header(), ?CONTAINER_CONTENT_TYPE_HEADER]
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_MISSING_REQUIRED_VALUE(<<"version">>)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).

