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
-module(cdmi_test_base).
-author("Tomasz Lichon").

-include("http/cdmi.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/metadata.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include("onenv_test_utils.hrl").
-include("cdmi_test.hrl").

-export([
%%    cdmi_misc_test_base %% testy które nie pasują
    list_dir/1,
    metadata/1,

    objectid/1,
    errors/1,

    delete_file/1,
    delete_dir/1,

    %% cdmi_create_tests
    create_file/1,
    create_raw_file_with_cdmi_version_header_should_succeed/1,
    create_cdmi_file_without_cdmi_version_header_should_fail/1,
    create_dir/1,
    create_raw_dir_with_cdmi_version_header_should_succeed/1,
    create_cdmi_dir_without_cdmi_version_header_should_fail/1,

    %% cdmi_copy_move_tests
    copy/1,
    move/1,
    moved_permanently/1,
    move_copy_conflict/1,

    capabilities/1,
    use_supported_cdmi_version/1,
    use_unsupported_cdmi_version/1,

    request_format_check/1,
    mimetype_and_encoding/1,
    out_of_range/1,

    partial_upload/1,
    get_file/1,
    update_file/1,

    acl/1,
    accept_header/1
]).

user_2_token_header() ->
    rest_test_utils:user_token_header(oct_background:get_user_access_token(user2)).

%%%===================================================================
%%% Test functions
%%%===================================================================

% Tests file creation (cdmi object PUT), It can be done with cdmi header (when file data is provided as cdmi-object
% json string), or without (when we treat request body as new file content)
create_file(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    ToCreate = filename:join([RootPath, "file1.txt"]),
    ToCreate2 = filename:join([RootPath, "file2.txt"]),
    ToCreate5 = filename:join([RootPath, "file3.txt"]),
    ToCreate4 = filename:join([RootPath, "file4.txt"]),
    FileContent = <<"File content!">>,

    %%-------- basic create --------
    ?assert(not object_exists(ToCreate, Config)),

    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody1 = #{<<"value">> => FileContent},
    RawRequestBody1 = json_utils:encode((RequestBody1)),
    {ok, Code1, _Headers1, Response1} = do_request(
        Workers, ToCreate, put, RequestHeaders1, RawRequestBody1
    ),

    RootDirPath = list_to_binary("/" ++ RootPath),
    ?assertEqual(?HTTP_201_CREATED, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    Metadata1 = maps:get(<<"metadata">>, CdmiResponse1),
    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-object">>}, CdmiResponse1),
    ?assertMatch(#{<<"objectName">> := <<"file1.txt">>}, CdmiResponse1),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse1),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse1),
    ?assertNotEqual([], Metadata1),

    ?assert(object_exists(ToCreate, Config)),
    ?assertEqual(FileContent, get_file_content(ToCreate, Config), ?ATTEMPTS),
    %%------------------------------

    %%------ base64 create ---------
    ?assert(not object_exists(ToCreate2, Config)),

    RequestHeaders2 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody2 = #{<<"valuetransferencoding">> => <<"base64">>,
        <<"value">> => base64:encode(FileContent)},
    RawRequestBody2 = json_utils:encode((RequestBody2)),
    {ok, Code2, _Headers2, Response2} =
        do_request(Workers, ToCreate2, put, RequestHeaders2, RawRequestBody2),
    ?assertEqual(?HTTP_201_CREATED, Code2),
    CdmiResponse2 = (json_utils:decode(Response2)),

    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-object">>}, CdmiResponse2),
    ?assertMatch(#{<<"objectName">> := <<"file2.txt">>}, CdmiResponse2),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse2),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse2),
    ?assert(maps:get(<<"metadata">>, CdmiResponse2) =/= <<>>),

    ?assert(object_exists(ToCreate2, Config)),
    ?assertEqual(FileContent, get_file_content(ToCreate2, Config), ?ATTEMPTS),
    %%------------------------------

    %%------- create empty ---------
    ?assert(not object_exists(ToCreate4, Config)),

    RequestHeaders4 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code4, _Headers4, _Response4} = do_request(Workers, ToCreate4, put, RequestHeaders4, []),
    ?assertEqual(?HTTP_201_CREATED, Code4),

    ?assert(object_exists(ToCreate4, Config)),
    ?assertEqual(<<>>, get_file_content(ToCreate4, Config), ?ATTEMPTS),
    %%------------------------------

    %%------ create noncdmi --------
    ?assert(not object_exists(ToCreate5, Config)),

    RequestHeaders5 = [{?HDR_CONTENT_TYPE, <<"application/binary">>}],
    {ok, Code5, _Headers5, _Response5} =
        do_request(Workers, ToCreate5, put,
            [user_2_token_header() | RequestHeaders5], FileContent),

    ?assertEqual(?HTTP_201_CREATED, Code5),

    ?assert(object_exists(ToCreate5, Config)),
    ?assertEqual(FileContent, get_file_content(ToCreate5, Config), ?ATTEMPTS).
%%------------------------------


create_raw_file_with_cdmi_version_header_should_succeed(Config) ->
    % given
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    % when
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders, _Response},
        do_request(Workers, RootPath ++ "/file1", put,
            [?CDMI_VERSION_HEADER, user_2_token_header()], <<"data">>
        )),
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders2, _Response2},
        do_request(Workers, RootPath ++ "/file2", put,
            [?CDMI_VERSION_HEADER, user_2_token_header(), {?HDR_CONTENT_TYPE, <<"text/plain">>}],
            <<"data2">>
        )).


create_cdmi_file_without_cdmi_version_header_should_fail(Config) ->
    % given
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    % when
    {ok, Code, _ResponseHeaders, Response} = do_request(
        Workers, RootPath ++ "/file1", put,
        [user_2_token_header(), ?OBJECT_CONTENT_TYPE_HEADER], <<"{}">>
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_MISSING_REQUIRED_VALUE(<<"version">>)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


% Tests dir creation (cdmi container PUT), remember that every container URI ends
% with '/'
create_dir(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    DirName = filename:join([RootPath, "toCreate1"]) ++ "/",
    DirName2 = filename:join([RootPath, "toCreate2"]) ++ "/",
    MissingParentName = filename:join([RootPath, "unknown"]) ++ "/",
    DirWithoutParentName = filename:join(MissingParentName, "dir") ++ "/",

    %%------ non-cdmi create -------
    ?assert(not object_exists(DirName, Config)),

    {ok, Code1, _Headers1, _Response1} =
        do_request(Workers, DirName, put, [user_2_token_header()]),
    ?assertEqual(?HTTP_201_CREATED, Code1),

    ?assert(object_exists(DirName, Config)),
    %%------------------------------

    %%------ basic create ----------
    ?assert(not object_exists(DirName2, Config)),

    RequestHeaders2 = [
        user_2_token_header(), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code2, _Headers2, Response2} = do_request(
        Workers, DirName2, put, RequestHeaders2, []
    ),

    ?assertEqual(?HTTP_201_CREATED, Code2),
    CdmiResponse2 = json_utils:decode(Response2),
    RootDirPath = list_to_binary("/" ++ RootPath),

    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-container">>}, CdmiResponse2),
    ?assertMatch(#{<<"objectName">> := <<"toCreate2/">>}, CdmiResponse2),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse2),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse2),
    ?assertMatch(#{<<"children">> := []}, CdmiResponse2),
    ?assert(maps:get(<<"metadata">>, CdmiResponse2) =/= <<>>),

    ?assert(object_exists(DirName2, Config)),
    %%------------------------------

    %%---------- update ------------
    ?assert(object_exists(DirName, Config)),

    RequestHeaders3 = [
        user_2_token_header(), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code3, _Headers3, _Response3} = do_request(
        Workers, DirName, put, RequestHeaders3, []
    ),

    ?assertEqual(?HTTP_204_NO_CONTENT, Code3),
    ?assert(object_exists(DirName, Config)),
    %%------------------------------

    %%----- missing parent ---------
    ?assert(not object_exists(MissingParentName, Config)),

    RequestHeaders4 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code4, _Headers4, Response4} = do_request(
        Workers, DirWithoutParentName, put, RequestHeaders4, []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?ENOENT)),
    ?assertMatch(ExpRestError, {Code4, json_utils:decode(Response4)}).
%%------------------------------


create_raw_dir_with_cdmi_version_header_should_succeed(Config) ->
    % given
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    % when
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders, _Response},
        do_request(Workers, RootPath ++ "/dir1/", put,
            [?CDMI_VERSION_HEADER, user_2_token_header()]
        )),
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _ResponseHeaders2, _Response2},
        do_request(Workers, RootPath ++ "/dir2/", put,
            [?CDMI_VERSION_HEADER, user_2_token_header(), {?HDR_CONTENT_TYPE, <<"application/json">>}],
            <<"{}">>
        )).


create_cdmi_dir_without_cdmi_version_header_should_fail(Config) ->
    % given
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    % when
    {ok, Code, _ResponseHeaders, Response} = do_request(
        Workers, RootPath ++ "/dir1/", put,
        [user_2_token_header(), ?CONTAINER_CONTENT_TYPE_HEADER]
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_MISSING_REQUIRED_VALUE(<<"version">>)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


%% setup do osobnej funkcji i podzielenie tych wielkoludow na mniejsze testy i do różnych plików

% Tests cdmi container GET request (also refered as LIST)
list_dir(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {SpaceName, ShortTestDirName, TestDirName, TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    TestDirNameCheck = list_to_binary(ShortTestDirName ++ "/"),
    TestFileNameBin = list_to_binary(TestFileName),
    %%------ list basic dir --------
    {ok, Code1, Headers1, Response1} =
        do_request(Workers, TestDirName ++ "/", get,
            [user_2_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(?HTTP_200_OK, Code1),
    CdmiResponse1 = json_utils:decode(Response1),
    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/cdmi-container">>}, Headers1),
    ?assertMatch(
        #{<<"objectType">> :=  <<"application/cdmi-container">>},
        CdmiResponse1
    ),
    ?assertMatch(#{<<"objectName">> := TestDirNameCheck}, CdmiResponse1),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse1),
    ?assertMatch(#{<<"children">> := [TestFileNameBin]}, CdmiResponse1),
    ?assert(maps:get(<<"metadata">>, CdmiResponse1) =/= <<>>),
    %%------------------------------

    %%------ list root space dir ---------
    {ok, Code2, _Headers2, Response2} =
        do_request(Workers, RootPath, get,
            [user_2_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(?HTTP_200_OK, Code2),
    CdmiResponse2 = json_utils:decode(Response2),
    RootNameBin = list_to_binary(RootName),
    ?assertMatch(#{<<"objectName">> := RootNameBin}, CdmiResponse2),
    ?assertMatch(#{<<"children">> := [TestDirNameCheck]}, CdmiResponse2),
    %%------------------------------

    %%--- list nonexisting dir -----
    {ok, Code3, _Headers3, _Response3} =
        do_request(Workers, "nonexisting_dir/",
            get, [user_2_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(?HTTP_404_NOT_FOUND, Code3),
    %%------------------------------

    %%-- selective params list -----
    {ok, Code4, _Headers4, Response4} =
        do_request(Workers, TestDirName ++ "/?children;objectName",
            get, [user_2_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(?HTTP_200_OK, Code4),
    CdmiResponse4 = json_utils:decode(Response4),
    ?assertMatch(#{<<"objectName">> := TestDirNameCheck}, CdmiResponse4),
    ?assertMatch(#{<<"children">> := [TestFileNameBin]}, CdmiResponse4),
    ?assertEqual(2, maps:size(CdmiResponse4)),
    %%------------------------------

    %%---- childrenrange list ------
    Children = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",
        "12", "13", "14"],
    ChildrenNameBinaries = lists:map(fun(X) -> list_to_binary(X) end, Children),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid), #dir_spec{
        name = <<"childrange">>,
        children = lists:map(
            fun(ChildName) ->
                #file_spec{name = ChildName} end, ChildrenNameBinaries
        )
    }, Config#cdmi_test_config.p1_selector),
    ChildrangeDir = RootPath ++ "childrange/",
    {ok, Code5, _Headers5, Response5} =
        do_request(Workers, ChildrangeDir ++ "?children;childrenrange",
            get, [user_2_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(?HTTP_200_OK, Code5),
    CdmiResponse5 = (json_utils:decode(Response5)),
    ChildrenResponse1 = maps:get(<<"children">>, CdmiResponse5),
    ?assert(is_list(ChildrenResponse1)),
    lists:foreach(fun(Name) ->
        ?assert(lists:member(Name, ChildrenResponse1))
    end, ChildrenNameBinaries),
    ?assertMatch(#{<<"childrenrange">> := <<"0-14">>}, CdmiResponse5),

    {ok, Code6, _, Response6} =
        do_request(Workers, ChildrangeDir ++ "?children:2-13;childrenrange", get,
            [user_2_token_header(), ?CDMI_VERSION_HEADER], []),
    {ok, Code7, _, Response7} =
        do_request(Workers, ChildrangeDir ++ "?children:0-1;childrenrange", get,
            [user_2_token_header(), ?CDMI_VERSION_HEADER], []),
    {ok, Code8, _, Response8} =
        do_request(Workers, ChildrangeDir ++ "?children:14-14;childrenrange", get,
            [user_2_token_header(), ?CDMI_VERSION_HEADER], []),

    ?assertEqual(?HTTP_200_OK, Code6),
    ?assertEqual(?HTTP_200_OK, Code7),
    ?assertEqual(?HTTP_200_OK, Code8),
    CdmiResponse6 = json_utils:decode(Response6),
    CdmiResponse7 = json_utils:decode(Response7),
    CdmiResponse8 = json_utils:decode(Response8),
    ChildrenResponse6 = maps:get(<<"children">>, CdmiResponse6),
    ChildrenResponse7 = maps:get(<<"children">>, CdmiResponse7),
    ChildrenResponse8 = maps:get(<<"children">>, CdmiResponse8),

    ?assert(is_list(ChildrenResponse6)),
    ?assert(is_list(ChildrenResponse7)),
    ?assert(is_list(ChildrenResponse8)),
    ?assertEqual(12, length(ChildrenResponse6)),
    ?assertEqual(2, length(ChildrenResponse7)),
    ?assertEqual(1, length(ChildrenResponse8)),
    ?assertMatch(#{<<"childrenrange">> := <<"2-13">>}, CdmiResponse6),
    ?assertMatch(#{<<"childrenrange">> := <<"0-1">>}, CdmiResponse7),
    ?assertMatch(#{<<"childrenrange">> := <<"14-14">>}, CdmiResponse8),
    lists:foreach(
        fun(Name) ->
            ?assert(lists:member(Name,
                ChildrenResponse6 ++ ChildrenResponse7 ++ ChildrenResponse8))
        end, ChildrenNameBinaries).
%%------------------------------


% tests access to file by objectid
objectid(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {SpaceName, ShortTestDirName, TestDirName, TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),

    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,
    TestDirNameCheck = list_to_binary(ShortTestDirName ++ "/"),
    ShortTestDirNameBin = list_to_binary(ShortTestDirName),
    TestFileNameBin = list_to_binary(TestFileName),

    %%-------- / objectid ----------
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code1, Headers1, Response1} = do_request(Workers, "", get, RequestHeaders1, []),
    ?assertEqual(?HTTP_200_OK, Code1),

    RequestHeaders0 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code0, _Headers0, Response0} = do_request(Workers, RootPath, get, RequestHeaders0, []),
    ?assertEqual(?HTTP_200_OK, Code0),
    CdmiResponse0 = json_utils:decode(Response0),
    SpaceRootId = maps:get(<<"objectID">>, CdmiResponse0),
    CdmiResponse1 = json_utils:decode(Response1),
    RootId = maps:get(<<"objectID">>, CdmiResponse1, undefined),

    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/cdmi-container">>}, Headers1),
    ?assertMatch(#{<<"objectName">> := <<"/">>}, CdmiResponse1),
    ?assertNotEqual(RootId, undefined),
    ?assert(is_binary(RootId)),
    ?assertMatch(#{<<"parentURI">> := <<>>}, CdmiResponse1),
    ?assertEqual(error, maps:find(<<"parentID">>, CdmiResponse1)),
    ?assertMatch(#{<<"capabilitiesURI">> := <<"cdmi_capabilities/container/">>}, CdmiResponse1),
    %%------------------------------

    %%------ /dir objectid ---------
    RequestHeaders2 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code2, _Headers2, Response2} = do_request(Workers, TestDirName ++ "/", get, RequestHeaders2, []),
    ?assertEqual(?HTTP_200_OK, Code2),
    CdmiResponse2 = (json_utils:decode(Response2)),
    DirId = maps:get(<<"objectID">>, CdmiResponse2, undefined),
    RootDirPath = list_to_binary("/" ++ RootPath),

    ?assertMatch(#{<<"objectName">> := TestDirNameCheck}, CdmiResponse2),
    ?assertNotEqual(DirId, undefined),
    ?assert(is_binary(DirId)),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse2),
    ?assertMatch(#{<<"parentID">> := SpaceRootId}, CdmiResponse2),
    ?assertMatch(#{<<"capabilitiesURI">> := <<"cdmi_capabilities/container/">>}, CdmiResponse2),
    %%------------------------------

    %%--- /dir 1/file.txt objectid ---
    RequestHeaders3 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code3, _Headers3, Response3} = do_request(
        Workers, filename:join(TestDirName, TestFileName), get, RequestHeaders3, []
    ),
    ?assertEqual(?HTTP_200_OK, Code3),
    CdmiResponse3 = json_utils:decode(Response3),
    FileId = maps:get(<<"objectID">>, CdmiResponse3, undefined),
    RootDirPath1 = list_to_binary("/" ++ RootPath ++ ShortTestDirName ++ "/" ),

    ?assertMatch(#{<<"objectName">> := TestFileNameBin}, CdmiResponse3),
    ?assertNotEqual(FileId, undefined),
    ?assert(is_binary(FileId)),
    ?assertMatch(#{<<"parentURI">> := RootDirPath1}, CdmiResponse3),
    ?assertMatch(#{<<"parentID">> := DirId}, CdmiResponse3),
    ?assertMatch(#{<<"capabilitiesURI">> := <<"cdmi_capabilities/dataobject/">>}, CdmiResponse3),
    %%------------------------------

    %%---- get / by objectid -------
    RequestHeaders4 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code4, _Headers4, Response4} = do_request(
        Workers, "cdmi_objectid/" ++ binary_to_list(RootId) ++ "/", get, RequestHeaders4, []
    ),
    ?assertEqual(?HTTP_200_OK, Code4),
    CdmiResponse4 = json_utils:decode(Response4),
    Meta1 = maps:remove(<<"cdmi_atime">>, maps:get(<<"metadata">>, CdmiResponse1)),
    CdmiResponse1WithoutAtime = maps:put(<<"metadata">>, Meta1, CdmiResponse1),
    Meta4 = maps:remove(<<"cdmi_atime">>, maps:get(<<"metadata">>, CdmiResponse4)),
    CdmiResponse4WithoutAtime = maps:put(<<"metadata">>, Meta4, CdmiResponse4),

    ?assertEqual(CdmiResponse1WithoutAtime, CdmiResponse4WithoutAtime), % should be the same as in 1 (except access time)
    %%------------------------------

    %%--- get /dir 1/ by objectid ----
    RequestHeaders5 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code5, _Headers5, Response5} = do_request(
        Workers, "cdmi_objectid/" ++ binary_to_list(DirId) ++ "/", get, RequestHeaders5, []
    ),
    ?assertEqual(?HTTP_200_OK, Code5),
    CdmiResponse5 = json_utils:decode(Response5),
    Meta2 = maps:remove(<<"cdmi_atime">>, (maps:get(<<"metadata">>, CdmiResponse2))),
    CdmiResponse2WithoutAtime = maps:put(<<"metadata">>, Meta2, CdmiResponse2),
    Meta5 = maps:remove(<<"cdmi_atime">>, (maps:get(<<"metadata">>, CdmiResponse5))),
    CdmiResponse5WithoutAtime = maps:put(<<"metadata">>, Meta5, CdmiResponse5),

    ?assertEqual( % should be the same as in 2 (except parent and access time)
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse2WithoutAtime)),
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse5WithoutAtime))
    ),
    %%------------------------------

    %% get /dir 1/file.txt by objectid
    RequestHeaders6 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code6, _Headers6, Response6} = do_request(
        Workers, "cdmi_objectid/" ++ binary_to_list(DirId) ++ "/" ++ TestFileName, get, RequestHeaders6, []
    ),
    ?assertEqual(?HTTP_200_OK, Code6),
    CdmiResponse6 = (json_utils:decode(Response6)),
    Meta3 = maps:remove(<<"cdmi_atime">>, (maps:get(<<"metadata">>, CdmiResponse3))),
    CdmiResponse3WithoutAtime = maps:put(<<"metadata">>, Meta3, CdmiResponse3),
    Meta6 = maps:remove(<<"cdmi_atime">>, (maps:get(<<"metadata">>, CdmiResponse6))),
    CdmiResponse6WithoutAtime = maps:put(<<"metadata">>, Meta6, CdmiResponse6),

    ?assertEqual( % should be the same as in 3 (except access time)
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse3WithoutAtime)),
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse6WithoutAtime))
    ),

    {ok, Code7, _Headers7, Response7} = do_request(
        Workers, "cdmi_objectid/" ++ binary_to_list(FileId), get, RequestHeaders6, []
    ),
    ?assertEqual(?HTTP_200_OK, Code7),
    CdmiResponse7 = (json_utils:decode(Response7)),
    Meta7 = maps:remove(<<"cdmi_atime">>, (maps:get(<<"metadata">>, CdmiResponse7))),
    CdmiResponse7WithoutAtime = maps:merge(#{<<"metadata">> => Meta7},maps:remove(<<"metadata">>, CdmiResponse7)),

    ?assertEqual( % should be the same as in 6 (except parent and access time)
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse6WithoutAtime)),
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse7WithoutAtime))
    ),
    %%------------------------------

    %%---- unauthorized access to / by objectid -------
    RequestHeaders8 = [?CDMI_VERSION_HEADER],
    {ok, Code8, _, Response8} = do_request(
        Workers, "cdmi_objectid/" ++ binary_to_list(RootId) ++ "/", get, RequestHeaders8, []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_UNAUTHORIZED),
    ?assertMatch(ExpRestError, {Code8, json_utils:decode(Response8)}).
%%------------------------------


% test error handling
errors(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {SpaceName, _ShortTestDirName, TestDirName, _TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    %%---- unauthorized access -----
    {ok, Code1, _Headers1, Response1} =
        do_request(Workers, TestDirName, get, [], []),
    ExpRestError1 = rest_test_utils:get_rest_error(?ERROR_UNAUTHORIZED),
    ?assertMatch(ExpRestError1, {Code1, json_utils:decode(Response1)}),
    %%------------------------------

    %%----- wrong create path ------
    RequestHeaders2 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code2, _Headers2, Response2} =
        do_request(Workers, RootPath ++ "/test_dir", put, RequestHeaders2, []),
    ExpRestError2 = rest_test_utils:get_rest_error(?ERROR_BAD_VALUE_IDENTIFIER(<<"path">>)),
    ?assertMatch(ExpRestError2, {Code2, json_utils:decode(Response2)}),
    %%------------------------------

    %%---- wrong create path 2 -----
    RequestHeaders3 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER
    ],
    {ok, Code3, _Headers3, Response3} =
        do_request(Workers, RootPath ++ "/test_dir/", put, RequestHeaders3, []),
    ExpRestError3 = rest_test_utils:get_rest_error(?ERROR_BAD_VALUE_IDENTIFIER(<<"path">>)),
    ?assertMatch(ExpRestError3, {Code3, json_utils:decode(Response3)}),
    %%------------------------------

    %%-------- wrong base64 --------
    RequestHeaders4 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER
    ],
    RequestBody4 = json_utils:encode(#{
        <<"valuetransferencoding">> => <<"base64">>,
        <<"value">> => <<"#$%">>
    }),
    {ok, Code4, _Headers4, Response4} = do_request(
        Workers, RootPath ++ "/some_file_b64", put, RequestHeaders4, RequestBody4
    ),
    ExpRestError4 = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(<<"base64">>)),
    ?assertMatch(ExpRestError4, {Code4, json_utils:decode(Response4)}),
    %%------------------------------

    %%-- reading non-existing file --
    RequestHeaders6 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER
    ],
    {ok, Code6, _Headers6, _Response6} = do_request(
        Workers, RootPath ++ "/nonexistent_file", get, RequestHeaders6
    ),
    ?assertEqual(Code6, ?HTTP_404_NOT_FOUND),
    %%------------------------------

    %%--- listing non-existing dir -----
    RequestHeaders7 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code7, _Headers7, _Response7} = do_request(
        Workers, RootPath ++ "/nonexisting_dir/", get, RequestHeaders7
    ),
    ?assertEqual(Code7, ?HTTP_404_NOT_FOUND),
    %%------------------------------

    %%--- open binary file without permission -----
    File8 = filename:join([RootPath, "file8"]),
    FileContent8 = <<"File content...">>,
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"file8">>,
            content = FileContent8
        }, Config#cdmi_test_config.p1_selector
    ),
    ?assertEqual(object_exists(File8, Config), true),

    write_to_file(File8, FileContent8, ?FILE_BEGINNING, Config),
    ?assertEqual(get_file_content(File8, Config), FileContent8, ?ATTEMPTS),
    RequestHeaders8 = [user_2_token_header()],

    mock_opening_file_without_perms(Config),
    {ok, Code8, _Headers8, Response8} =
        do_request(Workers, File8, get, RequestHeaders8),
    unmock_opening_file_without_perms(Config),
    ExpRestError8 = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError8, {Code8, json_utils:decode(Response8)}, ?ATTEMPTS),
    %%------------------------------

    %%--- open cdmi file without permission -----
    File9 = filename:join([RootPath, "file9"]),
    FileContent9 = <<"File content...">>,
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"file9">>,
            content = FileContent9
        }, Config#cdmi_test_config.p1_selector
    ),
    ?assertEqual(object_exists(File9, Config), true),

    write_to_file(File9, FileContent9, ?FILE_BEGINNING, Config),
    ?assertEqual(get_file_content(File9, Config), FileContent9, ?ATTEMPTS),
    RequestHeaders9 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER
    ],

    mock_opening_file_without_perms(Config),
    {ok, Code9, _Headers9, Response9} = do_request(
        Workers, File9, get, RequestHeaders9
    ),
    unmock_opening_file_without_perms(Config),
    ExpRestError9 = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError9, {Code9, json_utils:decode(Response9)}).
%%------------------------------


%%  Tests cdmi object GET request. Request can be done without cdmi header (in that case
%%  file conent is returned as response body), or with cdmi header (the response
%%  contains json string of type: application/cdmi-object, and we can specify what
%%  parameters we need by listing then as ';' separated list after '?' in URL )
get_file(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,
    EmptyFileName = filename:join([RootPath, "empty.txt"]),
    FilledFileName = filename:join([RootPath, "toRead.txt"]),
    FileContent = <<"Some content...">>,
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid), [
        #file_spec{
            name = <<"empty.txt">>
        },
        #file_spec{
            name = <<"toRead.txt">>,
            content = FileContent
        }
    ], Config#cdmi_test_config.p1_selector),
    %%-------- basic read ----------
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code1, _Headers1, Response1} = do_request(Workers, FilledFileName, get, RequestHeaders1, []),
    ?assertEqual(?HTTP_200_OK, Code1),
    CdmiResponse1 = json_utils:decode(Response1),
    FileContent1 = base64:encode(FileContent),
    RootDirPath = list_to_binary("/" ++ RootPath),
    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-object">>}, CdmiResponse1),
    ?assertMatch(#{<<"objectName">> := <<"toRead.txt">>}, CdmiResponse1),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse1),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"base64">>}, CdmiResponse1),
    ?assertMatch(#{<<"mimetype">> := <<"application/octet-stream">>}, CdmiResponse1),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse1),
    ?assertMatch(#{<<"value">> := FileContent1}, CdmiResponse1),
    ?assertMatch(#{<<"valuerange">> := <<"0-14">>}, CdmiResponse1),

    ?assert(maps:get(<<"metadata">>, CdmiResponse1) =/= <<>>),
    %%------------------------------

    %%-- selective params read -----
    RequestHeaders2 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code2, _Headers2, Response2} = do_request(
        Workers, FilledFileName ++ "?parentURI;completionStatus", get, RequestHeaders2, []
    ),
    ?assertEqual(?HTTP_200_OK, Code2),
    CdmiResponse2 = json_utils:decode(Response2),

    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse2),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse2),
    ?assertEqual(2, maps:size(CdmiResponse2)),
    %%------------------------------

    %%--- selective value read -----
    RequestHeaders3 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code3, _Headers3, Response3} = do_request(
        Workers, FilledFileName ++ "?value:1-3;valuerange", get, RequestHeaders3, []
    ),
    ?assertEqual(?HTTP_200_OK, Code3),
    CdmiResponse3 = json_utils:decode(Response3),
    ?assertMatch(#{<<"valuerange">> := <<"1-3">>}, CdmiResponse3),
    % 1-3 from FileContent = <<"Some content...">>
    ?assertEqual(<<"ome">>, base64:decode(maps:get(<<"value">>, CdmiResponse3))),
    %%------------------------------

    %%------- noncdmi read --------
    {ok, Code4, Headers4, Response4} =
        do_request(Workers, FilledFileName, get, [user_2_token_header()]),
    ?assertEqual(?HTTP_200_OK, Code4),
    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/octet-stream">>}, Headers4),
    ?assertEqual(FileContent, Response4),
    %%------------------------------

    %%------- objectid read --------
    RequestHeaders5 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code5, _Headers5, Response5} = do_request(Workers, FilledFileName ++ "?objectID", get, RequestHeaders5, []),
    ?assertEqual(?HTTP_200_OK, Code5),
    CdmiResponse5 = (json_utils:decode(Response5)),
    ObjectID = maps:get(<<"objectID">>, CdmiResponse5),

    ?assert(is_binary(ObjectID)),
    %%------------------------------

    %%-------- read by id ----------
    RequestHeaders6 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code6, _Headers6, Response6} = do_request(
        Workers, "cdmi_objectid/" ++ binary_to_list(ObjectID), get, RequestHeaders6, []
    ),
    ?assertEqual(?HTTP_200_OK, Code6),
    CdmiResponse6 = (json_utils:decode(Response6)),

    ?assertEqual(FileContent, base64:decode(maps:get(<<"value">>, CdmiResponse6))),
    %%------------------------------

    %% selective value single range read non-cdmi
    ?assertMatch(
        {ok, ?HTTP_206_PARTIAL_CONTENT, #{?HDR_CONTENT_RANGE := <<"bytes 5-8/15">>}, <<"cont">>},
        do_request(Workers, FilledFileName, get, [
            {?HDR_RANGE, <<"bytes=5-8">>}, user_2_token_header()
        ])
    ),
    %%------------------------------

    %% selective value multi range read non-cdmi
    {ok, _, #{
        ?HDR_CONTENT_TYPE := <<"multipart/byteranges; boundary=", Boundary/binary>>
    }, Response8} = ?assertMatch(
        {ok, ?HTTP_206_PARTIAL_CONTENT, #{?HDR_CONTENT_TYPE := <<"multipart/byteranges", _/binary>>}, _},
        do_request(Workers, FilledFileName, get, [
            {?HDR_RANGE, <<"bytes=1-3,5-5,-3">>}, user_2_token_header()
        ])
    ),
    ExpResponse8 = <<
        "--", Boundary/binary,
        "\r\ncontent-type: application/octet-stream\r\ncontent-range: bytes 1-3/15",
        "\r\n\r\nome",
        "--", Boundary/binary,
        "\r\ncontent-type: application/octet-stream\r\ncontent-range: bytes 5-5/15",
        "\r\n\r\nc",
        "--", Boundary/binary,
        "\r\ncontent-type: application/octet-stream\r\ncontent-range: bytes 12-14/15",
        "\r\n\r\n...\r\n",
        "--", Boundary/binary, "--"
    >>,
    ?assertEqual(ExpResponse8, Response8),
    %%------------------------------

    %% read file non-cdmi with invalid Range should fail
    lists:foreach(fun(InvalidRange) ->
        ?assertMatch(
            {ok, ?HTTP_416_RANGE_NOT_SATISFIABLE, #{?HDR_CONTENT_RANGE := <<"bytes */15">>}, <<>>},
            do_request(Workers, FilledFileName, get, [
                {?HDR_RANGE, InvalidRange}, user_2_token_header()
            ])
        )
    end, [
        <<"unicorns">>,
        <<"bytes:5-10">>,
        <<"bytes=5=10">>,
        <<"bytes=-15-10">>,
        <<"bytes=100-150">>,
        <<"bytes=10-5">>,
        <<"bytes=-5-">>,
        <<"bytes=10--5">>,
        <<"bytes=10-15-">>
    ]),
    %%------------------------------

    %% read empty file non-cdmi without Range
    ?assertMatch(
        {ok, ?HTTP_200_OK, _, <<>>},
        do_request(Workers, EmptyFileName, get, [user_2_token_header()])
    ),
    %%------------------------------

    %% read empty file non-cdmi with Range should return 416
    ?assertMatch(
        {ok, ?HTTP_416_RANGE_NOT_SATISFIABLE, #{?HDR_CONTENT_RANGE := <<"bytes */0">>}, <<>>},
        do_request(Workers, EmptyFileName, get, [
            {?HDR_RANGE, <<"bytes=10-15">>}, user_2_token_header()
        ])
    ).


% Tests cdmi metadata read on object GET request.
metadata(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,
    UserId2 = oct_background:get_user_id(user2),
    UserName2 = <<"Unnamed User">>,

    FileName = filename:join([RootPath, "metadataTest.txt"]),
    FileContent = <<"Some content...">>,
    DirName = filename:join([RootPath, "metadataTestDir"]) ++ "/",

    %%-------- create file with user metadata --------
    ?assert(not object_exists(FileName, Config)),
    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody1 = #{
        <<"value">> => FileContent,
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"text/plain">>,
        <<"metadata">> => #{<<"my_metadata">> => <<"my_value">>,
                            <<"cdmi_not_allowed">> => <<"my_value">>}},

    RawRequestBody1 = json_utils:encode(RequestBody1),
    Before = time:seconds_to_datetime(global_clock:timestamp_seconds()),
    {ok, Code1, _Headers1, Response1} = do_request(Workers, FileName, put, RequestHeaders1, RawRequestBody1),
    After = time:seconds_to_datetime(global_clock:timestamp_seconds()),

    CdmiResponse1 = (json_utils:decode(Response1)),
    Metadata = maps:get(<<"metadata">>, CdmiResponse1),
    Metadata1 = Metadata,
    CTime1 = time:iso8601_to_datetime(maps:get(<<"cdmi_ctime">>, Metadata1)),
    ATime1 = time:iso8601_to_datetime(maps:get(<<"cdmi_atime">>, Metadata1)),
    MTime1 = time:iso8601_to_datetime(maps:get(<<"cdmi_mtime">>, Metadata1)),

    ?assertEqual(?HTTP_201_CREATED, Code1),
    ?assertMatch(#{<<"cdmi_size">> := <<"15">>}, Metadata1),

    ?assert(Before =< ATime1),
    ?assert(Before =< MTime1),
    ?assert(Before =< CTime1),
    ?assert(ATime1 =< After),
    ?assert(MTime1 =< After),
    ?assert(CTime1 =< After),
    ?assertMatch(UserId2, maps:get(<<"cdmi_owner">>, Metadata1)),
    ?assertMatch(#{<<"my_metadata">> := <<"my_value">>}, Metadata1),
    ?assertEqual(6, maps:size(Metadata1)),

    %%-- selective metadata read -----
    {ok, ?HTTP_200_OK, _Headers2, Response2} = do_request(
        Workers, FileName ++ "?metadata", get, RequestHeaders1, []
    ),
    CdmiResponse2 = (json_utils:decode(Response2)),
    Metadata2 = maps:get(<<"metadata">>, CdmiResponse2),
    ?assertEqual(1, maps:size(CdmiResponse2)),
    ?assertEqual(6, maps:size(Metadata2)),

    %%-- selective metadata read with prefix -----
    {ok, ?HTTP_200_OK, _Headers3, Response3} = do_request(
        Workers, FileName ++ "?metadata:cdmi_", get, RequestHeaders1, []
    ),
    CdmiResponse3 = (json_utils:decode(Response3)),
    Metadata3 = maps:get(<<"metadata">>, CdmiResponse3),
    ?assertEqual(1, maps:size(CdmiResponse3)),
    ?assertEqual(5, maps:size(Metadata3)),

    {ok, ?HTTP_200_OK, _Headers4, Response4} = do_request(
        Workers, FileName ++ "?metadata:cdmi_o", get, RequestHeaders1, []
    ),
    CdmiResponse4 = json_utils:decode(Response4),
    Metadata4 = maps:get(<<"metadata">>, CdmiResponse4),
    ?assertEqual(1, maps:size(CdmiResponse4)),
    ?assertMatch(UserId2, maps:get(<<"cdmi_owner">>, Metadata4)),
    ?assertEqual(1, maps:size(Metadata4)),

    {ok, ?HTTP_200_OK, _Headers5, Response5} = do_request(
        Workers, FileName ++ "?metadata:cdmi_size", get, RequestHeaders1, []
    ),
    CdmiResponse5 = json_utils:decode(Response5),
    Metadata5 = maps:get(<<"metadata">>, CdmiResponse5),
    ?assertEqual(1, maps:size(CdmiResponse5)),
    ?assertMatch(#{<<"cdmi_size">> := <<"15">>}, Metadata5),
    ?assertEqual(1, maps:size(Metadata5)),

    {ok, ?HTTP_200_OK, _Headers6, Response6} = do_request(
        Workers, FileName ++ "?metadata:cdmi_no_such_metadata", get, RequestHeaders1, []
    ),
    CdmiResponse6 = json_utils:decode(Response6),
    ?assertEqual(1, maps:size(CdmiResponse6)),
    ?assertMatch(#{<<"metadata">> := #{}}, CdmiResponse6),

    %%------ update user metadata of a file ----------
    RequestBody7 = #{<<"metadata">> => #{<<"my_new_metadata">> => <<"my_new_value">>}},
    RawRequestBody7 = json_utils:encode(RequestBody7),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers, FileName, put, RequestHeaders1, RawRequestBody7
    ),
    {ok, ?HTTP_200_OK, _Headers7, Response7} = do_request(
        Workers, FileName ++ "?metadata:my", get, RequestHeaders1, []
    ),
    CdmiResponse7 = (json_utils:decode(Response7)),
    Metadata7 = maps:get(<<"metadata">>, CdmiResponse7),
    ?assertEqual(1, maps:size(CdmiResponse7)),
    ?assertMatch(#{<<"my_new_metadata">> := <<"my_new_value">>}, Metadata7),
    ?assertEqual(1, maps:size(Metadata7)),

    RequestBody8 = #{<<"metadata">> =>
                     #{<<"my_new_metadata_add">> => <<"my_new_value_add">>,
                       <<"my_new_metadata">> => <<"my_new_value_update">>,
                       <<"cdmi_not_allowed">> => <<"my_value">>}},
    RawRequestBody8 = json_utils:encode(RequestBody8),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers,
        FileName ++ "?metadata:my_new_metadata_add;metadata:my_new_metadata;metadata:cdmi_not_allowed",
        put,
        RequestHeaders1,
        RawRequestBody8
    ),
    {ok, ?HTTP_200_OK, _Headers8, Response8} = do_request(
        Workers, FileName ++ "?metadata:my", get, RequestHeaders1, []
    ),
    CdmiResponse8 = (json_utils:decode(Response8)),
    Metadata8 = maps:get(<<"metadata">>, CdmiResponse8),
    ?assertEqual(1, maps:size(CdmiResponse8)),
    ?assertMatch(#{<<"my_new_metadata_add">> := <<"my_new_value_add">>}, Metadata8),
    ?assertMatch(#{<<"my_new_metadata">> := <<"my_new_value_update">>}, Metadata8),
    ?assertEqual(2, maps:size(Metadata8)),
    {ok, ?HTTP_200_OK, _Headers9, Response9} = do_request(
        Workers, FileName ++ "?metadata:cdmi_", get, RequestHeaders1, []
    ),
    CdmiResponse9 = (json_utils:decode(Response9)),
    Metadata9 = maps:get(<<"metadata">>, CdmiResponse9),
    ?assertEqual(1, maps:size(CdmiResponse9)),
    ?assertEqual(5, maps:size(Metadata9)),

    RequestBody10 = #{<<"metadata">> => #{<<"my_new_metadata">> => <<"my_new_value_ignore">>}},
    RawRequestBody10 = json_utils:encode(RequestBody10),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers, FileName ++ "?metadata:my_new_metadata_add", put, RequestHeaders1, RawRequestBody10
    ),
    {ok, ?HTTP_200_OK, _Headers10, Response10} = do_request(
        Workers, FileName ++ "?metadata:my", get, RequestHeaders1, []
    ),
    CdmiResponse10 = (json_utils:decode(Response10)),
    Metadata10 = maps:get(<<"metadata">>, CdmiResponse10),
    ?assertEqual(1, maps:size(CdmiResponse10)),
    ?assertMatch(#{<<"my_new_metadata">> := <<"my_new_value_update">>}, Metadata10),
    ?assertEqual(1, maps:size(Metadata10)),

    %%------ create directory with user metadata  ----------
    RequestHeaders2 = [?CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody11 = #{<<"metadata">> => #{<<"my_metadata">> => <<"my_dir_value">>}},
    RawRequestBody11 = json_utils:encode(RequestBody11),
    {ok, ?HTTP_201_CREATED, _Headers11, Response11} = do_request(
        Workers, DirName, put, RequestHeaders2, RawRequestBody11
    ),
    CdmiResponse11 = (json_utils:decode(Response11)),
    Metadata11 = maps:get(<<"metadata">>, CdmiResponse11),
    ?assertMatch(#{<<"my_metadata">> := <<"my_dir_value">>}, Metadata11),

    %%------ update user metadata of a directory ----------
    RequestBody12 = #{<<"metadata">> => #{<<"my_metadata">> => <<"my_dir_value_update">>}},
    RawRequestBody12 = json_utils:encode(RequestBody12),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers, DirName, put, RequestHeaders2, RawRequestBody12
    ),
    {ok, ?HTTP_200_OK, _Headers13, Response13} = do_request(
        Workers, DirName ++ "?metadata:my", get, RequestHeaders1, []
    ),
    CdmiResponse13 = (json_utils:decode(Response13)),
    Metadata13 = maps:get(<<"metadata">>, CdmiResponse13),

    ?assertEqual(1, maps:size(CdmiResponse13)),
    ?assertMatch(#{<<"my_metadata">> := <<"my_dir_value_update">>}, Metadata13),
    ?assertEqual(1, maps:size(Metadata13)),
    %%------------------------------

    %%------ write acl metadata ----------
    FileName2 = filename:join([RootPath, "acl_test_file.txt"]),
    Ace1 = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?read_all_object_mask
    }, cdmi),
    Ace2 = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?write_all_object_mask
    }, cdmi),
    Ace2Full = #{
        <<"acetype">> => ?allow,
        <<"identifier">> => <<UserName2/binary, "#", UserId2/binary>>,
        <<"aceflags">> => ?no_flags,
        <<"acemask">> => <<
            ?write_object/binary, ",",
            ?write_metadata/binary, ",",
            ?write_attributes/binary, ",",
            ?delete/binary, ",",
            ?write_acl/binary
        >>
    },
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"acl_test_file.txt">>,
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),

    RequestBody15 = #{<<"metadata">> => #{<<"cdmi_acl">> => [Ace1, Ace2Full]}},
    RawRequestBody15 = json_utils:encode(RequestBody15),
    RequestHeaders15 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],

    {ok, Code15, _Headers15, Response15} = do_request(
        Workers, FileName2 ++ "?metadata:cdmi_acl", put, RequestHeaders15, RawRequestBody15
    ),
    ?assertMatch({?HTTP_204_NO_CONTENT, _}, {Code15, Response15}),

    {ok, Code16, _Headers16, Response16} = do_request(
        Workers, FileName2 ++ "?metadata", get, RequestHeaders1, []
    ),
    CdmiResponse16 = (json_utils:decode(Response16)),
    Metadata16 = maps:get(<<"metadata">>, CdmiResponse16),
    ?assertEqual(?HTTP_200_OK, Code16),
    ?assertEqual(1, maps:size(CdmiResponse16)),
    ?assertEqual(6, maps:size(Metadata16)),
    ?assertMatch(#{<<"cdmi_acl">> := [Ace1, Ace2]}, Metadata16),

    {ok, Code17, _Headers17, Response17} = do_request(
        Workers, FileName2, get, [user_2_token_header()], []
    ),
    ?assertEqual(?HTTP_200_OK, Code17),
    ?assertEqual(<<"data">>, Response17),
    %%------------------------------

    %%-- create forbidden by acl ---
    Ace3 = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?write_metadata_mask
    }, cdmi),
    Ace4 = ace:to_json(#access_control_entity{
        acetype = ?deny_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?write_object_mask
    }, cdmi),
    RequestBody18 = #{<<"metadata">> => #{<<"cdmi_acl">> => [Ace3, Ace4]}},
    RawRequestBody18 = json_utils:encode(RequestBody18),
    RequestHeaders18 = [user_2_token_header(), ?CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER],

    {ok, Code18, _Headers18, _Response18} = do_request(
        Workers, DirName ++ "?metadata:cdmi_acl", put, RequestHeaders18, RawRequestBody18
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code18),

    {ok, Code19, _Headers19, Response19} = do_request(
        Workers, filename:join(DirName, "some_file"), put, [user_2_token_header()], []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError, {Code19, json_utils:decode(Response19)}).
%%------------------------------


% Tests cdmi object DELETE requests
delete_file(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    FileName = filename:join([RootPath, "toDelete.txt"]),
    GroupFileName =
        filename:join([RootPath, "groupFile"]),

    %%----- basic delete -----------
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"toDelete.txt">>
        }, Config#cdmi_test_config.p1_selector
    ),
    RequestHeaders1 = [?CDMI_VERSION_HEADER],
    {ok, Code1, _Headers1, _Response1} = do_request(
            Workers, FileName, delete, [user_2_token_header() | RequestHeaders1]
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code1),
    ?assert(not object_exists(FileName, Config)),
    %%------------------------------

    %%----- delete group file ------
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"groupFile">>
        }, Config#cdmi_test_config.p1_selector
    ),
    RequestHeaders2 = [?CDMI_VERSION_HEADER],
    {ok, Code2, _Headers2, _Response2} =
        do_request(Workers, GroupFileName, delete,
            [user_2_token_header() | RequestHeaders2]),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code2),
    ?assert(not object_exists(GroupFileName, Config)).
%%------------------------------

% Tests cdmi container DELETE requests
delete_dir(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    DirName = filename:join([RootPath, "toDelete"]) ++ "/",
    ChildDirName = filename:join([RootPath, "toDelete", "child"]) ++ "/",

    %%----- basic delete -----------
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = <<"toDelete">>
        }, Config#cdmi_test_config.p1_selector
    ),
    RequestHeaders1 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code1, _Headers1, _Response1} =
        do_request(Workers, DirName, delete, RequestHeaders1, []),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code1),
    ?assert(not object_exists(DirName, Config)),
    %%------------------------------

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = <<"toDelete">>,
            children = [
                #dir_spec{
                    name = <<"toDeleteChild">>
                }
            ]
        }, Config#cdmi_test_config.p1_selector
    ),

    RequestHeaders2 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code2, _Headers2, _Response2} = do_request(
        Workers, DirName, delete, RequestHeaders2, []
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code2),
    ?assert(not object_exists(DirName, Config)),
    ?assert(not object_exists(ChildDirName, Config)),
    %%------------------------------

    %%----- delete root dir -------
    RequestHeaders3 = [user_2_token_header(), ?CDMI_VERSION_HEADER],
    ?assert(object_exists("/", Config)),

    {ok, Code3, _Headers3, Response3} = do_request(
        Workers, "/", delete, RequestHeaders3, []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EPERM)),
    ?assertMatch(ExpRestError, {Code3, json_utils:decode(Response3)}),
    ?assert(object_exists("/", Config)).
%%------------------------------


% Tests cdmi object PUT requests (updating content)
update_file(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {_SpaceName, _ShortTestDirName, _TestDirName, _TestFileName, FullTestFileName, TestFileContent} =
        create_test_dir_and_file(Config),

    NewValue = <<"New Value!">>,
    UpdatedValue = <<"123 Value!">>,

    %%--- value replace, cdmi ------
    ?assert(object_exists(FullTestFileName, Config)),
    ?assertEqual(TestFileContent, get_file_content(FullTestFileName, Config), ?ATTEMPTS),

    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody1 = #{<<"value">> => NewValue},
    RawRequestBody1 = json_utils:encode(RequestBody1),

    {ok, Code1, _Headers1, _Response1} = do_request(
        Workers, FullTestFileName, put, RequestHeaders1, RawRequestBody1
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code1),
    ?assert(object_exists(FullTestFileName, Config)),
    ?assertEqual(NewValue, get_file_content(FullTestFileName, Config), ?ATTEMPTS),
    %%------------------------------

    %%---- value update, cdmi ------
    UpdateValue = <<"123">>,
    RequestHeaders2 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody2 = #{<<"value">> => base64:encode(UpdateValue)},
    RawRequestBody2 = json_utils:encode(RequestBody2),
    {ok, Code2, _Headers2, _Response2} = do_request(
        Workers, FullTestFileName ++ "?value:0-2", put, RequestHeaders2, RawRequestBody2
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code2),
    ?assert(object_exists(FullTestFileName, Config)),
    ?assertEqual(UpdatedValue, get_file_content(FullTestFileName, Config), ?ATTEMPTS),
    %%------------------------------

    %%--- value replace, http ------
    RequestBody3 = TestFileContent,
    {ok, Code3, _Headers3, _Response3} =
        do_request(Workers, FullTestFileName, put, [user_2_token_header()], RequestBody3),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code3),
    ?assert(object_exists(FullTestFileName, Config)),
    ?assertEqual(TestFileContent,
        get_file_content(FullTestFileName, Config), ?ATTEMPTS),
    %%------------------------------

    %%---- value update, http ------
    UpdateValue = <<"123">>,
    RequestHeaders4 = [{?HDR_CONTENT_RANGE, <<"bytes 0-2/3">>}],
    {ok, Code4, _Headers4, _Response4} =
        do_request(Workers, FullTestFileName,
            put, [user_2_token_header() | RequestHeaders4], UpdateValue),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code4),
    ?assert(object_exists(FullTestFileName, Config)),
    ?assertEqual(<<"123t_file_content">>,
        get_file_content(FullTestFileName, Config), ?ATTEMPTS),
    %%------------------------------

    %%---- value update2, http -----
    UpdateValue2 = <<"00">>,
    RequestHeaders5 = [{?HDR_CONTENT_RANGE, <<"bytes 3-4/*">>}],
    {ok, Code5, _Headers5, _Response5} =
        do_request(Workers, FullTestFileName,
            put, [user_2_token_header() | RequestHeaders5], UpdateValue2),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code5),
    ?assert(object_exists(FullTestFileName, Config)),
    ?assertEqual(<<"12300file_content">>,
        get_file_content(FullTestFileName, Config), ?ATTEMPTS),
    %%------------------------------

    %%---- value update, http error ------
    UpdateValue = <<"123">>,
    RequestHeaders6 = [{?HDR_CONTENT_RANGE, <<"bytes 0-2,3-4/*">>}],
    {ok, Code6, _Headers6, Response6} =
        do_request(Workers, FullTestFileName, put, [user_2_token_header() | RequestHeaders6],
            UpdateValue),

    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(?HDR_CONTENT_RANGE)),
    ?assertMatch(ExpRestError, {Code6, json_utils:decode(Response6)}),
    ?assert(object_exists(FullTestFileName, Config)),
    ?assertEqual(<<"12300file_content">>,
        get_file_content(FullTestFileName, Config), ?ATTEMPTS).
%%------------------------------


% tests if capabilities of objects, containers, and whole storage system are set properly
capabilities(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],

    %%--- system capabilities ------
    RequestHeaders8 = [?CDMI_VERSION_HEADER],
    {ok, Code8, Headers8, Response8} =
        do_request(Workers, "cdmi_capabilities/", get, RequestHeaders8, []),

    ?assertEqual(?HTTP_200_OK, Code8),
    CdmiResponse8 = (json_utils:decode(Response8)),
    Capabilities = maps:get(<<"capabilities">>, CdmiResponse8),
    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/cdmi-capability">>}, Headers8),
    ?assertMatch(#{<<"objectID">> := ?ROOT_CAPABILITY_ID}, CdmiResponse8),
    ?assertMatch(#{<<"objectName">> := <<?ROOT_CAPABILITY_PATH>>}, CdmiResponse8),
    ?assertMatch(#{<<"childrenrange">> := <<"0-1">>}, CdmiResponse8),
    ?assertMatch(#{<<"children">> := [<<"container/">>, <<"dataobject/">>]}, CdmiResponse8),
    ?assertEqual(?ROOT_CAPABILITY_MAP, Capabilities),
    %%------------------------------

    %%-- container capabilities ----
    RequestHeaders9 = [?CDMI_VERSION_HEADER],
    {ok, Code9, _Headers9, Response9} =
        do_request(Workers, "cdmi_capabilities/container/", get, RequestHeaders9, []),
    ?assertEqual(?HTTP_200_OK, Code9),
    ?assertMatch(
        {ok, Code9, _, Response9},
        do_request(
            Workers, "cdmi_objectid/" ++ binary_to_list(?CONTAINER_CAPABILITY_ID) ++ "/", get, RequestHeaders9, []
        )
    ),

    CdmiResponse9 = (json_utils:decode(Response9)),
    Capabilities2 = maps:get(<<"capabilities">>, CdmiResponse9),

    ?assertMatch(#{<<"parentURI">> := <<?ROOT_CAPABILITY_PATH>>}, CdmiResponse9),
    ?assertMatch(#{<<"parentID">> := ?ROOT_CAPABILITY_ID}, CdmiResponse9),
    ?assertMatch(#{<<"objectID">> := ?CONTAINER_CAPABILITY_ID}, CdmiResponse9),
    ?assertMatch(#{<<"objectName">> := <<"container/">>}, CdmiResponse9),
    ?assertEqual(?CONTAINER_CAPABILITY_MAP, Capabilities2),
    %%------------------------------

    %%-- dataobject capabilities ---
    RequestHeaders10 = [?CDMI_VERSION_HEADER],
    {ok, Code10, _Headers10, Response10} =
        do_request(Workers, "cdmi_capabilities/dataobject/", get, RequestHeaders10, []),
    ?assertEqual(?HTTP_200_OK, Code10),
    ?assertMatch(
        {ok, Code10, _, Response10},
        do_request(
            Workers, "cdmi_objectid/" ++ binary_to_list(?DATAOBJECT_CAPABILITY_ID) ++ "/", get, RequestHeaders10, []
        )
    ),

    CdmiResponse10 = (json_utils:decode(Response10)),
    Capabilities3 = maps:get(<<"capabilities">>, CdmiResponse10),

    ?assertMatch(#{<<"parentURI">> := <<?ROOT_CAPABILITY_PATH>>}, CdmiResponse10),
    ?assertMatch(#{<<"parentID">> := ?ROOT_CAPABILITY_ID}, CdmiResponse10),
    ?assertMatch(#{<<"objectID">> := ?DATAOBJECT_CAPABILITY_ID}, CdmiResponse10),
    ?assertMatch(#{<<"objectName">> := <<"dataobject/">>}, CdmiResponse10),
    ?assertEqual(?DATAOBJECT_CAPABILITY_MAP, Capabilities3).
%%------------------------------


use_supported_cdmi_version(Config) ->
    % given
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    RequestHeaders = [?CDMI_VERSION_HEADER, user_2_token_header()],

    % when
    {ok, Code, _ResponseHeaders, _Response} =
        do_request(Workers, "/random", get, RequestHeaders),

    % then
    ?assertEqual(Code, ?HTTP_404_NOT_FOUND).


use_unsupported_cdmi_version(Config) ->
    % given
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    RequestHeaders = [{<<"X-CDMI-Specification-Version">>, <<"1.0.2">>}],

    % when
    {ok, Code, _ResponseHeaders, Response} =
        do_request(Workers, "/random", get, RequestHeaders),

    % then
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>])),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


copy(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    %%---------- file cp ----------- (copy file, with xattrs and acl)
    % create file to copy
    FileName2 = filename:join([RootPath, "copy_test_file.txt"]),
    UserId2 = oct_background:get_user_id(user2),
    UserName2 = <<"Unnamed User">>,
    FileData2 = <<"data">>,
    JsonMetadata = #{<<"a">> => <<"b">>, <<"c">> => 2, <<"d">> => []},
    Xattrs = #{<<"key1">> => <<"value1">>, <<"key2">> => <<"value2">>},
    FileAcl = [#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?all_object_perms_mask
    }],
    NewFileName2 = filename:join([RootPath, "copy_test_file2.txt"]),

    #object{guid = FileGuid} = onenv_file_test_utils:create_and_sync_file_tree(
        user2,
        node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"copy_test_file.txt">>,
            content = FileData2,
            metadata = #metadata_spec{
                json = JsonMetadata,
                xattrs = Xattrs
            }
        },
        Config#cdmi_test_config.p1_selector
    ),
    ok = set_acl(FileName2, FileAcl, Config),

    % assert source file is created and destination does not exist
    ?assert(object_exists(FileName2, Config)),
    ?assert(not object_exists(NewFileName2, Config)),
    ?assertEqual(FileData2, get_file_content(FileName2, Config), ?ATTEMPTS),
    ?assertEqual({ok, FileAcl}, get_acl(FileName2, Config), ?ATTEMPTS),

    % copy file using cdmi
    RequestHeaders4 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody4 = json_utils:encode(#{<<"copy">> => build_random_src_uri(FileName2, FileGuid)}),
    {ok, Code4, _Headers4, _Response4} = do_request(Workers, NewFileName2, put, RequestHeaders4, RequestBody4),
    ?assertEqual(?HTTP_201_CREATED, Code4),

    % assert new file is created
    ?assert(object_exists(FileName2, Config)),
    ?assert(object_exists(NewFileName2, Config)),
    ?assertEqual(FileData2, get_file_content(NewFileName2, Config), ?ATTEMPTS),
    ?assertEqual({ok, JsonMetadata}, get_json_metadata(NewFileName2, Config), ?ATTEMPTS),
    ?assertEqual([
        #xattr{name = <<"key1">>, value = <<"value1">>},
        #xattr{name = <<"key2">>, value = <<"value2">>},
        #xattr{name = ?JSON_METADATA_KEY, value = JsonMetadata}
    ], get_xattrs(NewFileName2, Config), ?ATTEMPTS
    ),
    ?assertEqual({ok, FileAcl}, get_acl(NewFileName2, Config), ?ATTEMPTS),
    %%------------------------------

    %%---------- dir cp ------------
    % create dir to copy (with some subdirs and subfiles)
    DirName2 = filename:join([RootPath, "copy_dir"]) ++ "/",
    NewDirName2 = filename:join([RootPath, "new_copy_dir"]) ++ "/",
    DirAcl = [#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?all_container_perms_mask
    }],
    #object{guid = DirGuid} = onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = <<"copy_dir">>,
            metadata = #metadata_spec{
                xattrs = Xattrs
            },
            children = [
                #dir_spec{
                    name = <<"dir1">>,
                    children = [
                        #file_spec{
                            name = <<"1">>
                        },
                        #file_spec{
                            name = <<"2">>
                        }
                    ]
                },
                #dir_spec{
                    name = <<"dir2">>
                },
                #file_spec{
                    name = <<"3">>
                }
            ]
        }, Config#cdmi_test_config.p1_selector
    ),
    ?assert(object_exists(DirName2, Config)),
    set_acl(DirName2, DirAcl, Config),

    % assert source files are successfully created, and destination file does not exist
    ?assert(object_exists(DirName2, Config)),
    ?assert(object_exists(filename:join(DirName2, "dir1"), Config)),
    ?assert(object_exists(filename:join(DirName2, "dir2"), Config)),
    ?assert(object_exists(filename:join([DirName2, "dir1", "1"]), Config)),
    ?assert(object_exists(filename:join([DirName2, "dir1", "2"]), Config)),
    ?assert(object_exists(filename:join(DirName2, "3"), Config)),
    ?assert(not object_exists(NewDirName2, Config)),

    % copy dir using cdmi
    RequestHeaders5 = [
        user_2_token_header(), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    RequestBody5 = json_utils:encode(#{
        <<"copy">> => build_random_src_uri(DirName2, DirGuid)
    }),
    {ok, Code5, _Headers5, _Response5} = do_request(
        Workers, NewDirName2, put, RequestHeaders5, RequestBody5
    ),
    ?assertEqual(?HTTP_201_CREATED, Code5),
    % assert source files still exists
    ?assert(object_exists(DirName2, Config)),
    ?assert(object_exists(filename:join(DirName2, "dir1"), Config)),
    ?assert(object_exists(filename:join(DirName2, "dir2"), Config)),
    ?assert(object_exists(filename:join([DirName2, "dir1", "1"]), Config)),
    ?assert(object_exists(filename:join([DirName2, "dir1", "2"]), Config)),
    ?assert(object_exists(filename:join(DirName2, "3"), Config)),

    % assert destination files have been created
    ?assert(object_exists(NewDirName2, Config)),
    ?assertEqual([
        #xattr{name = <<"key1">>, value = <<"value1">>},
        #xattr{name = <<"key2">>, value = <<"value2">>}
    ], get_xattrs(NewDirName2, Config), ?ATTEMPTS),
    ?assertEqual({ok, DirAcl}, get_acl(NewDirName2, Config)),
    ?assert(object_exists(filename:join(NewDirName2, "dir1"), Config)),
    ?assert(object_exists(filename:join(NewDirName2, "dir2"), Config)),
    ?assert(object_exists(filename:join([NewDirName2, "dir1", "1"]), Config)),
    ?assert(object_exists(filename:join([NewDirName2, "dir1", "2"]), Config)),
    ?assert(object_exists(filename:join(NewDirName2, "3"), Config)).
%%------------------------------


% tests copy and move operations on dataobjects and containers
move(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    FileName = filename:join([RootPath, "move_test_file.txt"]),
    DirName = filename:join([RootPath, "move_test_dir"]) ++ "/",
    FileData = <<"data">>,
    NewMoveFileName = filename:join([RootPath, "new_move_test_file"]),
    NewMoveDirName = filename:join([RootPath, "new_move_test_dir"]) ++ "/",

    [#object{guid = DirGuid}, #object{guid = FileGuid}]  =
        onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid), [
            #dir_spec{
                name = <<"move_test_dir">>
            },
            #file_spec{
                name = <<"move_test_file.txt">>,
                content = <<"data">>
            }
        ], Config#cdmi_test_config.p1_selector
        ),

    %%----------- dir mv -----------
    ?assert(object_exists(DirName, Config)),
    ?assert(not object_exists(NewMoveDirName, Config)),

    RequestHeaders2 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER],
    RequestBody2 = json_utils:encode(#{<<"move">> => build_random_src_uri(DirName, DirGuid)}),
    ?assertMatch(
        {ok, ?HTTP_201_CREATED, _Headers2, _Response2},
        do_request(Workers, NewMoveDirName, put, RequestHeaders2, RequestBody2)
    ),

    ?assert(not object_exists(DirName, Config)),
    ?assert(object_exists(NewMoveDirName, Config)),
    %%------------------------------

    %%---------- file mv -----------
    ?assert(object_exists(FileName, Config)),
    ?assert(not object_exists(NewMoveFileName, Config)),
    ?assertEqual(FileData, get_file_content(FileName, Config), ?ATTEMPTS),

    RequestHeaders3 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody3 = json_utils:encode(#{<<"move">> => build_random_src_uri(FileName, FileGuid)}),
    ?assertMatch(
        {ok, _Code3, _Headers3, _Response3},
        do_request(Workers, NewMoveFileName, put, RequestHeaders3, RequestBody3)
    ),
    ?assert(not object_exists(FileName, Config)),
    ?assert(object_exists(NewMoveFileName, Config)),
    ?assertEqual(FileData, get_file_content(NewMoveFileName, Config), ?ATTEMPTS).
%%------------------------------


% tests if cdmi returns 'moved permanently' code when we forget about '/' in path
moved_permanently(Config) ->
    [WorkerP2, _WorkerP1] = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    FileName = filename:join([RootPath, "somedir", "somefile.txt"]),
    DirNameWithoutSlash = filename:join([RootPath, "somedir"]),
    DirName = DirNameWithoutSlash ++ "/",
    FileNameWithSlash = FileName ++ "/",

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = <<"somedir">>,
            children = [
                #file_spec{
                    name = <<"somefile.txt">>
                }
            ]
        }
        , Config#cdmi_test_config.p1_selector
    ),

    Domain = oct_background:get_provider_domain(krakow),
    CDMIEndpoint = cdmi_test_utils:cdmi_endpoint(WorkerP2, Domain),
    %%--------- dir test -----------
    RequestHeaders1 = [
        ?CONTAINER_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        user_2_token_header()
    ],
    Location1 = list_to_binary(CDMIEndpoint ++ DirName),
    {ok, Code1, Headers1, _Response1} =
        do_request(WorkerP2, DirNameWithoutSlash, get, RequestHeaders1, []),
    ?assertEqual(?HTTP_302_FOUND, Code1),
    ?assertMatch(#{?HDR_LOCATION := Location1}, Headers1),
    %%------------------------------

    %%--------- dir test with QS-----------
    RequestHeaders2 = [
        ?CONTAINER_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        user_2_token_header()
    ],
    Location2 = list_to_binary(CDMIEndpoint ++ DirName ++ "?example_qs=1"),
    {ok, Code2, Headers2, _Response2} =
        do_request(WorkerP2, DirNameWithoutSlash ++ "?example_qs=1", get, RequestHeaders2, []),
    ?assertEqual(?HTTP_302_FOUND, Code2),
    ?assertMatch(#{?HDR_LOCATION := Location2}, Headers2),
    %%------------------------------

    %%--------- file test ----------
    RequestHeaders3 = [
        ?OBJECT_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        user_2_token_header()
    ],
    Location3 = list_to_binary(CDMIEndpoint ++ FileName),
    {ok, Code3, Headers3, _Response3} =
        do_request(WorkerP2, FileNameWithSlash, get, RequestHeaders3, []),
    ?assertEqual(?HTTP_302_FOUND, Code3),
    ?assertMatch(#{?HDR_LOCATION := Location3}, Headers3).
%%------------------------------


move_copy_conflict(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    FileName = filename:join([RootPath, "move_copy_conflict.txt"]),
    FileUri = list_to_binary(filename:join("/", FileName)),
    FileData = <<"data">>,
    NewMoveFileName = "new_move_copy_conflict",
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"move_copy_conflict.txt">>,
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),

    %%--- conflicting mv/cpy ------- (we cannot move and copy at the same time)
    ?assertEqual(FileData, get_file_content(FileName, Config), ?ATTEMPTS),

    RequestHeaders1 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody1 = json_utils:encode(#{<<"move">> => FileUri, <<"copy">> => FileUri}),
    {ok, Code1, _Headers1, Response1} = do_request(
        Workers, NewMoveFileName, put, RequestHeaders1, RequestBody1
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_MALFORMED_DATA),
    ?assertMatch(ExpRestError, {Code1, json_utils:decode(Response1)}),
    ?assertEqual(FileData, get_file_content(FileName, Config), ?ATTEMPTS).
%%------------------------------


% tests req format checking
request_format_check(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    FileToCreate = filename:join([RootPath, "file.txt"]),
    DirToCreate = filename:join([RootPath, "dir"]) ++ "/",
    FileContent = <<"File content!">>,

    %%-- obj missing content-type --
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody1 = #{<<"value">> => FileContent},
    RawRequestBody1 = json_utils:encode(RequestBody1),
    {ok, Code1, _Headers1, _Response1} = do_request(Workers, FileToCreate, put, RequestHeaders1, RawRequestBody1),
    ?assertEqual(?HTTP_201_CREATED, Code1),
    %%------------------------------

    %%-- dir missing content-type --
    RequestHeaders3 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody3 = #{<<"metadata">> => <<"">>},
    RawRequestBody3 = json_utils:encode(RequestBody3),
    {ok, Code3, _Headers3, _Response3} = do_request(Workers, DirToCreate, put, RequestHeaders3, RawRequestBody3),
    ?assertEqual(?HTTP_201_CREATED, Code3).
%%------------------------------


% tests mimetype and valuetransferencoding properties, they are part of cdmi-object and cdmi-container
% and should be changeble
mimetype_and_encoding(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {SpaceName, _ShortTestDirName, TestDirName, TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    %% get mimetype and valuetransferencoding of non-cdmi file
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code1, _Headers1, Response1} = do_request(
        Workers,
        filename:join(TestDirName, TestFileName) ++ "?mimetype;valuetransferencoding",
        get,
        RequestHeaders1,
        []
    ),
    ?assertEqual(?HTTP_200_OK, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    ?assertMatch(#{<<"mimetype">> := <<"application/octet-stream">>}, CdmiResponse1),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"base64">>}, CdmiResponse1),
    %%------------------------------

    %%-- update mime and encoding --
    RequestHeaders2 = [?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER, user_2_token_header()],
    RawBody2 = json_utils:encode(#{
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"application/binary">>
    }),
    {ok, Code2, _Headers2, _Response2} = do_request(
        Workers, filename:join(TestDirName, TestFileName), put, RequestHeaders2, RawBody2
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code2),

    {ok, Code3, _Headers3, Response3} = do_request(
        Workers,
        filename:join(TestDirName, TestFileName) ++ "?mimetype;valuetransferencoding",
        get,
        RequestHeaders2,
        []
    ),
    ?assertEqual(?HTTP_200_OK, Code3),
    CdmiResponse3 = (json_utils:decode(Response3)),
    ?assertMatch(#{<<"mimetype">> := <<"application/binary">>}, CdmiResponse3),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>}, CdmiResponse3),
    %%------------------------------

    %% create file with given mime and encoding
    FileName4 = filename:join([RootPath, "mime_file.txt"]),
    FileContent4 = <<"some content">>,
    RequestHeaders4 = [?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER, user_2_token_header()],
    RawBody4 = json_utils:encode(#{
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"text/plain">>,
        <<"value">> => FileContent4
    }),
    {ok, Code4, _Headers4, Response4} = do_request(
        Workers, FileName4, put, RequestHeaders4, RawBody4
    ),
    ?assertEqual(?HTTP_201_CREATED, Code4),
    CdmiResponse4 = (json_utils:decode(Response4)),
    ?assertMatch(#{<<"mimetype">> := <<"text/plain">>}, CdmiResponse4),

    RequestHeaders5 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code5, _Headers5, Response5} = do_request(
        Workers, FileName4 ++ "?value;mimetype;valuetransferencoding", get, RequestHeaders5, []
    ),
    ?assertEqual(?HTTP_200_OK, Code5),
    CdmiResponse5 = (json_utils:decode(Response5)),
    ?assertMatch(#{<<"mimetype">> := <<"text/plain">>}, CdmiResponse5),

    %TODO VFS-7376 what do we return here if file contains valid utf-8 string and we read byte range?
    ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>}, CdmiResponse5),
    ?assertMatch(#{<<"value">> := FileContent4}, CdmiResponse5),
    %%------------------------------

    %% create file with given mime and encoding using non-cdmi request
    FileName6 = filename:join([RootPath, "mime_file_noncdmi.txt"]),
    FileContent6 = <<"some content">>,
    RequestHeaders6 = [{?HDR_CONTENT_TYPE, <<"text/plain; charset=utf-8">>}, user_2_token_header()],
    {ok, Code6, _Headers6, _Response6} = do_request(Workers, FileName6, put, RequestHeaders6, FileContent6),
    ?assertEqual(?HTTP_201_CREATED, Code6),

    RequestHeaders7 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code7, _Headers7, Response7} = do_request(
        Workers, FileName6 ++ "?value;mimetype;valuetransferencoding", get, RequestHeaders7, []
    ),
    ?assertEqual(?HTTP_200_OK, Code7),
    CdmiResponse7 = (json_utils:decode(Response7)),
    ?assertMatch(#{<<"mimetype">> := <<"text/plain">>}, CdmiResponse7),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>}, CdmiResponse7),
    ?assertMatch(#{<<"value">> := FileContent6}, CdmiResponse7).
%%------------------------------


% tests reading&writing file at random ranges
out_of_range(Config) ->
    Workers = oct_background:get_provider_nodes(Config#cdmi_test_config.p1_selector),
    {SpaceName, _ShortTestDirName, TestDirName, _TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    FileName = filename:join([RootPath, "random_range_file.txt"]),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"random_range_file.txt">>
        }, Config#cdmi_test_config.p1_selector
    ),

    %%---- reading out of range ---- (shuld return empty binary)
    ?assertEqual(<<>>, get_file_content(FileName, Config), ?ATTEMPTS),
    RequestHeaders1 = [user_2_token_header(), ?CDMI_VERSION_HEADER],
    RequestBody1 = json_utils:encode(#{<<"value">> => <<"data">>}),

    {ok, Code1, _Headers1, Response1} = do_request(
        Workers, FileName ++ "?value:0-3", get, RequestHeaders1, RequestBody1
    ),
    ?assertEqual(?HTTP_200_OK, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    ?assertMatch(#{<<"value">> := <<>>}, CdmiResponse1),
    %%------------------------------

    %%------ writing at end -------- (shuld extend file)
    ?assertEqual(<<>>, get_file_content(FileName, Config), ?ATTEMPTS),

    RequestHeaders2 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody2 = json_utils:encode(#{<<"value">> => base64:encode(<<"data">>)}),
    {ok, Code2, _Headers2, _Response2} = do_request(
        Workers, FileName ++ "?value:0-3", put, RequestHeaders2, RequestBody2
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code2),
    ?assertEqual(<<"data">>, get_file_content(FileName, Config), ?ATTEMPTS),
    %%------------------------------

    %%------ writing at random -------- (should return zero bytes in any gaps)
     RequestBody3 = json_utils:encode(#{<<"value">> => base64:encode(<<"data">>)}),
     {ok, Code3, _Headers3, _Response3} = do_request(
         Workers, FileName ++ "?value:10-13", put, RequestHeaders2, RequestBody3
     ),
     ?assertEqual(?HTTP_204_NO_CONTENT, Code3),

    % "data(6x<0_byte>)data"
     ?assertEqual(
         <<100, 97, 116, 97, 0, 0, 0, 0, 0, 0, 100, 97, 116, 97>>,
         get_file_content(FileName, Config),
         ?ATTEMPTS
     ),
    %%------------------------------

    %%----- random childrange ------ (shuld fail)
    {ok, Code4, _Headers4, Response4} = do_request(
        Workers, TestDirName ++ "/?children:100-132", get, RequestHeaders2, []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(<<"childrenrange">>)),
    ?assertMatch(ExpRestError, {Code4, json_utils:decode(Response4)}).
%%------------------------------


% tests cdmi and non-cdmi partial upload feature (requests with x-cdmi-partial flag set to true)
partial_upload(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    FileName = filename:join([RootPath, "partial.txt"]),
    FileName2 = filename:join([RootPath, "partial2.txt"]),
    Chunk1 = <<"some">>,
    Chunk2 = <<"_">>,
    Chunk3 = <<"value">>,

    %%------ cdmi request partial upload ------
    ?assert(not object_exists(FileName, Config)),

    % upload first chunk of file
    RequestHeaders1 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER,
        {"X-CDMI-Partial", "true"}
    ],
    RequestBody1 = json_utils:encode(#{<<"value">> => Chunk1}),
    {ok, Code1, _Headers1, Response1} = do_request(
        Workers, FileName, put, RequestHeaders1, RequestBody1
    ),
    ?assertEqual(?HTTP_201_CREATED, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    ?assertMatch(#{<<"completionStatus">> := <<"Processing">>}, CdmiResponse1),

    % upload second chunk of file
    RequestBody2 = json_utils:encode(#{<<"value">> => base64:encode(Chunk2)}),
    {ok, Code2, _Headers2, _Response2} = do_request(
        Workers, FileName ++ "?value:4-4", put, RequestHeaders1, RequestBody2
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code2),

    % upload third chunk of file
    RequestHeaders3 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody3 = json_utils:encode(#{<<"value">> => base64:encode(Chunk3)}),
    {ok, Code3, _Headers3, _Response3} = do_request(
        Workers, FileName ++ "?value:5-9", put, RequestHeaders3, RequestBody3
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code3),

    % get created file and check its consistency
    RequestHeaders4 = [user_2_token_header(), ?CDMI_VERSION_HEADER],
    % TODO Verify once after VFS-2023
    CheckAllChunks = fun() ->
        {ok, Code4, _Headers4, Response4} = do_request(Workers, FileName, get, RequestHeaders4, []),
        ?assertEqual(?HTTP_200_OK, Code4),
        CdmiResponse4 = (json_utils:decode(Response4)),
        ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse4),
        ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>}, CdmiResponse4),
        maps:get(<<"value">>, CdmiResponse4)
    end,
    % File size event change is async
    Chunks123 = <<Chunk1/binary, Chunk2/binary, Chunk3/binary>>,
    ?assertMatch(Chunks123, CheckAllChunks(), 2),
    %%------------------------------

    %%----- non-cdmi request partial upload -------
    ?assert(not object_exists(FileName2, Config)),

    % upload first chunk of file
    RequestHeaders5 = [user_2_token_header(), {<<"X-CDMI-Partial">>, <<"true">>}],
    {ok, Code5, _Headers5, _Response5} = do_request(
        Workers, FileName2, put, RequestHeaders5, Chunk1
    ),
    ?assertEqual(?HTTP_201_CREATED, Code5),

    % check "completionStatus", should be set to "Processing"
    {ok, Code5_1, _Headers5_1, Response5_1} = do_request(
        Workers, FileName2 ++ "?completionStatus", get, RequestHeaders4, Chunk1
    ),
    ?assertEqual(?HTTP_200_OK, Code5_1),
    CdmiResponse5_1 = (json_utils:decode(Response5_1)),
    ?assertMatch(#{<<"completionStatus">> := <<"Processing">>}, CdmiResponse5_1),

    % upload second chunk of file
    RequestHeaders6 = [
        user_2_token_header(),
        {?HDR_CONTENT_RANGE, <<"bytes 4-4/10">>}, {<<"X-CDMI-Partial">>, <<"true">>}
    ],
    {ok, Code6, _Headers6, _Response6} = do_request(
        Workers, FileName2, put, RequestHeaders6, Chunk2
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code6),

    % upload third chunk of file
    RequestHeaders7 = [
        user_2_token_header(),
        {?HDR_CONTENT_RANGE, <<"bytes 5-9/10">>},
        {<<"X-CDMI-Partial">>, <<"false">>}
    ],
    {ok, Code7, _Headers7, _Response7} = do_request(
        Workers, FileName2, put, RequestHeaders7, Chunk3
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code7),

    % get created file and check its consistency
    RequestHeaders8 = [user_2_token_header(), ?CDMI_VERSION_HEADER],
    % TODO Verify once after VFS-2023
    CheckAllChunks2 = fun() ->
        {ok, Code8, _Headers8, Response8} = do_request(Workers, FileName2, get, RequestHeaders8, []),
        ?assertEqual(?HTTP_200_OK, Code8),
        CdmiResponse8 = (json_utils:decode(Response8)),
        ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse8),
        base64:decode(maps:get(<<"value">>, CdmiResponse8))
    end,
    % File size event change is async
    ?assertMatch(Chunks123, CheckAllChunks2(), 2).
%%------------------------------


% tests access control lists
acl(Config) ->
    [_WorkerP2, WorkerP1] = Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    Filename1 = filename:join([RootPath, "acl_test_file1"]),
    Dirname1 = filename:join([RootPath, "acl_test_dir1"]) ++ "/",
    UserId2 = oct_background:get_user_id(user2),
    UserName2 = <<"Unnamed User">>,
    Identifier1 = <<UserName2/binary, "#", UserId2/binary>>,

    Read = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?read_all_object_mask
    }, cdmi),
    ReadFull = #{
        <<"acetype">> => ?allow,
        <<"identifier">> => Identifier1,
        <<"aceflags">> => ?no_flags,
        <<"acemask">> => <<
            ?read_object/binary, ",",
            ?read_metadata/binary, ",",
            ?read_attributes/binary, ",",
            ?read_acl/binary
        >>
    },
    Write = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?write_all_object_mask
    }, cdmi),
    ReadWriteVerbose = #{
        <<"acetype">> => ?allow,
        <<"identifier">> => Identifier1,
        <<"aceflags">> => ?no_flags,
        <<"acemask">> => <<
            ?read_object/binary, ",",
            ?read_metadata/binary, ",",
            ?read_attributes/binary, ",",
            ?read_acl/binary, ",",
            ?write_object/binary, ",",
            ?write_metadata/binary, ",",
            ?write_attributes/binary, ",",
            ?delete/binary, ",",
            ?write_acl/binary
        >>
    },
    WriteAcl = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?write_acl_mask
    }, cdmi),
    Delete = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?delete_mask
    }, cdmi),

    MetadataAclReadFull = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [ReadFull, WriteAcl]}}),
    MetadataAclDelete = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [Delete]}}),
    MetadataAclWrite = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [Write]}}),
    MetadataAclReadWrite = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [Write, Read]}}),
    MetadataAclReadWriteFull = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [ReadWriteVerbose]}}),

    %%----- read file test ---------
    % create test file with dummy data
    ?assert(not object_exists(Filename1, Config)),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"acl_test_file1">>,
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),

    EaccesError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),

    % set acl to 'write' and test cdmi/non-cdmi get request (should return 403 forbidden)
    RequestHeaders1 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(Workers, Filename1, put, RequestHeaders1, MetadataAclWrite),
    {ok, Code1, _, Response1} = do_request(Workers, Filename1, get, RequestHeaders1, []),
    ?assertMatch(EaccesError, {Code1, json_utils:decode(Response1)}),

    {ok, Code2, _, Response2} = do_request(Workers, Filename1, get, [user_2_token_header()], []),
    ?assertMatch(EaccesError, {Code2, json_utils:decode(Response2)}),
    ?assertEqual({error, ?EACCES}, open_file(Config#cdmi_test_config.p2_selector, Filename1, read, Config), ?ATTEMPTS),

    % set acl to 'read&write' and test cdmi/non-cdmi get request (should succeed)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers, Filename1, put, RequestHeaders1, MetadataAclReadWriteFull
    ),
    {ok, ?HTTP_200_OK, _, _} = do_request(Workers, Filename1, get, RequestHeaders1, []),
    {ok, ?HTTP_200_OK, _, _} = do_request(Workers, Filename1, get, [user_2_token_header()], []),
    %%------------------------------

    %%------- write file test ------
    % set acl to 'read&write' and test cdmi/non-cdmi put request (should succeed)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers, Filename1, put, RequestHeaders1, MetadataAclReadWrite
    ),
    RequestBody4 = json_utils:encode(#{<<"value">> => <<"new_data">>}),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(Workers, Filename1, put, RequestHeaders1, RequestBody4),
    ?assertEqual(<<"new_data">>, get_file_content(Filename1, Config), ?ATTEMPTS),

    write_to_file(Filename1, <<"1">>, 8, Config),
    ?assertEqual(<<"new_data1">>, get_file_content(Filename1, Config), ?ATTEMPTS),

    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers, Filename1, put, [user_2_token_header()], <<"new_data2">>
    ),
    ?assertEqual(<<"new_data2">>, get_file_content(Filename1, Config), ?ATTEMPTS),

    % set acl to 'read' and test cdmi/non-cdmi put request (should return 403 forbidden)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers, Filename1, put, RequestHeaders1, MetadataAclReadFull
    ),
    RequestBody6 = json_utils:encode(#{<<"value">> => <<"new_data3">>}),
    {ok, Code3, _, Response3} = do_request(Workers, Filename1, put, RequestHeaders1, RequestBody6),
    ?assertMatch(EaccesError, {Code3, json_utils:decode(Response3)}),

    {ok, Code4, _, Response4} = do_request(Workers, Filename1, put, [user_2_token_header()], <<"new_data4">>),
    ?assertMatch(EaccesError, {Code4, json_utils:decode(Response4)}),
    ?assertEqual(<<"new_data2">>, get_file_content(Filename1, Config), ?ATTEMPTS),
    ?assertEqual({error, ?EACCES}, open_file(Config#cdmi_test_config.p2_selector, Filename1, write, Config), ?ATTEMPTS),
    ?assertEqual(<<"new_data2">>, get_file_content(Filename1, Config), ?ATTEMPTS),
    %%------------------------------

    %%------ delete file test ------
    % set acl to 'delete'
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers, Filename1, put, RequestHeaders1, MetadataAclDelete
    ),

    % delete file
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers, Filename1, delete, [user_2_token_header()], []
    ),
    ?assert(not object_exists(Filename1, Config)),
    %%------------------------------

    %%--- read write dir test ------
    ?assert(not object_exists(Dirname1, Config)),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = <<"acl_test_dir1">>,
            children = [
                #file_spec{
                    name = <<"3">>
                }
            ]
        }, Config#cdmi_test_config.p1_selector
    ),
    File1 = filename:join(Dirname1, "1"),
    File2 = filename:join(Dirname1, "2"),
    File3 = filename:join(Dirname1, "3"),
    File4 = filename:join(Dirname1, "4"),

    DirRead = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?read_all_container_mask bor ?traverse_container_mask
    }, cdmi),
    DirWrite = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?write_all_container_mask bor ?traverse_container_mask
    }, cdmi),
    DirMetadataAclReadWrite = json_utils:encode(#{
        <<"metadata">> => #{<<"cdmi_acl">> => [DirWrite, DirRead]}
    }),
    DirMetadataAclRead = json_utils:encode(#{
        <<"metadata">> => #{<<"cdmi_acl">> => [DirRead, WriteAcl]}
    }),
    DirMetadataAclWrite = json_utils:encode(#{
        <<"metadata">> => #{<<"cdmi_acl">> => [DirWrite]}
    }),

    % set acl to 'read&write' and test cdmi get request (should succeed)
    RequestHeaders2 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers, Dirname1, put, RequestHeaders2, DirMetadataAclReadWrite
    ),
    {ok, ?HTTP_200_OK, _, _} = do_request(
        Workers, Dirname1, get, RequestHeaders2, []
    ),

    % create files in directory (should succeed)
    {ok, ?HTTP_201_CREATED, _, _} = do_request(
        Workers, File1, put, [user_2_token_header()], []
    ),
    ?assert(object_exists(File1, Config)),

    {ok, ?HTTP_201_CREATED, _, _} = do_request(
        Workers, File2, put, RequestHeaders1, <<"{\"value\":\"val\"}">>
    ),
    ?assert(object_exists(File2, Config)),
    ?assert(object_exists(File3, Config)),

    % delete files (should succeed)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers, File1, delete, [user_2_token_header()], []
    ),
    ?assert(not object_exists(File1, Config)),

    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers, File2, delete, [user_2_token_header()], []
    ),
    ?assert(not object_exists(File2, Config)),

    % set acl to 'write' and test cdmi get request (should return 403 forbidden)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers, Dirname1, put, RequestHeaders2, DirMetadataAclWrite
    ),
    {ok, Code5, _, Response5} = do_request(Workers, Dirname1, get, RequestHeaders2, []),
    ?assertMatch(EaccesError, {Code5, json_utils:decode(Response5)}),

    % set acl to 'read' and test cdmi put request (should return 403 forbidden)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = do_request(
        Workers, Dirname1, put, RequestHeaders2, DirMetadataAclRead
    ),
    {ok, ?HTTP_200_OK, _, _} = do_request(Workers, Dirname1, get, RequestHeaders2, []),
    {ok, Code6, _, Response6} = do_request(
        Workers, Dirname1, put, RequestHeaders2,
        json_utils:encode(#{<<"metadata">> => #{<<"my_meta">> => <<"value">>}})
    ),
    ?assertMatch(EaccesError, {Code6, json_utils:decode(Response6)}),

    % create files (should return 403 forbidden)
    {ok, Code7, _, Response7} = do_request(Workers, File1, put, [user_2_token_header()], []),
    ?assertMatch(EaccesError, {Code7, json_utils:decode(Response7)}),
    ?assert(not object_exists(File1, Config)),

    {ok, Code8, _, Response8} = do_request(
        Workers, File2, put, RequestHeaders1, <<"{\"value\":\"val\"}">>
    ),
    ?assertMatch(EaccesError, {Code8, json_utils:decode(Response8)}),
    ?assert(not object_exists(File2, Config)),
    ?assertEqual({error, ?EACCES}, create_new_file(File4, Config)),
    ?assert(not object_exists(File4, Config)),

    % delete files (should return 403 forbidden)
    {ok, Code9, _, Response9} = do_request(Workers, File3, delete, [user_2_token_header()], []),
    ?assertMatch(EaccesError, {Code9, json_utils:decode(Response9)}),
    ?assert(object_exists(File3, Config)).
%%------------------------------


accept_header(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    AcceptHeader = {?HDR_ACCEPT, <<"*/*">>},

    % when
    {ok, Code1, _Headers1, _Response1} =
        do_request(Workers, [], get,
            [user_2_token_header(), ?CDMI_VERSION_HEADER, AcceptHeader], []),

    % then
    ?assertEqual(?HTTP_200_OK, Code1).


%%%===================================================================
%%% Internal functions
%%%===================================================================


do_request(Node, RestSubpath, Method, Headers) ->
    do_request(Node, RestSubpath, Method, Headers, []).

do_request([_ | _] = Nodes, RestSubpath, get, Headers, Body) ->
    [FRes | _] = Responses = lists:filtermap(fun(Node) ->
        case make_request(Node, RestSubpath, get, Headers, Body) of
            space_not_supported -> false;
            Result -> {true, Result}
        end
    end, Nodes),

    case FRes of
        {error, _} ->
            ok;
        {ok, ?HTTP_206_PARTIAL_CONTENT, #{?HDR_CONTENT_TYPE := <<"multipart/byteranges", _/binary>>}, _} ->
            lists:foreach(fun({ok, LCode, _, _}) ->
                ?assertMatch(?HTTP_206_PARTIAL_CONTENT, LCode)
            end, Responses);
        {ok, RCode, _, RResponse} ->
            RResponseJSON = try_to_decode(RResponse),
            lists:foreach(fun({ok, LCode, _, LResponse}) ->
                LResponseJSON = try_to_decode(LResponse),
                ?assertMatch({RCode, RResponseJSON}, {LCode, LResponseJSON})

            end, Responses)
    end,
    FRes;

do_request([_ | _] = Nodes, RestSubpath, Method, Headers, Body) ->
    lists:foldl(fun
        (Node, space_not_supported) ->
            make_request(Node, RestSubpath, Method, Headers, Body);
        (_Node, Result) ->
            Result
    end, space_not_supported, lists_utils:shuffle(Nodes));

do_request(Node, RestSubpath, Method, Headers, Body) when is_atom(Node) ->
    make_request(Node, RestSubpath, Method, Headers, Body).


make_request(Node, RestSubpath, Method, Headers, Body) ->
    case cdmi_test_utils:do_request(Node, RestSubpath, Method, Headers, Body) of
        {ok, RespCode, _RespHeaders, RespBody} = Result ->
            case is_space_supported(Node, RestSubpath) of
                true ->
                    Result;
                false ->
                    % Returned error may not be necessarily ?ERROR_SPACE_NOT_SUPPORTED(_, _)
                    % as some errors may be thrown even before file path resolution attempt
                    % (and such errors are explicitly checked by some tests),
                    % but it should never be any successful response
                    ?assert(RespCode >= 300),
                    case {RespCode, try_to_decode(RespBody)} of
                        {?HTTP_400_BAD_REQUEST, #{<<"error">> :=  #{
                            <<"id">> := <<"spaceNotSupportedBy">>
                        }}}->
                            space_not_supported;
                        _ ->
                            Result
                    end
            end;
        {error, _} = Error ->
            Error
    end.


is_space_supported(_Node, "") ->
    true;
is_space_supported(_Node, "/") ->
    true;
is_space_supported(Node, CdmiPath) ->
    {ok, SuppSpaces} = rpc:call(Node, provider_logic, get_spaces, []),
    SpecialObjectIds = [?ROOT_CAPABILITY_ID, ?CONTAINER_CAPABILITY_ID, ?DATAOBJECT_CAPABILITY_ID],

    case binary:split(list_to_binary(CdmiPath), <<"/">>, [global, trim_all]) of
        [<<"cdmi_capabilities">> | _] ->
            true;
        [<<"cdmi_objectid">>, ObjectId | _] ->
            case lists:member(ObjectId, SpecialObjectIds) of
                true ->
                    true;
                false ->
                    {ok, FileGuid} = file_id:objectid_to_guid(ObjectId),
                    SpaceId = file_id:guid_to_space_id(FileGuid),
                    SpaceId == <<"rootDirVirtualSpaceId">> orelse lists:member(SpaceId, SuppSpaces)
            end;
        [SpaceName | _] ->
            lists:any(fun(SpaceId) -> get_space_name(Node, SpaceId) == SpaceName end, SuppSpaces)
    end.


get_space_name(Node, SpaceId) ->
    {ok, SpaceName} = rpc:call(Node, space_logic, get_name, [<<"0">>, SpaceId]),
    SpaceName.


try_to_decode(Body) ->
    try
        remove_times_metadata(json_utils:decode(Body))
    catch _:invalid_json ->
        Body
    end.


remove_times_metadata(ResponseJSON) ->
    Metadata = maps:get(<<"metadata">>, ResponseJSON, undefined),
    case Metadata of
        undefined -> ResponseJSON;
        _ -> Metadata1 = maps:without( [<<"cdmi_ctime">>,
            <<"cdmi_atime">>,
            <<"cdmi_mtime">>], Metadata),
            maps:put(<<"metadata">>, Metadata1, ResponseJSON)
    end.


create_test_dir_and_file(Config) ->
    SpaceName = oct_background:get_space_name(Config#cdmi_test_config.space_selector),
    TestDirName = get_random_string(),
    TestFileName = get_random_string(),
    FullTestDirName = filename:join([binary_to_list(SpaceName), node_cache:get(root_dir_name),TestDirName]),
    FullTestFileName = filename:join(["/", binary_to_list(SpaceName), node_cache:get(root_dir_name), TestDirName, TestFileName]),
    TestFileContent = <<"test_file_content">>,

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = list_to_binary(TestDirName),
            children = [
                #file_spec{
                    name = list_to_binary(TestFileName),
                    content = TestFileContent
                }
            ]
        }, Config#cdmi_test_config.p1_selector
    ),

    {binary_to_list(SpaceName), TestDirName, FullTestDirName, TestFileName, FullTestFileName, TestFileContent}.


object_exists(Path, Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    SessionId = oct_background:get_user_session_id(user2, Config#cdmi_test_config.p1_selector),
    case lfm_proxy:stat(WorkerP1, SessionId,
        {path, absolute_binary_path(Path)}) of
        {ok, _} ->
            true;
        {error, ?ENOENT} ->
            false
    end.


create_new_file(Path, Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    SessionId = oct_background:get_user_session_id(user2, Config#cdmi_test_config.p1_selector),
    lfm_proxy:create(WorkerP1, SessionId, absolute_binary_path(Path)).


open_file(ProviderSelector, Path, OpenMode, Config) ->
    Worker = oct_background:get_random_provider_node(ProviderSelector),
    SessionId = oct_background:get_user_session_id(user2, ProviderSelector),
    lfm_proxy:open(Worker, SessionId, {path, absolute_binary_path(Path)}, OpenMode).


write_to_file(Path, Data, Offset, Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    {ok, FileHandle} = ?assertMatch({ok, _}, open_file(Config#cdmi_test_config.p1_selector, Path, write, Config), ?ATTEMPTS),
    Result = lfm_proxy:write(WorkerP1, FileHandle, Offset, Data),
    lfm_proxy:close(WorkerP1, FileHandle),
    Result.


get_file_content(Path, Config) ->
    WorkerP2 = oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector),
    case open_file(Config#cdmi_test_config.p2_selector, Path, read, Config) of
        {ok, FileHandle} ->
            Result = case lfm_proxy:check_size_and_read(WorkerP2, FileHandle, ?FILE_BEGINNING, ?INFINITY) of
                {error, Error} -> {error, Error};
                {ok, Content} -> Content
            end,
            lfm_proxy:close(WorkerP2, FileHandle),
            Result;
        {error, Error} -> {error, Error}
    end.


set_acl(Path, Acl, Config) ->
    WorkerP1 = oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    SessionId = oct_background:get_user_session_id(user2, Config#cdmi_test_config.p1_selector),
    lfm_proxy:set_acl(WorkerP1, SessionId, {path, absolute_binary_path(Path)}, Acl).


get_acl(Path, Config) ->
    WorkerP2 = oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector),
    SessionId = oct_background:get_user_session_id(user2, Config#cdmi_test_config.p2_selector),
    lfm_proxy:get_acl(WorkerP2, SessionId, {path, absolute_binary_path(Path)}).


get_xattrs(Path, Config) ->
    WorkerP2 = oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector),
    SessionId = oct_background:get_user_session_id(user2, Config#cdmi_test_config.p2_selector),
    case lfm_proxy:list_xattr(WorkerP2, SessionId, {path, absolute_binary_path(Path)}, false, true) of
        {ok, Xattrs} ->
            lists:filtermap(
                fun
                    (<<"cdmi_", _/binary>>) ->
                        false;
                    (XattrName) ->
                        {ok, Xattr} = lfm_proxy:get_xattr(
                            WorkerP2, SessionId, {path, absolute_binary_path(Path)}, XattrName
                        ),
                        {true, Xattr}
                end, Xattrs);
        {error, Error} -> {error, Error}
    end.


get_json_metadata(Path, Config) ->
    WorkerP2 = oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector),
    SessionId = oct_background:get_user_session_id(user2, Config#cdmi_test_config.p2_selector),
    {ok, FileGuid} = lfm_proxy:resolve_guid(WorkerP2, SessionId, absolute_binary_path(Path)),
    opt_file_metadata:get_custom_metadata(WorkerP2, SessionId, ?FILE_REF(FileGuid), json, [], false).


absolute_binary_path(Path) ->
    list_to_binary(ensure_begins_with_slash(Path)).


ensure_begins_with_slash(Path) ->
    ReversedBinary = list_to_binary(lists:reverse(Path)),
    lists:reverse(binary_to_list(filepath_utils:ensure_ends_with_slash(ReversedBinary))).


mock_opening_file_without_perms(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    test_node_starter:load_modules(Workers, [?MODULE]),
    test_utils:mock_new(Workers, lfm),
    test_utils:mock_expect(
        Workers, lfm, monitored_open, fun(_, _, _) -> {error, ?EACCES} end).


unmock_opening_file_without_perms(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    test_utils:mock_unload(Workers, lfm).


get_random_string() ->
    get_random_string(10, "abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ").


get_random_string(Length, AllowedChars) ->
    lists:foldl(fun(_, Acc) ->
        [lists:nth(rand:uniform(length(AllowedChars)),
            AllowedChars)]
        ++ Acc
    end, [], lists:seq(1, Length)).


-spec build_random_src_uri(list(), file_id:file_guid()) -> binary().
build_random_src_uri(Path, Guid) ->
    PathBin = str_utils:to_binary(Path),
    case rand:uniform(3) of
        1 ->
            PathBin;
        2 ->
            {ok, ObjectId} = file_id:guid_to_objectid(Guid),
            <<"/cdmi_objectid/", ObjectId/binary>>;
        3 ->
            [_SpaceName, RootDir, PathTokens] = filepath_utils:split(PathBin),
            {ok, SpaceObjectId} = file_id:guid_to_objectid(
                fslogic_file_id:spaceid_to_space_dir_guid(file_id:guid_to_space_id(Guid))
            ),

            PathBin2 = filepath_utils:join([SpaceObjectId, RootDir, PathTokens]),
            <<"/cdmi_objectid/", PathBin2/binary>>
    end.
