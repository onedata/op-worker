%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% CDMI get tests
%%% @end
%%%-------------------------------------------------------------------
-module(cdmi_get_test_base).
-author("Katarzyna Such").

-include("cdmi_test.hrl").
-include("http/cdmi.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").

-include_lib("ctool/include/test/test_utils.hrl").

%% Tests
-export([
    get_system_capabilities_test/1,
    get_container_capabilities_test/1,
    get_dataobject_capabilities_test/1,

    get_file_cdmi_test/1,
    get_file_cdmi_attributes_test/1,
    get_file_noncdmi_test/1,

    get_root_with_objectid_endpoint_test/1,
    get_dir_with_objectid_endpoint_test/1,
    get_file_with_objectid_endpoint_test/1,
    unauthorized_access_by_object_id_test/1,

    list_basic_dir_test/1,
    list_root_space_dir_test/1,
    list_nonexisting_dir_test/1,
    selective_params_list_test/1,
    childrenrange_list_test/1,

    get_non_existing_file_error_test/1,
    list_non_existing_dir_error_test/1,
    download_empty_file_test/1
]).


%%%===================================================================
%%% Test functions
%%%===================================================================

% tests if capabilities of objects, containers, and whole storage system are set properly
get_system_capabilities_test(Config) ->
    RequestHeaders = [?CDMI_VERSION_HEADER],
    {ok, _Code, Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), "cdmi_capabilities/", get, RequestHeaders, [])
    ),

    CdmiResponse = json_utils:decode(Response),
    Capabilities = maps:get(<<"capabilities">>, CdmiResponse),
    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/cdmi-capability">>}, Headers),
    ?assertMatch(#{<<"objectID">> := ?ROOT_CAPABILITY_ID}, CdmiResponse),
    ?assertMatch(#{<<"objectName">> := <<?ROOT_CAPABILITY_PATH>>}, CdmiResponse),
    ?assertMatch(#{<<"childrenrange">> := <<"0-1">>}, CdmiResponse),
    ?assertMatch(#{<<"children">> := [<<"container/">>, <<"dataobject/">>]}, CdmiResponse),
    ?assertEqual(?ROOT_CAPABILITY_MAP, Capabilities).


get_container_capabilities_test(Config) ->
    RequestHeaders = [?CDMI_VERSION_HEADER],
    {ok, Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), "cdmi_capabilities/container/", get, RequestHeaders, [])
    ),
    ?assertMatch(
        {ok, Code, _, Response},
        cdmi_test_utils:do_request(
            ?WORKERS(Config), filename:join("cdmi_objectid/", binary_to_list(?CONTAINER_CAPABILITY_ID))
            ++ "/", get, RequestHeaders, []
        )
    ),

    CdmiResponse = json_utils:decode(Response),
    Capabilities = maps:get(<<"capabilities">>, CdmiResponse),

    ?assertMatch(#{<<"parentURI">> := <<?ROOT_CAPABILITY_PATH>>}, CdmiResponse),
    ?assertMatch(#{<<"parentID">> := ?ROOT_CAPABILITY_ID}, CdmiResponse),
    ?assertMatch(#{<<"objectID">> := ?CONTAINER_CAPABILITY_ID}, CdmiResponse),
    ?assertMatch(#{<<"objectName">> := <<"container/">>}, CdmiResponse),
    ?assertEqual(?CONTAINER_CAPABILITY_MAP, Capabilities).


get_dataobject_capabilities_test(Config) ->
    RequestHeaders = [?CDMI_VERSION_HEADER],
    {ok, Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), "cdmi_capabilities/dataobject/", get, RequestHeaders, [])
    ),

    ?assertMatch(
        {ok, Code, _, Response},
        cdmi_test_utils:do_request(
            ?WORKERS(Config), filename:join(
                "cdmi_objectid/", binary_to_list(?DATAOBJECT_CAPABILITY_ID)) ++ "/",
            get, RequestHeaders, []
        )
    ),

    CdmiResponse = json_utils:decode(Response),
    Capabilities = maps:get(<<"capabilities">>, CdmiResponse),

    ?assertMatch(#{<<"parentURI">> := <<?ROOT_CAPABILITY_PATH>>}, CdmiResponse),
    ?assertMatch(#{<<"parentID">> := ?ROOT_CAPABILITY_ID}, CdmiResponse),
    ?assertMatch(#{<<"objectID">> := ?DATAOBJECT_CAPABILITY_ID}, CdmiResponse),
    ?assertMatch(#{<<"objectName">> := <<"dataobject/">>}, CdmiResponse),
    ?assertEqual(?DATAOBJECT_CAPABILITY_MAP, Capabilities).


get_file_cdmi_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    FilledFilePath = ?build_test_root_path(Config),
    FilePathBin = atom_to_binary(?FUNCTION_NAME),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid), [
        #file_spec{name = list_to_binary(atom_to_list(?FUNCTION_NAME) ++ "empty")},
        #file_spec{
            name = FilePathBin,
            content = ?FILE_CONTENT
        }
    ], Config#cdmi_test_config.p1_selector),

    RequestHeaders = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, _Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), FilledFilePath, get, RequestHeaders, [])
    ),

    CdmiResponse = json_utils:decode(Response),
    FileContent = base64:encode(?FILE_CONTENT),
    RootDirPath = list_to_binary("/" ++ RootPath),
    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-object">>}, CdmiResponse),
    ?assertMatch(#{<<"objectName">> := FilePathBin}, CdmiResponse),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"base64">>}, CdmiResponse),
    ?assertMatch(#{<<"mimetype">> := <<"application/octet-stream">>}, CdmiResponse),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse),
    ?assertMatch(#{<<"value">> := FileContent}, CdmiResponse),
    ?assertMatch(#{<<"valuerange">> := <<"0-12">>}, CdmiResponse),

    ?assert(maps:get(<<"metadata">>, CdmiResponse) =/= <<>>).


get_file_cdmi_attributes_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    FilledFilePath = ?build_test_root_path(Config),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid), [
        #file_spec{name = list_to_binary(atom_to_list(?FUNCTION_NAME) ++ "empty")},
        #file_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            content = ?FILE_CONTENT
        }
    ], Config#cdmi_test_config.p1_selector),
    RootDirPath = list_to_binary("/" ++ RootPath),
    %%-- selective params read -----
    RequestHeaders = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, _Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS(Config), FilledFilePath ++ "?parentURI;completionStatus", get, RequestHeaders, []
    )),
    CdmiResponse = json_utils:decode(Response),

    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse),
    ?assertEqual(2, maps:size(CdmiResponse)),

    %%--- selective value read -----
    RequestHeaders2 = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, _Code2, _Headers2, Response2} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS(Config), FilledFilePath ++ "?value:1-3;valuerange", get, RequestHeaders2, []
    )),
    CdmiResponse2 = json_utils:decode(Response2),
    ?assertMatch(#{<<"valuerange">> := <<"1-3">>}, CdmiResponse2),
    % 1-3 from FileContent = <<"Some content...">>
    ?assertEqual(<<"ile">>, base64:decode(maps:get(<<"value">>, CdmiResponse2))),

    %%------- objectid read --------
    RequestHeaders3 = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, _Code3, _Headers3, Response3} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS(Config), FilledFilePath ++ "?objectID", get, RequestHeaders3, []
    )),
    CdmiResponse3 = json_utils:decode(Response3),
    ObjectID = maps:get(<<"objectID">>, CdmiResponse3),

    ?assert(is_binary(ObjectID)),

    %%-------- read by id ----------
    RequestHeaders4 = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, _Code4, _Headers4, Response4} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS(Config), filename:join("cdmi_objectid/", binary_to_list(ObjectID)), get, RequestHeaders4, []
    )),
    CdmiResponse4 = json_utils:decode(Response4),

    ?assertEqual(?FILE_CONTENT, base64:decode(maps:get(<<"value">>, CdmiResponse4))).


get_file_noncdmi_test(Config) ->
    FilledFilePath = ?build_test_root_path(Config),
    EmptyFilePath = cdmi_test_utils:build_test_root_path(Config, atom_to_list(?FUNCTION_NAME) ++ "empty"),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid), [
        #file_spec{name = list_to_binary(atom_to_list(?FUNCTION_NAME) ++ "empty")},
        #file_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            content = ?FILE_CONTENT
        }
    ], Config#cdmi_test_config.p1_selector),

    {ok, _Code, Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), FilledFilePath, get, [cdmi_test_utils:user_2_token_header()])
    ),
    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/octet-stream">>}, Headers),
    ?assertEqual(?FILE_CONTENT, Response),

    %% selective value single range read non-cdmi
    ?assertMatch(
        {ok, ?HTTP_206_PARTIAL_CONTENT, #{?HDR_CONTENT_RANGE := <<"bytes 5-8/13">>}, <<"cont">>},
        cdmi_test_utils:do_request(?WORKERS(Config), FilledFilePath, get, [
            {?HDR_RANGE, <<"bytes=5-8">>}, cdmi_test_utils:user_2_token_header()
        ])
    ),

    %% selective value multi range read non-cdmi
    {ok, _, #{?HDR_CONTENT_TYPE := <<"multipart/byteranges; boundary=", Boundary/binary>>},
        Response2} = ?assertMatch( {ok, ?HTTP_206_PARTIAL_CONTENT, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), FilledFilePath, get, [
            {?HDR_RANGE, <<"bytes=1-3,5-5,-3">>}, cdmi_test_utils:user_2_token_header()]
        )),
    ExpResponse2 = <<
        "--", Boundary/binary,
        "\r\ncontent-type: application/octet-stream\r\ncontent-range: bytes 1-3/13",
        "\r\n\r\nile",
        "--", Boundary/binary,
        "\r\ncontent-type: application/octet-stream\r\ncontent-range: bytes 5-5/13",
        "\r\n\r\nc",
        "--", Boundary/binary,
        "\r\ncontent-type: application/octet-stream\r\ncontent-range: bytes 10-12/13",
        "\r\n\r\nnt!\r\n",
        "--", Boundary/binary, "--"
    >>,
    ?assertEqual(ExpResponse2, Response2),

    %% read file non-cdmi with invalid Range should fail
    lists:foreach(fun(InvalidRange) ->
        ?assertMatch(
            {ok, ?HTTP_416_RANGE_NOT_SATISFIABLE, #{?HDR_CONTENT_RANGE := <<"bytes */13">>}, <<>>},
            cdmi_test_utils:do_request(?WORKERS(Config), FilledFilePath, get, [
                {?HDR_RANGE, InvalidRange}, cdmi_test_utils:user_2_token_header()
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

    %% read empty file non-cdmi without Range
    ?assertMatch(
        {ok, ?HTTP_200_OK, _, <<>>},
        cdmi_test_utils:do_request(?WORKERS(Config), EmptyFilePath, get, [cdmi_test_utils:user_2_token_header()])
    ),

    %% read empty file non-cdmi with Range should return 416
    ?assertMatch(
        {ok, ?HTTP_416_RANGE_NOT_SATISFIABLE, #{?HDR_CONTENT_RANGE := <<"bytes */0">>}, <<>>},
        cdmi_test_utils:do_request(?WORKERS(Config), EmptyFilePath, get, [
            {?HDR_RANGE, <<"bytes=10-15">>}, cdmi_test_utils:user_2_token_header()
        ])
    ).


get_root_with_objectid_endpoint_test(Config) ->
    [WorkerP1, _WorkerP2] = ?WORKERS(Config),
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    RequestHeaders = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, _Code, Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), "", get, RequestHeaders, [])
    ),

    RequestHeaders2 = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],

    % TODO VFS-7288 clarify what should be written to cdmi_size for directories
    ?assertMatch({ok, ?HTTP_200_OK, _, _}, cdmi_test_utils:do_request(
        WorkerP1, RootPath, get, RequestHeaders2, [])
    ),
    CdmiResponse = json_utils:decode(Response),
    RootId = maps:get(<<"objectID">>, CdmiResponse, undefined),

    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/cdmi-container">>}, Headers),
    ?assertMatch(#{<<"objectName">> := <<"/">>}, CdmiResponse),
    ?assertNotEqual(RootId, undefined),
    ?assert(is_binary(RootId)),
    ?assertMatch(#{<<"parentURI">> := <<>>}, CdmiResponse),
    ?assertEqual(error, maps:find(<<"parentID">>, CdmiResponse)),
    ?assertMatch(#{<<"capabilitiesURI">> := <<"cdmi_capabilities/container/">>}, CdmiResponse),

    RequestHeaders3 = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, _Code3, _Headers3, Response3} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _}, cdmi_test_utils:do_request(
            ?WORKERS(Config), filename:join("cdmi_objectid/", binary_to_list(RootId)) ++ "/", get, RequestHeaders3, []
    )),
    CdmiResponse3 = json_utils:decode(Response3),
    Meta = maps:remove(<<"cdmi_atime">>, maps:get(<<"metadata">>, CdmiResponse)),
    CdmiResponse1WithoutAtime = maps:put(<<"metadata">>, Meta, CdmiResponse),
    Meta3 = maps:remove(<<"cdmi_atime">>, maps:get(<<"metadata">>, CdmiResponse3)),
    CdmiResponse3WithoutAtime = maps:put(<<"metadata">>, Meta3, CdmiResponse3),

    ?assertEqual(CdmiResponse1WithoutAtime, CdmiResponse3WithoutAtime). % should be the same as in 1 (except access time)


get_dir_with_objectid_endpoint_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    TestDirPath = ?build_test_root_path(Config),
    TestDirPathCheck = list_to_binary(atom_to_list(?FUNCTION_NAME) ++ "/"),
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

    RequestHeaders = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, _Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS(Config), TestDirPath ++ "/", get, RequestHeaders, []
    )),

    CdmiResponse = json_utils:decode(Response),
    DirId = maps:get(<<"objectID">>, CdmiResponse, undefined),
    RootDirPath = list_to_binary("/" ++ RootPath),
    {ok, RootId} = file_id:guid_to_objectid(node_cache:get(root_dir_guid)),

    ?assertMatch(#{<<"objectName">> := TestDirPathCheck}, CdmiResponse),
    ?assertNotEqual(DirId, undefined),
    ?assert(is_binary(DirId)),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse),
    ?assertMatch(#{<<"parentID">> := RootId}, CdmiResponse),
    ?assertMatch(#{<<"capabilitiesURI">> := <<"cdmi_capabilities/container/">>}, CdmiResponse),
    RequestHeaders2 = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, _Code2, _Headers2, Response2} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), filename:join(
            "cdmi_objectid/", binary_to_list(DirId)) ++ "/", get, RequestHeaders2, []
    )),
    CdmiResponse2 = json_utils:decode(Response2),
    Meta = maps:remove(<<"cdmi_atime">>, (maps:get(<<"metadata">>, CdmiResponse))),
    CdmiResponse1WithoutAtime = maps:put(<<"metadata">>, Meta, CdmiResponse),
    Meta2 = maps:remove(<<"cdmi_atime">>, (maps:get(<<"metadata">>, CdmiResponse2))),
    CdmiResponse2WithoutAtime = maps:put(<<"metadata">>, Meta2, CdmiResponse2),

    ?assertEqual( % should be the same as in 2 (except parent and access time)
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse1WithoutAtime)),
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse2WithoutAtime))
    ).


get_file_with_objectid_endpoint_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    TestFilePath = ?build_test_root_path(Config),
    TestFilePathBin = atom_to_binary(?FUNCTION_NAME),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{name = TestFilePathBin},
    Config#cdmi_test_config.p1_selector),
    RequestHeaders = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, _Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), TestFilePath, get, RequestHeaders, [])
    ),
    CdmiResponse = json_utils:decode(Response),
    FileId = maps:get(<<"objectID">>, CdmiResponse, undefined),
    RootDirPath = list_to_binary("/" ++ RootPath),
    {ok, RootId} = file_id:guid_to_objectid(node_cache:get(root_dir_guid)),

    ?assertMatch(#{<<"objectName">> := TestFilePathBin}, CdmiResponse),
    ?assertNotEqual(FileId, undefined),
    ?assert(is_binary(FileId)),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse),
    ?assertMatch(#{<<"parentID">> := RootId}, CdmiResponse),
    ?assertMatch(#{<<"capabilitiesURI">> := <<"cdmi_capabilities/dataobject/">>}, CdmiResponse),

    RequestHeaders2 = [?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    {ok, _Code2, _Headers2, Response2} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), filename:join("cdmi_objectid/", binary_to_list(RootId))
        ++ "/" ++ TestFilePathBin, get, RequestHeaders2, []
    )),
    CdmiResponse2 = json_utils:decode(Response2),
    Meta = maps:remove(<<"cdmi_atime">>, (maps:get(<<"metadata">>, CdmiResponse))),
    CdmiResponse1WithoutAtime = maps:put(<<"metadata">>, Meta, CdmiResponse),
    Meta2 = maps:remove(<<"cdmi_atime">>, (maps:get(<<"metadata">>, CdmiResponse2))),
    CdmiResponse2WithoutAtime = maps:put(<<"metadata">>, Meta2, CdmiResponse2),

    ?assertEqual( % should be the same as in 3 (except access time)
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse1WithoutAtime)),
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse2WithoutAtime))
    ),

    {ok, _Code3, _Headers3, Response3} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS(Config), filename:join("cdmi_objectid/", binary_to_list(FileId)), get, RequestHeaders2, [])
    ),
    CdmiResponse3 = json_utils:decode(Response3),
    Meta3 = maps:remove(<<"cdmi_atime">>, (maps:get(<<"metadata">>, CdmiResponse3))),
    CdmiResponse3WithoutAtime = maps:merge(
        #{<<"metadata">> => Meta3},maps:remove(<<"metadata">>, CdmiResponse3)
    ),

    ?assertEqual( % should be the same as in 6 (except parent and access time)
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse2WithoutAtime)),
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse3WithoutAtime))
    ).


unauthorized_access_by_object_id_test(Config) ->
    {ok, RootId} = file_id:guid_to_objectid(node_cache:get(root_dir_guid)),
    RequestHeaders = [?CDMI_VERSION_HEADER],
    {ok, Code, _, Response} = cdmi_test_utils:do_request(
        ?WORKERS(Config), filename:join("cdmi_objectid/", binary_to_list(RootId)) ++ "/",
        get, RequestHeaders, []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_UNAUTHORIZED),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


list_basic_dir_test(Config) ->
    TestDirPath = ?build_test_root_path(Config),
    TestDirPathCheck = list_to_binary(atom_to_list(?FUNCTION_NAME) ++ "/"),
    TestFileNameBin = <<"some_file.txt">>,

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            children = [
                #file_spec{
                    name = TestFileNameBin,
                    content = ?FILE_CONTENT
                }
            ]
        }, Config#cdmi_test_config.p1_selector
    ),

    {ok, _Code, Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), TestDirPath ++ "/", get,
            [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER], [])
    ),

    CdmiResponse = json_utils:decode(Response),
    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/cdmi-container">>}, Headers),
    ?assertMatch(
        #{<<"objectType">> :=  <<"application/cdmi-container">>},
        CdmiResponse
    ),
    ?assertMatch(#{<<"objectName">> := TestDirPathCheck}, CdmiResponse),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse),
    ?assertMatch(#{<<"children">> := [TestFileNameBin]}, CdmiResponse),
    ?assert(maps:get(<<"metadata">>, CdmiResponse) =/= <<>>).


list_root_space_dir_test(Config) ->
    TestDirNameCheck = list_to_binary(atom_to_list(?FUNCTION_NAME) ++ "/"),
    RootPath = cdmi_test_utils:get_tests_root_path(Config),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{name = atom_to_binary(?FUNCTION_NAME)},
    Config#cdmi_test_config.p1_selector),

    % TODO VFS-7288 clarify what should be written to cdmi_size for directories
    [WorkerP1, _WorkerP2] = ?WORKERS(Config),
    {ok, _Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(WorkerP1, RootPath, get,
            [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER], [])
    ),
    CdmiResponse = json_utils:decode(Response),
    RootNameBin = list_to_binary(node_cache:get(root_dir_name) ++ "/"),
    ?assertMatch(#{<<"objectName">> := RootNameBin}, CdmiResponse),
    ?assertMatch(#{<<"children">> := [TestDirNameCheck]}, CdmiResponse).


list_nonexisting_dir_test(Config) ->
    {ok, _Code, _Headers, _Response} = ?assertMatch(
        {ok, ?HTTP_404_NOT_FOUND, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), "nonexisting_dir/",
            get, [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER], [])
    ).


selective_params_list_test(Config) ->
    TestDirPath = ?build_test_root_path(Config),
    TestDirPathCheck = list_to_binary(atom_to_list(?FUNCTION_NAME) ++ "/"),
    TestFileNameBin = <<"some_file.txt">>,

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = atom_to_binary(?FUNCTION_NAME),
            children = [
                #file_spec{
                    name = TestFileNameBin,
                    content = ?FILE_CONTENT
                }
            ]
        }, Config#cdmi_test_config.p1_selector
    ),
    {ok, _Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), filename:join(TestDirPath, "?children;objectName"),
            get, [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER], [])
    ),
    CdmiResponse = json_utils:decode(Response),
    ?assertMatch(#{<<"objectName">> := TestDirPathCheck}, CdmiResponse),
    ?assertMatch(#{<<"children">> := [TestFileNameBin]}, CdmiResponse),
    ?assertEqual(2, maps:size(CdmiResponse)).


childrenrange_list_test(Config) ->
    Children = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",
        "12", "13", "14"],
    ChildrenNameBinaries = lists:map(fun(X) -> list_to_binary(X) end, Children),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid), #dir_spec{
        name = atom_to_binary(?FUNCTION_NAME),
        children = lists:map(
            fun(ChildName) ->
                #file_spec{name = ChildName} end, ChildrenNameBinaries
        )
    }, Config#cdmi_test_config.p1_selector),
    ChildrangeDir = ?build_test_root_path(Config) ++ "/",
    {ok, _Code, _Headers, Response} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), ChildrangeDir ++ "?children;childrenrange",
            get, [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER], [])
    ),
    CdmiResponse = json_utils:decode(Response),
    ChildrenResponse = maps:get(<<"children">>, CdmiResponse),
    ?assert(is_list(ChildrenResponse)),
    lists:foreach(fun(Name) ->
        ?assert(lists:member(Name, ChildrenResponse))
    end, ChildrenNameBinaries),
    ?assertMatch(#{<<"childrenrange">> := <<"0-14">>}, CdmiResponse),

    {ok, _Code2, _, Response2} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), ChildrangeDir ++ "?children:2-13;childrenrange", get,
            [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER], [])
    ),
    {ok, _Code3, _, Response3} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), ChildrangeDir ++ "?children:0-1;childrenrange", get,
            [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER], [])
    ),
    {ok, _Code4, _, Response4} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), ChildrangeDir ++ "?children:14-14;childrenrange", get,
            [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER], [])
    ),

    CdmiResponse2 = json_utils:decode(Response2),
    CdmiResponse3 = json_utils:decode(Response3),
    CdmiResponse4 = json_utils:decode(Response4),
    ChildrenResponse2 = maps:get(<<"children">>, CdmiResponse2),
    ChildrenResponse3 = maps:get(<<"children">>, CdmiResponse3),
    ChildrenResponse4 = maps:get(<<"children">>, CdmiResponse4),

    ?assert(is_list(ChildrenResponse2)),
    ?assert(is_list(ChildrenResponse3)),
    ?assert(is_list(ChildrenResponse4)),
    ?assertEqual(12, length(ChildrenResponse2)),
    ?assertEqual(2, length(ChildrenResponse3)),
    ?assertEqual(1, length(ChildrenResponse4)),
    ?assertMatch(#{<<"childrenrange">> := <<"2-13">>}, CdmiResponse2),
    ?assertMatch(#{<<"childrenrange">> := <<"0-1">>}, CdmiResponse3),
    ?assertMatch(#{<<"childrenrange">> := <<"14-14">>}, CdmiResponse4),

    ?assertEqual([<<"10">>,<<"11">>,<<"12">>,<<"13">>,<<"14">>,<<"2">>,<<"3">>,
        <<"4">>,<<"5">>,<<"6">>,<<"7">>,<<"8">>], ChildrenResponse2),
    ?assertEqual([<<"0">>,<<"1">>], ChildrenResponse3),
    ?assertEqual([<<"9">>], ChildrenResponse4).


get_non_existing_file_error_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    RequestHeaders = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_OBJECT_CONTENT_TYPE_HEADER
    ],
    ?assertMatch(
        {ok, ?HTTP_404_NOT_FOUND, _, _},
        cdmi_test_utils:do_request(
            ?WORKERS(Config), filename:join(RootPath, "nonexistent_file"), get, RequestHeaders
        )).


list_non_existing_dir_error_test(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    RequestHeaders2 = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    ?assertMatch(
        {ok, ?HTTP_404_NOT_FOUND, _, _},
        cdmi_test_utils:do_request(
            ?WORKERS(Config), filename:join(RootPath, "/nonexisting_dir") ++ "/", get, RequestHeaders2
        )).


download_empty_file_test(Config) ->
    [_WorkerP1, WorkerP2] = ?WORKERS(Config),
    RootPath = cdmi_test_utils:get_tests_root_path(Config),

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
        {ok, ?HTTP_200_OK, _Headers, _Response},
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


