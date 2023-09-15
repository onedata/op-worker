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

-include("http/cdmi.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include("cdmi_test.hrl").

%% API
-export([
    get_system_capabilities/1,
    get_container_capabilities/1,
    get_dataobject_capabilities/1,

    basic_read/1,
    get_file_cdmi/1,
    get_file_non_cdmi/1,

    get_root_with_objectid_endpoint/1,
    get_dir_with_objectid_endpoint/1,
    get_file_with_objectid_endpoint/1,
    unauthorized_access_by_object_id/1,

    list_basic_dir/1,
    list_root_space_dir/1,
    list_nonexisting_dir/1,
    selective_params_list/1,
    childrenrange_list/1
    ]).


user_2_token_header() ->
    rest_test_utils:user_token_header(oct_background:get_user_access_token(user2)).


% tests if capabilities of objects, containers, and whole storage system are set properly
get_system_capabilities(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],

    %%--- system capabilities ------
    RequestHeaders8 = [?CDMI_VERSION_HEADER],
    {ok, Code8, Headers8, Response8} =
        cdmi_internal:do_request(Workers, "cdmi_capabilities/", get, RequestHeaders8, []),

    ?assertEqual(?HTTP_200_OK, Code8),
    CdmiResponse8 = (json_utils:decode(Response8)),
    Capabilities = maps:get(<<"capabilities">>, CdmiResponse8),
    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/cdmi-capability">>}, Headers8),
    ?assertMatch(#{<<"objectID">> := ?ROOT_CAPABILITY_ID}, CdmiResponse8),
    ?assertMatch(#{<<"objectName">> := <<?ROOT_CAPABILITY_PATH>>}, CdmiResponse8),
    ?assertMatch(#{<<"childrenrange">> := <<"0-1">>}, CdmiResponse8),
    ?assertMatch(#{<<"children">> := [<<"container/">>, <<"dataobject/">>]}, CdmiResponse8),
    ?assertEqual(?ROOT_CAPABILITY_MAP, Capabilities).


get_container_capabilities(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    %%-- container capabilities ----
    RequestHeaders9 = [?CDMI_VERSION_HEADER],
    {ok, Code9, _Headers9, Response9} =
        cdmi_internal:do_request(Workers, "cdmi_capabilities/container/", get, RequestHeaders9, []),
    ?assertEqual(?HTTP_200_OK, Code9),
    ?assertMatch(
        {ok, Code9, _, Response9},
        cdmi_internal:do_request(
            Workers, "cdmi_objectid/" ++ binary_to_list(?CONTAINER_CAPABILITY_ID) ++ "/", get, RequestHeaders9, []
        )
    ),

    CdmiResponse9 = (json_utils:decode(Response9)),
    Capabilities2 = maps:get(<<"capabilities">>, CdmiResponse9),

    ?assertMatch(#{<<"parentURI">> := <<?ROOT_CAPABILITY_PATH>>}, CdmiResponse9),
    ?assertMatch(#{<<"parentID">> := ?ROOT_CAPABILITY_ID}, CdmiResponse9),
    ?assertMatch(#{<<"objectID">> := ?CONTAINER_CAPABILITY_ID}, CdmiResponse9),
    ?assertMatch(#{<<"objectName">> := <<"container/">>}, CdmiResponse9),
    ?assertEqual(?CONTAINER_CAPABILITY_MAP, Capabilities2).


get_dataobject_capabilities(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],

    %%-- dataobject capabilities ---
    RequestHeaders10 = [?CDMI_VERSION_HEADER],
    {ok, Code10, _Headers10, Response10} =
        cdmi_internal:do_request(Workers, "cdmi_capabilities/dataobject/", get, RequestHeaders10, []),
    ?assertEqual(?HTTP_200_OK, Code10),
    ?assertMatch(
        {ok, Code10, _, Response10},
        cdmi_internal:do_request(
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


%%  Tests cdmi object GET request. Request can be done without cdmi header (in that case
%%  file conent is returned as response body), or with cdmi header (the response
%%  contains json string of type: application/cdmi-object, and we can specify what
%%  parameters we need by listing then as ';' separated list after '?' in URL )
basic_read(Config) ->
    {Workers, RootPath, FilledFileName, FileContent, FileNameBin, _} = get_file_base(Config),
    %%-------- basic read ----------
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code1, _Headers1, Response1} = cdmi_internal:do_request(Workers, FilledFileName, get, RequestHeaders1, []),
    ?assertEqual(?HTTP_200_OK, Code1),
    CdmiResponse1 = json_utils:decode(Response1),
    FileContent1 = base64:encode(FileContent),
    RootDirPath = list_to_binary("/" ++ RootPath),
    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-object">>}, CdmiResponse1),
    ?assertMatch(#{<<"objectName">> := FileNameBin}, CdmiResponse1),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse1),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"base64">>}, CdmiResponse1),
    ?assertMatch(#{<<"mimetype">> := <<"application/octet-stream">>}, CdmiResponse1),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse1),
    ?assertMatch(#{<<"value">> := FileContent1}, CdmiResponse1),
    ?assertMatch(#{<<"valuerange">> := <<"0-14">>}, CdmiResponse1),

    ?assert(maps:get(<<"metadata">>, CdmiResponse1) =/= <<>>).


get_file_cdmi(Config) ->
    {Workers, RootPath, FilledFileName, FileContent,_, _} = get_file_base(Config),
    RootDirPath = list_to_binary("/" ++ RootPath),
    %%-- selective params read -----
    RequestHeaders2 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code2, _Headers2, Response2} = cdmi_internal:do_request(
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
    {ok, Code3, _Headers3, Response3} = cdmi_internal:do_request(
        Workers, FilledFileName ++ "?value:1-3;valuerange", get, RequestHeaders3, []
    ),
    ?assertEqual(?HTTP_200_OK, Code3),
    CdmiResponse3 = json_utils:decode(Response3),
    ?assertMatch(#{<<"valuerange">> := <<"1-3">>}, CdmiResponse3),
    % 1-3 from FileContent = <<"Some content...">>
    ?assertEqual(<<"ome">>, base64:decode(maps:get(<<"value">>, CdmiResponse3))),

    %%------- objectid read --------
    RequestHeaders5 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code5, _Headers5, Response5} = cdmi_internal:do_request(
        Workers, FilledFileName ++ "?objectID", get, RequestHeaders5, []
    ),
    ?assertEqual(?HTTP_200_OK, Code5),
    CdmiResponse5 = (json_utils:decode(Response5)),
    ObjectID = maps:get(<<"objectID">>, CdmiResponse5),

    ?assert(is_binary(ObjectID)),

    %%-------- read by id ----------
    RequestHeaders6 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code6, _Headers6, Response6} = cdmi_internal:do_request(
        Workers, "cdmi_objectid/" ++ binary_to_list(ObjectID), get, RequestHeaders6, []
    ),
    ?assertEqual(?HTTP_200_OK, Code6),
    CdmiResponse6 = (json_utils:decode(Response6)),

    ?assertEqual(FileContent, base64:decode(maps:get(<<"value">>, CdmiResponse6))).


get_file_non_cdmi(Config) ->
    {Workers, _, FilledFileName, FileContent, _, EmptyFileName} = get_file_base(Config),
    %%------- noncdmi read --------
    {ok, Code4, Headers4, Response4} =
        cdmi_internal:do_request(Workers, FilledFileName, get, [user_2_token_header()]),
    ?assertEqual(?HTTP_200_OK, Code4),
    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/octet-stream">>}, Headers4),
    ?assertEqual(FileContent, Response4),

    %% selective value single range read non-cdmi
    ?assertMatch(
        {ok, ?HTTP_206_PARTIAL_CONTENT, #{?HDR_CONTENT_RANGE := <<"bytes 5-8/15">>}, <<"cont">>},
        cdmi_internal:do_request(Workers, FilledFileName, get, [
            {?HDR_RANGE, <<"bytes=5-8">>}, user_2_token_header()
        ])
    ),

    %% selective value multi range read non-cdmi
    {ok, _, #{
        ?HDR_CONTENT_TYPE := <<"multipart/byteranges; boundary=", Boundary/binary>>
    }, Response8} = ?assertMatch(
        {ok, ?HTTP_206_PARTIAL_CONTENT, #{?HDR_CONTENT_TYPE := <<"multipart/byteranges", _/binary>>}, _},
        cdmi_internal:do_request(Workers, FilledFileName, get, [
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

    %% read file non-cdmi with invalid Range should fail
    lists:foreach(fun(InvalidRange) ->
        ?assertMatch(
            {ok, ?HTTP_416_RANGE_NOT_SATISFIABLE, #{?HDR_CONTENT_RANGE := <<"bytes */15">>}, <<>>},
            cdmi_internal:do_request(Workers, FilledFileName, get, [
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

    %% read empty file non-cdmi without Range
    ?assertMatch(
        {ok, ?HTTP_200_OK, _, <<>>},
        cdmi_internal:do_request(Workers, EmptyFileName, get, [user_2_token_header()])
    ),
    %%------------------------------

    %% read empty file non-cdmi with Range should return 416
    ?assertMatch(
        {ok, ?HTTP_416_RANGE_NOT_SATISFIABLE, #{?HDR_CONTENT_RANGE := <<"bytes */0">>}, <<>>},
        cdmi_internal:do_request(Workers, EmptyFileName, get, [
            {?HDR_RANGE, <<"bytes=10-15">>}, user_2_token_header()
        ])
    ).


get_root_with_objectid_endpoint(Config) ->
    {Workers, RootPath, _, _} = objectid_base(Config),
    [WorkerP1, _WorkerP2] = Workers,
    %%-------- / objectid ----------
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code1, Headers1, Response1} = cdmi_internal:do_request(Workers, "", get, RequestHeaders1, []),
    ?assertEqual(?HTTP_200_OK, Code1),

    RequestHeaders0 = [?CDMI_VERSION_HEADER, user_2_token_header()],

    % TODO VFS-7288 clarify what should be written to cdmi_size for directories
    {ok, Code0, _Headers0, _Response0} =
        cdmi_internal:do_request(WorkerP1, RootPath, get, RequestHeaders0, []),
    ?assertEqual(?HTTP_200_OK, Code0),
    CdmiResponse1 = json_utils:decode(Response1),
    RootId = maps:get(<<"objectID">>, CdmiResponse1, undefined),

    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/cdmi-container">>}, Headers1),
    ?assertMatch(#{<<"objectName">> := <<"/">>}, CdmiResponse1),
    ?assertNotEqual(RootId, undefined),
    ?assert(is_binary(RootId)),
    ?assertMatch(#{<<"parentURI">> := <<>>}, CdmiResponse1),
    ?assertEqual(error, maps:find(<<"parentID">>, CdmiResponse1)),
    ?assertMatch(#{<<"capabilitiesURI">> := <<"cdmi_capabilities/container/">>}, CdmiResponse1),

    %%---- get / by objectid -------
    RequestHeaders4 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code4, _Headers4, Response4} = cdmi_internal:do_request(
        Workers, "cdmi_objectid/" ++ binary_to_list(RootId) ++ "/", get, RequestHeaders4, []
    ),
    ?assertEqual(?HTTP_200_OK, Code4),
    CdmiResponse4 = json_utils:decode(Response4),
    Meta1 = maps:remove(<<"cdmi_atime">>, maps:get(<<"metadata">>, CdmiResponse1)),
    CdmiResponse1WithoutAtime = maps:put(<<"metadata">>, Meta1, CdmiResponse1),
    Meta4 = maps:remove(<<"cdmi_atime">>, maps:get(<<"metadata">>, CdmiResponse4)),
    CdmiResponse4WithoutAtime = maps:put(<<"metadata">>, Meta4, CdmiResponse4),

    ?assertEqual(CdmiResponse1WithoutAtime, CdmiResponse4WithoutAtime). % should be the same as in 1 (except access time)


get_dir_with_objectid_endpoint(Config) ->
    {Workers, RootPath, TestDirName, ShortTestDirName} = objectid_base(Config),
    TestDirNameCheck = list_to_binary(ShortTestDirName ++ "/"),
    %%------ /dir objectid ---------
    RequestHeaders2 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code2, _Headers2, Response2} = cdmi_internal:do_request(Workers, TestDirName ++ "/", get, RequestHeaders2, []),
    ?assertEqual(?HTTP_200_OK, Code2),
    CdmiResponse2 = (json_utils:decode(Response2)),
    DirId = maps:get(<<"objectID">>, CdmiResponse2, undefined),
    RootDirPath = list_to_binary("/" ++ RootPath),
    {ok, RootId} = file_id:guid_to_objectid(node_cache:get(root_dir_guid)),

    ?assertMatch(#{<<"objectName">> := TestDirNameCheck}, CdmiResponse2),
    ?assertNotEqual(DirId, undefined),
    ?assert(is_binary(DirId)),
    ?assertMatch(#{<<"parentURI">> := RootDirPath}, CdmiResponse2),
    ?assertMatch(#{<<"parentID">> := RootId}, CdmiResponse2),
    ?assertMatch(#{<<"capabilitiesURI">> := <<"cdmi_capabilities/container/">>}, CdmiResponse2),
    RequestHeaders5 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code5, _Headers5, Response5} = cdmi_internal:do_request(
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
    ).


get_file_with_objectid_endpoint(Config) ->
    {Workers, RootPath, _, _} = objectid_base(Config),
    TestFileName = RootPath ++ "new_file",
    TestFileNameBin = list_to_binary("new_file"),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = TestFileNameBin
        }, Config#cdmi_test_config.p1_selector
    ),
    RequestHeaders3 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code3, _Headers3, Response3} = cdmi_internal:do_request(
        Workers, TestFileName, get, RequestHeaders3, []
    ),
    ?assertEqual(?HTTP_200_OK, Code3),
    CdmiResponse3 = json_utils:decode(Response3),
    FileId = maps:get(<<"objectID">>, CdmiResponse3, undefined),
    RootDirPath1 = list_to_binary("/" ++ RootPath),
    {ok, RootId} = file_id:guid_to_objectid(node_cache:get(root_dir_guid)),

    ?assertMatch(#{<<"objectName">> := TestFileNameBin}, CdmiResponse3),
    ?assertNotEqual(FileId, undefined),
    ?assert(is_binary(FileId)),
    ?assertMatch(#{<<"parentURI">> := RootDirPath1}, CdmiResponse3),
    ?assertMatch(#{<<"parentID">> := RootId}, CdmiResponse3),
    ?assertMatch(#{<<"capabilitiesURI">> := <<"cdmi_capabilities/dataobject/">>}, CdmiResponse3),

    RequestHeaders6 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code6, _Headers6, Response6} = cdmi_internal:do_request(
        Workers, "cdmi_objectid/" ++ binary_to_list(RootId) ++ "/new_file", get, RequestHeaders6, []
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

    {ok, Code7, _Headers7, Response7} = cdmi_internal:do_request(
        Workers, "cdmi_objectid/" ++ binary_to_list(FileId), get, RequestHeaders6, []
    ),
    ?assertEqual(?HTTP_200_OK, Code7),
    CdmiResponse7 = (json_utils:decode(Response7)),
    Meta7 = maps:remove(<<"cdmi_atime">>, (maps:get(<<"metadata">>, CdmiResponse7))),
    CdmiResponse7WithoutAtime = maps:merge(#{<<"metadata">> => Meta7},maps:remove(<<"metadata">>, CdmiResponse7)),

    ?assertEqual( % should be the same as in 6 (except parent and access time)
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse6WithoutAtime)),
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse7WithoutAtime))
    ).


unauthorized_access_by_object_id(Config) ->
    {Workers, _, _, _} = objectid_base(Config),
    {ok, RootId} = file_id:guid_to_objectid(node_cache:get(root_dir_guid)),
    %%---- unauthorized access to / by objectid -------
    RequestHeaders8 = [?CDMI_VERSION_HEADER],
    {ok, Code8, _, Response8} = cdmi_internal:do_request(
        Workers, "cdmi_objectid/" ++ binary_to_list(RootId) ++ "/", get, RequestHeaders8, []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_UNAUTHORIZED),
    ?assertMatch(ExpRestError, {Code8, json_utils:decode(Response8)}).


list_basic_dir(Config) ->
    {Workers, _, _, TestDirName, TestDirNameCheck, TestFileNameBin} = list_dir_base(Config),
    %%------ list basic dir --------
    {ok, Code1, Headers1, Response1} =
        cdmi_internal:do_request(Workers, TestDirName ++ "/", get,
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
    ?assert(maps:get(<<"metadata">>, CdmiResponse1) =/= <<>>).


list_root_space_dir(Config) ->
    {Workers, RootName, RootPath, _, TestDirNameCheck, _} = list_dir_base(Config),

    % TODO VFS-7288 clarify what should be written to cdmi_size for directories
    [WorkerP1, _WorkerP2] = Workers,
    %%------ list root space dir ---------
    {ok, Code2, _Headers2, Response2} =
        cdmi_internal:do_request(WorkerP1, RootPath, get,
            [user_2_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(?HTTP_200_OK, Code2),
    CdmiResponse2 = json_utils:decode(Response2),
    RootNameBin = list_to_binary(RootName),
    ?assertMatch(#{<<"objectName">> := RootNameBin}, CdmiResponse2),
    ?assertMatch(#{<<"children">> := [TestDirNameCheck]}, CdmiResponse2).


list_nonexisting_dir(Config) ->
    {Workers, _, _, _, _, _} = list_dir_base(Config),
    %%--- list nonexisting dir -----
    {ok, Code3, _Headers3, _Response3} =
        cdmi_internal:do_request(Workers, "nonexisting_dir/",
            get, [user_2_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(?HTTP_404_NOT_FOUND, Code3).


selective_params_list(Config) ->
    {Workers, _, _, TestDirName, TestDirNameCheck, TestFileNameBin} = list_dir_base(Config),
    %%-- selective params list -----
    {ok, Code4, _Headers4, Response4} =
        cdmi_internal:do_request(Workers, TestDirName ++ "/?children;objectName",
            get, [user_2_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(?HTTP_200_OK, Code4),
    CdmiResponse4 = json_utils:decode(Response4),
    ?assertMatch(#{<<"objectName">> := TestDirNameCheck}, CdmiResponse4),
    ?assertMatch(#{<<"children">> := [TestFileNameBin]}, CdmiResponse4),
    ?assertEqual(2, maps:size(CdmiResponse4)).


childrenrange_list(Config) ->
    {Workers, _, RootPath, _, _, _} = list_dir_base(Config),
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
        cdmi_internal:do_request(Workers, ChildrangeDir ++ "?children;childrenrange",
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
        cdmi_internal:do_request(Workers, ChildrangeDir ++ "?children:2-13;childrenrange", get,
            [user_2_token_header(), ?CDMI_VERSION_HEADER], []),
    {ok, Code7, _, Response7} =
        cdmi_internal:do_request(Workers, ChildrangeDir ++ "?children:0-1;childrenrange", get,
            [user_2_token_header(), ?CDMI_VERSION_HEADER], []),
    {ok, Code8, _, Response8} =
        cdmi_internal:do_request(Workers, ChildrangeDir ++ "?children:14-14;childrenrange", get,
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


%% @private
objectid_base(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {SpaceName, ShortTestDirName, TestDirName, _, _, _} =
        cdmi_internal:create_test_dir_and_file(Config),

    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,
    {Workers, RootPath, TestDirName, ShortTestDirName}.


%% @private
list_dir_base(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {SpaceName, ShortTestDirName, TestDirName, TestFileName, _FullTestFileName, _TestFileContent} =
        cdmi_internal:create_test_dir_and_file(Config),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    TestDirNameCheck = list_to_binary(ShortTestDirName ++ "/"),
    TestFileNameBin = list_to_binary(TestFileName),
    {Workers, RootName, RootPath, TestDirName, TestDirNameCheck, TestFileNameBin}.

%% @private
get_file_base(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,
    EmptyFile = cdmi_internal:get_random_string(),
    EmptyFileName = filename:join([RootPath, EmptyFile]),
    FileName = cdmi_internal:get_random_string(),
    FilledFileName = filename:join([RootPath, FileName]),
    FileNameBin = list_to_binary(FileName),
    FileContent = <<"Some content...">>,
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid), [
        #file_spec{
            name = list_to_binary(EmptyFile)
        },
        #file_spec{
            name = FileNameBin,
            content = FileContent
        }
    ], Config#cdmi_test_config.p1_selector),
    {Workers, RootPath, FilledFileName, FileContent, FileNameBin, EmptyFileName}.