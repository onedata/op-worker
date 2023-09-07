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
    list_basic_dir/1,
    list_root_space_dir/1,
    list_nonexisting_dir/1,
    selective_params_list/1,
    childrenrange_list/1,

    create_file_with_metadata/1,
    selective_metadata_read/1,
    update_user_metadata_file/1,
    create_and_update_dir_with_user_metadata/1,
    write_acl_metadata/1,

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

    delete_file/1,
    delete_dir/1,
    update_file_cdmi/1,
    update_file_http/1,

    system_capabilities/1,
    container_capabilities/1,
    dataobject_capabilities/1,
    use_supported_cdmi_version/1,
    use_unsupported_cdmi_version/1,

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

user_2_token_header() ->
    rest_test_utils:user_token_header(oct_background:get_user_access_token(user2)).

%%%===================================================================
%%% Test functions
%%%===================================================================


% Tests cdmi container GET request (also refered as LIST)
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
    %%------------------------------


list_root_space_dir(Config) ->
    {Workers, RootName, RootPath, _, TestDirNameCheck, _} = list_dir_base(Config),
    %%------ list root space dir ---------
    {ok, Code2, _Headers2, Response2} =
        cdmi_internal:do_request(Workers, RootPath, get,
            [user_2_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(?HTTP_200_OK, Code2),
    CdmiResponse2 = json_utils:decode(Response2),
    RootNameBin = list_to_binary(RootName),
    ?assertMatch(#{<<"objectName">> := RootNameBin}, CdmiResponse2),
    ?assertMatch(#{<<"children">> := [TestDirNameCheck]}, CdmiResponse2).
    %%------------------------------


list_nonexisting_dir(Config) ->
    {Workers, _, _, _, _, _} = list_dir_base(Config),
    %%--- list nonexisting dir -----
    {ok, Code3, _Headers3, _Response3} =
        cdmi_internal:do_request(Workers, "nonexisting_dir/",
            get, [user_2_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(?HTTP_404_NOT_FOUND, Code3).
    %%------------------------------


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
    %%------------------------------


childrenrange_list(Config) ->
    {Workers, RootName, RootPath, TestDirName, TestDirNameCheck, TestFileNameBin} = list_dir_base(Config),
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
%%------------------------------


% tests access to file by objectid
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


objectid_root(Config) ->
    {Workers, RootPath, _, _} = objectid_base(Config),
    %%-------- / objectid ----------
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code1, Headers1, Response1} = cdmi_internal:do_request(Workers, "", get, RequestHeaders1, []),
    ?assertEqual(?HTTP_200_OK, Code1),

    RequestHeaders0 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code0, _Headers0, Response0} = cdmi_internal:do_request(Workers, RootPath, get, RequestHeaders0, []),
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


objectid_dir(Config) ->
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


objectid_file(Config) ->
    {Workers, RootPath, _, _} = objectid_base(Config),
    TestFileName = RootPath ++ "new_file",
    TestFileNameBin = list_to_binary("new_file"),
    {ok, _} = cdmi_internal:create_new_file(TestFileName, Config),
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
%%------------------------------


% test error handling
%% @private
errors_base(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {SpaceName, _ShortTestDirName, TestDirName, _TestFileName, _FullTestFileName, _TestFileContent} =
        cdmi_internal:create_test_dir_and_file(Config),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    {Workers, RootPath, TestDirName}.

unauthorized_access_error(Config) ->
    {Workers, _, TestDirName} = errors_base(Config),
    %%---- unauthorized access -----
    {ok, Code1, _Headers1, Response1} =
        cdmi_internal:do_request(Workers, TestDirName, get, [], []),
    ExpRestError1 = rest_test_utils:get_rest_error(?ERROR_UNAUTHORIZED),
    ?assertMatch(ExpRestError1, {Code1, json_utils:decode(Response1)}).
    %%------------------------------


wrong_create_path_error(Config) ->
    {Workers, RootPath, _} = errors_base(Config),
    %%----- wrong create path ------
    RequestHeaders2 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code2, _Headers2, Response2} =
        cdmi_internal:do_request(Workers, RootPath ++ "/test_dir", put, RequestHeaders2, []),
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
        cdmi_internal:do_request(Workers, RootPath ++ "/test_dir/", put, RequestHeaders3, []),
    ExpRestError3 = rest_test_utils:get_rest_error(?ERROR_BAD_VALUE_IDENTIFIER(<<"path">>)),
    ?assertMatch(ExpRestError3, {Code3, json_utils:decode(Response3)}).
    %%------------------------------


wrong_base_error(Config) ->
    {Workers, RootPath, _} = errors_base(Config),
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
    {ok, Code4, _Headers4, Response4} = cdmi_internal:do_request(
        Workers, RootPath ++ "/some_file_b64", put, RequestHeaders4, RequestBody4
    ),
    ExpRestError4 = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(<<"base64">>)),
    ?assertMatch(ExpRestError4, {Code4, json_utils:decode(Response4)}).
    %%------------------------------


non_existing_file_error(Config) ->
    {Workers, RootPath, _} = errors_base(Config),
    %%-- reading non-existing file --
    RequestHeaders6 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER
    ],
    {ok, Code6, _Headers6, _Response6} = cdmi_internal:do_request(
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
    {ok, Code7, _Headers7, _Response7} = cdmi_internal:do_request(
        Workers, RootPath ++ "/nonexisting_dir/", get, RequestHeaders7
    ),
    ?assertEqual(Code7, ?HTTP_404_NOT_FOUND).
    %%------------------------------


open_binary_file_without_permission(Config) ->
    {Workers, RootPath, _} = errors_base(Config),
    %%--- open binary file without permission -----
    File8 = filename:join([RootPath, "file8"]),
    FileContent8 = <<"File content...">>,
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"file8">>,
            content = FileContent8
        }, Config#cdmi_test_config.p1_selector
    ),
    ?assertEqual(cdmi_internal:object_exists(File8, Config), true),

    cdmi_internal:write_to_file(File8, FileContent8, ?FILE_BEGINNING, Config),
    ?assertEqual(cdmi_internal:get_file_content(File8, Config), FileContent8, ?ATTEMPTS),
    RequestHeaders8 = [user_2_token_header()],

    cdmi_internal:mock_opening_file_without_perms(Config),
    {ok, Code8, _Headers8, Response8} =
        cdmi_internal:do_request(Workers, File8, get, RequestHeaders8),
    cdmi_internal:unmock_opening_file_without_perms(Config),
    ExpRestError8 = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError8, {Code8, json_utils:decode(Response8)}, ?ATTEMPTS).
    %%------------------------------


open_cdmi_file_without_permission(Config) ->
    {Workers, RootPath, _} = errors_base(Config),
    %%--- open cdmi file without permission -----
    File9 = filename:join([RootPath, "file9"]),
    FileContent9 = <<"File content...">>,
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"file9">>,
            content = FileContent9
        }, Config#cdmi_test_config.p1_selector
    ),
    ?assertEqual(cdmi_internal:object_exists(File9, Config), true),

    cdmi_internal:write_to_file(File9, FileContent9, ?FILE_BEGINNING, Config),
    ?assertEqual(cdmi_internal:get_file_content(File9, Config), FileContent9, ?ATTEMPTS),
    RequestHeaders9 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER
    ],

    cdmi_internal:mock_opening_file_without_perms(Config),
    {ok, Code9, _Headers9, Response9} = cdmi_internal:do_request(
        Workers, File9, get, RequestHeaders9
    ),
    cdmi_internal:unmock_opening_file_without_perms(Config),
    ExpRestError9 = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError9, {Code9, json_utils:decode(Response9)}, ?ATTEMPTS).
%%------------------------------


%%  Tests cdmi object GET request. Request can be done without cdmi header (in that case
%%  file conent is returned as response body), or with cdmi header (the response
%%  contains json string of type: application/cdmi-object, and we can specify what
%%  parameters we need by listing then as ';' separated list after '?' in URL )
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
    %%------------------------------


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
    %%------------------------------

    %%------- objectid read --------
    RequestHeaders5 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code5, _Headers5, Response5} = cdmi_internal:do_request(
        Workers, FilledFileName ++ "?objectID", get, RequestHeaders5, []
    ),
    ?assertEqual(?HTTP_200_OK, Code5),
    CdmiResponse5 = (json_utils:decode(Response5)),
    ObjectID = maps:get(<<"objectID">>, CdmiResponse5),

    ?assert(is_binary(ObjectID)),
    %%------------------------------

    %%-------- read by id ----------
    RequestHeaders6 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code6, _Headers6, Response6} = cdmi_internal:do_request(
        Workers, "cdmi_objectid/" ++ binary_to_list(ObjectID), get, RequestHeaders6, []
    ),
    ?assertEqual(?HTTP_200_OK, Code6),
    CdmiResponse6 = (json_utils:decode(Response6)),

    ?assertEqual(FileContent, base64:decode(maps:get(<<"value">>, CdmiResponse6))).
    %%------------------------------


get_file_non_cdmi(Config) ->
    {Workers, _, FilledFileName, FileContent, _, EmptyFileName} = get_file_base(Config),
    %%------- noncdmi read --------
    {ok, Code4, Headers4, Response4} =
        cdmi_internal:do_request(Workers, FilledFileName, get, [user_2_token_header()]),
    ?assertEqual(?HTTP_200_OK, Code4),
    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/octet-stream">>}, Headers4),
    ?assertEqual(FileContent, Response4),
    %%------------------------------

    %% selective value single range read non-cdmi
    ?assertMatch(
        {ok, ?HTTP_206_PARTIAL_CONTENT, #{?HDR_CONTENT_RANGE := <<"bytes 5-8/15">>}, <<"cont">>},
        cdmi_internal:do_request(Workers, FilledFileName, get, [
            {?HDR_RANGE, <<"bytes=5-8">>}, user_2_token_header()
        ])
    ),
    %%------------------------------

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
    %%------------------------------

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
    %%------------------------------

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


% Tests cdmi metadata read on object GET request.
%% @private
metadata_base(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,
    UserId2 = oct_background:get_user_id(user2),

    {Workers, RootPath, UserId2}.


create_file_with_metadata(Config) ->
    {Workers, RootPath, UserId2} = metadata_base(Config),
    FileName = filename:join([RootPath, "metadataTest1.txt"]),
    FileContent = <<"Some content...">>,
    %%-------- create file with user metadata --------
    ?assert(not cdmi_internal:object_exists(FileName, Config)),
    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody1 = #{
        <<"value">> => FileContent,
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"text/plain">>,
        <<"metadata">> => #{<<"my_metadata">> => <<"my_value">>,
                            <<"cdmi_not_allowed">> => <<"my_value">>}},

    RawRequestBody1 = json_utils:encode(RequestBody1),
    Before = time:seconds_to_datetime(global_clock:timestamp_seconds()),
    {ok, Code1, _Headers1, Response1} = cdmi_internal:do_request(
        Workers, FileName, put, RequestHeaders1, RawRequestBody1
    ),
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
    ?assertEqual(6, maps:size(Metadata1)).


selective_metadata_read(Config) ->
    {Workers, RootPath, UserId2} = metadata_base(Config),
    FileName = filename:join([RootPath, "metadataTest2.txt"]),
    FileContent = <<"Some content...">>,
    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody1 = #{
        <<"value">> => FileContent,
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"text/plain">>,
        <<"metadata">> => #{<<"my_metadata">> => <<"my_value">>,
            <<"cdmi_not_allowed">> => <<"my_value">>}},

    RawRequestBody1 = json_utils:encode(RequestBody1),
    {ok, _, _Headers1, _} = cdmi_internal:do_request(
        Workers, FileName, put, RequestHeaders1, RawRequestBody1
    ),

    %%-- selective metadata read -----
    {ok, ?HTTP_200_OK, _Headers2, Response2} = cdmi_internal:do_request(
        Workers, FileName ++ "?metadata", get, RequestHeaders1, []
    ),
    CdmiResponse2 = (json_utils:decode(Response2)),
    Metadata2 = maps:get(<<"metadata">>, CdmiResponse2),
    ?assertEqual(1, maps:size(CdmiResponse2)),
    ?assertEqual(6, maps:size(Metadata2)),

    %%-- selective metadata read with prefix -----
    {ok, ?HTTP_200_OK, _Headers3, Response3} = cdmi_internal:do_request(
        Workers, FileName ++ "?metadata:cdmi_", get, RequestHeaders1, []
    ),
    CdmiResponse3 = (json_utils:decode(Response3)),
    Metadata3 = maps:get(<<"metadata">>, CdmiResponse3),
    ?assertEqual(1, maps:size(CdmiResponse3)),
    ?assertEqual(5, maps:size(Metadata3)),

    {ok, ?HTTP_200_OK, _Headers4, Response4} = cdmi_internal:do_request(
        Workers, FileName ++ "?metadata:cdmi_o", get, RequestHeaders1, []
    ),
    CdmiResponse4 = json_utils:decode(Response4),
    Metadata4 = maps:get(<<"metadata">>, CdmiResponse4),
    ?assertEqual(1, maps:size(CdmiResponse4)),
    ?assertMatch(UserId2, maps:get(<<"cdmi_owner">>, Metadata4)),
    ?assertEqual(1, maps:size(Metadata4)),

    {ok, ?HTTP_200_OK, _Headers5, Response5} = cdmi_internal:do_request(
        Workers, FileName ++ "?metadata:cdmi_size", get, RequestHeaders1, []
    ),
    CdmiResponse5 = json_utils:decode(Response5),
    Metadata5 = maps:get(<<"metadata">>, CdmiResponse5),
    ?assertEqual(1, maps:size(CdmiResponse5)),
    ?assertMatch(#{<<"cdmi_size">> := <<"15">>}, Metadata5),
    ?assertEqual(1, maps:size(Metadata5)),

    {ok, ?HTTP_200_OK, _Headers6, Response6} = cdmi_internal:do_request(
        Workers, FileName ++ "?metadata:cdmi_no_such_metadata", get, RequestHeaders1, []
    ),
    CdmiResponse6 = json_utils:decode(Response6),
    ?assertEqual(1, maps:size(CdmiResponse6)),
    ?assertMatch(#{<<"metadata">> := #{}}, CdmiResponse6).


update_user_metadata_file(Config) ->
    {Workers, RootPath, _} = metadata_base(Config),
    FileName = filename:join([RootPath, "metadataTest3.txt"]),
    FileContent = <<"Some content...">>,
    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody1 = #{
        <<"value">> => FileContent,
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"text/plain">>,
        <<"metadata">> => #{<<"my_metadata">> => <<"my_value">>,
            <<"cdmi_not_allowed">> => <<"my_value">>}},

    RawRequestBody1 = json_utils:encode(RequestBody1),
    {ok, _, _Headers1, _} = cdmi_internal:do_request(
        Workers, FileName, put, RequestHeaders1, RawRequestBody1
    ),
    %%------ update user metadata of a file ----------
    RequestBody7 = #{<<"metadata">> => #{<<"my_new_metadata">> => <<"my_new_value">>}},
    RawRequestBody7 = json_utils:encode(RequestBody7),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, FileName, put, RequestHeaders1, RawRequestBody7
    ),
    {ok, ?HTTP_200_OK, _Headers7, Response7} = cdmi_internal:do_request(
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
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers,
        FileName ++ "?metadata:my_new_metadata_add;metadata:my_new_metadata;metadata:cdmi_not_allowed",
        put,
        RequestHeaders1,
        RawRequestBody8
    ),
    {ok, ?HTTP_200_OK, _Headers8, Response8} = cdmi_internal:do_request(
        Workers, FileName ++ "?metadata:my", get, RequestHeaders1, []
    ),
    CdmiResponse8 = (json_utils:decode(Response8)),
    Metadata8 = maps:get(<<"metadata">>, CdmiResponse8),
    ?assertEqual(1, maps:size(CdmiResponse8)),
    ?assertMatch(#{<<"my_new_metadata_add">> := <<"my_new_value_add">>}, Metadata8),
    ?assertMatch(#{<<"my_new_metadata">> := <<"my_new_value_update">>}, Metadata8),
    ?assertEqual(2, maps:size(Metadata8)),
    {ok, ?HTTP_200_OK, _Headers9, Response9} = cdmi_internal:do_request(
        Workers, FileName ++ "?metadata:cdmi_", get, RequestHeaders1, []
    ),
    CdmiResponse9 = (json_utils:decode(Response9)),
    Metadata9 = maps:get(<<"metadata">>, CdmiResponse9),
    ?assertEqual(1, maps:size(CdmiResponse9)),
    ?assertEqual(5, maps:size(Metadata9)),

    RequestBody10 = #{<<"metadata">> => #{<<"my_new_metadata">> => <<"my_new_value_ignore">>}},
    RawRequestBody10 = json_utils:encode(RequestBody10),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, FileName ++ "?metadata:my_new_metadata_add", put, RequestHeaders1, RawRequestBody10
    ),
    {ok, ?HTTP_200_OK, _Headers10, Response10} = cdmi_internal:do_request(
        Workers, FileName ++ "?metadata:my", get, RequestHeaders1, []
    ),
    CdmiResponse10 = (json_utils:decode(Response10)),
    Metadata10 = maps:get(<<"metadata">>, CdmiResponse10),
    ?assertEqual(1, maps:size(CdmiResponse10)),
    ?assertMatch(#{<<"my_new_metadata">> := <<"my_new_value_update">>}, Metadata10),
    ?assertEqual(1, maps:size(Metadata10)).


create_and_update_dir_with_user_metadata(Config) ->
    {Workers, RootPath, _} = metadata_base(Config),
    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    DirName = filename:join([RootPath, "metadataTestDir1"]) ++ "/",

    %%------ create directory with user metadata  ----------
    RequestHeaders2 = [?CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody11 = #{<<"metadata">> => #{<<"my_metadata">> => <<"my_dir_value">>}},
    RawRequestBody11 = json_utils:encode(RequestBody11),
    {ok, ?HTTP_201_CREATED, _Headers11, Response11} = cdmi_internal:do_request(
        Workers, DirName, put, RequestHeaders2, RawRequestBody11
    ),
    CdmiResponse11 = (json_utils:decode(Response11)),
    Metadata11 = maps:get(<<"metadata">>, CdmiResponse11),
    ?assertMatch(#{<<"my_metadata">> := <<"my_dir_value">>}, Metadata11),

    %%------ update user metadata of a directory ----------
    RequestBody12 = #{<<"metadata">> => #{<<"my_metadata">> => <<"my_dir_value_update">>}},
    RawRequestBody12 = json_utils:encode(RequestBody12),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, DirName, put, RequestHeaders2, RawRequestBody12
    ),
    {ok, ?HTTP_200_OK, _Headers13, Response13} = cdmi_internal:do_request(
        Workers, DirName ++ "?metadata:my", get, RequestHeaders1, []
    ),
    CdmiResponse13 = (json_utils:decode(Response13)),
    Metadata13 = maps:get(<<"metadata">>, CdmiResponse13),

    ?assertEqual(1, maps:size(CdmiResponse13)),
    ?assertMatch(#{<<"my_metadata">> := <<"my_dir_value_update">>}, Metadata13),
    ?assertEqual(1, maps:size(Metadata13)).
    %%------------------------------


write_acl_metadata(Config) ->
    UserName2 = <<"Unnamed User">>,
    {Workers, RootPath, UserId2} = metadata_base(Config),
    DirName = filename:join([RootPath, "metadataTestDir2"]) ++ "/",
    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],

    RequestHeaders2 = [?CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody11 = #{<<"metadata">> => #{<<"my_metadata">> => <<"my_dir_value">>}},
    RawRequestBody11 = json_utils:encode(RequestBody11),
    {ok, ?HTTP_201_CREATED, _Headers11, _Response11} = cdmi_internal:do_request(
        Workers, DirName, put, RequestHeaders2, RawRequestBody11
    ),
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

    {ok, Code15, _Headers15, Response15} = cdmi_internal:do_request(
        Workers, FileName2 ++ "?metadata:cdmi_acl", put, RequestHeaders15, RawRequestBody15
    ),
    ?assertMatch({?HTTP_204_NO_CONTENT, _}, {Code15, Response15}),

    {ok, Code16, _Headers16, Response16} = cdmi_internal:do_request(
        Workers, FileName2 ++ "?metadata", get, RequestHeaders1, []
    ),
    CdmiResponse16 = (json_utils:decode(Response16)),
    Metadata16 = maps:get(<<"metadata">>, CdmiResponse16),
    ?assertEqual(?HTTP_200_OK, Code16),
    ?assertEqual(1, maps:size(CdmiResponse16)),
    ?assertEqual(6, maps:size(Metadata16)),
    ?assertMatch(#{<<"cdmi_acl">> := [Ace1, Ace2]}, Metadata16),

    {ok, Code17, _Headers17, Response17} = cdmi_internal:do_request(
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

    {ok, Code18, _Headers18, _Response18} = cdmi_internal:do_request(
        Workers, DirName ++ "?metadata:cdmi_acl", put, RequestHeaders18, RawRequestBody18
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code18),

    {ok, Code19, _Headers19, Response19} = cdmi_internal:do_request(
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
    {ok, Code1, _Headers1, _Response1} = cdmi_internal:do_request(
            Workers, FileName, delete, [user_2_token_header() | RequestHeaders1]
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code1),
    ?assert(not cdmi_internal:object_exists(FileName, Config)),
    %%------------------------------

    %%----- delete group file ------
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"groupFile">>
        }, Config#cdmi_test_config.p1_selector
    ),
    RequestHeaders2 = [?CDMI_VERSION_HEADER],
    {ok, Code2, _Headers2, _Response2} =
        cdmi_internal:do_request(Workers, GroupFileName, delete,
            [user_2_token_header() | RequestHeaders2]),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code2),
    ?assert(not cdmi_internal:object_exists(GroupFileName, Config)).
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
        cdmi_internal:do_request(Workers, DirName, delete, RequestHeaders1, []),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code1),
    ?assert(not cdmi_internal:object_exists(DirName, Config)),
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
    {ok, Code2, _Headers2, _Response2} = cdmi_internal:do_request(
        Workers, DirName, delete, RequestHeaders2, []
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code2),
    ?assert(not cdmi_internal:object_exists(DirName, Config)),
    ?assert(not cdmi_internal:object_exists(ChildDirName, Config)),
    %%------------------------------

    %%----- delete root dir -------
    RequestHeaders3 = [user_2_token_header(), ?CDMI_VERSION_HEADER],
    ?assert(cdmi_internal:object_exists("/", Config)),

    {ok, Code3, _Headers3, Response3} = cdmi_internal:do_request(
        Workers, "/", delete, RequestHeaders3, []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EPERM)),
    ?assertMatch(ExpRestError, {Code3, json_utils:decode(Response3)}),
    ?assert(cdmi_internal:object_exists("/", Config)).
%%------------------------------


% Tests cdmi object PUT requests (updating content)
update_file_cdmi(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {_SpaceName, _ShortTestDirName, _TestDirName, _TestFileName, FullTestFileName, TestFileContent} =
        cdmi_internal:create_test_dir_and_file(Config),

    NewValue = <<"New Value!">>,
    UpdatedValue = <<"123 Value!">>,

    %%--- value replace, cdmi ------
    ?assert(cdmi_internal:object_exists(FullTestFileName, Config)),
    ?assertEqual(TestFileContent, cdmi_internal:get_file_content(FullTestFileName, Config), ?ATTEMPTS),

    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody1 = #{<<"value">> => NewValue},
    RawRequestBody1 = json_utils:encode(RequestBody1),

    {ok, Code1, _Headers1, _Response1} = cdmi_internal:do_request(
        Workers, FullTestFileName, put, RequestHeaders1, RawRequestBody1
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code1),
    ?assert(cdmi_internal:object_exists(FullTestFileName, Config)),
    ?assertEqual(NewValue, cdmi_internal:get_file_content(FullTestFileName, Config), ?ATTEMPTS),
    %%------------------------------

    %%---- value update, cdmi ------
    UpdateValue = <<"123">>,
    RequestHeaders2 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody2 = #{<<"value">> => base64:encode(UpdateValue)},
    RawRequestBody2 = json_utils:encode(RequestBody2),
    {ok, Code2, _Headers2, _Response2} = cdmi_internal:do_request(
        Workers, FullTestFileName ++ "?value:0-2", put, RequestHeaders2, RawRequestBody2
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code2),
    ?assert(cdmi_internal:object_exists(FullTestFileName, Config)),
    ?assertEqual(UpdatedValue, cdmi_internal:get_file_content(FullTestFileName, Config), ?ATTEMPTS).
    %%------------------------------


update_file_http(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {_SpaceName, _ShortTestDirName, _TestDirName, _TestFileName, FullTestFileName, TestFileContent} =
        cdmi_internal:create_test_dir_and_file(Config),

    NewValue = <<"New Value!">>,
    UpdatedValue = <<"123 Value!">>,
    %%--- value replace, http ------
    RequestBody3 = TestFileContent,
    {ok, Code3, _Headers3, _Response3} =
        cdmi_internal:do_request(Workers, FullTestFileName, put, [user_2_token_header()], RequestBody3),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code3),
    ?assert(cdmi_internal:object_exists(FullTestFileName, Config)),
    ?assertEqual(TestFileContent,
        cdmi_internal:get_file_content(FullTestFileName, Config), ?ATTEMPTS),
    %%------------------------------

    %%---- value update, http ------
    UpdateValue = <<"123">>,
    RequestHeaders4 = [{?HDR_CONTENT_RANGE, <<"bytes 0-2/3">>}],
    {ok, Code4, _Headers4, _Response4} =
        cdmi_internal:do_request(Workers, FullTestFileName,
            put, [user_2_token_header() | RequestHeaders4], UpdateValue),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code4),
    ?assert(cdmi_internal:object_exists(FullTestFileName, Config)),
    ?assertEqual(<<"123t_file_content">>,
        cdmi_internal:get_file_content(FullTestFileName, Config), ?ATTEMPTS),
    %%------------------------------

    %%---- value update2, http -----
    UpdateValue2 = <<"00">>,
    RequestHeaders5 = [{?HDR_CONTENT_RANGE, <<"bytes 3-4/*">>}],
    {ok, Code5, _Headers5, _Response5} =
        cdmi_internal:do_request(Workers, FullTestFileName,
            put, [user_2_token_header() | RequestHeaders5], UpdateValue2),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code5),
    ?assert(cdmi_internal:object_exists(FullTestFileName, Config)),
    ?assertEqual(<<"12300file_content">>,
        cdmi_internal:get_file_content(FullTestFileName, Config), ?ATTEMPTS),
    %%------------------------------

    %%---- value update, http error ------
    UpdateValue = <<"123">>,
    RequestHeaders6 = [{?HDR_CONTENT_RANGE, <<"bytes 0-2,3-4/*">>}],
    {ok, Code6, _Headers6, Response6} =
        cdmi_internal:do_request(Workers, FullTestFileName, put, [user_2_token_header() | RequestHeaders6],
            UpdateValue),

    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(?HDR_CONTENT_RANGE)),
    ?assertMatch(ExpRestError, {Code6, json_utils:decode(Response6)}),
    ?assert(cdmi_internal:object_exists(FullTestFileName, Config)),
    ?assertEqual(<<"12300file_content">>,
        cdmi_internal:get_file_content(FullTestFileName, Config), ?ATTEMPTS).
%%------------------------------


% tests if capabilities of objects, containers, and whole storage system are set properly
system_capabilities(Config) ->
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
    %%------------------------------


container_capabilities(Config) ->
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
    %%------------------------------


dataobject_capabilities(Config) ->
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
        cdmi_internal:do_request(Workers, "/random", get, RequestHeaders),

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
        cdmi_internal:do_request(Workers, "/random", get, RequestHeaders),

    % then
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>])),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


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
    {ok, Code1, _Headers1, _Response1} = cdmi_internal:do_request(
        Workers, FileToCreate, put, RequestHeaders1, RawRequestBody1
    ),
    ?assertEqual(?HTTP_201_CREATED, Code1),
    %%------------------------------

    %%-- dir missing content-type --
    RequestHeaders3 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody3 = #{<<"metadata">> => <<"">>},
    RawRequestBody3 = json_utils:encode(RequestBody3),
    {ok, Code3, _Headers3, _Response3} = cdmi_internal:do_request(
        Workers, DirToCreate, put, RequestHeaders3, RawRequestBody3
    ),
    ?assertEqual(?HTTP_201_CREATED, Code3).
%%------------------------------


% tests mimetype and valuetransferencoding properties, they are part of cdmi-object and cdmi-container
% and should be changeble
mimetype_and_encoding_non_cdmi_file(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {_SpaceName, _ShortTestDirName, TestDirName, TestFileName, _FullTestFileName, _TestFileContent} =
        cdmi_internal:create_test_dir_and_file(Config),

    %% get mimetype and valuetransferencoding of non-cdmi file
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code1, _Headers1, Response1} = cdmi_internal:do_request(
        Workers,
        filename:join(TestDirName, TestFileName) ++ "?mimetype;valuetransferencoding",
        get,
        RequestHeaders1,
        []
    ),
    ?assertEqual(?HTTP_200_OK, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    ?assertMatch(#{<<"mimetype">> := <<"application/octet-stream">>}, CdmiResponse1),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"base64">>}, CdmiResponse1).
    %%------------------------------


update_mimetype_and_encoding(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {_SpaceName, _ShortTestDirName, TestDirName, TestFileName, _FullTestFileName, _TestFileContent} =
        cdmi_internal:create_test_dir_and_file(Config),
    %%-- update mime and encoding --
    RequestHeaders2 = [?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER, user_2_token_header()],
    RawBody2 = json_utils:encode(#{
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"application/binary">>
    }),
    {ok, Code2, _Headers2, _Response2} = cdmi_internal:do_request(
        Workers, filename:join(TestDirName, TestFileName), put, RequestHeaders2, RawBody2
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code2),

    {ok, Code3, _Headers3, Response3} = cdmi_internal:do_request(
        Workers,
        filename:join(TestDirName, TestFileName) ++ "?mimetype;valuetransferencoding",
        get,
        RequestHeaders2,
        []
    ),
    ?assertEqual(?HTTP_200_OK, Code3),
    CdmiResponse3 = (json_utils:decode(Response3)),
    ?assertMatch(#{<<"mimetype">> := <<"application/binary">>}, CdmiResponse3),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>}, CdmiResponse3).
    %%------------------------------


mimetype_and_encoding_create_file(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {SpaceName, _ShortTestDirName, _TestDirName, _TestFileName, _FullTestFileName, _TestFileContent} =
        cdmi_internal:create_test_dir_and_file(Config),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,
    %% create file with given mime and encoding
    FileName4 = filename:join([RootPath, "mime_file.txt"]),
    FileContent4 = <<"some content">>,
    RequestHeaders4 = [?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER, user_2_token_header()],
    RawBody4 = json_utils:encode(#{
        <<"valuetransferencoding">> => <<"utf-8">>,
        <<"mimetype">> => <<"text/plain">>,
        <<"value">> => FileContent4
    }),
    {ok, Code4, _Headers4, Response4} = cdmi_internal:do_request(
        Workers, FileName4, put, RequestHeaders4, RawBody4
    ),
    ?assertEqual(?HTTP_201_CREATED, Code4),
    CdmiResponse4 = (json_utils:decode(Response4)),
    ?assertMatch(#{<<"mimetype">> := <<"text/plain">>}, CdmiResponse4),

    RequestHeaders5 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code5, _Headers5, Response5} = cdmi_internal:do_request(
        Workers, FileName4 ++ "?value;mimetype;valuetransferencoding", get, RequestHeaders5, []
    ),
    ?assertEqual(?HTTP_200_OK, Code5),
    CdmiResponse5 = (json_utils:decode(Response5)),
    ?assertMatch(#{<<"mimetype">> := <<"text/plain">>}, CdmiResponse5),

    %TODO VFS-7376 what do we return here if file contains valid utf-8 string and we read byte range?
    ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>}, CdmiResponse5),
    ?assertMatch(#{<<"value">> := FileContent4}, CdmiResponse5).
    %%------------------------------


mimetype_and_encoding_create_file_non_cdmi_request(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {SpaceName, _ShortTestDirName, _TestDirName, _TestFileName, _FullTestFileName, _TestFileContent} =
        cdmi_internal:create_test_dir_and_file(Config),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,
    %% create file with given mime and encoding using non-cdmi request
    FileName6 = filename:join([RootPath, "mime_file_noncdmi.txt"]),
    FileContent6 = <<"some content">>,
    RequestHeaders6 = [{?HDR_CONTENT_TYPE, <<"text/plain; charset=utf-8">>}, user_2_token_header()],
    {ok, Code6, _Headers6, _Response6} = cdmi_internal:do_request(
        Workers, FileName6, put, RequestHeaders6, FileContent6
    ),
    ?assertEqual(?HTTP_201_CREATED, Code6),

    RequestHeaders7 = [?CDMI_VERSION_HEADER, user_2_token_header()],
    {ok, Code7, _Headers7, Response7} = cdmi_internal:do_request(
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
        cdmi_internal:create_test_dir_and_file(Config),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    FileName = filename:join([RootPath, "random_range_file.txt"]),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"random_range_file.txt">>
        }, Config#cdmi_test_config.p1_selector
    ),

    %%---- reading out of range ---- (shuld return empty binary)
    ?assertEqual(<<>>, cdmi_internal:get_file_content(FileName, Config), ?ATTEMPTS),
    RequestHeaders1 = [user_2_token_header(), ?CDMI_VERSION_HEADER],
    RequestBody1 = json_utils:encode(#{<<"value">> => <<"data">>}),

    {ok, Code1, _Headers1, Response1} = cdmi_internal:do_request(
        Workers, FileName ++ "?value:0-3", get, RequestHeaders1, RequestBody1
    ),
    ?assertEqual(?HTTP_200_OK, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    ?assertMatch(#{<<"value">> := <<>>}, CdmiResponse1),
    %%------------------------------

    %%------ writing at end -------- (shuld extend file)
    ?assertEqual(<<>>, cdmi_internal:get_file_content(FileName, Config), ?ATTEMPTS),

    RequestHeaders2 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody2 = json_utils:encode(#{<<"value">> => base64:encode(<<"data">>)}),
    {ok, Code2, _Headers2, _Response2} = cdmi_internal:do_request(
        Workers, FileName ++ "?value:0-3", put, RequestHeaders2, RequestBody2
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code2),
    ?assertEqual(<<"data">>, cdmi_internal:get_file_content(FileName, Config), ?ATTEMPTS),
    %%------------------------------

    %%------ writing at random -------- (should return zero bytes in any gaps)
     RequestBody3 = json_utils:encode(#{<<"value">> => base64:encode(<<"data">>)}),
     {ok, Code3, _Headers3, _Response3} = cdmi_internal:do_request(
         Workers, FileName ++ "?value:10-13", put, RequestHeaders2, RequestBody3
     ),
     ?assertEqual(?HTTP_204_NO_CONTENT, Code3),

    % "data(6x<0_byte>)data"
     ?assertEqual(
         <<100, 97, 116, 97, 0, 0, 0, 0, 0, 0, 100, 97, 116, 97>>,
         cdmi_internal:get_file_content(FileName, Config),
         ?ATTEMPTS
     ),
    %%------------------------------

    %%----- random childrange ------ (shuld fail)
    {ok, Code4, _Headers4, Response4} = cdmi_internal:do_request(
        Workers, TestDirName ++ "/?children:100-132", get, RequestHeaders2, []
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(<<"childrenrange">>)),
    ?assertMatch(ExpRestError, {Code4, json_utils:decode(Response4)}).
%%------------------------------


% tests cdmi and non-cdmi partial upload feature (requests with x-cdmi-partial flag set to true)
partial_upload_cdmi(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    FileName = filename:join([RootPath, "partial.txt"]),
    Chunk1 = <<"some">>,
    Chunk2 = <<"_">>,
    Chunk3 = <<"value">>,

    %%------ cdmi request partial upload ------
    ?assert(not cdmi_internal:object_exists(FileName, Config)),

    % upload first chunk of file
    RequestHeaders1 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER,
        {"X-CDMI-Partial", "true"}
    ],
    RequestBody1 = json_utils:encode(#{<<"value">> => Chunk1}),
    {ok, Code1, _Headers1, Response1} = cdmi_internal:do_request(
        Workers, FileName, put, RequestHeaders1, RequestBody1
    ),
    ?assertEqual(?HTTP_201_CREATED, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    ?assertMatch(#{<<"completionStatus">> := <<"Processing">>}, CdmiResponse1),

    % upload second chunk of file
    RequestBody2 = json_utils:encode(#{<<"value">> => base64:encode(Chunk2)}),
    {ok, Code2, _Headers2, _Response2} = cdmi_internal:do_request(
        Workers, FileName ++ "?value:4-4", put, RequestHeaders1, RequestBody2
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code2),

    % upload third chunk of file
    RequestHeaders3 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody3 = json_utils:encode(#{<<"value">> => base64:encode(Chunk3)}),
    {ok, Code3, _Headers3, _Response3} = cdmi_internal:do_request(
        Workers, FileName ++ "?value:5-9", put, RequestHeaders3, RequestBody3
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code3),

    % get created file and check its consistency
    RequestHeaders4 = [user_2_token_header(), ?CDMI_VERSION_HEADER],
    % TODO Verify once after VFS-2023
    CheckAllChunks = fun() ->
        {ok, Code4, _Headers4, Response4} = cdmi_internal:do_request(Workers, FileName, get, RequestHeaders4, []),
        ?assertEqual(?HTTP_200_OK, Code4),
        CdmiResponse4 = (json_utils:decode(Response4)),
        ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse4),
        ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>}, CdmiResponse4),
        maps:get(<<"value">>, CdmiResponse4)
    end,
    % File size event change is async
    Chunks123 = <<Chunk1/binary, Chunk2/binary, Chunk3/binary>>,
    ?assertMatch(Chunks123, CheckAllChunks(), 2).
    %%------------------------------


partial_upload_non_cdmi(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    FileName2 = filename:join([RootPath, "partial2.txt"]),
    Chunk1 = <<"some">>,
    Chunk2 = <<"_">>,
    Chunk3 = <<"value">>,
    %%----- non-cdmi request partial upload -------
    ?assert(not cdmi_internal:object_exists(FileName2, Config)),

    % upload first chunk of file
    RequestHeaders5 = [user_2_token_header(), {<<"X-CDMI-Partial">>, <<"true">>}],
    {ok, Code5, _Headers5, _Response5} = cdmi_internal:do_request(
        Workers, FileName2, put, RequestHeaders5, Chunk1
    ),
    ?assertEqual(?HTTP_201_CREATED, Code5),

    RequestHeaders4 = [user_2_token_header(), ?CDMI_VERSION_HEADER],
    % check "completionStatus", should be set to "Processing"
    {ok, Code5_1, _Headers5_1, Response5_1} = cdmi_internal:do_request(
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
    {ok, Code6, _Headers6, _Response6} = cdmi_internal:do_request(
        Workers, FileName2, put, RequestHeaders6, Chunk2
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code6),

    % upload third chunk of file
    RequestHeaders7 = [
        user_2_token_header(),
        {?HDR_CONTENT_RANGE, <<"bytes 5-9/10">>},
        {<<"X-CDMI-Partial">>, <<"false">>}
    ],
    {ok, Code7, _Headers7, _Response7} = cdmi_internal:do_request(
        Workers, FileName2, put, RequestHeaders7, Chunk3
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code7),

    % get created file and check its consistency
    RequestHeaders8 = [user_2_token_header(), ?CDMI_VERSION_HEADER],
    % TODO Verify once after VFS-2023
    CheckAllChunks2 = fun() ->
        {ok, Code8, _Headers8, Response8} = cdmi_internal:do_request(Workers, FileName2, get, RequestHeaders8, []),
        ?assertEqual(?HTTP_200_OK, Code8),
        CdmiResponse8 = (json_utils:decode(Response8)),
        ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse8),
        base64:decode(maps:get(<<"value">>, CdmiResponse8))
    end,
    % File size event change is async
    Chunks123 = <<Chunk1/binary, Chunk2/binary, Chunk3/binary>>,
    ?assertMatch(Chunks123, CheckAllChunks2(), 2).
%%------------------------------


% tests access control lists
acl_file_base(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

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
    {
        Workers, RootPath, MetadataAclReadFull, MetadataAclDelete,
        MetadataAclWrite, MetadataAclReadWrite, MetadataAclReadWriteFull
    }.


acl_read_file(Config) ->
    {Workers, RootPath, _, _, MetadataAclWrite, _, MetadataAclReadWriteFull} = acl_file_base(Config),
    Filename1 = filename:join([RootPath, "acl_test_file1"]),

    %%----- read file test ---------
    % create test file with dummy data
    ?assert(not cdmi_internal:object_exists(Filename1, Config)),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"acl_test_file1">>,
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),

    EaccesError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),

    % set acl to 'write' and test cdmi/non-cdmi get request (should return 403 forbidden)
    RequestHeaders1 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, Filename1, put, RequestHeaders1, MetadataAclWrite
    ),
    {ok, Code1, _, Response1} = cdmi_internal:do_request(Workers, Filename1, get, RequestHeaders1, []),
    ?assertMatch(EaccesError, {Code1, json_utils:decode(Response1)}),

    {ok, Code2, _, Response2} = cdmi_internal:do_request(Workers, Filename1, get, [user_2_token_header()], []),
    ?assertMatch(EaccesError, {Code2, json_utils:decode(Response2)}),
    ?assertEqual({error, ?EACCES}, cdmi_internal:open_file(
        Config#cdmi_test_config.p2_selector, Filename1, read, Config), ?ATTEMPTS
    ),

    % set acl to 'read&write' and test cdmi/non-cdmi get request (should succeed)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, Filename1, put, RequestHeaders1, MetadataAclReadWriteFull
    ),
    {ok, ?HTTP_200_OK, _, _} = cdmi_internal:do_request(Workers, Filename1, get, RequestHeaders1, []),
    {ok, ?HTTP_200_OK, _, _} = cdmi_internal:do_request(Workers, Filename1, get, [user_2_token_header()], []).
    %%------------------------------


acl_write_file(Config) ->
    {Workers, RootPath, MetadataAclReadFull, _, _, MetadataAclReadWrite, _} = acl_file_base(Config),
    Filename1 = filename:join([RootPath, "acl_test_file2"]),
    % create test file with dummy data
    ?assert(not cdmi_internal:object_exists(Filename1, Config)),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"acl_test_file2">>,
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),
    RequestHeaders1 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    EaccesError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    %%------- write file test ------
    % set acl to 'read&write' and test cdmi/non-cdmi put request (should succeed)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, Filename1, put, RequestHeaders1, MetadataAclReadWrite
    ),
    RequestBody4 = json_utils:encode(#{<<"value">> => <<"new_data">>}),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, Filename1, put, RequestHeaders1, RequestBody4
    ),
    ?assertEqual(<<"new_data">>, cdmi_internal:get_file_content(Filename1, Config), ?ATTEMPTS),

    cdmi_internal:write_to_file(Filename1, <<"1">>, 8, Config),
    ?assertEqual(<<"new_data1">>, cdmi_internal:get_file_content(Filename1, Config), ?ATTEMPTS),

    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, Filename1, put, [user_2_token_header()], <<"new_data2">>
    ),
    ?assertEqual(<<"new_data2">>, cdmi_internal:get_file_content(Filename1, Config), ?ATTEMPTS),

    % set acl to 'read' and test cdmi/non-cdmi put request (should return 403 forbidden)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, Filename1, put, RequestHeaders1, MetadataAclReadFull
    ),
    RequestBody6 = json_utils:encode(#{<<"value">> => <<"new_data3">>}),
    {ok, Code3, _, Response3} = cdmi_internal:do_request(Workers, Filename1, put, RequestHeaders1, RequestBody6),
    ?assertMatch(EaccesError, {Code3, json_utils:decode(Response3)}),

    {ok, Code4, _, Response4} = cdmi_internal:do_request(
        Workers, Filename1, put, [user_2_token_header()], <<"new_data4">>
    ),
    ?assertMatch(EaccesError, {Code4, json_utils:decode(Response4)}),
    ?assertEqual(<<"new_data2">>, cdmi_internal:get_file_content(Filename1, Config), ?ATTEMPTS),
    ?assertEqual(
        {error, ?EACCES},
        cdmi_internal:open_file(Config#cdmi_test_config.p2_selector, Filename1, write, Config),
        ?ATTEMPTS
    ),
    ?assertEqual(<<"new_data2">>, cdmi_internal:get_file_content(Filename1, Config), ?ATTEMPTS).
    %%------------------------------


acl_delete_file(Config) ->
    {Workers, RootPath, _, MetadataAclDelete, _, _, _} = acl_file_base(Config),
    Filename3 = filename:join([RootPath, "acl_test_file3"]),
    % create test file with dummy data
    ?assert(not cdmi_internal:object_exists(Filename3, Config)),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"acl_test_file3">>,
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),
    RequestHeaders1 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    %%------ delete file test ------
    % set acl to 'delete'
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, Filename3, put, RequestHeaders1, MetadataAclDelete
    ),

    % delete file
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, Filename3, delete, [user_2_token_header()], []
    ),
    ?assert(not cdmi_internal:object_exists(Filename3, Config)).
    %%------------------------------

%% @private
acl_dir_base(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    UserId2 = oct_background:get_user_id(user2),
    UserName2 = <<"Unnamed User">>,
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
    WriteAcl = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?write_acl_mask
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
    {Workers, RootPath, DirMetadataAclReadWrite, DirMetadataAclRead, DirMetadataAclWrite}.


acl_read_write_dir(Config) ->
    {Workers, RootPath, DirMetadataAclReadWrite, DirMetadataAclRead, DirMetadataAclWrite} = acl_dir_base(Config),
    Dirname1 = filename:join([RootPath, "acl_test_dir1"]) ++ "/",
    EaccesError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),

    %%--- read write dir test ------
    ?assert(not cdmi_internal:object_exists(Dirname1, Config)),
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

    RequestHeaders1 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    % set acl to 'read&write' and test cdmi get request (should succeed)
    RequestHeaders2 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, Dirname1, put, RequestHeaders2, DirMetadataAclReadWrite
    ),
    {ok, ?HTTP_200_OK, _, _} = cdmi_internal:do_request(
        Workers, Dirname1, get, RequestHeaders2, []
    ),

    % create files in directory (should succeed)
    {ok, ?HTTP_201_CREATED, _, _} = cdmi_internal:do_request(
        Workers, File1, put, [user_2_token_header()], []
    ),
    ?assert(cdmi_internal:object_exists(File1, Config)),

    {ok, ?HTTP_201_CREATED, _, _} = cdmi_internal:do_request(
        Workers, File2, put, RequestHeaders1, <<"{\"value\":\"val\"}">>
    ),
    ?assert(cdmi_internal:object_exists(File2, Config)),
    ?assert(cdmi_internal:object_exists(File3, Config)),

    % delete files (should succeed)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, File1, delete, [user_2_token_header()], []
    ),
    ?assert(not cdmi_internal:object_exists(File1, Config)),

    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, File2, delete, [user_2_token_header()], []
    ),
    ?assert(not cdmi_internal:object_exists(File2, Config)),

    % set acl to 'write' and test cdmi get request (should return 403 forbidden)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, Dirname1, put, RequestHeaders2, DirMetadataAclWrite
    ),
    {ok, Code5, _, Response5} = cdmi_internal:do_request(Workers, Dirname1, get, RequestHeaders2, []),
    ?assertMatch(EaccesError, {Code5, json_utils:decode(Response5)}),

    % set acl to 'read' and test cdmi put request (should return 403 forbidden)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, Dirname1, put, RequestHeaders2, DirMetadataAclRead
    ),
    {ok, ?HTTP_200_OK, _, _} = cdmi_internal:do_request(Workers, Dirname1, get, RequestHeaders2, []),
    {ok, Code6, _, Response6} = cdmi_internal:do_request(
        Workers, Dirname1, put, RequestHeaders2,
        json_utils:encode(#{<<"metadata">> => #{<<"my_meta">> => <<"value">>}})
    ),
    ?assertMatch(EaccesError, {Code6, json_utils:decode(Response6)}),

    % create files (should return 403 forbidden)
    {ok, Code7, _, Response7} = cdmi_internal:do_request(
        Workers, File1, put, [user_2_token_header()], []
    ),
    ?assertMatch(EaccesError, {Code7, json_utils:decode(Response7)}),
    ?assert(not cdmi_internal:object_exists(File1, Config)),

    {ok, Code8, _, Response8} = cdmi_internal:do_request(
        Workers, File2, put, RequestHeaders1, <<"{\"value\":\"val\"}">>
    ),
    ?assertMatch(EaccesError, {Code8, json_utils:decode(Response8)}),
    ?assert(not cdmi_internal:object_exists(File2, Config)),
    ?assertEqual({error, ?EACCES}, cdmi_internal:create_new_file(File4, Config)),
    ?assert(not cdmi_internal:object_exists(File4, Config)),

    % delete files (should return 403 forbidden)
    {ok, Code9, _, Response9} = cdmi_internal:do_request(
        Workers, File3, delete, [user_2_token_header()], []
    ),
    ?assertMatch(EaccesError, {Code9, json_utils:decode(Response9)}),
    ?assert(cdmi_internal:object_exists(File3, Config)).
%%------------------------------


accept_header(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    AcceptHeader = {?HDR_ACCEPT, <<"*/*">>},

    % when
    {ok, Code1, _Headers1, _Response1} =
        cdmi_internal:do_request(Workers, [], get,
            [user_2_token_header(), ?CDMI_VERSION_HEADER, AcceptHeader], []),

    % then
    ?assertEqual(?HTTP_200_OK, Code1).

