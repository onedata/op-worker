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

-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include("onenv_test_utils.hrl").
-include("cdmi_test.hrl").

-export([metadata_base/1, errors_base/1]).

-export([
    selective_metadata_read/1,
    update_user_metadata_file/1,
    unauthorized_access_error/1,
    wrong_base_error/1,
    non_existing_file_error/1,
    open_binary_file_without_permission/1,
    open_cdmi_file_without_permission/1,
    delete_file/1,
    delete_dir/1,
    update_file_cdmi/1,
    update_file_http/1,
    use_supported_cdmi_version/1,
    use_unsupported_cdmi_version/1,
    request_format_check/1,
    mimetype_and_encoding_non_cdmi_file/1,
    update_mimetype_and_encoding/1,
    out_of_range/1,
    partial_upload_cdmi/1,
    partial_upload_non_cdmi/1,
    accept_header/1,
    download_empty_file/1
]).

user_2_token_header() ->
    rest_test_utils:user_token_header(oct_background:get_user_access_token(user2)).

%%%===================================================================
%%% Test functions
%%%===================================================================


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
    {[WorkerP1, _WorkerP2], RootPath, _} = errors_base(Config),
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
    timer:sleep(1000),
    {ok, Code8, _Headers8, Response8} = ?assertMatch(
        {ok, 400, _, _},
        cdmi_internal:do_request(WorkerP1, File8, get, RequestHeaders8),
        ?ATTEMPTS
    ),
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
    {ok, Code9, _Headers9, Response9} = ?assertMatch(
        {ok, 400, _, _},
        cdmi_internal:do_request(
            Workers, File9, get, RequestHeaders9
        ),
        ?ATTEMPTS
    ),
    cdmi_internal:unmock_opening_file_without_perms(Config),
    ExpRestError9 = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError9, {Code9, json_utils:decode(Response9)}, ?ATTEMPTS).


% Tests cdmi metadata read on object GET request.
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
    {ok, ?HTTP_200_OK, _Headers2, Response2} = ?assertMatch(
        {ok, 200, _, _}, cdmi_internal:do_request(
            Workers, FileName ++ "?metadata", get, RequestHeaders1, []), ?ATTEMPTS),
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
    [WorkerP1, WorkerP2] = Workers,
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
    {ok, _, _Headers1, _} = ?assertMatch(
        {ok, _, _, _},
        cdmi_internal:do_request(Workers, FileName, put, RequestHeaders1, RawRequestBody1),
        ?ATTEMPTS
    ),
    %%------ update user metadata of a file ----------
    RequestBody7 = #{<<"metadata">> => #{<<"my_new_metadata">> => <<"my_new_value">>}},
    RawRequestBody7 = json_utils:encode(RequestBody7),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            WorkerP1, FileName, put, RequestHeaders1, RawRequestBody7
        ),
        ?ATTEMPTS
    ),
    ?assertEqual(1, maps:size(get_metadata_from_request(FileName ++ "?metadata:my", WorkerP2, RequestHeaders1)), ?ATTEMPTS),
    ?assertMatch(
        #{<<"my_new_metadata">> := <<"my_new_value">>},
        get_metadata_from_request(FileName ++ "?metadata:my", Workers, RequestHeaders1),
        ?ATTEMPTS
    ),

    {ok, ?HTTP_200_OK, _Headers7, Response7} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_internal:do_request(
            Workers, FileName ++ "?metadata:my", get, RequestHeaders1, []
        ),
        ?ATTEMPTS
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
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            WorkerP1,
            FileName ++ "?metadata:my_new_metadata_add;metadata:my_new_metadata;metadata:cdmi_not_allowed",
            put,
            RequestHeaders1,
            RawRequestBody8
        ),
        ?ATTEMPTS
    ),

    {ok, ?HTTP_200_OK, _Headers8, Response8} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_internal:do_request(WorkerP1, FileName ++ "?metadata:my", get, RequestHeaders1, []),
        ?ATTEMPTS
    ),
    CdmiResponse8 = (json_utils:decode(Response8)),
    Metadata8 = maps:get(<<"metadata">>, CdmiResponse8),
    ?assertEqual(1, maps:size(CdmiResponse8)),
    ?assertMatch(#{<<"my_new_metadata_add">> := <<"my_new_value_add">>}, Metadata8),
    ?assertMatch(#{<<"my_new_metadata">> := <<"my_new_value_update">>}, Metadata8),
    ?assertEqual(2, maps:size(Metadata8)),

    {ok, ?HTTP_200_OK, _Headers9, Response9} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_internal:do_request(WorkerP1, FileName ++ "?metadata:cdmi_", get, RequestHeaders1, []),
        ?ATTEMPTS
    ),
    CdmiResponse9 = (json_utils:decode(Response9)),
    Metadata9 = maps:get(<<"metadata">>, CdmiResponse9),
    ?assertEqual(1, maps:size(CdmiResponse9)),
    ?assertEqual(5, maps:size(Metadata9)),

    RequestBody10 = #{<<"metadata">> => #{<<"my_new_metadata">> => <<"my_new_value_ignore">>}},
    RawRequestBody10 = json_utils:encode(RequestBody10),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            Workers, FileName ++ "?metadata:my_new_metadata_add", put, RequestHeaders1, RawRequestBody10
        ),
        ?ATTEMPTS
    ),

    {ok, ?HTTP_200_OK, _Headers10, Response10} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_internal:do_request(
            Workers, FileName ++ "?metadata:my", get, RequestHeaders1, []
        ),
        ?ATTEMPTS
    ),
    CdmiResponse10 = (json_utils:decode(Response10)),
    Metadata10 = maps:get(<<"metadata">>, CdmiResponse10),

    ?assertEqual(1, maps:size(CdmiResponse10)),
    ?assertMatch(#{<<"my_new_metadata">> := <<"my_new_value_update">>}, Metadata10).


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
    ?assert(not cdmi_internal:object_exists(FileName, Config), ?ATTEMPTS),
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
    ?assert(not cdmi_internal:object_exists(GroupFileName, Config), ?ATTEMPTS).
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
    ?assert(not cdmi_internal:object_exists(DirName, Config), ?ATTEMPTS),
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
    ?assert(not cdmi_internal:object_exists(DirName, Config), ?ATTEMPTS),
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
    [WorkerP1, _WorkerP2] = [
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

    {ok, Code1, _Headers1, _Response1} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            WorkerP1, FullTestFileName, put, RequestHeaders1, RawRequestBody1
        ),
        ?ATTEMPTS
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
        WorkerP1, FullTestFileName ++ "?value:0-2", put, RequestHeaders2, RawRequestBody2
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code2),
    ?assert(cdmi_internal:object_exists(FullTestFileName, Config)),
    ?assertEqual(UpdatedValue, cdmi_internal:get_file_content(FullTestFileName, Config), ?ATTEMPTS).
    %%------------------------------


update_file_http(Config) ->
    [WorkerP1, _WorkerP2] = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    {_SpaceName, _ShortTestDirName, _TestDirName, _TestFileName, FullTestFileName, TestFileContent} =
        cdmi_internal:create_test_dir_and_file(Config),

    %%--- value replace, http ------
    RequestBody3 = TestFileContent,
    {ok, ?HTTP_204_NO_CONTENT, _Headers3, _Response3} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(WorkerP1, FullTestFileName, put, [user_2_token_header()], RequestBody3),
        ?ATTEMPTS
    ),
    ?assert(cdmi_internal:object_exists(FullTestFileName, Config)),
    ?assertEqual(TestFileContent,
        cdmi_internal:get_file_content(FullTestFileName, Config), ?ATTEMPTS),
    %%------------------------------

    %%---- value update, http ------
    UpdateValue = <<"123">>,
    RequestHeaders4 = [{?HDR_CONTENT_RANGE, <<"bytes 0-2/3">>}],
    {ok, ?HTTP_204_NO_CONTENT, _Headers4, _Response4} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(WorkerP1, FullTestFileName,
            put, [user_2_token_header() | RequestHeaders4], UpdateValue),
        ?ATTEMPTS
    ),
    ?assert(cdmi_internal:object_exists(FullTestFileName, Config)),
    ?assertEqual(<<"123t_file_content">>,
        cdmi_internal:get_file_content(FullTestFileName, Config), ?ATTEMPTS),
    %%------------------------------

    %%---- value update2, http -----
    UpdateValue2 = <<"00">>,
    RequestHeaders5 = [{?HDR_CONTENT_RANGE, <<"bytes 3-4/*">>}],
    {ok, ?HTTP_204_NO_CONTENT, _Headers5, _Response5} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(WorkerP1, FullTestFileName,
            put, [user_2_token_header() | RequestHeaders5], UpdateValue2),
        ?ATTEMPTS
    ),
    ?assert(cdmi_internal:object_exists(FullTestFileName, Config)),
    ?assertEqual(<<"12300file_content">>,
        cdmi_internal:get_file_content(FullTestFileName, Config), ?ATTEMPTS),
    %%------------------------------

    %%---- value update, http error ------
    UpdateValue = <<"123">>,
    RequestHeaders6 = [{?HDR_CONTENT_RANGE, <<"bytes 0-2,3-4/*">>}],
    {ok, Code6, _Headers6, Response6} =
        cdmi_internal:do_request(WorkerP1, FullTestFileName, put, [user_2_token_header() | RequestHeaders6],
            UpdateValue),

    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(?HDR_CONTENT_RANGE)),
    ?assertMatch(ExpRestError, {Code6, json_utils:decode(Response6)}),
    ?assert(cdmi_internal:object_exists(FullTestFileName, Config)),
    ?assertEqual(<<"12300file_content">>,
        cdmi_internal:get_file_content(FullTestFileName, Config), ?ATTEMPTS).
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
    [WorkerP1, _WorkerP2] = [
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
        WorkerP1, filename:join(TestDirName, TestFileName), put, RequestHeaders2, RawBody2
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code2),

    {ok, Code3, _Headers3, Response3} = cdmi_internal:do_request(
        WorkerP1,
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
    [WorkerP1, WorkerP2] = Workers,
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
    {ok, ?HTTP_201_CREATED, _Headers1, Response1} = ?assertMatch(
        {ok, 201, _, _},
        cdmi_internal:do_request(
            WorkerP2, FileName, put, RequestHeaders1, RequestBody1
        ),
        ?ATTEMPTS
    ),
%%    ?assertEqual(?HTTP_201_CREATED, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    ?assertMatch(#{<<"completionStatus">> := <<"Processing">>}, CdmiResponse1),

    % upload second chunk of file
    RequestBody2 = json_utils:encode(#{<<"value">> => base64:encode(Chunk2)}),
    {ok, ?HTTP_204_NO_CONTENT, _Headers2, _Response2} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            WorkerP2, FileName ++ "?value:4-4", put, RequestHeaders1, RequestBody2
        ),
        ?ATTEMPTS
    ),
    % upload third chunk of file
    RequestHeaders3 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody3 = json_utils:encode(#{<<"value">> => base64:encode(Chunk3)}),
    {ok, ?HTTP_204_NO_CONTENT, _Headers3, _Response3} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            WorkerP2, FileName ++ "?value:5-9", put, RequestHeaders3, RequestBody3
        ),
        ?ATTEMPTS
    ),
    timer:sleep(2000),
    % get created file and check its consistency
    RequestHeaders4 = [user_2_token_header(), ?CDMI_VERSION_HEADER],
    % TODO Verify once after VFS-2023
    CheckAllChunks = fun() ->
        ?assertMatch(#{<<"completionStatus">> := <<"Complete">>},
            get_cdmi_response_from_request(FileName, WorkerP1, RequestHeaders4),
            ?ATTEMPTS
        ),
        ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>},
            get_cdmi_response_from_request(FileName, WorkerP1, RequestHeaders4),
            ?ATTEMPTS
        ),
        {ok, ?HTTP_200_OK, _Headers4, Response4} = ?assertMatch(
            {ok, 200, _, _},
            cdmi_internal:do_request(WorkerP2, FileName, get, RequestHeaders4, []),
            ?ATTEMPTS
        ),
        CdmiResponse4 = (json_utils:decode(Response4)),
        maps:get(<<"value">>, CdmiResponse4)
    end,
    % File size event change is async
    Chunks123 = <<Chunk1/binary, Chunk2/binary, Chunk3/binary>>,
    ?assertMatch(Chunks123, CheckAllChunks(), ?ATTEMPTS).


partial_upload_non_cdmi(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    [WorkerP1, WorkerP2] = Workers,
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
    {ok, ?HTTP_201_CREATED, _Headers5, _Response5} = ?assertMatch(
        {ok, 201, _, _},
        cdmi_internal:do_request(
            WorkerP2, FileName2, put, RequestHeaders5, Chunk1
        ),
        ?ATTEMPTS
    ),
    RequestHeaders4 = [user_2_token_header(), ?CDMI_VERSION_HEADER],
    % check "completionStatus", should be set to "Processing"
    {ok, ?HTTP_200_OK, _Headers5_1, Response5_1} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_internal:do_request(
            WorkerP1, FileName2 ++ "?completionStatus", get, RequestHeaders4, Chunk1
        ),
        ?ATTEMPTS
    ),
    CdmiResponse5_1 = (json_utils:decode(Response5_1)),
    ?assertMatch(#{<<"completionStatus">> := <<"Processing">>}, CdmiResponse5_1),

    % upload second chunk of file
    RequestHeaders6 = [
        user_2_token_header(),
        {?HDR_CONTENT_RANGE, <<"bytes 4-4/10">>}, {<<"X-CDMI-Partial">>, <<"true">>}
    ],
    {ok, ?HTTP_204_NO_CONTENT, _Headers6, _Response6} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            WorkerP2, FileName2, put, RequestHeaders6, Chunk2
        ),
        ?ATTEMPTS
    ),

    % upload third chunk of file
    RequestHeaders7 = [
        user_2_token_header(),
        {?HDR_CONTENT_RANGE, <<"bytes 5-9/10">>},
        {<<"X-CDMI-Partial">>, <<"false">>}
    ],
    {ok, ?HTTP_204_NO_CONTENT, _Headers7, _Response7} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            WorkerP2, FileName2, put, RequestHeaders7, Chunk3
        ),
        ?ATTEMPTS
    ),
    timer:sleep(5000),
    % get created file and check its consistency
    RequestHeaders8 = [user_2_token_header(), ?CDMI_VERSION_HEADER],
    % TODO Verify once after VFS-2023
    CheckAllChunks2 = fun() ->
        ?assertMatch(#{<<"completionStatus">> := <<"Complete">>},
            get_cdmi_response_from_request(FileName2, WorkerP2, RequestHeaders8), ?ATTEMPTS),
        {ok, ?HTTP_200_OK, _Headers8, Response8} = ?assertMatch(
            {ok, 200, _, _},
            cdmi_internal:do_request(WorkerP1, FileName2, get, RequestHeaders8, []),
            ?ATTEMPTS
        ),
        CdmiResponse8 = (json_utils:decode(Response8)),
        base64:decode(maps:get(<<"value">>, CdmiResponse8))
    end,
    % File size event change is async
    Chunks123 = <<Chunk1/binary, Chunk2/binary, Chunk3/binary>>,
    ?assertMatch(Chunks123, CheckAllChunks2(), ?ATTEMPTS).


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


download_empty_file(Config) ->
    [_WorkerP1, WorkerP2] = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    AuthHeaders = [rest_test_utils:user_token_header(oct_background:get_user_access_token(user2))],
    SessionId = oct_background:get_user_session_id(user2, Config#cdmi_test_config.p2_selector),
    UserId2 = oct_background:get_user_id(user2),

    % Create file
    FileName = <<"download_empty_file">>,
    FilePath = filename:join(["/", RootPath, FileName]),
    {ok, FileGuid} = cdmi_internal:create_new_file(binary_to_list(FilePath), Config),
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),

    ?assertMatch(ok, lfm_proxy:truncate(WorkerP2, SessionId, ?FILE_REF(FileGuid), 0), ?ATTEMPTS),

    {ok, _, _, Response} = ?assertMatch(
        {ok, 200, _Headers, _Response},
        cdmi_test_utils:do_request(
            WorkerP2, FilePath, get, [?CDMI_VERSION_HEADER | AuthHeaders], <<>>
        ),
        ?ATTEMPTS
    ),
    ?assertMatch(
        #{
            <<"completionStatus">> := <<"Complete">>,
            <<"metadata">> := #{
                <<"cdmi_owner">> := UserId2,
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
        cdmi_internal:do_request(
            Workers, Metadata, get, RequestHeaders1, []
        ),
        ?ATTEMPTS
    ),
    CdmiResponse = (json_utils:decode(Response)),
    maps:get(<<"metadata">>, CdmiResponse).


get_cdmi_response_from_request(FileName, Workers, RequestHeaders1) ->
    {ok, ?HTTP_200_OK, _Headers, Response} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_internal:do_request(
            Workers, FileName, get, RequestHeaders1, []
        ),
        ?ATTEMPTS
    ),
    json_utils:decode(Response).
