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

-include("global_definitions.hrl").
-include("http/rest.hrl").
-include("http/cdmi.hrl").
-include("proto/common/credentials.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    list_dir/1,
    get_file/1,
    metadata/1,
    delete_file/1,
    delete_dir/1,
    create_file/1,
    update_file/1,
    create_dir/1,
    capabilities/1,
    use_supported_cdmi_version/1,
    use_unsupported_cdmi_version/1,
    moved_permanently/1,
    objectid/1,
    request_format_check/1,
    mimetype_and_encoding/1,
    out_of_range/1,
    partial_upload/1,
    acl/1,
    errors/1,
    accept_header/1,
    move_copy_conflict/1,
    move/1,
    copy/1,
    create_raw_file_with_cdmi_version_header_should_succeed/1,
    create_raw_dir_with_cdmi_version_header_should_succeed/1,
    create_cdmi_file_without_cdmi_version_header_should_fail/1,
    create_cdmi_dir_without_cdmi_version_header_should_fail/1
]).

-define(TIMEOUT, timer:seconds(5)).

user_1_token_header(Config) ->
    rest_test_utils:user_token_header(Config, <<"user1">>).

-define(CDMI_VERSION_HEADER, {<<"X-CDMI-Specification-Version">>, <<"1.1.1">>}).
-define(CONTAINER_CONTENT_TYPE_HEADER, {?HDR_CONTENT_TYPE, <<"application/cdmi-container">>}).
-define(OBJECT_CONTENT_TYPE_HEADER, {?HDR_CONTENT_TYPE, <<"application/cdmi-object">>}).

-define(DEFAULT_FILE_MODE, 8#664).
-define(FILE_BEGINNING, 0).
-define(INFINITY, 9999).


%%%===================================================================
%%% Test functions
%%%===================================================================

% Tests cdmi container GET request (also refered as LIST)
list_dir(Config) ->
    [_WorkerP1, WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    {SpaceName, ShortTestDirName, TestDirName, TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),

    TestDirNameCheck = list_to_binary(ShortTestDirName ++ "/"),
    TestFileNameBin = list_to_binary(TestFileName),

    %%------ list basic dir --------
    {ok, Code1, Headers1, Response1} =
        do_request(WorkerP2, TestDirName ++ "/", get,
            [user_1_token_header(Config), ?CDMI_VERSION_HEADER], []),

    ?assertEqual(200, Code1),
    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/cdmi-container">>}, Headers1),
    CdmiResponse1 = json_utils:decode(Response1),
    ?assertMatch(#{<<"objectType">> :=  <<"application/cdmi-container">>}, 
                 CdmiResponse1),
    ?assertMatch(#{<<"objectName">> := TestDirNameCheck}, CdmiResponse1),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse1),
    ?assertMatch(#{<<"children">> := [TestFileNameBin]}, CdmiResponse1),

    ?assert(maps:get(<<"metadata">>, CdmiResponse1) =/= <<>>),
    %%------------------------------

    %%------ list root space dir ---------
    {ok, Code2, _Headers2, Response2} =
        do_request(WorkerP2, SpaceName ++ "/", get,
            [user_1_token_header(Config), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(200, Code2),
    CdmiResponse2 = json_utils:decode(Response2),
    SpaceDirName = list_to_binary(SpaceName ++ "/"),
    ?assertMatch(#{<<"objectName">> := SpaceDirName}, CdmiResponse2),
    ?assertMatch(#{<<"children">> := [TestDirNameCheck]}, CdmiResponse2),
    %%------------------------------

    %%--- list nonexisting dir -----
    {ok, Code3, _Headers3, _Response3} =
        do_request(Workers, "nonexisting_dir/",
            get, [user_1_token_header(Config), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(404, Code3),
    %%------------------------------

    %%-- selective params list -----
    {ok, Code4, _Headers4, Response4} =
        do_request(Workers, TestDirName ++ "/?children;objectName",
            get, [user_1_token_header(Config), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(200, Code4),
    CdmiResponse4 = json_utils:decode(Response4),
    ?assertMatch(#{<<"objectName">> := TestDirNameCheck}, CdmiResponse4),
    ?assertMatch(#{<<"children">> := [TestFileNameBin]}, CdmiResponse4),
    ?assertEqual(2, maps:size(CdmiResponse4)),
    %%------------------------------

    %%---- childrenrange list ------
    ChildrangeDir = SpaceName ++ "/childrange/",
    mkdir(Config, ChildrangeDir),
    Childs = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",
        "12", "13", "14"],
    ChildsBinaries = lists:map(fun(X) -> list_to_binary(X) end, Childs),
    lists:map(fun(FileName) ->
        create_file(Config, filename:join(ChildrangeDir, FileName))
    end, Childs),

    {ok, Code5, _Headers5, Response5} =
        do_request(Workers, ChildrangeDir ++ "?children;childrenrange",
            get, [user_1_token_header(Config), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(200, Code5),
    CdmiResponse5 = (json_utils:decode(Response5)),
    ChildrenResponse1 = maps:get(<<"children">>, CdmiResponse5),
    ?assert(is_list(ChildrenResponse1)),
    lists:foreach(fun(Name) ->
        ?assert(lists:member(Name, ChildrenResponse1))
    end, ChildsBinaries),
    ?assertMatch(#{<<"childrenrange">> := <<"0-14">>}, CdmiResponse5),

    {ok, Code6, _, Response6} =
        do_request(Workers, ChildrangeDir ++ "?children:2-13;childrenrange", get,
            [user_1_token_header(Config), ?CDMI_VERSION_HEADER], []),
    {ok, Code7, _, Response7} =
        do_request(Workers, ChildrangeDir ++ "?children:0-1;childrenrange", get,
            [user_1_token_header(Config), ?CDMI_VERSION_HEADER], []),
    {ok, Code8, _, Response8} =
        do_request(Workers, ChildrangeDir ++ "?children:14-14;childrenrange", get,
            [user_1_token_header(Config), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(200, Code6),
    ?assertEqual(200, Code7),
    ?assertEqual(200, Code8),
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
        end, ChildsBinaries).
%%------------------------------

%%  Tests cdmi object GET request. Request can be done without cdmi header (in that case
%%  file conent is returned as response body), or with cdmi header (the response
%%  contains json string of type: application/cdmi-object, and we can specify what
%%  parameters we need by listing then as ';' separated list after '?' in URL )
get_file(Config) ->
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    FileName = filename:join([binary_to_list(SpaceName), "toRead.txt"]),

    FileContent = <<"Some content...">>,
    [_WorkerP1, WorkerP2] = Workers = ?config(op_worker_nodes, Config),

    {ok, _} = create_file(Config, FileName),
    ?assert(object_exists(Config, FileName)),

    {ok, _} = write_to_file(Config, FileName, FileContent, ?FILE_BEGINNING),
    ?assertEqual(FileContent, get_file_content(Config, FileName)),

    %%-------- basic read ----------
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code1, _Headers1, Response1} = do_request(WorkerP2, FileName, get, RequestHeaders1, []),
    ?assertEqual(200, Code1),
    CdmiResponse1 = json_utils:decode(Response1),
    FileContent1 = base64:encode(FileContent),

    SpaceName1 = <<"/", SpaceName/binary, "/">>,
    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-object">>}, CdmiResponse1),
    ?assertMatch(#{<<"objectName">> := <<"toRead.txt">>}, CdmiResponse1),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse1),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"base64">>}, CdmiResponse1),
    ?assertMatch(#{<<"mimetype">> := <<"application/octet-stream">>}, CdmiResponse1),
    ?assertMatch(#{<<"parentURI">> := SpaceName1}, CdmiResponse1),
    ?assertMatch(#{<<"value">> := FileContent1}, CdmiResponse1),
    ?assertMatch(#{<<"valuerange">> := <<"0-14">>}, CdmiResponse1),

    ?assert(maps:get(<<"metadata">>, CdmiResponse1) =/= <<>>),
    %%------------------------------

    %%-- selective params read -----
    RequestHeaders2 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code2, _Headers2, Response2} = do_request(Workers, FileName ++ "?parentURI;completionStatus", get, RequestHeaders2, []),
    ?assertEqual(200, Code2),
    CdmiResponse2 = json_utils:decode(Response2),

    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse2),
    ?assertMatch(#{<<"parentURI">> := SpaceName1}, CdmiResponse2),

    ?assertEqual(2, maps:size(CdmiResponse2)),
    %%------------------------------

    %%--- selective value read -----
    RequestHeaders3 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code3, _Headers3, Response3} = do_request(Workers, FileName ++ "?value:1-3;valuerange", get, RequestHeaders3, []),
    ?assertEqual(200, Code3),
    CdmiResponse3 = json_utils:decode(Response3),

    ?assertMatch(#{<<"valuerange">> := <<"1-3">>}, CdmiResponse3),
    ?assertEqual(<<"ome">>, base64:decode(maps:get(<<"value">>, CdmiResponse3))), % 1-3 from FileContent = <<"Some content...">>
    %%------------------------------

    %%------- noncdmi read --------
    {ok, Code4, Headers4, Response4} =
        do_request(WorkerP2, FileName, get, [user_1_token_header(Config)]),
    ?assertEqual(200, Code4),

    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/octet-stream">>}, Headers4),
    ?assertEqual(FileContent, Response4),
    %%------------------------------

    %%------- objectid read --------
    RequestHeaders5 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code5, _Headers5, Response5} = do_request(Workers, FileName ++ "?objectID", get, RequestHeaders5, []),
    ?assertEqual(200, Code5),

    CdmiResponse5 = (json_utils:decode(Response5)),
    ObjectID = maps:get(<<"objectID">>, CdmiResponse5),
    ?assert(is_binary(ObjectID)),
    %%------------------------------

    %%-------- read by id ----------
    RequestHeaders6 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code6, _Headers6, Response6} = do_request(Workers, "cdmi_objectid/" ++ binary_to_list(ObjectID), get, RequestHeaders6, []),
    ?assertEqual(200, Code6),

    CdmiResponse6 = (json_utils:decode(Response6)),
    ?assertEqual(FileContent, base64:decode(maps:get(<<"value">>, CdmiResponse6))),
    %%------------------------------

    %% selective value read non-cdmi
    RequestHeaders7 = [{<<"Range">>, <<"bytes=1-3,5-5,-3">>}],
    {ok, Code7, _Headers7, Response7} =
        do_request(WorkerP2, FileName, get, [user_1_token_header(Config) | RequestHeaders7]),
    ?assertEqual(206, Code7),
    ?assertEqual(<<"omec...">>, Response7), % 1-3,5-5,12-14  from FileContent = <<"Some content...">>
    %%------------------------------

    %% selective value read non-cdmi error
    RequestHeaders8 = [{<<"Range">>, <<"bytes=1-3,6-4,-3">>}],
    {ok, Code8, _Headers8, Response8} =
        do_request(WorkerP2, FileName, get, [user_1_token_header(Config) | RequestHeaders8]),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(<<"range">>)),
    ?assertMatch(ExpRestError, {Code8, json_utils:decode(Response8)}).
%%------------------------------

% Tests cdmi metadata read on object GET request.
metadata(Config) ->
    [_WorkerP1, WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    UserId1 = ?config({user_id, <<"user1">>}, Config),
    FileName = filename:join([binary_to_list(SpaceName), "metadataTest.txt"]),
    FileContent = <<"Some content...">>,
    DirName = filename:join([binary_to_list(SpaceName), "metadataTestDir"]) ++ "/",

    %%-------- create file with user metadata --------
    ?assert(not object_exists(Config, FileName)),

    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    RequestBody1 = #{
        <<"value">> => FileContent, 
        <<"valuetransferencoding">> => <<"utf-8">>, 
        <<"mimetype">> => <<"text/plain">>,
        <<"metadata">> => #{<<"my_metadata">> => <<"my_value">>, 
                            <<"cdmi_not_allowed">> => <<"my_value">>}},

    RawRequestBody1 = json_utils:encode(RequestBody1),
    Before = time_utils:seconds_to_datetime(time_utils:timestamp_seconds()),
    {ok, Code1, _Headers1, Response1} = do_request(Workers, FileName, put, RequestHeaders1, RawRequestBody1),
    After = time_utils:seconds_to_datetime(time_utils:timestamp_seconds()),

    ?assertEqual(201, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    Metadata = maps:get(<<"metadata">>, CdmiResponse1),
    Metadata1 = Metadata,
    ?assertMatch(#{<<"cdmi_size">> := <<"15">>}, Metadata1),
    CTime1 = iso8601:parse(maps:get(<<"cdmi_ctime">>, Metadata1)),
    ATime1 = iso8601:parse(maps:get(<<"cdmi_atime">>, Metadata1)),
    MTime1 = iso8601:parse(maps:get(<<"cdmi_mtime">>, Metadata1)),
    ?assert(Before =< ATime1),
    ?assert(Before =< MTime1),
    ?assert(Before =< CTime1),
    ?assert(ATime1 =< After),
    ?assert(MTime1 =< After),
    ?assert(CTime1 =< After),
    ?assertMatch(UserId1, maps:get(<<"cdmi_owner">>, Metadata1)),
    ?assertMatch(#{<<"my_metadata">> := <<"my_value">>}, Metadata1),
    ?assertEqual(6, maps:size(Metadata1)),

    %%-- selective metadata read -----
    {ok, 200, _Headers2, Response2} = do_request(Workers, FileName ++ "?metadata", get, RequestHeaders1, []),
    CdmiResponse2 = (json_utils:decode(Response2)),
    ?assertEqual(1, maps:size(CdmiResponse2)),
    Metadata2 = maps:get(<<"metadata">>, CdmiResponse2),
    ?assertEqual(6, maps:size(Metadata2)),

    %%-- selective metadata read with prefix -----
    {ok, 200, _Headers3, Response3} = do_request(Workers, FileName ++ "?metadata:cdmi_", get, RequestHeaders1, []),
    CdmiResponse3 = (json_utils:decode(Response3)),
    ?assertEqual(1, maps:size(CdmiResponse3)),
    Metadata3 = maps:get(<<"metadata">>, CdmiResponse3),
    ?assertEqual(5, maps:size(Metadata3)),

    {ok, 200, _Headers4, Response4} = do_request(Workers, FileName ++ "?metadata:cdmi_o", get, RequestHeaders1, []),
    CdmiResponse4 = json_utils:decode(Response4),
    ?assertEqual(1, maps:size(CdmiResponse4)),
    Metadata4 = maps:get(<<"metadata">>, CdmiResponse4),
    ?assertMatch(UserId1, maps:get(<<"cdmi_owner">>, Metadata4)),
    ?assertEqual(1, maps:size(Metadata4)),

    {ok, 200, _Headers5, Response5} = do_request(Workers, FileName ++ "?metadata:cdmi_size", get, RequestHeaders1, []),
    CdmiResponse5 = json_utils:decode(Response5),
    ?assertEqual(1, maps:size(CdmiResponse5)),
    Metadata5 = maps:get(<<"metadata">>, CdmiResponse5),
    ?assertMatch(#{<<"cdmi_size">> := <<"15">>}, Metadata5),
    ?assertEqual(1, maps:size(Metadata5)),

    {ok, 200, _Headers6, Response6} = do_request(Workers, FileName ++ "?metadata:cdmi_no_such_metadata", get, RequestHeaders1, []),
    CdmiResponse6 = json_utils:decode(Response6),
    ?assertEqual(1, maps:size(CdmiResponse6)),
    ?assertMatch(#{<<"metadata">> := #{}}, CdmiResponse6),

    %%------ update user metadata of a file ----------
    RequestBody7 = #{<<"metadata">> => #{<<"my_new_metadata">> => <<"my_new_value">>}},
    RawRequestBody7 = json_utils:encode(RequestBody7),
    {ok, 204, _, _} = do_request(Workers, FileName, put, RequestHeaders1, RawRequestBody7),
    {ok, 200, _Headers7, Response7} = do_request(Workers, FileName ++ "?metadata:my", get, RequestHeaders1, []),
    CdmiResponse7 = (json_utils:decode(Response7)),
    ?assertEqual(1, maps:size(CdmiResponse7)),
    Metadata7 = maps:get(<<"metadata">>, CdmiResponse7),
    ?assertMatch(#{<<"my_new_metadata">> := <<"my_new_value">>}, Metadata7),
    ?assertEqual(1, maps:size(Metadata7)),

    RequestBody8 = #{<<"metadata">> => 
                     #{<<"my_new_metadata_add">> => <<"my_new_value_add">>,
                       <<"my_new_metadata">> => <<"my_new_value_update">>, 
                       <<"cdmi_not_allowed">> => <<"my_value">>}},
    RawRequestBody8 = json_utils:encode(RequestBody8),
    {ok, 204, _, _} = do_request(Workers, FileName ++ "?metadata:my_new_metadata_add;metadata:my_new_metadata;metadata:cdmi_not_allowed",
        put, RequestHeaders1, RawRequestBody8),
    {ok, 200, _Headers8, Response8} = do_request(Workers, FileName ++ "?metadata:my", get, RequestHeaders1, []),
    CdmiResponse8 = (json_utils:decode(Response8)),
    ?assertEqual(1, maps:size(CdmiResponse8)),
    Metadata8 = maps:get(<<"metadata">>, CdmiResponse8),
    ?assertMatch(#{<<"my_new_metadata_add">> := <<"my_new_value_add">>}, Metadata8),
    ?assertMatch(#{<<"my_new_metadata">> := <<"my_new_value_update">>}, Metadata8),
    ?assertEqual(2, maps:size(Metadata8)),
    {ok, 200, _Headers9, Response9} = do_request(Workers, FileName ++ "?metadata:cdmi_", get, RequestHeaders1, []),
    CdmiResponse9 = (json_utils:decode(Response9)),
    ?assertEqual(1, maps:size(CdmiResponse9)),
    Metadata9 = maps:get(<<"metadata">>, CdmiResponse9),
    ?assertEqual(5, maps:size(Metadata9)),

    RequestBody10 = #{<<"metadata">> => #{<<"my_new_metadata">> => <<"my_new_value_ignore">>}},
    RawRequestBody10 = json_utils:encode(RequestBody10),
    {ok, 204, _, _} = do_request(Workers, FileName ++ "?metadata:my_new_metadata_add", put, RequestHeaders1,
        RawRequestBody10),
    {ok, 200, _Headers10, Response10} = do_request(Workers, FileName ++ "?metadata:my", get, RequestHeaders1, []),
    CdmiResponse10 = (json_utils:decode(Response10)),
    ?assertEqual(1, maps:size(CdmiResponse10)),
    Metadata10 = maps:get(<<"metadata">>, CdmiResponse10),
    ?assertMatch(#{<<"my_new_metadata">> := <<"my_new_value_update">>}, Metadata10),
    ?assertEqual(1, maps:size(Metadata10)),

    %%------ create directory with user metadata  ----------
    RequestHeaders2 = [?CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    RequestBody11 = #{<<"metadata">> => #{<<"my_metadata">> => <<"my_dir_value">>}},
    RawRequestBody11 = json_utils:encode(RequestBody11),
    {ok, 201, _Headers11, Response11} = do_request(Workers, DirName, put, RequestHeaders2, RawRequestBody11),
    CdmiResponse11 = (json_utils:decode(Response11)),
    Metadata11 = maps:get(<<"metadata">>, CdmiResponse11),
    ?assertMatch(#{<<"my_metadata">> := <<"my_dir_value">>}, Metadata11),

    %%------ update user metadata of a directory ----------
    RequestBody12 = #{<<"metadata">> => #{<<"my_metadata">> => <<"my_dir_value_update">>}},
    RawRequestBody12 = json_utils:encode(RequestBody12),
    {ok, 204, _, _} = do_request(Workers, DirName, put, RequestHeaders2, RawRequestBody12),
    {ok, 200, _Headers13, Response13} = do_request(Workers, DirName ++ "?metadata:my", get, RequestHeaders1, []),
    CdmiResponse13 = (json_utils:decode(Response13)),
    ?assertEqual(1, maps:size(CdmiResponse13)),
    Metadata13 = maps:get(<<"metadata">>, CdmiResponse13),
    ?assertMatch(#{<<"my_metadata">> := <<"my_dir_value_update">>}, Metadata13),
    ?assertEqual(1, maps:size(Metadata13)),
    %%------------------------------

    %%------ write acl metadata ----------
    UserId1 = ?config({user_id, <<"user1">>}, Config),
    UserName1 = ?config({user_name, <<"user1">>}, Config),
    FileName2 = filename:join([binary_to_list(SpaceName), "acl_test_file.txt"]),
    Ace1 = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId1,
        name = UserName1,
        aceflags = ?no_flags_mask,
        acemask = ?read_all_object_mask
    }, cdmi),
    Ace2 = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId1,
        name = UserName1,
        aceflags = ?no_flags_mask,
        acemask = ?write_all_object_mask
    }, cdmi),
    Ace2Full = #{
        <<"acetype">> => ?allow,
        <<"identifier">> => <<UserName1/binary, "#", UserId1/binary>>,
        <<"aceflags">> => ?no_flags,
        <<"acemask">> => <<
            ?write_object/binary, ",",
            ?write_metadata/binary, ",",
            ?write_attributes/binary, ",",
            ?delete/binary, ",",
            ?write_acl/binary
        >>
    },

    create_file(Config, FileName2),
    write_to_file(Config, FileName2, <<"data">>, 0),
    RequestBody15 = #{<<"metadata">> => #{<<"cdmi_acl">> => [Ace1, Ace2Full]}},
    RawRequestBody15 = json_utils:encode(RequestBody15),
    RequestHeaders15 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header(Config)],

    {ok, Code15, _Headers15, Response15} = do_request(Workers, FileName2 ++ "?metadata:cdmi_acl", put, RequestHeaders15, RawRequestBody15),
    ?assertMatch({204, _}, {Code15, Response15}),

    {ok, Code16, _Headers16, Response16} = do_request(Workers, FileName2 ++ "?metadata", get, RequestHeaders1, []),
    ?assertEqual(200, Code16),
    CdmiResponse16 = (json_utils:decode(Response16)),
    ?assertEqual(1, maps:size(CdmiResponse16)),
    Metadata16 = maps:get(<<"metadata">>, CdmiResponse16),
    ?assertEqual(6, maps:size(Metadata16)),
    ?assertMatch(#{<<"cdmi_acl">> := [Ace1, Ace2]}, Metadata16),

    {ok, Code17, _Headers17, Response17} = do_request(WorkerP2, FileName2, get, [user_1_token_header(Config)], []),
    ?assertEqual(200, Code17),
    ?assertEqual(<<"data">>, Response17),
    %%------------------------------

    %%-- create forbidden by acl ---
    Ace3 = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId1,
        name = UserName1,
        aceflags = ?no_flags_mask,
        acemask = ?write_metadata_mask
    }, cdmi),
    Ace4 = ace:to_json(#access_control_entity{
        acetype = ?deny_mask,
        identifier = UserId1,
        name = UserName1,
        aceflags = ?no_flags_mask,
        acemask = ?write_object_mask
    }, cdmi),
    RequestBody18 = #{<<"metadata">> => #{<<"cdmi_acl">> => [Ace3, Ace4]}},
    RawRequestBody18 = json_utils:encode(RequestBody18),
    RequestHeaders18 = [user_1_token_header(Config), ?CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER],

    {ok, Code18, _Headers18, _Response18} = do_request(Workers, DirName ++ "?metadata:cdmi_acl", put, RequestHeaders18, RawRequestBody18),
    ?assertEqual(204, Code18),

    {ok, Code19, _Headers19, Response19} = do_request(Workers, filename:join(DirName, "some_file"), put, [user_1_token_header(Config)], []),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError, {Code19, json_utils:decode(Response19)}).
%%------------------------------

% Tests cdmi object DELETE requests
delete_file(Config) ->
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    FileName = filename:join([binary_to_list(SpaceName), "toDelete.txt"]),

    [WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    GroupFileName =
        filename:join([binary_to_list(SpaceName), "groupFile"]),

    %%----- basic delete -----------
    {ok, _} = create_file(Config, "/" ++ FileName),
    ?assert(object_exists(Config, FileName)),
    RequestHeaders1 = [?CDMI_VERSION_HEADER],
    {ok, Code1, _Headers1, _Response1} =
        do_request(
            WorkerP1, FileName, delete, [user_1_token_header(Config) | RequestHeaders1]),
    ?assertEqual(204, Code1),

    ?assert(not object_exists(Config, FileName)),
    %%------------------------------

    %%----- delete group file ------
    {ok, _} = create_file(Config, GroupFileName),

    RequestHeaders2 = [?CDMI_VERSION_HEADER],
    {ok, Code2, _Headers2, _Response2} =
        do_request(Workers, GroupFileName, delete,
            [user_1_token_header(Config) | RequestHeaders2]),
    ?assertEqual(204, Code2),

    ?assert(not object_exists(Config, GroupFileName)).
%%------------------------------

% Tests cdmi container DELETE requests
delete_dir(Config) ->
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    DirName = filename:join([binary_to_list(SpaceName), "toDelete"]) ++ "/",
    ChildDirName = filename:join([binary_to_list(SpaceName), "toDelete", "child"]) ++ "/",

    %%----- basic delete -----------
    mkdir(Config, DirName),
    ?assert(object_exists(Config, DirName)),

    RequestHeaders1 = [
        user_1_token_header(Config),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code1, _Headers1, _Response1} =
        do_request(Workers, DirName, delete, RequestHeaders1, []),

    ?assertEqual(204, Code1),
    ?assert(not object_exists(Config, DirName)),
    %%------------------------------

    %%------ recursive delete ------
    mkdir(Config, DirName),
    ?assert(object_exists(Config, DirName)),
    mkdir(Config, ChildDirName),
    ?assert(object_exists(Config, DirName)),

    RequestHeaders2 = [
        user_1_token_header(Config),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code2, _Headers2, _Response2} =
        do_request(Workers, DirName, delete, RequestHeaders2, []),

    ?assertEqual(204, Code2),
    ?assert(not object_exists(Config, DirName)),
    ?assert(not object_exists(Config, ChildDirName)),
    %%------------------------------

    %%----- delete root dir -------
    ?assert(object_exists(Config, "/")),

    RequestHeaders3 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER],
    ?assert(object_exists(Config, "/")),
    {ok, Code3, _Headers3, Response3} =
        do_request(Workers, "/", delete, RequestHeaders3, []),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError, {Code3, json_utils:decode(Response3)}),
    ?assert(object_exists(Config, "/")).
%%------------------------------

% Tests file creation (cdmi object PUT), It can be done with cdmi header (when file data is provided as cdmi-object
% json string), or without (when we treat request body as new file content)
create_file(Config) ->
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ToCreate = filename:join([binary_to_list(SpaceName), "file1.txt"]),
    ToCreate2 = filename:join([binary_to_list(SpaceName), "file2.txt"]),
    ToCreate5 = filename:join([binary_to_list(SpaceName), "file3.txt"]),
    ToCreate4 = filename:join([binary_to_list(SpaceName), "file4.txt"]),
    FileContent = <<"File content!">>,

    %%-------- basic create --------
    ?assert(not object_exists(Config, ToCreate)),

    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    RequestBody1 = #{<<"value">> => FileContent},
    RawRequestBody1 = json_utils:encode((RequestBody1)),
    {ok, Code1, _Headers1, Response1} = do_request(Workers, ToCreate, put, RequestHeaders1, RawRequestBody1),

    ?assertEqual(201, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-object">>}, CdmiResponse1),
    ?assertMatch(#{<<"objectName">> := <<"file1.txt">>}, CdmiResponse1),
    SpaceName1 = <<"/", SpaceName/binary, "/">>,
    ?assertMatch(#{<<"parentURI">> := SpaceName1}, CdmiResponse1),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse1),
    Metadata1 = maps:get(<<"metadata">>, CdmiResponse1),
    ?assertNotEqual([], Metadata1),

    ?assert(object_exists(Config, ToCreate)),
    ?assertEqual(FileContent, get_file_content(Config, ToCreate)),
    %%------------------------------

    %%------ base64 create ---------
    ?assert(not object_exists(Config, ToCreate2)),

    RequestHeaders2 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    RequestBody2 = #{<<"valuetransferencoding">> => <<"base64">>, 
                     <<"value">> => base64:encode(FileContent)},
    RawRequestBody2 = json_utils:encode((RequestBody2)),
    {ok, Code2, _Headers2, Response2} =
        do_request(Workers, ToCreate2, put, RequestHeaders2, RawRequestBody2),

    ?assertEqual(201, Code2),
    CdmiResponse2 = (json_utils:decode(Response2)),
    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-object">>}, CdmiResponse2),
    ?assertMatch(#{<<"objectName">> := <<"file2.txt">>}, CdmiResponse2),
    SpaceName1 = <<"/", SpaceName/binary, "/">>,
    ?assertMatch(#{<<"parentURI">> := SpaceName1}, CdmiResponse2),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse2),
    ?assert(maps:get(<<"metadata">>, CdmiResponse2) =/= <<>>),

    ?assert(object_exists(Config, ToCreate2)),
    ?assertEqual(FileContent, get_file_content(Config, ToCreate2)),
    %%------------------------------

    %%------- create empty ---------
    ?assert(not object_exists(Config, ToCreate4)),

    RequestHeaders4 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code4, _Headers4, _Response4} = do_request(Workers, ToCreate4, put, RequestHeaders4, []),
    ?assertEqual(201, Code4),

    ?assert(object_exists(Config, ToCreate4)),
    ?assertEqual(<<>>, get_file_content(Config, ToCreate4)),
    %%------------------------------

    %%------ create noncdmi --------
    ?assert(not object_exists(Config, ToCreate5)),

    RequestHeaders5 = [{?HDR_CONTENT_TYPE, <<"application/binary">>}],
    {ok, Code5, _Headers5, _Response5} =
        do_request(Workers, ToCreate5, put,
            [user_1_token_header(Config) | RequestHeaders5], FileContent),

    ?assertEqual(201, Code5),

    ?assert(object_exists(Config, ToCreate5)),
    ?assertEqual(FileContent, get_file_content(Config, ToCreate5)).
%%------------------------------

% Tests cdmi object PUT requests (updating content)
update_file(Config) ->
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    {_SpaceName, _ShortTestDirName, _TestDirName, _TestFileName, FullTestFileName, TestFileContent} =
        create_test_dir_and_file(Config),
    NewValue = <<"New Value!">>,
    UpdatedValue = <<"123 Value!">>,

    %%--- value replace, cdmi ------
    ?assert(object_exists(Config, FullTestFileName)),
    ?assertEqual(TestFileContent, get_file_content(Config, FullTestFileName)),

    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    RequestBody1 = #{<<"value">> => NewValue},
    RawRequestBody1 = json_utils:encode(RequestBody1),

    {ok, Code1, _Headers1, _Response1} = do_request(Workers, FullTestFileName, put, RequestHeaders1, RawRequestBody1),
    ?assertEqual(204, Code1),

    ?assert(object_exists(Config, FullTestFileName)),
    ?assertEqual(NewValue, get_file_content(Config, FullTestFileName)),
    %%------------------------------

    %%---- value update, cdmi ------
    UpdateValue = <<"123">>,
    RequestHeaders2 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    RequestBody2 = #{<<"value">> => base64:encode(UpdateValue)},
    RawRequestBody2 = json_utils:encode(RequestBody2),
    {ok, Code2, _Headers2, _Response2} = do_request(Workers, FullTestFileName ++ "?value:0-2", put, RequestHeaders2, RawRequestBody2),
    ?assertEqual(204, Code2),

    ?assert(object_exists(Config, FullTestFileName)),
    ?assertEqual(UpdatedValue, get_file_content(Config, FullTestFileName)),
    %%------------------------------

    %%--- value replace, http ------
    RequestBody3 = TestFileContent,
    {ok, Code3, _Headers3, _Response3} =
        do_request(Workers, FullTestFileName, put, [user_1_token_header(Config)], RequestBody3),
    ?assertEqual(204, Code3),

    ?assert(object_exists(Config, FullTestFileName)),
    ?assertEqual(TestFileContent,
        get_file_content(Config, FullTestFileName)),
    %%------------------------------

    %%---- value update, http ------
    UpdateValue = <<"123">>,
    RequestHeaders4 = [{<<"content-range">>, <<"bytes 0-2/3">>}],
    {ok, Code4, _Headers4, _Response4} =
        do_request(Workers, FullTestFileName,
            put, [user_1_token_header(Config) | RequestHeaders4], UpdateValue),
    ?assertEqual(204, Code4),

    ?assert(object_exists(Config, FullTestFileName)),
    ?assertEqual(<<"123t_file_content">>,
        get_file_content(Config, FullTestFileName)),
    %%------------------------------

    %%---- value update2, http -----
    UpdateValue2 = <<"00">>,
    RequestHeaders5 = [{<<"content-range">>, <<"bytes 3-4/*">>}],
    {ok, Code5, _Headers5, _Response5} =
        do_request(Workers, FullTestFileName,
            put, [user_1_token_header(Config) | RequestHeaders5], UpdateValue2),
    ?assertEqual(204, Code5),

    ?assert(object_exists(Config, FullTestFileName)),
    ?assertEqual(<<"12300file_content">>,
        get_file_content(Config, FullTestFileName)),
    %%------------------------------

    %%---- value update, http error ------
    UpdateValue = <<"123">>,
    RequestHeaders6 = [{<<"content-range">>, <<"bytes 0-2,3-4/*">>}],
    {ok, Code6, _Headers6, Response6} =
        do_request(Workers, FullTestFileName, put, [user_1_token_header(Config) | RequestHeaders6],
            UpdateValue),

    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(<<"content-range">>)),
    ?assertMatch(ExpRestError, {Code6, json_utils:decode(Response6)}),

    ?assert(object_exists(Config, FullTestFileName)),
    ?assertEqual(<<"12300file_content">>,
        get_file_content(Config, FullTestFileName)).
%%------------------------------

use_supported_cdmi_version(Config) ->
    % given
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    RequestHeaders = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],

    % when
    {ok, Code, _ResponseHeaders, _Response} =
        do_request(Workers, "/random", get, RequestHeaders),

    % then
    ?assertEqual(Code, ?HTTP_404_NOT_FOUND).

use_unsupported_cdmi_version(Config) ->
    % given
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    RequestHeaders = [{<<"X-CDMI-Specification-Version">>, <<"1.0.2">>}],

    % when
    {ok, Code, _ResponseHeaders, Response} =
        do_request(Workers, "/random", get, RequestHeaders),

    % then
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_VERSION([<<"1.1.1">>, <<"1.1">>])),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).

% Tests dir creation (cdmi container PUT), remember that every container URI ends
% with '/'
create_dir(Config) ->
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),

    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    DirName = filename:join([binary_to_list(SpaceName), "toCreate1"]) ++ "/",
    DirName2 = filename:join([binary_to_list(SpaceName), "toCreate2"]) ++ "/",
    MissingParentName = filename:join([binary_to_list(SpaceName), "unknown"]) ++ "/",
    DirWithoutParentName = filename:join(MissingParentName, "dir") ++ "/",

    %%------ non-cdmi create -------
    ?assert(not object_exists(Config, DirName)),

    {ok, Code1, _Headers1, _Response1} =
        do_request(Workers, DirName, put, [user_1_token_header(Config)]),
    ?assertEqual(201, Code1),

    ?assert(object_exists(Config, DirName)),
    %%------------------------------

    %%------ basic create ----------
    ?assert(not object_exists(Config, DirName2)),

    RequestHeaders2 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER],
    {ok, Code2, _Headers2, Response2} = do_request(Workers, DirName2, put, RequestHeaders2, []),

    ?assertEqual(201, Code2),
    CdmiResponse2 = (json_utils:decode(Response2)),
    ?assertMatch(#{<<"objectType">> := <<"application/cdmi-container">>}, CdmiResponse2),
    ?assertMatch(#{<<"objectName">> := <<"toCreate2/">>}, CdmiResponse2),
    SpaceName1 = <<"/", SpaceName/binary, "/">>,
    ?assertMatch(#{<<"parentURI">> := SpaceName1}, CdmiResponse2),
    ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse2),
    ?assertMatch(#{<<"children">> := []}, CdmiResponse2),
    ?assert(maps:get(<<"metadata">>, CdmiResponse2) =/= <<>>),

    ?assert(object_exists(Config, DirName2)),
    %%------------------------------

    %%---------- update ------------
    ?assert(object_exists(Config, DirName)),

    RequestHeaders3 = [
        user_1_token_header(Config), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code3, _Headers3, _Response3} =
        do_request(Workers, DirName, put, RequestHeaders3, []),
    ?assertEqual(204, Code3),

    ?assert(object_exists(Config, DirName)),
    %%------------------------------

    %%----- missing parent ---------
    ?assert(not object_exists(Config, MissingParentName)),

    RequestHeaders4 = [
        user_1_token_header(Config),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code4, _Headers4, Response4} =
        do_request(Workers, DirWithoutParentName, put, RequestHeaders4, []),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?ENOENT)),
    ?assertMatch(ExpRestError, {Code4, json_utils:decode(Response4)}).
%%------------------------------

% tests access to file by objectid
objectid(Config) ->
    [WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    {SpaceName, ShortTestDirName, TestDirName, TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),
    TestDirNameCheck = list_to_binary(ShortTestDirName ++ "/"),
    ShortTestDirNameBin = list_to_binary(ShortTestDirName),
    TestFileNameBin = list_to_binary(TestFileName),

    %%-------- / objectid ----------
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code1, Headers1, Response1} = do_request(WorkerP2, "", get, RequestHeaders1, []),
    ?assertEqual(200, Code1),

    RequestHeaders0 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code0, _Headers0, Response0} = do_request(WorkerP2, SpaceName ++ "/", get, RequestHeaders0, []),
    ?assertEqual(200, Code0),
    CdmiResponse0 = json_utils:decode(Response0),
    SpaceRootId = maps:get(<<"objectID">>, CdmiResponse0),

    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/cdmi-container">>}, Headers1),
    CdmiResponse1 = json_utils:decode(Response1),
    ?assertMatch(#{<<"objectName">> := <<"/">>}, CdmiResponse1),
    RootId = maps:get(<<"objectID">>, CdmiResponse1, undefined),
    ?assertNotEqual(RootId, undefined),
    ?assert(is_binary(RootId)),
    ?assertMatch(#{<<"parentURI">> := <<>>}, CdmiResponse1),
    ?assertEqual(error, maps:find(<<"parentID">>, CdmiResponse1)),
    ?assertMatch(#{<<"capabilitiesURI">> := <<"cdmi_capabilities/container/">>}, CdmiResponse1),
    %%------------------------------

    %%------ /dir objectid ---------
    RequestHeaders2 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code2, _Headers2, Response2} = do_request(WorkerP2, TestDirName ++ "/", get, RequestHeaders2, []),
    ?assertEqual(200, Code2),

    CdmiResponse2 = (json_utils:decode(Response2)),
    ?assertMatch(#{<<"objectName">> := TestDirNameCheck}, CdmiResponse2),
    DirId = maps:get(<<"objectID">>, CdmiResponse2, undefined),
    ?assertNotEqual(DirId, undefined),
    ?assert(is_binary(DirId)),
    ParentURI = <<"/", (list_to_binary(SpaceName))/binary, "/">>,
    ?assertMatch(#{<<"parentURI">> := ParentURI}, CdmiResponse2),
    ?assertMatch(#{<<"parentID">> := SpaceRootId}, CdmiResponse2),
    ?assertMatch(#{<<"capabilitiesURI">> := <<"cdmi_capabilities/container/">>}, CdmiResponse2),
    %%------------------------------

    %%--- /dir 1/file.txt objectid ---
    RequestHeaders3 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code3, _Headers3, Response3} = do_request(WorkerP2, filename:join(TestDirName, TestFileName), get, RequestHeaders3, []),
    ?assertEqual(200, Code3),

    CdmiResponse3 = json_utils:decode(Response3),
    ?assertMatch(#{<<"objectName">> := TestFileNameBin}, CdmiResponse3),
    FileId = maps:get(<<"objectID">>, CdmiResponse3, undefined),
    ?assertNotEqual(FileId, undefined),
    ?assert(is_binary(FileId)),
    ParentURI1 = <<"/", (list_to_binary(SpaceName))/binary, "/", ShortTestDirNameBin/binary, "/">>,
    ?assertMatch(#{<<"parentURI">> := ParentURI1}, CdmiResponse3),
    ?assertMatch(#{<<"parentID">> := DirId}, CdmiResponse3),
    ?assertMatch(#{<<"capabilitiesURI">> := <<"cdmi_capabilities/dataobject/">>}, CdmiResponse3),
    %%------------------------------

    %%---- get / by objectid -------
    RequestHeaders4 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code4, _Headers4, Response4} = do_request(WorkerP2, "cdmi_objectid/" ++ binary_to_list(RootId) ++ "/", get, RequestHeaders4, []),
    ?assertEqual(200, Code4),
    CdmiResponse4 = json_utils:decode(Response4),
    Meta1 = maps:remove(<<"cdmi_atime">>, maps:get(<<"metadata">>, CdmiResponse1)),
    CdmiResponse1WithoutAtime = maps:put(<<"metadata">>, Meta1, CdmiResponse1),
    Meta4 = maps:remove(<<"cdmi_atime">>, maps:get(<<"metadata">>, CdmiResponse4)),
    CdmiResponse4WithoutAtime = maps:put(<<"metadata">>, Meta4, CdmiResponse4),
    ?assertEqual(CdmiResponse1WithoutAtime, CdmiResponse4WithoutAtime), % should be the same as in 1 (except access time)
    %%------------------------------

    %%--- get /dir 1/ by objectid ----
    RequestHeaders5 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code5, _Headers5, Response5} = do_request(WorkerP2, "cdmi_objectid/" ++ binary_to_list(DirId) ++ "/", get, RequestHeaders5, []),
    ?assertEqual(200, Code5),
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
    RequestHeaders6 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code6, _Headers6, Response6} = do_request(WorkerP2, "cdmi_objectid/" ++ binary_to_list(DirId) ++ "/" ++ TestFileName, get, RequestHeaders6, []),
    ?assertEqual(200, Code6),
    CdmiResponse6 = (json_utils:decode(Response6)),
    Meta3 = maps:remove(<<"cdmi_atime">>, (maps:get(<<"metadata">>, CdmiResponse3))),
    CdmiResponse3WithoutAtime = maps:put(<<"metadata">>, Meta3, CdmiResponse3),
    Meta6 = maps:remove(<<"cdmi_atime">>, (maps:get(<<"metadata">>, CdmiResponse6))),
    CdmiResponse6WithoutAtime = maps:put(<<"metadata">>, Meta6, CdmiResponse6),
    ?assertEqual( % should be the same as in 3 (except access time)
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse3WithoutAtime)),
        maps:remove(<<"parentURI">>, maps:remove(<<"parentID">>, CdmiResponse6WithoutAtime))
    ),

    {ok, Code7, _Headers7, Response7} = do_request(WorkerP1, "cdmi_objectid/" ++ binary_to_list(FileId), get, RequestHeaders6, []),
    ?assertEqual(200, Code7),
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
    {ok, Code8, _, Response8} = do_request(WorkerP2, "cdmi_objectid/" ++ binary_to_list(RootId) ++ "/", get, RequestHeaders8, []),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_UNAUTHORIZED),
    ?assertMatch(ExpRestError, {Code8, json_utils:decode(Response8)}).
%%------------------------------

% tests if capabilities of objects, containers, and whole storage system are set properly
capabilities(Config) ->
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),

    %%--- system capabilities ------
    RequestHeaders8 = [?CDMI_VERSION_HEADER],
    {ok, Code8, Headers8, Response8} =
        do_request(Workers, "cdmi_capabilities/", get, RequestHeaders8, []),
    ?assertEqual(200, Code8),

    ?assertMatch(#{?HDR_CONTENT_TYPE := <<"application/cdmi-capability">>}, Headers8),
    CdmiResponse8 = (json_utils:decode(Response8)),
    ?assertMatch(#{<<"objectID">> := ?ROOT_CAPABILITY_ID}, CdmiResponse8),
    ?assertMatch(#{<<"objectName">> := <<?ROOT_CAPABILITY_PATH>>}, CdmiResponse8),
    ?assertMatch(#{<<"childrenrange">> := <<"0-1">>}, CdmiResponse8),
    ?assertMatch(#{<<"children">> := [<<"container/">>, <<"dataobject/">>]}, CdmiResponse8),
    Capabilities = maps:get(<<"capabilities">>, CdmiResponse8),
    ?assertEqual(?ROOT_CAPABILITY_MAP, Capabilities),
    %%------------------------------

    %%-- container capabilities ----
    RequestHeaders9 = [?CDMI_VERSION_HEADER],
    {ok, Code9, _Headers9, Response9} =
        do_request(Workers, "cdmi_capabilities/container/", get, RequestHeaders9, []),
    ?assertEqual(200, Code9),
    ?assertMatch({ok, Code9, _, Response9}, do_request(Workers, "cdmi_objectid/" ++ binary_to_list(?CONTAINER_CAPABILITY_ID) ++ "/", get, RequestHeaders9, [])),

    CdmiResponse9 = (json_utils:decode(Response9)),
    ?assertMatch(#{<<"parentURI">> := <<?ROOT_CAPABILITY_PATH>>}, CdmiResponse9),
    ?assertMatch(#{<<"parentID">> := ?ROOT_CAPABILITY_ID}, CdmiResponse9),
    ?assertMatch(#{<<"objectID">> := ?CONTAINER_CAPABILITY_ID}, CdmiResponse9),
    ?assertMatch(#{<<"objectName">> := <<"container/">>}, CdmiResponse9),
    Capabilities2 = maps:get(<<"capabilities">>, CdmiResponse9),
    ?assertEqual(?CONTAINER_CAPABILITY_MAP, Capabilities2),
    %%------------------------------

    %%-- dataobject capabilities ---
    RequestHeaders10 = [?CDMI_VERSION_HEADER],
    {ok, Code10, _Headers10, Response10} =
        do_request(Workers, "cdmi_capabilities/dataobject/", get, RequestHeaders10, []),
    ?assertEqual(200, Code10),
    ?assertMatch({ok, Code10, _, Response10}, do_request(Workers, "cdmi_objectid/" ++ binary_to_list(?DATAOBJECT_CAPABILITY_ID) ++ "/", get, RequestHeaders10, [])),

    CdmiResponse10 = (json_utils:decode(Response10)),
    ?assertMatch(#{<<"parentURI">> := <<?ROOT_CAPABILITY_PATH>>}, CdmiResponse10),
    ?assertMatch(#{<<"parentID">> := ?ROOT_CAPABILITY_ID}, CdmiResponse10),
    ?assertMatch(#{<<"objectID">> := ?DATAOBJECT_CAPABILITY_ID}, CdmiResponse10),
    ?assertMatch(#{<<"objectName">> := <<"dataobject/">>}, CdmiResponse10),
    Capabilities3 = maps:get(<<"capabilities">>, CdmiResponse10),
    ?assertEqual(?DATAOBJECT_CAPABILITY_MAP, Capabilities3).
%%------------------------------

% tests if cdmi returns 'moved permanently' code when we forget about '/' in path
moved_permanently(Config) ->
    [WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),

    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    FileName = filename:join([binary_to_list(SpaceName), "somedir", "somefile.txt"]),
    DirNameWithoutSlash = filename:join([binary_to_list(SpaceName), "somedir"]),

    DirName = DirNameWithoutSlash ++ "/",
    FileNameWithSlash = FileName ++ "/",

    mkdir(Config, DirName),
    ?assert(object_exists(Config, DirName)),
    create_file(Config, FileName),
    ?assert(object_exists(Config, FileName)),

    CDMIEndpoint = cdmi_test_utils:cdmi_endpoint(WorkerP1),
    %%--------- dir test -----------
    RequestHeaders1 = [
        ?CONTAINER_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        user_1_token_header(Config)
    ],
    Location1 = list_to_binary(CDMIEndpoint ++ DirName),
    {ok, Code1, Headers1, _Response1} =
        do_request(Workers, DirNameWithoutSlash, get, RequestHeaders1, []),
    ?assertEqual(?HTTP_302_FOUND, Code1),
    ?assertMatch(#{<<"location">> := Location1}, Headers1),
    %%------------------------------

    %%--------- dir test with QS-----------
    RequestHeaders2 = [
        ?CONTAINER_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        user_1_token_header(Config)
    ],
    Location2 = list_to_binary(CDMIEndpoint ++ DirName ++ "?example_qs=1"),
    {ok, Code2, Headers2, _Response2} =
        do_request(Workers, DirNameWithoutSlash ++ "?example_qs=1", get, RequestHeaders2, []),
    ?assertEqual(?HTTP_302_FOUND, Code2),
    ?assertMatch(#{<<"location">> := Location2}, Headers2),
    %%------------------------------

    %%--------- file test ----------
    RequestHeaders3 = [
        ?OBJECT_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        user_1_token_header(Config)
    ],
    Location3 = list_to_binary(CDMIEndpoint ++ FileName),
    {ok, Code3, Headers3, _Response3} =
        do_request(Workers, FileNameWithSlash, get, RequestHeaders3, []),
    ?assertEqual(?HTTP_302_FOUND, Code3),
    ?assertMatch(#{<<"location">> := Location3}, Headers3).
%%------------------------------

% tests req format checking
request_format_check(Config) ->
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),

    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    FileToCreate = filename:join([binary_to_list(SpaceName), "file.txt"]),
    DirToCreate = filename:join([binary_to_list(SpaceName), "dir"]) ++ "/",

    FileContent = <<"File content!">>,

    %%-- obj missing content-type --
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    RequestBody1 = #{<<"value">> => FileContent},
    RawRequestBody1 = json_utils:encode(RequestBody1),
    {ok, Code1, _Headers1, _Response1} = do_request(Workers, FileToCreate, put, RequestHeaders1, RawRequestBody1),
    ?assertEqual(201, Code1),
    %%------------------------------

    %%-- dir missing content-type --
    RequestHeaders3 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    RequestBody3 = #{<<"metadata">> => <<"">>},
    RawRequestBody3 = json_utils:encode(RequestBody3),
    {ok, Code3, _Headers3, _Response3} = do_request(Workers, DirToCreate, put, RequestHeaders3, RawRequestBody3),
    ?assertEqual(201, Code3).
%%------------------------------

% tests mimetype and valuetransferencoding properties, they are part of cdmi-object and cdmi-container
% and should be changeble
mimetype_and_encoding(Config) ->
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    {_SpaceName, _ShortTestDirName, TestDirName, TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),

    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    %% get mimetype and valuetransferencoding of non-cdmi file
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code1, _Headers1, Response1} = do_request(Workers, filename:join(TestDirName, TestFileName) ++ "?mimetype;valuetransferencoding", get, RequestHeaders1, []),
    ?assertEqual(200, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    ?assertMatch(#{<<"mimetype">> := <<"application/octet-stream">>}, CdmiResponse1),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"base64">>}, CdmiResponse1),
    %%------------------------------

    %%-- update mime and encoding --
    RequestHeaders2 = [?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER, user_1_token_header(Config)],
    RawBody2 = json_utils:encode(#{<<"valuetransferencoding">> => <<"utf-8">>,
                                  <<"mimetype">> => <<"application/binary">>}),
    {ok, Code2, _Headers2, _Response2} = do_request(Workers, filename:join(TestDirName, TestFileName), put, RequestHeaders2, RawBody2),
    ?assertEqual(204, Code2),

    {ok, Code3, _Headers3, Response3} = do_request(Workers, filename:join(TestDirName, TestFileName) ++ "?mimetype;valuetransferencoding", get, RequestHeaders2, []),
    ?assertEqual(200, Code3),
    CdmiResponse3 = (json_utils:decode(Response3)),
    ?assertMatch(#{<<"mimetype">> := <<"application/binary">>}, CdmiResponse3),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>}, CdmiResponse3),
    %%------------------------------

    %% create file with given mime and encoding
    FileName4 = filename:join([binary_to_list(SpaceName), "mime_file.txt"]),
    FileContent4 = <<"some content">>,
    RequestHeaders4 = [?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER, user_1_token_header(Config)],
    RawBody4 = json_utils:encode(#{<<"valuetransferencoding">> => <<"utf-8">>,
                                  <<"mimetype">> => <<"text/plain">>, 
                                  <<"value">> => FileContent4}),
    {ok, Code4, _Headers4, Response4} = do_request(Workers, FileName4, put, RequestHeaders4, RawBody4),
    ?assertEqual(201, Code4),
    CdmiResponse4 = (json_utils:decode(Response4)),
    ?assertMatch(#{<<"mimetype">> := <<"text/plain">>}, CdmiResponse4),

    RequestHeaders5 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code5, _Headers5, Response5} = do_request(Workers, FileName4 ++ "?value;mimetype;valuetransferencoding", get, RequestHeaders5, []),
    ?assertEqual(200, Code5),
    CdmiResponse5 = (json_utils:decode(Response5)),
    ?assertMatch(#{<<"mimetype">> := <<"text/plain">>}, CdmiResponse5),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>}, CdmiResponse5), %todo what do we return here if file contains valid utf-8 string and we read byte range?
    ?assertMatch(#{<<"value">> := FileContent4}, CdmiResponse5),
    %%------------------------------

    %% create file with given mime and encoding using non-cdmi request
    FileName6 = filename:join([binary_to_list(SpaceName), "mime_file_noncdmi.txt"]),
    FileContent6 = <<"some content">>,
    RequestHeaders6 = [{?HDR_CONTENT_TYPE, <<"text/plain; charset=utf-8">>}, user_1_token_header(Config)],
    {ok, Code6, _Headers6, _Response6} = do_request(Workers, FileName6, put, RequestHeaders6, FileContent6),
    ?assertEqual(201, Code6),

    RequestHeaders7 = [?CDMI_VERSION_HEADER, user_1_token_header(Config)],
    {ok, Code7, _Headers7, Response7} = do_request(Workers, FileName6 ++ "?value;mimetype;valuetransferencoding", get, RequestHeaders7, []),
    ?assertEqual(200, Code7),
    CdmiResponse7 = (json_utils:decode(Response7)),
    ?assertMatch(#{<<"mimetype">> := <<"text/plain">>}, CdmiResponse7),
    ?assertMatch(#{<<"valuetransferencoding">> := <<"utf-8">>}, CdmiResponse7),
    ?assertMatch(#{<<"value">> := FileContent6}, CdmiResponse7).
%%------------------------------

% tests reading&writing file at random ranges
out_of_range(Config) ->
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    {_SpaceName, _ShortTestDirName, TestDirName, _TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),

    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    FileName = filename:join([binary_to_list(SpaceName), "random_range_file.txt"]),

    {ok, _} = create_file(Config, FileName),

    %%---- reading out of range ---- (shuld return empty binary)
    ?assertEqual(<<>>, get_file_content(Config, FileName)),
    RequestHeaders1 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER],

    RequestBody1 = json_utils:encode(#{<<"value">> => <<"data">>}),
    {ok, Code1, _Headers1, Response1} = do_request(Workers, FileName ++ "?value:0-3", get, RequestHeaders1, RequestBody1),
    ?assertEqual(200, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    ?assertMatch(#{<<"value">> := <<>>}, CdmiResponse1),
    %%------------------------------

    %%------ writing at end -------- (shuld extend file)
    ?assertEqual(<<>>, get_file_content(Config, FileName)),

    RequestHeaders2 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody2 = json_utils:encode(#{<<"value">> => base64:encode(<<"data">>)}),
    {ok, Code2, _Headers2, _Response2} = do_request(Workers, FileName ++ "?value:0-3", put, RequestHeaders2, RequestBody2),
    ?assertEqual(204, Code2),

    ?assertEqual(<<"data">>, get_file_content(Config, FileName)),
    %%------------------------------

    %%------ writing at random -------- (should return zero bytes in any gaps)
%%     RequestBody3 = json_utils:encode(#{<<"value">> => base64:encode(<<"data">>)}), todo fix https://jira.onedata.org/jira/browse/VFS-1443 and uncomment
%%     {ok, Code3, _Headers3, _Response3} = do_request(Workers, FileName ++ "?value:10-13", put, RequestHeaders2, RequestBody3),
%%     ?assertEqual(204, Code3),
%%
%%     ?assertEqual(<<100, 97, 116, 97, 0, 0, 0, 0, 0, 0, 100, 97, 116, 97>>, get_file_content(Config, FileName)), % "data(6x<0_byte>)data"
    %%------------------------------

    %%----- random childrange ------ (shuld fail)
    {ok, Code4, _Headers4, Response4} = do_request(Workers, TestDirName ++ "/?children:100-132", get, RequestHeaders2, []),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(<<"childrenrange">>)),
    ?assertMatch(ExpRestError, {Code4, json_utils:decode(Response4)}).
%%------------------------------

move_copy_conflict(Config) ->
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),

    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    FileName = filename:join([binary_to_list(SpaceName), "move_test_file.txt"]),
    FileUri = list_to_binary(filename:join("/", FileName)),
    FileData = <<"data">>,
    create_file(Config, FileName),
    write_to_file(Config, FileName, FileData, 0),
    NewMoveFileName = "new_move_test_file",

    %%--- conflicting mv/cpy ------- (we cannot move and copy at the same time)
    ?assertEqual(FileData, get_file_content(Config, FileName)),

    RequestHeaders1 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody1 = json_utils:encode(#{<<"move">> => FileUri, 
                                       <<"copy">> => FileUri}),
    {ok, Code1, _Headers1, Response1} = do_request(Workers, NewMoveFileName, put, RequestHeaders1, RequestBody1),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_MALFORMED_DATA),
    ?assertMatch(ExpRestError, {Code1, json_utils:decode(Response1)}),
    ?assertEqual(FileData, get_file_content(Config, FileName)).
%%------------------------------

% tests copy and move operations on dataobjects and containers
move(Config) ->
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),

    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    FileName = filename:join([binary_to_list(SpaceName), "move_test_file.txt"]),
    DirName = filename:join([binary_to_list(SpaceName), "move_test_dir"]) ++ "/",

    FileData = <<"data">>,
    create_file(Config, FileName),
    mkdir(Config, DirName),
    write_to_file(Config, FileName, FileData, 0),
    NewMoveFileName = filename:join([binary_to_list(SpaceName), "new_move_test_file"]),
    NewMoveDirName = filename:join([binary_to_list(SpaceName), "new_move_test_dir"]) ++ "/",

    %%----------- dir mv -----------
    ?assert(object_exists(Config, DirName)),
    ?assert(not object_exists(Config, NewMoveDirName)),

    RequestHeaders2 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER],
    RequestBody2 = json_utils:encode(#{<<"move">> => list_to_binary(DirName)}),
    ?assertMatch({ok, 201, _Headers2, _Response2}, do_request(Workers, NewMoveDirName, put, RequestHeaders2, RequestBody2)),

    ?assert(not object_exists(Config, DirName)),
    ?assert(object_exists(Config, NewMoveDirName)),
    %%------------------------------

    %%---------- file mv -----------
    ?assert(object_exists(Config, FileName)),
    ?assert(not object_exists(Config, NewMoveFileName)),
    ?assertEqual(FileData, get_file_content(Config, FileName)),
    RequestHeaders3 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody3 = json_utils:encode(#{<<"move">> => list_to_binary(FileName)}),
    ?assertMatch({ok, _Code3, _Headers3, _Response3}, do_request(Workers, NewMoveFileName, put, RequestHeaders3, RequestBody3)),

    ?assert(not object_exists(Config, FileName)),
    ?assert(object_exists(Config, NewMoveFileName)),
    ?assertEqual(FileData, get_file_content(Config, NewMoveFileName)).
%%------------------------------

copy(Config) ->
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    %%---------- file cp ----------- (copy file, with xattrs and acl)
    % create file to copy
    FileName2 = filename:join([binary_to_list(SpaceName), "copy_test_file.txt"]),
    UserId1 = ?config({user_id, <<"user1">>}, Config),
    UserName1 = ?config({user_name, <<"user1">>}, Config),
    create_file(Config, FileName2),
    FileData2 = <<"data">>,
    FileAcl = [#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId1,
        name = UserName1,
        aceflags = ?no_flags_mask,
        acemask = ?all_object_perms_mask
    }],
    JsonMetadata = #{<<"a">> => <<"b">>, <<"c">> => 2, <<"d">> => []},
    Xattrs = [#xattr{name = <<"key1">>, value = <<"value1">>}, #xattr{name = <<"key2">>, value = <<"value2">>}],
    ok = set_acl(Config, FileName2, FileAcl),
    ok = set_json_metadata(Config, FileName2, JsonMetadata),
    ok = add_xattrs(Config, FileName2, Xattrs),
    {ok, _} = write_to_file(Config, FileName2, FileData2, 0),

    % assert source file is created and destination does not exist
    NewFileName2 = filename:join([binary_to_list(SpaceName), "copy_test_file2.txt"]),
    ?assert(object_exists(Config, FileName2)),
    ?assert(not object_exists(Config, NewFileName2)),
    ?assertEqual(FileData2, get_file_content(Config, FileName2)),
    ?assertEqual({ok, FileAcl}, get_acl(Config, FileName2)),

    % copy file using cdmi
    RequestHeaders4 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody4 = json_utils:encode(#{<<"copy">> => list_to_binary(FileName2)}),
    {ok, Code4, _Headers4, _Response4} = do_request(Workers, NewFileName2, put, RequestHeaders4, RequestBody4),
    ?assertEqual(201, Code4),

    % assert new file is created
    ?assert(object_exists(Config, FileName2)),
    ?assert(object_exists(Config, NewFileName2)),
    ?assertEqual(FileData2, get_file_content(Config, NewFileName2)),
    ?assertEqual({ok, JsonMetadata}, get_json_metadata(Config, NewFileName2)),
    ?assertEqual(Xattrs ++ [#xattr{name = ?JSON_METADATA_KEY, value = JsonMetadata}],
        get_xattrs(Config, NewFileName2)),
    ?assertEqual({ok, FileAcl}, get_acl(Config, NewFileName2)),
    %%------------------------------

    %%---------- dir cp ------------
    % create dir to copy (with some subdirs and subfiles)
    DirName2 = filename:join([binary_to_list(SpaceName), "copy_dir"]) ++ "/",
    NewDirName2 = filename:join([binary_to_list(SpaceName), "new_copy_dir"]) ++ "/",
    DirAcl = [#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId1,
        name = UserName1,
        aceflags = ?no_flags_mask,
        acemask = ?all_container_perms_mask
    }],

    mkdir(Config, DirName2),
    ?assert(object_exists(Config, DirName2)),
    set_acl(Config, DirName2, DirAcl),
    add_xattrs(Config, DirName2, Xattrs),
    mkdir(Config, filename:join(DirName2, "dir1")),
    mkdir(Config, filename:join(DirName2, "dir2")),
    create_file(Config, filename:join([DirName2, "dir1", "1"])),
    create_file(Config, filename:join([DirName2, "dir1", "2"])),
    create_file(Config, filename:join(DirName2, "3")),

    % assert source files are successfully created, and destination file does not exist
    ?assert(object_exists(Config, DirName2)),
    ?assert(object_exists(Config, filename:join(DirName2, "dir1"))),
    ?assert(object_exists(Config, filename:join(DirName2, "dir2"))),
    ?assert(object_exists(Config, filename:join([DirName2, "dir1", "1"]))),
    ?assert(object_exists(Config, filename:join([DirName2, "dir1", "2"]))),
    ?assert(object_exists(Config, filename:join(DirName2, "3"))),
    ?assert(not object_exists(Config, NewDirName2)),

    % copy dir using cdmi
    RequestHeaders5 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER],
    RequestBody5 = json_utils:encode(#{<<"copy">> => list_to_binary(DirName2)}),
    {ok, Code5, _Headers5, _Response5} = do_request(Workers, NewDirName2, put, RequestHeaders5, RequestBody5),
    ?assertEqual(201, Code5),

    % assert source files still exists
    ?assert(object_exists(Config, DirName2)),
    ?assert(object_exists(Config, filename:join(DirName2, "dir1"))),
    ?assert(object_exists(Config, filename:join(DirName2, "dir2"))),
    ?assert(object_exists(Config, filename:join([DirName2, "dir1", "1"]))),
    ?assert(object_exists(Config, filename:join([DirName2, "dir1", "2"]))),
    ?assert(object_exists(Config, filename:join(DirName2, "3"))),

    % assert destination files have been created
    ?assert(object_exists(Config, NewDirName2)),
    ?assertEqual(Xattrs, get_xattrs(Config, NewDirName2)),
    ?assertEqual({ok, DirAcl}, get_acl(Config, NewDirName2)),
    ?assert(object_exists(Config, filename:join(NewDirName2, "dir1"))),
    ?assert(object_exists(Config, filename:join(NewDirName2, "dir2"))),
    ?assert(object_exists(Config, filename:join([NewDirName2, "dir1", "1"]))),
    ?assert(object_exists(Config, filename:join([NewDirName2, "dir1", "2"]))),
    ?assert(object_exists(Config, filename:join(NewDirName2, "3"))).
%%------------------------------

% tests cdmi and non-cdmi partial upload feature (requests with x-cdmi-partial flag set to true)
partial_upload(Config) ->
    [_WorkerP1, WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    FileName = filename:join([binary_to_list(SpaceName), "partial.txt"]),
    FileName2 = filename:join([binary_to_list(SpaceName), "partial2.txt"]),

    Chunk1 = <<"some">>,
    Chunk2 = <<"_">>,
    Chunk3 = <<"value">>,

    %%------ cdmi request partial upload ------
    ?assert(not object_exists(Config, FileName)),

    % upload first chunk of file
    RequestHeaders1 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER, {"X-CDMI-Partial", "true"}],
    RequestBody1 = json_utils:encode(#{<<"value">> => Chunk1}),
    {ok, Code1, _Headers1, Response1} = do_request(Workers, FileName, put, RequestHeaders1, RequestBody1),
    ?assertEqual(201, Code1),
    CdmiResponse1 = (json_utils:decode(Response1)),
    ?assertMatch(#{<<"completionStatus">> := <<"Processing">>}, CdmiResponse1),

    % upload second chunk of file
    RequestBody2 = json_utils:encode(#{<<"value">> => base64:encode(Chunk2)}),
    {ok, Code2, _Headers2, _Response2} = do_request(Workers, FileName ++ "?value:4-4", put, RequestHeaders1, RequestBody2),
    ?assertEqual(204, Code2),

    % upload third chunk of file
    RequestHeaders3 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody3 = json_utils:encode(#{<<"value">> => base64:encode(Chunk3)}),
    {ok, Code3, _Headers3, _Response3} = do_request(Workers, FileName ++ "?value:5-9", put, RequestHeaders3, RequestBody3),
    ?assertEqual(204, Code3),

    % get created file and check its consistency
    RequestHeaders4 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER],
    % TODO Verify once after VFS-2023
    CheckAllChunks = fun() ->
        {ok, Code4, _Headers4, Response4} = do_request(WorkerP2, FileName, get, RequestHeaders4, []),
        ?assertEqual(200, Code4),
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
    ?assert(not object_exists(Config, FileName2)),

    % upload first chunk of file
    RequestHeaders5 = [user_1_token_header(Config), {<<"X-CDMI-Partial">>, <<"true">>}],
    {ok, Code5, _Headers5, _Response5} = do_request(Workers, FileName2, put, RequestHeaders5, Chunk1),
    ?assertEqual(201, Code5),

    % check "completionStatus", should be set to "Processing"
    {ok, Code5_1, _Headers5_1, Response5_1} = do_request(Workers, FileName2 ++ "?completionStatus", get, RequestHeaders4, Chunk1),
    CdmiResponse5_1 = (json_utils:decode(Response5_1)),
    ?assertEqual(200, Code5_1),
    ?assertMatch(#{<<"completionStatus">> := <<"Processing">>}, CdmiResponse5_1),

    % upload second chunk of file
    RequestHeaders6 = [user_1_token_header(Config), {<<"content-range">>, <<"bytes 4-4/10">>}, {<<"X-CDMI-Partial">>, <<"true">>}],
    {ok, Code6, _Headers6, _Response6} = do_request(Workers, FileName2, put, RequestHeaders6, Chunk2),
    ?assertEqual(204, Code6),

    % upload third chunk of file
    RequestHeaders7 = [user_1_token_header(Config), {<<"content-range">>, <<"bytes 5-9/10">>}, {<<"X-CDMI-Partial">>, <<"false">>}],
    {ok, Code7, _Headers7, _Response7} = do_request(Workers, FileName2, put, RequestHeaders7, Chunk3),
    ?assertEqual(204, Code7),

    % get created file and check its consistency
    RequestHeaders8 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER],
    % TODO Verify once after VFS-2023
    CheckAllChunks2 = fun() ->
        {ok, Code8, _Headers8, Response8} = do_request(WorkerP2, FileName2, get, RequestHeaders8, []),
        ?assertEqual(200, Code8),
        CdmiResponse8 = (json_utils:decode(Response8)),
        ?assertMatch(#{<<"completionStatus">> := <<"Complete">>}, CdmiResponse8),
        base64:decode(maps:get(<<"value">>, CdmiResponse8))
    end,
    % File size event change is async
    ?assertMatch(Chunks123, CheckAllChunks2(), 2).
%%------------------------------

% tests access control lists
acl(Config) ->
    [WorkerP1, WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Filename1 = filename:join([binary_to_list(SpaceName), "acl_test_file1"]),
    Dirname1 = filename:join([binary_to_list(SpaceName), "acl_test_dir1"]) ++ "/",

    UserId1 = ?config({user_id, <<"user1">>}, Config),
    UserName1 = ?config({user_name, <<"user1">>}, Config),
    Identifier1 = <<UserName1/binary, "#", UserId1/binary>>,

    Read = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId1,
        name = UserName1,
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
        identifier = UserId1,
        name = UserName1,
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
        identifier = UserId1,
        name = UserName1,
        aceflags = ?no_flags_mask,
        acemask = ?write_acl_mask
    }, cdmi),
    Delete = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId1,
        name = UserName1,
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
    ?assert(not object_exists(Config, Filename1)),
    create_file(Config, filename:join("/", Filename1)),
    write_to_file(Config, Filename1, <<"data">>, 0),

    EaccesError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),

    % set acl to 'write' and test cdmi/non-cdmi get request (should return 403 forbidden)
    RequestHeaders1 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    {ok, 204, _, _} = do_request(Workers, Filename1, put, RequestHeaders1, MetadataAclWrite),
    {ok, Code1, _, Response1} = do_request(Workers, Filename1, get, RequestHeaders1, []),
    ?assertMatch(EaccesError, {Code1, json_utils:decode(Response1)}),
    {ok, Code2, _, Response2} = do_request(Workers, Filename1, get, [user_1_token_header(Config)], []),
    ?assertMatch(EaccesError, {Code2, json_utils:decode(Response2)}),
    ?assertEqual({error, ?EACCES}, open_file(WorkerP1, Config, Filename1, read)),

    % set acl to 'read&write' and test cdmi/non-cdmi get request (should succeed)
    {ok, 204, _, _} = do_request(Workers, Filename1, put, RequestHeaders1, MetadataAclReadWriteFull),
    {ok, 200, _, _} = do_request(WorkerP2, Filename1, get, RequestHeaders1, []),
    {ok, 200, _, _} = do_request(WorkerP2, Filename1, get, [user_1_token_header(Config)], []),
    %%------------------------------

    %%------- write file test ------
    % set acl to 'read&write' and test cdmi/non-cdmi put request (should succeed)
    {ok, 204, _, _} = do_request(Workers, Filename1, put, RequestHeaders1, MetadataAclReadWrite),
    RequestBody4 = json_utils:encode(#{<<"value">> => <<"new_data">>}),
    {ok, 204, _, _} = do_request(Workers, Filename1, put, RequestHeaders1, RequestBody4),
    ?assertEqual(<<"new_data">>, get_file_content(Config, Filename1)),
    write_to_file(Config, Filename1, <<"1">>, 8),
    ?assertEqual(<<"new_data1">>, get_file_content(Config, Filename1)),
    {ok, 204, _, _} = do_request(Workers, Filename1, put, [user_1_token_header(Config)], <<"new_data2">>),
    ?assertEqual(<<"new_data2">>, get_file_content(Config, Filename1)),

    % set acl to 'read' and test cdmi/non-cdmi put request (should return 403 forbidden)
    {ok, 204, _, _} = do_request(Workers, Filename1, put, RequestHeaders1, MetadataAclReadFull),
    RequestBody6 = json_utils:encode(#{<<"value">> => <<"new_data3">>}),
    {ok, Code3, _, Response3} = do_request(Workers, Filename1, put, RequestHeaders1, RequestBody6),
    ?assertMatch(EaccesError, {Code3, json_utils:decode(Response3)}),
    {ok, Code4, _, Response4} = do_request(Workers, Filename1, put, [user_1_token_header(Config)], <<"new_data4">>),
    ?assertMatch(EaccesError, {Code4, json_utils:decode(Response4)}),
    ?assertEqual(<<"new_data2">>, get_file_content(Config, Filename1)),
    ?assertEqual({error, ?EACCES}, open_file(WorkerP1, Config, Filename1, write)),
    ?assertEqual(<<"new_data2">>, get_file_content(Config, Filename1)),
    %%------------------------------

    %%------ delete file test ------
    % set acl to 'delete'
    {ok, 204, _, _} = do_request(Workers, Filename1, put, RequestHeaders1, MetadataAclDelete),

    % delete file
    {ok, 204, _, _} = do_request(Workers, Filename1, delete, [user_1_token_header(Config)], []),
    ?assert(not object_exists(Config, Filename1)),
    %%------------------------------

    %%--- read write dir test ------
    ?assert(not object_exists(Config, Dirname1)),
    mkdir(Config, filename:join("/", Dirname1)),
    File1 = filename:join(Dirname1, "1"),
    File2 = filename:join(Dirname1, "2"),
    File3 = filename:join(Dirname1, "3"),
    File4 = filename:join(Dirname1, "4"),

    DirRead = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId1,
        name = UserName1,
        aceflags = ?no_flags_mask,
        acemask = ?read_all_container_mask bor ?traverse_container_mask
    }, cdmi),
    DirWrite = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId1,
        name = UserName1,
        aceflags = ?no_flags_mask,
        acemask = ?write_all_container_mask bor ?traverse_container_mask
    }, cdmi),
    DirMetadataAclReadWrite = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [DirWrite, DirRead]}}),
    DirMetadataAclRead = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [DirRead, WriteAcl]}}),
    DirMetadataAclWrite = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [DirWrite]}}),

    % set acl to 'read&write' and test cdmi get request (should succeed)
    RequestHeaders2 = [user_1_token_header(Config), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER],
    {ok, 204, _, _} = do_request(Workers, Dirname1, put, RequestHeaders2, DirMetadataAclReadWrite),
    {ok, 200, _, _} = do_request(WorkerP2, Dirname1, get, RequestHeaders2, []),

    % create files in directory (should succeed)
    {ok, 201, _, _} = do_request(Workers, File1, put, [user_1_token_header(Config)], []),
    ?assert(object_exists(Config, File1)),
    {ok, 201, _, _} = do_request(Workers, File2, put, RequestHeaders1, <<"{\"value\":\"val\"}">>),
    ?assert(object_exists(Config, File2)),
    create_file(Config, File3),
    ?assert(object_exists(Config, File3)),

    % delete files (should succeed)
    {ok, 204, _, _} = do_request(Workers, File1, delete, [user_1_token_header(Config)], []),
    ?assert(not object_exists(Config, File1)),
    {ok, 204, _, _} = do_request(Workers, File2, delete, [user_1_token_header(Config)], []),
    ?assert(not object_exists(Config, File2)),

    % set acl to 'write' and test cdmi get request (should return 403 forbidden)
    {ok, 204, _, _} = do_request(Workers, Dirname1, put, RequestHeaders2, DirMetadataAclWrite),
    {ok, Code5, _, Response5} = do_request(Workers, Dirname1, get, RequestHeaders2, []),
    ?assertMatch(EaccesError, {Code5, json_utils:decode(Response5)}),

    % set acl to 'read' and test cdmi put request (should return 403 forbidden)
    {ok, 204, _, _} = do_request(Workers, Dirname1, put, RequestHeaders2, DirMetadataAclRead),
    {ok, 200, _, _} = do_request(WorkerP2, Dirname1, get, RequestHeaders2, []),
    {ok, Code6, _, Response6} = do_request(Workers, Dirname1, put, RequestHeaders2, json_utils:encode(#{<<"metadata">> => #{<<"my_meta">> => <<"value">>}})),
    ?assertMatch(EaccesError, {Code6, json_utils:decode(Response6)}),

    % create files (should return 403 forbidden)
    {ok, Code7, _, Response7} = do_request(Workers, File1, put, [user_1_token_header(Config)], []),
    ?assertMatch(EaccesError, {Code7, json_utils:decode(Response7)}),
    ?assert(not object_exists(Config, File1)),
    {ok, Code8, _, Response8} = do_request(Workers, File2, put, RequestHeaders1, <<"{\"value\":\"val\"}">>),
    ?assertMatch(EaccesError, {Code8, json_utils:decode(Response8)}),
    ?assert(not object_exists(Config, File2)),
    ?assertEqual({error, ?EACCES}, create_file(Config, File4)),
    ?assert(not object_exists(Config, File4)),

    % delete files (should return 403 forbidden)
    {ok, Code9, _, Response9} = do_request(Workers, File3, delete, [user_1_token_header(Config)], []),
    ?assertMatch(EaccesError, {Code9, json_utils:decode(Response9)}),
    ?assert(object_exists(Config, File3)).
%%------------------------------

% test error handling
errors(Config) ->
    [WorkerP1, WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    {SpaceName, _ShortTestDirName, TestDirName, _TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),

    %%---- unauthorized access -----
    {ok, Code1, _Headers1, Response1} =
        do_request(WorkerP2, TestDirName, get, [], []),
    ExpRestError1 = rest_test_utils:get_rest_error(?ERROR_UNAUTHORIZED),
    ?assertMatch(ExpRestError1, {Code1, json_utils:decode(Response1)}),
    %%------------------------------

    %%----- wrong create path ------
    RequestHeaders2 = [
        user_1_token_header(Config),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code2, _Headers2, Response2} =
        do_request(Workers, SpaceName ++ "/test_dir", put, RequestHeaders2, []),
    ExpRestError2 = rest_test_utils:get_rest_error(?ERROR_BAD_VALUE_IDENTIFIER(<<"path">>)),
    ?assertMatch(ExpRestError2, {Code2, json_utils:decode(Response2)}),
    %%------------------------------

    %%---- wrong create path 2 -----
    RequestHeaders3 = [
        user_1_token_header(Config),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER
    ],
    {ok, Code3, _Headers3, Response3} =
        do_request(Workers, SpaceName ++ "/test_dir/", put, RequestHeaders3, []),
    ExpRestError3 = rest_test_utils:get_rest_error(?ERROR_BAD_VALUE_IDENTIFIER(<<"path">>)),
    ?assertMatch(ExpRestError3, {Code3, json_utils:decode(Response3)}),
    %%------------------------------

    %%-------- wrong base64 --------
    RequestHeaders4 = [
        user_1_token_header(Config),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER
    ],
    RequestBody4 = json_utils:encode(#{<<"valuetransferencoding">> => <<"base64">>, 
                                       <<"value">> => <<"#$%">>}),
    {ok, Code4, _Headers4, Response4} =
        do_request(Workers, SpaceName ++ "/some_file_b64", put, RequestHeaders4, RequestBody4),
    ExpRestError4 = rest_test_utils:get_rest_error(?ERROR_BAD_DATA(<<"base64">>)),
    ?assertMatch(ExpRestError4, {Code4, json_utils:decode(Response4)}),
    %%------------------------------

    %%-- reading non-existing file --
    RequestHeaders6 = [
        user_1_token_header(Config),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER
    ],
    {ok, Code6, _Headers6, _Response6} =
        do_request(WorkerP2, SpaceName ++ "/nonexistent_file", get, RequestHeaders6),
    ?assertEqual(Code6, ?HTTP_404_NOT_FOUND),
    %%------------------------------

    %%--- listing non-existing dir -----
    RequestHeaders7 = [
        user_1_token_header(Config),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code7, _Headers7, _Response7} =
        do_request(WorkerP2, SpaceName ++ "/nonexisting_dir/", get, RequestHeaders7),
    ?assertEqual(Code7, ?HTTP_404_NOT_FOUND),
    %%------------------------------

    %%--- open binary file without permission -----
    File8 = filename:join([SpaceName, "file8"]),
    FileContent8 = <<"File content...">>,
    create_file(Config, File8),
    ?assertEqual(object_exists(Config, File8), true),
    write_to_file(Config, File8, FileContent8, ?FILE_BEGINNING),
    ?assertEqual(get_file_content(Config, File8), FileContent8),
    RequestHeaders8 = [user_1_token_header(Config)],

    mock_opening_file_without_perms(Config),
    {ok, Code8, _Headers8, Response8} =
        do_request(WorkerP1, File8, get, RequestHeaders8),
    unmock_opening_file_without_perms(Config),
    ExpRestError8 = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError8, {Code8, json_utils:decode(Response8)}),
    %%------------------------------

    %%--- open cdmi file without permission -----
    File9 = filename:join([SpaceName, "file9"]),
    FileContent9 = <<"File content...">>,
    create_file(Config, File9),
    ?assertEqual(object_exists(Config, File9), true),
    write_to_file(Config, File9, FileContent9, ?FILE_BEGINNING),
    ?assertEqual(get_file_content(Config, File9), FileContent9),
    RequestHeaders9 = [
        user_1_token_header(Config),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER
    ],

    mock_opening_file_without_perms(Config),
    {ok, Code9, _Headers9, Response9} =
        do_request(WorkerP2, File9, get, RequestHeaders9),
    unmock_opening_file_without_perms(Config),
    ExpRestError9 = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError9, {Code9, json_utils:decode(Response9)}).
%%------------------------------

accept_header(Config) ->
    [_WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    AcceptHeader = {?HDR_ACCEPT, <<"*/*">>},

    % when
    {ok, Code1, _Headers1, _Response1} =
        do_request(WorkerP2, [], get,
            [user_1_token_header(Config), ?CDMI_VERSION_HEADER, AcceptHeader], []),

    % then
    ?assertEqual(200, Code1).

create_raw_file_with_cdmi_version_header_should_succeed(Config) ->
    % given
    Workers = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    % when
    ?assertMatch(
        {ok, 201, _ResponseHeaders, _Response},
        do_request(Workers, binary_to_list(SpaceName) ++ "/file1", put,
            [?CDMI_VERSION_HEADER, user_1_token_header(Config)], <<"data">>
        )),
    ?assertMatch(
        {ok, 201, _ResponseHeaders2, _Response2},
        do_request(Workers, binary_to_list(SpaceName) ++ "/file2", put,
            [?CDMI_VERSION_HEADER, user_1_token_header(Config), {?HDR_CONTENT_TYPE, <<"text/plain">>}],
            <<"data2">>
        )).

create_raw_dir_with_cdmi_version_header_should_succeed(Config) ->
    % given
    Workers = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    % when
    ?assertMatch(
        {ok, 201, _ResponseHeaders, _Response},
        do_request(Workers, binary_to_list(SpaceName) ++ "/dir1/", put,
            [?CDMI_VERSION_HEADER, user_1_token_header(Config)]
        )),
    ?assertMatch(
        {ok, 201, _ResponseHeaders2, _Response2},
        do_request(Workers, binary_to_list(SpaceName) ++ "/dir2/", put,
            [?CDMI_VERSION_HEADER, user_1_token_header(Config), {?HDR_CONTENT_TYPE, <<"application/json">>}],
            <<"{}">>
        )).

create_cdmi_file_without_cdmi_version_header_should_fail(Config) ->
    % given
    Workers = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    % when
    {ok, Code, _ResponseHeaders, Response} = do_request(
        Workers, binary_to_list(SpaceName) ++ "/file1", put,
        [user_1_token_header(Config), ?OBJECT_CONTENT_TYPE_HEADER], <<"{}">>
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_MISSING_REQUIRED_VALUE(<<"version">>)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).

create_cdmi_dir_without_cdmi_version_header_should_fail(Config) ->
    % given
    Workers = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    % when
    {ok, Code, _ResponseHeaders, Response} = do_request(
        Workers, binary_to_list(SpaceName) ++ "/dir1/", put,
        [user_1_token_header(Config), ?CONTAINER_CONTENT_TYPE_HEADER]
    ),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_MISSING_REQUIRED_VALUE(<<"version">>)),
    ?assertMatch(ExpRestError, {Code, json_utils:decode(Response)}).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

%%%===================================================================
%%% Internal functions
%%%===================================================================


do_request(Node, RestSubpath, Method, Headers) ->
    do_request(Node, RestSubpath, Method, Headers, []).

do_request([_ | _] = Nodes, RestSubpath, get, Headers, Body) ->
    [FRes | _] = Responses = [cdmi_test_utils:do_request(Node, RestSubpath, get, Headers, Body) || Node <- Nodes],
    case FRes of
        {error, _} ->
            ok;
        {ok, RCode, _, RResponse} ->
            RResponseJSON = remove_times_metadata(json_utils:decode(RResponse)),
            lists:foreach(fun({ok, LCode, _, LResponse}) ->
                LResponseJSON = remove_times_metadata(json_utils:decode(LResponse)),
%%                ct:print("~p ~p ~p ~p", [RCode, RResponseJSON, LCode, LResponseJSON]), %% Useful log for debugging
                ?assertMatch({RCode, RResponseJSON}, {LCode, LResponseJSON})
            end, Responses)
    end,
    FRes;
do_request([_ | _] = Nodes, RestSubpath, Method, Headers, Body) ->
    cdmi_test_utils:do_request(lists:nth(rand:uniform(length(Nodes)), Nodes), RestSubpath, Method, Headers, Body);
do_request(Node, RestSubpath, Method, Headers, Body) when is_atom(Node) ->
    cdmi_test_utils:do_request(Node, RestSubpath, Method, Headers, Body).

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
    [{_SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    TestDirName = get_random_string(),
    TestFileName = get_random_string(),
    FullTestDirName = filename:join([binary_to_list(SpaceName), TestDirName]),
    FullTestFileName = filename:join(["/", binary_to_list(SpaceName), TestDirName, TestFileName]),
    TestFileContent = <<"test_file_content">>,

    case object_exists(Config, TestDirName) of
        false ->
            {ok, _} = mkdir(Config, FullTestDirName),
            ?assert(object_exists(Config, FullTestDirName)),
            {ok, _} = create_file(Config, FullTestFileName),
            ?assert(object_exists(Config, FullTestFileName)),
            {ok, _} = write_to_file(Config, FullTestFileName, TestFileContent, 0),
            ?assertEqual(TestFileContent, get_file_content(Config, FullTestFileName));
        true -> ok
    end,

    {binary_to_list(SpaceName), TestDirName, FullTestDirName, TestFileName, FullTestFileName, TestFileContent}.

object_exists(Config, Path) ->
    [WorkerP1, _WorkerP2] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),

    case lfm_proxy:stat(WorkerP1, SessionId,
        {path, absolute_binary_path(Path)}) of
        {ok, _} ->
            true;
        {error, ?ENOENT} ->
            false
    end.

create_file(Config, Path) ->
    [WorkerP1, _WorkerP2] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    lfm_proxy:create(WorkerP1, SessionId, absolute_binary_path(Path), ?DEFAULT_FILE_MODE).

open_file(Worker, Config, Path, OpenMode) ->
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    lfm_proxy:open(Worker, SessionId, {path, absolute_binary_path(Path)}, OpenMode).

write_to_file(Config, Path, Data, Offset) ->
    [WorkerP1, _WorkerP2] = ?config(op_worker_nodes, Config),
    {ok, FileHandle} = open_file(WorkerP1, Config, Path, write),
    Result = lfm_proxy:write(WorkerP1, FileHandle, Offset, Data),
    lfm_proxy:close(WorkerP1, FileHandle),
    Result.

get_file_content(Config, Path) ->
    [_WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    {ok, FileHandle} = open_file(WorkerP2, Config, Path, read),
    Result = case lfm_proxy:read(WorkerP2, FileHandle, ?FILE_BEGINNING, ?INFINITY) of
        {error, Error} -> {error, Error};
        {ok, Content} -> Content
    end,
    lfm_proxy:close(WorkerP2, FileHandle),
    Result.

mkdir(Config, Path) ->
    [WorkerP1, _WorkerP2] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    lfm_proxy:mkdir(WorkerP1, SessionId, absolute_binary_path(Path)).

set_acl(Config, Path, Acl) ->
    [WorkerP1, _WorkerP2] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    lfm_proxy:set_acl(WorkerP1, SessionId, {path, absolute_binary_path(Path)}, Acl).

get_acl(Config, Path) ->
    [_WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    lfm_proxy:get_acl(WorkerP2, SessionId, {path, absolute_binary_path(Path)}).

add_xattrs(Config, Path, Xattrs) ->
    [WorkerP1, _WorkerP2] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    lists:foreach(fun(Xattr) ->
        ok = lfm_proxy:set_xattr(WorkerP1, SessionId, {path, absolute_binary_path(Path)}, Xattr)
    end, Xattrs).

get_xattrs(Config, Path) ->
    [_WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    {ok, Xattrs} = lfm_proxy:list_xattr(WorkerP2, SessionId, {path, absolute_binary_path(Path)}, false, true),
    lists:filtermap(
        fun
            (<<"cdmi_", _/binary>>) ->
                false;
            (XattrName) ->
                {ok, Xattr} = lfm_proxy:get_xattr(WorkerP2, SessionId, {path, absolute_binary_path(Path)}, XattrName),
                {true, Xattr}
        end, Xattrs).

set_json_metadata(Config, Path, JsonTerm) ->
    [WorkerP1, _WorkerP2] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {path, absolute_binary_path(Path)}, json, JsonTerm, []).

get_json_metadata(Config, Path) ->
    [_WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    lfm_proxy:get_metadata(WorkerP2, SessionId, {path, absolute_binary_path(Path)}, json, [], false).


absolute_binary_path(Path) ->
    list_to_binary(ensure_begins_with_slash(Path)).

ensure_begins_with_slash(Path) ->
    ReversedBinary = list_to_binary(lists:reverse(Path)),
    lists:reverse(binary_to_list(filepath_utils:ensure_ends_with_slash(ReversedBinary))).

mock_opening_file_without_perms(Config) ->
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    test_node_starter:load_modules(Workers, [?MODULE]),
    test_utils:mock_new(Workers, lfm),
    test_utils:mock_expect(
        Workers, lfm, monitored_open, fun(_, _, _) -> {error, ?EACCES} end).

unmock_opening_file_without_perms(Config) ->
    [_WorkerP1, _WorkerP2] = Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, lfm).

get_random_string() ->
    get_random_string(10, "abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ").

get_random_string(Length, AllowedChars) ->
    lists:foldl(fun(_, Acc) ->
        [lists:nth(rand:uniform(length(AllowedChars)),
            AllowedChars)]
        ++ Acc
    end, [], lists:seq(1, Length)).
