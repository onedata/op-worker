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
-module(cdmi_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include("http/rest/cdmi/cdmi_capabilities.hrl").
-include("http/rest/http_status.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    list_dir_test/1,
    get_file_test/1,
    metadata_test/1,
    delete_file_test/1,
    delete_dir_test/1,
    create_file_test/1,
    update_file_test/1,
    create_dir_test/1,
    capabilities_test/1,
    choose_adequate_handler/1,
    use_supported_cdmi_version/1,
    use_unsupported_cdmi_version/1,
    moved_permanently_test/1,
    objectid_test/1,
    request_format_check_test/1,
    mimetype_and_encoding_test/1,
    out_of_range_test/1,
    partial_upload_test/1,
    acl_test/1,
    errors_test/1,
    accept_header_test/1,
    copy_move_test/1
]).

all() ->
    ?ALL([
        list_dir_test,
        get_file_test,
        metadata_test,
        delete_file_test,
        delete_dir_test,
        create_file_test,
        update_file_test,
        create_dir_test,
        capabilities_test,
        choose_adequate_handler,
        use_supported_cdmi_version,
        use_unsupported_cdmi_version,
        moved_permanently_test,
        objectid_test,
        request_format_check_test,
        mimetype_and_encoding_test,
        out_of_range_test,
        partial_upload_test,
        acl_test,
        errors_test,
        accept_header_test,
        copy_move_test
    ]).

-define(TIMEOUT, timer:seconds(5)).

user_1_token_header() ->
    {ok, Srlzd} = macaroon:serialize(macaroon:create("a", "b", "c")),
    {<<"X-Auth-Token">>, Srlzd}.

-define(CDMI_VERSION_HEADER, {<<"X-CDMI-Specification-Version">>, <<"1.1.1">>}).
-define(CONTAINER_CONTENT_TYPE_HEADER, {<<"content-type">>, <<"application/cdmi-container">>}).
-define(OBJECT_CONTENT_TYPE_HEADER, {<<"content-type">>, <<"application/cdmi-object">>}).

-define(DEFAULT_FILE_MODE, 8#664).
-define(FILE_BEGINNING, 0).
-define(INFINITY, 9999).


%%%===================================================================
%%% Test functions
%%%===================================================================

% Tests cdmi container GET request (also refered as LIST)
list_dir_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {TestDirName, _TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),

    %%------ list basic dir --------
    {ok, Code1, Headers1, Response1} =
        do_request(Worker, TestDirName ++ "/", get,
            [user_1_token_header(), ?CDMI_VERSION_HEADER], []),

    ?assertEqual(200, Code1),
    ?assertEqual(proplists:get_value(<<"content-type">>, Headers1),
        <<"application/cdmi-container">>),
    CdmiResponse1 = json_utils:decode(Response1),
    ?assertEqual(<<"application/cdmi-container">>,
        proplists:get_value(<<"objectType">>, CdmiResponse1)),
    ?assertEqual(<<"dir/">>,
        proplists:get_value(<<"objectName">>, CdmiResponse1)),
    ?assertEqual(<<"Complete">>,
        proplists:get_value(<<"completionStatus">>, CdmiResponse1)),
    ?assertEqual([<<"file.txt">>],
        proplists:get_value(<<"children">>, CdmiResponse1)),
    ?assert(proplists:get_value(<<"metadata">>, CdmiResponse1) =/= <<>>),
    %%------------------------------

    %%------ list root dir ---------
    {ok, Code2, _Headers2, Response2} =
        do_request(Worker, [], get,
            [user_1_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(200, Code2),
    CdmiResponse2 = json_utils:decode(Response2),
    ?assertEqual(<<"/">>, proplists:get_value(<<"objectName">>, CdmiResponse2)),
    ?assertEqual([<<"spaces/">>, <<"dir/">>],
        proplists:get_value(<<"children">>, CdmiResponse2)),
    %%------------------------------

    %%--- list nonexisting dir -----
    {ok, Code3, _Headers3, _Response3} =
        do_request(Worker, "nonexisting_dir/",
            get, [user_1_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(404, Code3),
    %%------------------------------

    %%-- selective params list -----
    {ok, Code4, _Headers4, Response4} =
        do_request(Worker, TestDirName ++ "/?children;objectName",
            get, [user_1_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(200, Code4),
    CdmiResponse4 = json_utils:decode(Response4),
    ?assertEqual(<<"dir/">>,
        proplists:get_value(<<"objectName">>, CdmiResponse4)),
    ?assertEqual([<<"file.txt">>],
        proplists:get_value(<<"children">>, CdmiResponse4)),
    ?assertEqual(2, length(CdmiResponse4)),
    %%------------------------------

    %%---- childrenrange list ------
    ChildrangeDir = "childrange/",
    mkdir(Config, ChildrangeDir),
    Childs = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",
        "12", "13", "14"],
    ChildsBinaries = lists:map(fun(X) -> list_to_binary(X) end, Childs),
    lists:map(fun(FileName) ->
        create_file(Config, filename:join(ChildrangeDir, FileName))
    end, Childs),

    {ok, Code5, _Headers5, Response5} =
        do_request(Worker, ChildrangeDir ++ "?children;childrenrange",
            get, [user_1_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(200, Code5),
    CdmiResponse5 = json_utils:decode(Response5),
    ChildrenResponse1 = proplists:get_value(<<"children">>, CdmiResponse5),
    ?assert(is_list(ChildrenResponse1)),
    lists:foreach(fun(Name) ->
        ?assert(lists:member(Name, ChildrenResponse1))
    end, ChildsBinaries),
    ?assertEqual(<<"0-14">>,
        proplists:get_value(<<"childrenrange">>, CdmiResponse5)),

    {ok, Code6, _, Response6} =
        do_request(Worker, ChildrangeDir ++ "?children:2-13;childrenrange", get,
            [user_1_token_header(), ?CDMI_VERSION_HEADER], []),
    {ok, Code7, _, Response7} =
        do_request(Worker, ChildrangeDir ++ "?children:0-1;childrenrange", get,
            [user_1_token_header(), ?CDMI_VERSION_HEADER], []),
    {ok, Code8, _, Response8} =
        do_request(Worker, ChildrangeDir ++ "?children:14-14;childrenrange", get,
            [user_1_token_header(), ?CDMI_VERSION_HEADER], []),
    ?assertEqual(200, Code6),
    ?assertEqual(200, Code7),
    ?assertEqual(200, Code8),
    CdmiResponse6 = json_utils:decode(Response6),
    CdmiResponse7 = json_utils:decode(Response7),
    CdmiResponse8 = json_utils:decode(Response8),
    ChildrenResponse6 = proplists:get_value(<<"children">>, CdmiResponse6),
    ChildrenResponse7 = proplists:get_value(<<"children">>, CdmiResponse7),
    ChildrenResponse8 = proplists:get_value(<<"children">>, CdmiResponse8),

    ?assert(is_list(ChildrenResponse6)),
    ?assert(is_list(ChildrenResponse7)),
    ?assert(is_list(ChildrenResponse8)),
    ?assertEqual(12, length(ChildrenResponse6)),
    ?assertEqual(2, length(ChildrenResponse7)),
    ?assertEqual(1, length(ChildrenResponse8)),
    ?assertEqual(<<"2-13">>,
        proplists:get_value(<<"childrenrange">>, CdmiResponse6)),
    ?assertEqual(<<"0-1">>,
        proplists:get_value(<<"childrenrange">>, CdmiResponse7)),
    ?assertEqual(<<"14-14">>,
        proplists:get_value(<<"childrenrange">>, CdmiResponse8)),
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
get_file_test(Config) ->
    FileName = "toRead.txt",
    FileContent = <<"Some content...">>,
    [Worker | _] = ?config(op_worker_nodes, Config),

    {ok, _} = create_file(Config, FileName),
    ?assert(object_exists(Config, FileName)),
    {ok, _} = write_to_file(Config, FileName, FileContent, ?FILE_BEGINNING),
    ?assertEqual(FileContent, get_file_content(Config, FileName)),

    %%-------- basic read ----------
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code1, _Headers1, Response1} = do_request(Worker, FileName, get, RequestHeaders1, []),
    ?assertEqual(200, Code1),
    CdmiResponse1 = json_utils:decode(Response1),

    ?assertEqual(<<"application/cdmi-object">>, proplists:get_value(<<"objectType">>, CdmiResponse1)),
    ?assertEqual(<<"toRead.txt">>, proplists:get_value(<<"objectName">>, CdmiResponse1)),
    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>, CdmiResponse1)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>, CdmiResponse1)),
    ?assertEqual(<<"base64">>, proplists:get_value(<<"valuetransferencoding">>, CdmiResponse1)),
    ?assertEqual(<<"application/octet-stream">>, proplists:get_value(<<"mimetype">>, CdmiResponse1)),
    ?assertEqual(<<"0-14">>, proplists:get_value(<<"valuerange">>, CdmiResponse1)),
    ?assert(proplists:get_value(<<"metadata">>, CdmiResponse1) =/= <<>>),
    ?assertEqual(FileContent, base64:decode(proplists:get_value(<<"value">>, CdmiResponse1))),
    %%------------------------------

    %%-- selective params read -----
    RequestHeaders2 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code2, _Headers2, Response2} = do_request(Worker, FileName ++ "?parentURI;completionStatus", get, RequestHeaders2, []),
    ?assertEqual(200, Code2),
    CdmiResponse2 = json_utils:decode(Response2),

    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>, CdmiResponse2)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>, CdmiResponse2)),
    ?assertEqual(2, length(CdmiResponse2)),
    %%------------------------------

    %%--- selective value read -----
    RequestHeaders3 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code3, _Headers3, Response3} = do_request(Worker, FileName ++ "?value:1-3;valuerange", get, RequestHeaders3, []),
    ?assertEqual(200, Code3),
    CdmiResponse3 = json_utils:decode(Response3),

    ?assertEqual(<<"1-3">>, proplists:get_value(<<"valuerange">>, CdmiResponse3)),
    ?assertEqual(<<"ome">>, base64:decode(proplists:get_value(<<"value">>, CdmiResponse3))), % 1-3 from FileContent = <<"Some content...">>
    %%------------------------------

    %%------- noncdmi read --------
    {ok, Code4, Headers4, Response4} =
        do_request(Worker, FileName, get, [user_1_token_header()]),
    ?assertEqual(200, Code4),

    ?assertEqual(<<"application/octet-stream">>, proplists:get_value(<<"content-type">>, Headers4)),
    ?assertEqual(FileContent, Response4),
    %%------------------------------

    %%------- objectid read --------
    RequestHeaders5 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code5, _Headers5, Response5} = do_request(Worker, FileName ++ "?objectID", get, RequestHeaders5, []),
    ?assertEqual(200, Code5),

    CdmiResponse5 = json_utils:decode(Response5),
    ObjectID = proplists:get_value(<<"objectID">>, CdmiResponse5),
    ?assert(is_binary(ObjectID)),
    %%------------------------------

    %%-------- read by id ----------
    RequestHeaders6 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code6, _Headers6, Response6} = do_request(Worker, "cdmi_objectid/" ++ binary_to_list(ObjectID), get, RequestHeaders6, []),
    ?assertEqual(200, Code6),

    CdmiResponse6 = json_utils:decode(Response6),
    ?assertEqual(FileContent, base64:decode(proplists:get_value(<<"value">>, CdmiResponse6))),
    %%------------------------------

    %% selective value read non-cdmi
    RequestHeaders7 = [{<<"Range">>, <<"1-3,5-5,-3">>}],
    {ok, Code7, _Headers7, Response7} =
        do_request(Worker, FileName, get, [user_1_token_header() | RequestHeaders7]),
    ?assertEqual(206, Code7),
    ?assertEqual(<<"omec...">>, Response7), % 1-3,5-5,12-14  from FileContent = <<"Some content...">>
    %%------------------------------

    %% selective value read non-cdmi error
    RequestHeaders8 = [{<<"Range">>, <<"1-3,6-4,-3">>}],
    {ok, Code8, _Headers8, _Response8} =
        do_request(Worker, FileName, get, [user_1_token_header() | RequestHeaders8]),
    ?assertEqual(400, Code8).
    %%------------------------------

% Tests cdmi metadata read on object GET request.
metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    FileName = "metadataTest.txt",
    FileContent = <<"Some content...">>,
    DirName = "metadataTestDir/",

    %%-------- create file with user metadata --------
    ?assert(not object_exists(Config, FileName)),

    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header()],
    RequestBody1 = [
        {<<"value">>, FileContent}, {<<"valuetransferencoding">>, <<"utf-8">>}, {<<"mimetype">>, <<"text/plain">>},
        {<<"metadata">>, [{<<"my_metadata">>, <<"my_value">>}, {<<"cdmi_not_allowed">>, <<"my_value">>}]}],
    RawRequestBody1 = json_utils:encode(RequestBody1),
    Before = calendar:now_to_datetime(now()),
    {ok, Code1, _Headers1, Response1} = do_request(Worker, FileName, put, RequestHeaders1, RawRequestBody1),
    After = calendar:now_to_datetime(now()),

    ?assertEqual(201, Code1),
    CdmiResponse1 = json_utils:decode(Response1),
    Metadata1 = proplists:get_value(<<"metadata">>, CdmiResponse1),
    ?assertEqual(<<"15">>, proplists:get_value(<<"cdmi_size">>, Metadata1)),
    CTime1 = iso8601:parse(proplists:get_value(<<"cdmi_ctime">>, Metadata1)),
    ATime1 = iso8601:parse(proplists:get_value(<<"cdmi_atime">>, Metadata1)),
    MTime1 = iso8601:parse(proplists:get_value(<<"cdmi_mtime">>, Metadata1)),
    ?assert(Before =< ATime1),
    ?assert(Before =< MTime1),
    ?assert(Before =< CTime1),
    ?assert(ATime1 =< After),
    ?assert(MTime1 =< After),
    ?assert(CTime1 =< After),
    ?assertMatch(<<_/binary>>, proplists:get_value(<<"cdmi_owner">>, Metadata1)),
    ?assertEqual(<<"my_value">>, proplists:get_value(<<"my_metadata">>, Metadata1)),
    ?assertEqual(6, length(Metadata1)),

    %%-- selective metadata read -----
    {ok, 200, _Headers2, Response2} = do_request(Worker, FileName ++ "?metadata", get, RequestHeaders1, []),
    CdmiResponse2 = json_utils:decode(Response2),
    ?assertEqual(1, length(CdmiResponse2)),
    Metadata2 = proplists:get_value(<<"metadata">>, CdmiResponse2),
    ?assertEqual(6, length(Metadata2)),

    %%-- selective metadata read with prefix -----
    {ok, 200, _Headers3, Response3} = do_request(Worker, FileName ++ "?metadata:cdmi_", get, RequestHeaders1, []),
    CdmiResponse3 = json_utils:decode(Response3),
    ?assertEqual(1, length(CdmiResponse3)),
    Metadata3 = proplists:get_value(<<"metadata">>, CdmiResponse3),
    ?assertEqual(5, length(Metadata3)),

    {ok, 200, _Headers4, Response4} = do_request(Worker, FileName ++ "?metadata:cdmi_o", get, RequestHeaders1, []),
    CdmiResponse4 = json_utils:decode(Response4),
    ?assertEqual(1, length(CdmiResponse4)),
    Metadata4 = proplists:get_value(<<"metadata">>, CdmiResponse4),
    ?assertMatch(<<_/binary>>, proplists:get_value(<<"cdmi_owner">>, Metadata4)),
    ?assertEqual(1, length(Metadata4)),

    {ok, 200, _Headers5, Response5} = do_request(Worker, FileName ++ "?metadata:cdmi_size", get, RequestHeaders1, []),
    CdmiResponse5 = json_utils:decode(Response5),
    ?assertEqual(1, length(CdmiResponse5)),
    Metadata5 = proplists:get_value(<<"metadata">>, CdmiResponse5),
    ?assertEqual(<<"15">>, proplists:get_value(<<"cdmi_size">>, Metadata5)),
    ?assertEqual(1, length(Metadata5)),

    {ok, 200, _Headers6, Response6} = do_request(Worker, FileName ++ "?metadata:cdmi_no_such_metadata", get, RequestHeaders1, []),
    CdmiResponse6 = json_utils:decode(Response6),
    ?assertEqual(1, length(CdmiResponse6)),
    ?assertEqual([], proplists:get_value(<<"metadata">>, CdmiResponse6)),

    %%------ update user metadata of a file ----------
    RequestBody7 = [{<<"metadata">>, [{<<"my_new_metadata">>, <<"my_new_value">>}]}],
    RawRequestBody7 = json_utils:encode(RequestBody7),
    {ok, 204, _, _} = do_request(Worker, FileName, put, RequestHeaders1, RawRequestBody7),
    {ok, 200, _Headers7, Response7} = do_request(Worker, FileName ++ "?metadata:my", get, RequestHeaders1, []),
    CdmiResponse7 = json_utils:decode(Response7),
    ?assertEqual(1, length(CdmiResponse7)),
    Metadata7 = proplists:get_value(<<"metadata">>, CdmiResponse7),
    ?assertEqual(<<"my_new_value">>, proplists:get_value(<<"my_new_metadata">>, Metadata7)),
    ?assertEqual(1, length(Metadata7)),

    RequestBody8 = [{<<"metadata">>, [{<<"my_new_metadata_add">>, <<"my_new_value_add">>},
        {<<"my_new_metadata">>, <<"my_new_value_update">>}, {<<"cdmi_not_allowed">>, <<"my_value">>}]}],
    RawRequestBody8 = json_utils:encode(RequestBody8),
    {ok, 204, _, _} = do_request(Worker, FileName ++ "?metadata:my_new_metadata_add;metadata:my_new_metadata;metadata:cdmi_not_allowed",
        put, RequestHeaders1, RawRequestBody8),
    {ok, 200, _Headers8, Response8} = do_request(Worker, FileName ++ "?metadata:my", get, RequestHeaders1, []),
    CdmiResponse8 = json_utils:decode(Response8),
    ?assertEqual(1, length(CdmiResponse8)),
    Metadata8 = proplists:get_value(<<"metadata">>, CdmiResponse8),
    ?assertEqual(<<"my_new_value_add">>, proplists:get_value(<<"my_new_metadata_add">>, Metadata8)),
    ?assertEqual(<<"my_new_value_update">>, proplists:get_value(<<"my_new_metadata">>, Metadata8)),
    ?assertEqual(2, length(Metadata8)),
    {ok, 200, _Headers9, Response9} = do_request(Worker, FileName ++ "?metadata:cdmi_", get, RequestHeaders1, []),
    CdmiResponse9 = json_utils:decode(Response9),
    ?assertEqual(1, length(CdmiResponse9)),
    Metadata9 = proplists:get_value(<<"metadata">>, CdmiResponse9),
    ?assertEqual(5, length(Metadata9)),

    RequestBody10 = [{<<"metadata">>, [{<<"my_new_metadata">>, <<"my_new_value_ignore">>}]}],
    RawRequestBody10 = json_utils:encode(RequestBody10),
    {ok, 204, _, _} = do_request(Worker, FileName ++ "?metadata:my_new_metadata_add", put, RequestHeaders1,
        RawRequestBody10),
    {ok, 200, _Headers10, Response10} = do_request(Worker, FileName ++ "?metadata:my", get, RequestHeaders1, []),
    CdmiResponse10 = json_utils:decode(Response10),
    ?assertEqual(1, length(CdmiResponse10)),
    Metadata10 = proplists:get_value(<<"metadata">>, CdmiResponse10),
    ?assertEqual(<<"my_new_value_update">>, proplists:get_value(<<"my_new_metadata">>, Metadata10)),
    ?assertEqual(1, length(Metadata10)),

    %%------ create directory with user metadata  ----------
    RequestHeaders2 = [?CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header()],
    RequestBody11 = [{<<"metadata">>, [{<<"my_metadata">>, <<"my_dir_value">>}]}],
    RawRequestBody11 = json_utils:encode(RequestBody11),
    {ok, 201, _Headers11, Response11} = do_request(Worker, DirName, put, RequestHeaders2, RawRequestBody11),
    CdmiResponse11 = json_utils:decode(Response11),
    Metadata11 = proplists:get_value(<<"metadata">>, CdmiResponse11),
    ?assertEqual(<<"my_dir_value">>, proplists:get_value(<<"my_metadata">>, Metadata11)),

    %%------ update user metadata of a directory ----------
    RequestBody12 = [{<<"metadata">>, [{<<"my_metadata">>, <<"my_dir_value_update">>}]}],
    RawRequestBody12 = json_utils:encode(RequestBody12),
    {ok, 204, _, _} = do_request(Worker, DirName, put, RequestHeaders2, RawRequestBody12),
    {ok, 200, _Headers13, Response13} = do_request(Worker, DirName ++ "?metadata:my", get, RequestHeaders1, []),
    CdmiResponse13 = json_utils:decode(Response13),
    ?assertEqual(1, length(CdmiResponse13)),
    Metadata13 = proplists:get_value(<<"metadata">>, CdmiResponse13),
    ?assertEqual(<<"my_dir_value_update">>, proplists:get_value(<<"my_metadata">>, Metadata13)),
    ?assertEqual(1, length(Metadata13)),
    %%------------------------------

    %%------ write acl metadata ----------
    UserId1 = ?config({user_id, 1}, Config),
    UserName1 = ?config({user_name, 1}, Config),
    FileName2 = "acl_test_file.txt",
    Ace1 = [
        {<<"acetype">>, fslogic_acl:bitmask_to_binary(?allow_mask)},
        {<<"identifier">>, <<UserName1/binary, "#", UserId1/binary>>},
        {<<"aceflags">>, fslogic_acl:bitmask_to_binary(?no_flags_mask)},
        {<<"acemask">>, fslogic_acl:bitmask_to_binary(?read_mask)}
    ],
    Ace2 = [
        {<<"acetype">>, fslogic_acl:bitmask_to_binary(?deny_mask)},
        {<<"identifier">>, <<UserName1/binary, "#", UserId1/binary>>},
        {<<"aceflags">>, fslogic_acl:bitmask_to_binary(?no_flags_mask)},
        {<<"acemask">>, fslogic_acl:bitmask_to_binary(?read_mask bor ?execute_mask)}
    ],
    Ace3 = [
        {<<"acetype">>, fslogic_acl:bitmask_to_binary(?allow_mask)},
        {<<"identifier">>, <<UserName1/binary, "#", UserId1/binary>>},
        {<<"aceflags">>, fslogic_acl:bitmask_to_binary(?no_flags_mask)},
        {<<"acemask">>, fslogic_acl:bitmask_to_binary(?write_mask)}
    ],

    create_file(Config, FileName2),
    write_to_file(Config, FileName2, <<"data">>, 0),
    RequestBody15 = [{<<"metadata">>, [{<<"cdmi_acl">>, [Ace1, Ace2, Ace3]}]}],
    RawRequestBody15 = json_utils:encode(RequestBody15),
    RequestHeaders15 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header()],

    {ok, Code15, _Headers15, Response15} = do_request(Worker, FileName2 ++ "?metadata:cdmi_acl", put, RequestHeaders15, RawRequestBody15),
    ?assertMatch({204, _}, {Code15, Response15}),

    {ok, Code16, _Headers16, Response16} = do_request(Worker, FileName2 ++ "?metadata", get, RequestHeaders1, []),
    ?assertEqual(200, Code16),
    CdmiResponse16 = json_utils:decode(Response16),
    ?assertEqual(1, length(CdmiResponse16)),
    Metadata16 = proplists:get_value(<<"metadata">>, CdmiResponse16),
    ?assertEqual(6, length(Metadata16)),
    ?assertEqual([Ace1, Ace2, Ace3], proplists:get_value(<<"cdmi_acl">>, Metadata16)),

    {ok, Code17, _Headers17, Response17} = do_request(Worker, FileName2, get, [user_1_token_header()], []),
    ?assertEqual(200, Code17),
    ?assertEqual(<<"data">>, Response17),
    %%------------------------------

    %%-- create forbidden by acl ---
    Ace4 = [
        {<<"acetype">>, fslogic_acl:bitmask_to_binary(?deny_mask)},
        {<<"identifier">>, <<UserName1/binary, "#", UserId1/binary>>},
        {<<"aceflags">>, fslogic_acl:bitmask_to_binary(?no_flags_mask)},
        {<<"acemask">>, fslogic_acl:bitmask_to_binary(?write_mask)}],
    RequestBody18 = [{<<"metadata">>, [{<<"cdmi_acl">>, [Ace1, Ace4]}]}],
    RawRequestBody18 = json_utils:encode(RequestBody18),
    RequestHeaders18 = [user_1_token_header(), ?CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER],

    {ok, Code18, _Headers18, _Response18} = do_request(Worker, DirName ++ "?metadata:cdmi_acl", put, RequestHeaders18, RawRequestBody18),
    ?assertEqual(204, Code18),

    {ok, Code19, _Headers19, _Response19} = do_request(Worker, filename:join(DirName, "some_file"), put, [user_1_token_header()], []),
    ?assertEqual(403, Code19).
    %%------------------------------

% Tests cdmi object DELETE requests
delete_file_test(Config) ->
    FileName = "toDelete",
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, 1}, Config),
    GroupFileName =
        filename:join(["spaces", binary_to_list(SpaceName), "groupFile"]),

    %%----- basic delete -----------
    {ok, _} = create_file(Config, "/" ++ FileName),
    ?assert(object_exists(Config, FileName)),
    RequestHeaders1 = [?CDMI_VERSION_HEADER],
    {ok, Code1, _Headers1, _Response1} =
        do_request(
            Worker, FileName, delete, [user_1_token_header() | RequestHeaders1]),
    ?assertEqual(204, Code1),

    ?assert(not object_exists(Config, FileName)),
    %%------------------------------

    %%----- delete group file ------
    {ok, _} = create_file(Config, GroupFileName),

    RequestHeaders2 = [?CDMI_VERSION_HEADER],
    {ok, Code2, _Headers2, _Response2} =
        do_request(Worker, GroupFileName, delete,
            [user_1_token_header() | RequestHeaders2]),
    ?assertEqual(204, Code2),

    ?assert(not object_exists(Config, GroupFileName)).
    %%------------------------------

% Tests cdmi container DELETE requests
delete_dir_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    DirName = "toDelete/",
    ChildDirName = "toDelete/child/",
    SpacesDirName = "spaces/",

    %%----- basic delete -----------
    mkdir(Config, DirName),
    ?assert(object_exists(Config, DirName)),

    RequestHeaders1 = [
        user_1_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code1, _Headers1, _Response1} =
        do_request(Worker, DirName, delete, RequestHeaders1, []),

    ?assertEqual(204, Code1),
    ?assert(not object_exists(Config, DirName)),
    %%------------------------------

    %%------ recursive delete ------
    mkdir(Config, DirName),
    ?assert(object_exists(Config, DirName)),
    mkdir(Config, ChildDirName),
    ?assert(object_exists(Config, DirName)),

    RequestHeaders2 = [
        user_1_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code2, _Headers2, _Response2} =
        do_request(Worker, DirName, delete, RequestHeaders2, []),

    ?assertEqual(204,Code2),
    ?assert(not object_exists(Config, DirName)),
    ?assert(not object_exists(Config, ChildDirName)),
    %%------------------------------

    %%----- delete group dir -------
    ?assert(object_exists(Config, SpacesDirName)),

    RequestHeaders3 = [user_1_token_header(), ?CDMI_VERSION_HEADER],
    ?assert(object_exists(Config, SpacesDirName)),
    {ok, Code3, _Headers3, _Response3} =
        do_request(Worker, SpacesDirName, delete, RequestHeaders3, []),
    ?assertEqual(403, Code3),
    ?assert(object_exists(Config, SpacesDirName)).
    %%------------------------------

% Tests file creation (cdmi object PUT), It can be done with cdmi header (when file data is provided as cdmi-object
% json string), or without (when we treat request body as new file content)
create_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, 1}, Config),
    ToCreate = "file.txt",
    ToCreate2 = filename:join(["spaces", binary_to_list(SpaceName), "file1.txt"]),
    ToCreate4 = "file2",
    ToCreate5 = "file3",
    FileContent = <<"File content!">>,

    %%-------- basic create --------
    ?assert(not object_exists(Config, ToCreate)),

    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header()],
    RequestBody1 = [{<<"value">>, FileContent}],
    RawRequestBody1 = json_utils:encode(RequestBody1),
    {ok, Code1, _Headers1, Response1} = do_request(Worker, ToCreate, put, RequestHeaders1, RawRequestBody1),

    ?assertEqual(201, Code1),
    CdmiResponse1 = json_utils:decode(Response1),
    ?assertEqual(<<"application/cdmi-object">>, proplists:get_value(<<"objectType">>, CdmiResponse1)),
    ?assertEqual(<<"file.txt">>, proplists:get_value(<<"objectName">>, CdmiResponse1)),
    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>, CdmiResponse1)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>, CdmiResponse1)),
    Metadata1 = proplists:get_value(<<"metadata">>, CdmiResponse1),
    ?assertNotEqual([], Metadata1),

    ?assert(object_exists(Config, ToCreate)),
    ?assertEqual(FileContent, get_file_content(Config, ToCreate)),
    %%------------------------------

    %%------ base64 create ---------
    ?assert(not object_exists(Config, ToCreate2)),

    RequestHeaders2 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header()],
    RequestBody2 = [{<<"valuetransferencoding">>, <<"base64">>}, {<<"value">>, base64:encode(FileContent)}],
    RawRequestBody2 = json_utils:encode(RequestBody2),
    {ok, Code2, _Headers2, Response2} =
        do_request(Worker, ToCreate2, put, RequestHeaders2, RawRequestBody2),

    ?assertEqual(201, Code2),
    CdmiResponse2 = json_utils:decode(Response2),
    ?assertEqual(<<"application/cdmi-object">>, proplists:get_value(<<"objectType">>, CdmiResponse2)),
    ?assertEqual(<<"file1.txt">>, proplists:get_value(<<"objectName">>, CdmiResponse2)),
    ?assertEqual(<<"/spaces/", SpaceName/binary, "/">>, proplists:get_value(<<"parentURI">>, CdmiResponse2)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>, CdmiResponse2)),
    ?assert(proplists:get_value(<<"metadata">>, CdmiResponse2) =/= <<>>),

    ?assert(object_exists(Config, ToCreate2)),
    ?assertEqual(FileContent, get_file_content(Config, ToCreate2)),
    %%------------------------------

    %%------- create empty ---------
    ?assert(not object_exists(Config, ToCreate4)),

    RequestHeaders4 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code4, _Headers4, _Response4} = do_request(Worker, ToCreate4, put, RequestHeaders4, []),
    ?assertEqual(201, Code4),

    ?assert(object_exists(Config, ToCreate4)),
    ?assertEqual(<<>>, get_file_content(Config, ToCreate4)),
    %%------------------------------

    %%------ create noncdmi --------
    ?assert(not object_exists(Config, ToCreate5)),

    RequestHeaders5 = [{<<"content-type">>, <<"application/binary">>}],
    {ok, Code5, _Headers5, _Response5} =
        do_request(Worker, ToCreate5, put,
            [user_1_token_header() | RequestHeaders5], FileContent),

    ?assertEqual(201, Code5),

    ?assert(object_exists(Config, ToCreate5)),
    ?assertEqual(FileContent, get_file_content(Config, ToCreate5)).
    %%------------------------------

% Tests cdmi object PUT requests (updating content)
update_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {_TestDirName, _TestFileName, FullTestFileName, TestFileContent} =
        create_test_dir_and_file(Config),
    NewValue = <<"New Value!">>,
    UpdatedValue = <<"123 Value!">>,

    %%--- value replace, cdmi ------
    ?assert(object_exists(Config, FullTestFileName)),
    ?assertEqual(TestFileContent, get_file_content(Config, FullTestFileName)),

    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header()],
    RequestBody1 = [{<<"value">>, NewValue}],
    RawRequestBody1 = json_utils:encode(RequestBody1),
    {ok, Code1, _Headers1, _Response1} = do_request(Worker, FullTestFileName, put, RequestHeaders1, RawRequestBody1),
    ?assertEqual(204, Code1),

    ?assert(object_exists(Config, FullTestFileName)),
    ?assertEqual(NewValue, get_file_content(Config, FullTestFileName)),
    %%------------------------------

    %%---- value update, cdmi ------
    UpdateValue = <<"123">>,
    RequestHeaders2 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_1_token_header()],
    RequestBody2 = [{<<"value">>, base64:encode(UpdateValue)}],
    RawRequestBody2 = json_utils:encode(RequestBody2),
    {ok, Code2, _Headers2, _Response2} = do_request(Worker, FullTestFileName ++ "?value:0-2", put, RequestHeaders2, RawRequestBody2),
    ?assertEqual(204, Code2),

    ?assert(object_exists(Config, FullTestFileName)),
    ?assertEqual(UpdatedValue, get_file_content(Config, FullTestFileName)),
    %%------------------------------

    %%--- value replace, http ------
    RequestBody3 = TestFileContent,
    {ok, Code3, _Headers3, _Response3} =
        do_request(Worker, FullTestFileName, put, [user_1_token_header()], RequestBody3),
    ?assertEqual(204, Code3),

    ?assert(object_exists(Config, FullTestFileName)),
    ?assertEqual(TestFileContent,
        get_file_content(Config, FullTestFileName)),
    %%------------------------------

    %%---- value update, http ------
    UpdateValue = <<"123">>,
    RequestHeaders4 = [{<<"content-range">>, <<"0-2">>}],
    {ok, Code4, _Headers4, _Response4} =
        do_request(Worker, FullTestFileName,
            put, [user_1_token_header() | RequestHeaders4], UpdateValue),
    ?assertEqual(204, Code4),

    ?assert(object_exists(Config, FullTestFileName)),
    ?assertEqual(<<"123t_file_content">>,
        get_file_content(Config, FullTestFileName)),
    %%------------------------------

    %%---- value update, http error ------
    UpdateValue = <<"123">>,
    RequestHeaders5 = [{<<"content-range">>, <<"0-2,3-4">>}],
    {ok, Code5, _Headers5, _Response5} =
        do_request(Worker, FullTestFileName, put, [user_1_token_header() | RequestHeaders5],
            UpdateValue),
    ?assertEqual(400, Code5),

    ?assert(object_exists(Config, FullTestFileName)),
    ?assertEqual(<<"123t_file_content">>,
        get_file_content(Config, FullTestFileName)).
    %%------------------------------

choose_adequate_handler(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    File = "file",
    Dir = "dir/",

    % when
    {ok, _, _, _} = do_request(Worker, File, get, [], []),
    % then
    ?assert(rpc:call(Worker, meck, called, [cdmi_object_handler, rest_init, '_'])),

    % when
    {ok, _, _, _} = do_request(Worker, Dir, get, [], []),
    % then
    ?assert(rpc:call(Worker, meck, called, [cdmi_container_handler, rest_init, '_'])).

use_supported_cdmi_version(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    RequestHeaders = [?CDMI_VERSION_HEADER, user_1_token_header()],

    % when
    {ok, Code, _ResponseHeaders, _Response} =
        do_request(Worker, "/random", get, RequestHeaders),

    % then
    ?assertEqual(404, Code).

use_unsupported_cdmi_version(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    RequestHeaders = [{<<"X-CDMI-Specification-Version">>, <<"1.0.2">>}],

    % when
    {ok, Code, _ResponseHeaders, _Response} =
        do_request(Worker, "/random", get, RequestHeaders),

    % then
    ?assertEqual(400, Code).

% Tests dir creation (cdmi container PUT), remember that every container URI ends
% with '/'
create_dir_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    DirName = "toCreate/",
    DirName2 = "toCreate2/",
    MissingParentName = "unknown/",
    DirWithoutParentName = filename:join(MissingParentName, "dir") ++ "/",

    %%------ non-cdmi create -------
    ?assert(not object_exists(Config, DirName)),

    {ok, Code1, _Headers1, _Response1} =
        do_request(Worker, DirName, put, [user_1_token_header()]),
    ?assertEqual(201, Code1),

    ?assert(object_exists(Config, DirName)),
    %%------------------------------

    %%------ basic create ----------
    ?assert(not object_exists(Config, DirName2)),

    RequestHeaders2 = [user_1_token_header(), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER],
    {ok, Code2, _Headers2, Response2} = do_request(Worker, DirName2, put, RequestHeaders2, []),

    ?assertEqual(201, Code2),
    CdmiResponse2 = json_utils:decode(Response2),
    ?assertEqual(<<"application/cdmi-container">>, proplists:get_value(<<"objectType">>, CdmiResponse2)),
    ?assertEqual(list_to_binary(DirName2), proplists:get_value(<<"objectName">>, CdmiResponse2)),
    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>, CdmiResponse2)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>, CdmiResponse2)),
    ?assertEqual([], proplists:get_value(<<"children">>, CdmiResponse2)),
    ?assert(proplists:get_value(<<"metadata">>, CdmiResponse2) =/= <<>>),

    ?assert(object_exists(Config, DirName2)),
    %%------------------------------

    %%---------- update ------------
    ?assert(object_exists(Config, DirName)),

    RequestHeaders3 = [
        user_1_token_header(), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code3, _Headers3, _Response3} =
        do_request(Worker, DirName, put, RequestHeaders3, []),
    ?assertEqual(204, Code3),

    ?assert(object_exists(Config, DirName)),
    %%------------------------------

    %%----- missing parent ---------
    ?assert(not object_exists(Config, MissingParentName)),

    RequestHeaders4 = [
        user_1_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code4, _Headers4, _Response4} =
        do_request(Worker, DirWithoutParentName, put, RequestHeaders4, []),
    ?assertEqual(404, Code4).
    %%------------------------------

% tests access to file by objectid
objectid_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {TestDirName, TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),

    %%-------- / objectid ----------
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code1, Headers1, Response1} = do_request(Worker, "", get, RequestHeaders1, []),
    ?assertEqual(200, Code1),

    ?assertEqual(<<"application/cdmi-container">>, proplists:get_value(<<"content-type">>, Headers1)),
    CdmiResponse1 = json_utils:decode(Response1),
    ?assertEqual(<<"/">>, proplists:get_value(<<"objectName">>, CdmiResponse1)),
    RootId = proplists:get_value(<<"objectID">>, CdmiResponse1),
    ?assertNotEqual(RootId, undefined),
    ?assert(is_binary(RootId)),
    ?assertEqual(<<>>, proplists:get_value(<<"parentURI">>, CdmiResponse1)),
    ?assertEqual(undefined, proplists:get_value(<<"parentID">>, CdmiResponse1)),
    ?assertEqual(<<"cdmi_capabilities/container/">>, proplists:get_value(<<"capabilitiesURI">>, CdmiResponse1)),
    %%------------------------------

    %%------ /dir objectid ---------
    RequestHeaders2 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code2, _Headers2, Response2} = do_request(Worker, TestDirName ++ "/", get, RequestHeaders2, []),
    ?assertEqual(200, Code2),

    CdmiResponse2 = json_utils:decode(Response2),
    ?assertEqual(<<"dir/">>, proplists:get_value(<<"objectName">>, CdmiResponse2)),
    DirId = proplists:get_value(<<"objectID">>, CdmiResponse2),
    ?assertNotEqual(DirId, undefined),
    ?assert(is_binary(DirId)),
    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>, CdmiResponse2)),
    ?assertEqual(RootId, proplists:get_value(<<"parentID">>, CdmiResponse2)),
    ?assertEqual(<<"cdmi_capabilities/container/">>, proplists:get_value(<<"capabilitiesURI">>, CdmiResponse2)),
    %%------------------------------

    %%--- /dir/file.txt objectid ---
    RequestHeaders3 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code3, _Headers3, Response3} = do_request(Worker, filename:join(TestDirName, TestFileName), get, RequestHeaders3, []),
    ?assertEqual(200, Code3),

    CdmiResponse3 = json_utils:decode(Response3),
    ?assertEqual(<<"file.txt">>, proplists:get_value(<<"objectName">>, CdmiResponse3)),
    FileId = proplists:get_value(<<"objectID">>, CdmiResponse3),
    ?assertNotEqual(FileId, undefined),
    ?assert(is_binary(FileId)),
    ?assertEqual(<<"/dir/">>, proplists:get_value(<<"parentURI">>, CdmiResponse3)),
    ?assertEqual(DirId, proplists:get_value(<<"parentID">>, CdmiResponse3)),
    ?assertEqual(<<"cdmi_capabilities/dataobject/">>, proplists:get_value(<<"capabilitiesURI">>, CdmiResponse3)),
    %%------------------------------

    %%---- get / by objectid -------
    RequestHeaders4 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code4, _Headers4, Response4} = do_request(Worker, "cdmi_objectid/" ++ binary_to_list(RootId) ++ "/", get, RequestHeaders4, []),
    ?assertEqual(200, Code4),
    CdmiResponse4 = json_utils:decode(Response4),
    Meta1 = proplists:delete(<<"cdmi_atime">>, proplists:get_value(<<"metadata">>, CdmiResponse1)),
    CdmiResponse1WithoutAtime = [{<<"metadata">>, Meta1} | proplists:delete(<<"metadata">>, CdmiResponse1)],
    Meta4 = proplists:delete(<<"cdmi_atime">>, proplists:get_value(<<"metadata">>, CdmiResponse4)),
    CdmiResponse4WithoutAtime = [{<<"metadata">>, Meta4} | proplists:delete(<<"metadata">>, CdmiResponse4)],
    ?assertEqual(CdmiResponse1WithoutAtime, CdmiResponse4WithoutAtime), % should be the same as in 1 (except access time)
    %%------------------------------

    %%--- get /dir/ by objectid ----
    RequestHeaders5 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code5, _Headers5, Response5} = do_request(Worker, "cdmi_objectid/" ++ binary_to_list(DirId) ++ "/", get, RequestHeaders5, []),
    ?assertEqual(200, Code5),
    CdmiResponse5 = json_utils:decode(Response5),
    Meta2 = proplists:delete(<<"cdmi_atime">>, proplists:get_value(<<"metadata">>, CdmiResponse2)),
    CdmiResponse2WithoutAtime = [{<<"metadata">>, Meta2} | proplists:delete(<<"metadata">>, CdmiResponse2)],
    Meta5 = proplists:delete(<<"cdmi_atime">>, proplists:get_value(<<"metadata">>, CdmiResponse5)),
    CdmiResponse5WithoutAtime = [{<<"metadata">>, Meta5} | proplists:delete(<<"metadata">>, CdmiResponse5)],
    ?assertEqual( % should be the same as in 2 (except parent and access time)
        proplists:delete(<<"parentURI">>, proplists:delete(<<"parentID">>, CdmiResponse2WithoutAtime)),
        proplists:delete(<<"parentURI">>, proplists:delete(<<"parentID">>, CdmiResponse5WithoutAtime))
    ),
    %%------------------------------

    %% get /dir/file.txt by objectid
    RequestHeaders6 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code6, _Headers6, Response6} = do_request(Worker, "cdmi_objectid/" ++ binary_to_list(DirId) ++ "/file.txt", get, RequestHeaders6, []),
    ?assertEqual(200, Code6),
    CdmiResponse6 = json_utils:decode(Response6),
    Meta3 = proplists:delete(<<"cdmi_atime">>, proplists:get_value(<<"metadata">>, CdmiResponse3)),
    CdmiResponse3WithoutAtime = [{<<"metadata">>, Meta3} | proplists:delete(<<"metadata">>, CdmiResponse3)],
    Meta6 = proplists:delete(<<"cdmi_atime">>, proplists:get_value(<<"metadata">>, CdmiResponse6)),
    CdmiResponse6WithoutAtime = [{<<"metadata">>, Meta6} | proplists:delete(<<"metadata">>, CdmiResponse6)],
    ?assertEqual( % should be the same as in 3 (except access time)
        proplists:delete(<<"parentURI">>, proplists:delete(<<"parentID">>, CdmiResponse3WithoutAtime)),
        proplists:delete(<<"parentURI">>, proplists:delete(<<"parentID">>, CdmiResponse6WithoutAtime))
    ),

    {ok, Code7, _Headers7, Response7} = do_request(Worker, "cdmi_objectid/" ++ binary_to_list(FileId), get, RequestHeaders6, []),
    ?assertEqual(200, Code7),
    CdmiResponse7 = json_utils:decode(Response7),
    Meta7 = proplists:delete(<<"cdmi_atime">>, proplists:get_value(<<"metadata">>, CdmiResponse7)),
    CdmiResponse7WithoutAtime = [{<<"metadata">>, Meta7} | proplists:delete(<<"metadata">>, CdmiResponse7)],
    ?assertEqual( % should be the same as in 6 (except parent and access time)
        proplists:delete(<<"parentURI">>, proplists:delete(<<"parentID">>, CdmiResponse6WithoutAtime)),
        proplists:delete(<<"parentURI">>, proplists:delete(<<"parentID">>, CdmiResponse7WithoutAtime))
    ),
    %%------------------------------

    %%---- unauthorized access to / by objectid -------
    RequestHeaders8 = [?CDMI_VERSION_HEADER],
    {ok, Code8, _, _} = do_request(Worker, "cdmi_objectid/" ++ binary_to_list(RootId) ++ "/", get, RequestHeaders8, []),
    ?assertEqual(401, Code8).
    %%------------------------------

% tests if capabilities of objects, containers, and whole storage system are set properly
capabilities_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    %%--- system capabilities ------
    RequestHeaders8 = [?CDMI_VERSION_HEADER],
    {ok, Code8, Headers8, Response8} =
        do_request(Worker, "cdmi_capabilities/", get, RequestHeaders8, []),
    ?assertEqual(200, Code8),

    ?assertEqual(<<"application/cdmi-capability">>,
        proplists:get_value(<<"content-type">>, Headers8)),
    CdmiResponse8 = json_utils:decode(Response8),
    ?assertEqual(?root_capability_id, proplists:get_value(<<"objectID">>, CdmiResponse8)),
    ?assertEqual(?root_capability_path,
        proplists:get_value(<<"objectName">>, CdmiResponse8)),
    ?assertEqual(<<"0-1">>,
        proplists:get_value(<<"childrenrange">>, CdmiResponse8)),
    ?assertEqual([<<"container/">>, <<"dataobject/">>],
        proplists:get_value(<<"children">>, CdmiResponse8)),
    Capabilities = proplists:get_value(<<"capabilities">>, CdmiResponse8),
    ?assertEqual(?root_capability_list, Capabilities),
    %%------------------------------

    %%-- container capabilities ----
    RequestHeaders9 = [?CDMI_VERSION_HEADER],
    {ok, Code9, _Headers9, Response9} =
        do_request(Worker, "cdmi_capabilities/container/", get, RequestHeaders9, []),
    ?assertEqual(200, Code9),
    ?assertMatch({ok, Code9, _, Response9}, do_request(Worker, "cdmi_objectid/" ++ binary_to_list(?container_capability_id) ++ "/", get, RequestHeaders9, [])),

    CdmiResponse9 = json_utils:decode(Response9),
    ?assertEqual(?root_capability_path,
        proplists:get_value(<<"parentURI">>, CdmiResponse9)),
    ?assertEqual(?root_capability_id, proplists:get_value(<<"parentID">>, CdmiResponse9)),
    ?assertEqual(?container_capability_id, proplists:get_value(<<"objectID">>, CdmiResponse9)),
    ?assertEqual(<<"container/">>,
        proplists:get_value(<<"objectName">>, CdmiResponse9)),
    Capabilities2 = proplists:get_value(<<"capabilities">>, CdmiResponse9),
    ?assertEqual(?container_capability_list, Capabilities2),
    %%------------------------------

    %%-- dataobject capabilities ---
    RequestHeaders10 = [?CDMI_VERSION_HEADER],
    {ok, Code10, _Headers10, Response10} =
    do_request(Worker, "cdmi_capabilities/dataobject/", get, RequestHeaders10, []),
    ?assertEqual(200, Code10),
    ?assertMatch({ok, Code10, _, Response10}, do_request(Worker, "cdmi_objectid/" ++ binary_to_list(?dataobject_capability_id) ++ "/", get, RequestHeaders10, [])),

    CdmiResponse10 = json_utils:decode(Response10),
    ?assertEqual(?root_capability_path,
        proplists:get_value(<<"parentURI">>, CdmiResponse10)),
    ?assertEqual(?root_capability_id, proplists:get_value(<<"parentID">>, CdmiResponse10)),
    ?assertEqual(?dataobject_capability_id, proplists:get_value(<<"objectID">>, CdmiResponse10)),
    ?assertEqual(<<"dataobject/">>,
        proplists:get_value(<<"objectName">>, CdmiResponse10)),
    Capabilities3 = proplists:get_value(<<"capabilities">>, CdmiResponse10),
    ?assertEqual(?dataobject_capability_list, Capabilities3).
    %%------------------------------

% tests if cdmi returns 'moved permanently' code when we forget about '/' in path
moved_permanently_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    DirName = "somedir/",
    DirNameWithoutSlash = "somedir",
    FileName = "somedir/somefile.txt",
    FileNameWithSlash = "somedir/somefile.txt/",
    mkdir(Config, DirName),
    ?assert(object_exists(Config, DirName)),
    create_file(Config, FileName),
    ?assert(object_exists(Config, FileName)),

    CDMIEndpoint = cdmi_endpoint(Worker),
    %%--------- dir test -----------
    RequestHeaders1 = [
        ?CONTAINER_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        user_1_token_header()
    ],
    Location1 = list_to_binary(CDMIEndpoint ++ DirName),
    {ok, Code1, Headers1, _Response1} =
        do_request(Worker, DirNameWithoutSlash, get, RequestHeaders1, []),
    ?assertEqual(?MOVED_PERMANENTLY, Code1),
    ?assertEqual(Location1,
        proplists:get_value(<<"Location">>, Headers1)),
    %%------------------------------

    %%--------- dir test with QS-----------
    RequestHeaders2 = [
        ?CONTAINER_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        user_1_token_header()
    ],
    Location2 = list_to_binary(CDMIEndpoint ++ DirName++"?example_qs=1"),
    {ok, Code2, Headers2, _Response2} =
        do_request(Worker, DirNameWithoutSlash++"?example_qs=1", get, RequestHeaders2, []),
    ?assertEqual(?MOVED_PERMANENTLY, Code2),
    ?assertEqual(Location2,
        proplists:get_value(<<"Location">>, Headers2)),
    %%------------------------------

    %%--------- file test ----------
    RequestHeaders3 = [
        ?OBJECT_CONTENT_TYPE_HEADER,
        ?CDMI_VERSION_HEADER,
        user_1_token_header()
    ],
    Location3 = list_to_binary(CDMIEndpoint ++ FileName),
    {ok, Code3, Headers3, _Response3} =
        do_request(Worker, FileNameWithSlash, get, RequestHeaders3, []),
    ?assertEqual(?MOVED_PERMANENTLY, Code3),
    ?assertEqual(Location3,
        proplists:get_value(<<"Location">>, Headers3)).
    %%------------------------------

% tests req format checking
request_format_check_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    FileToCreate = "file.txt",
    DirToCreate = "dir/",
    FileContent = <<"File content!">>,

    %%-- obj missing content-type --
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    RequestBody1 = [{<<"value">>, FileContent}],
    RawRequestBody1 = json_utils:encode(RequestBody1),
    {ok, Code1, _Headers1, _Response1} = do_request(Worker, FileToCreate, put, RequestHeaders1, RawRequestBody1),
    ?assertEqual(415, Code1),
    %%------------------------------

    %%-- dir missing content-type --
    RequestHeaders3 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    RequestBody3 = [{<<"metadata">>, <<"">>}],
    RawRequestBody3 = json_utils:encode(RequestBody3),
    {ok, Code3, _Headers3, _Response3} = do_request(Worker, DirToCreate, put, RequestHeaders3, RawRequestBody3),
    ?assertEqual(415, Code3).
    %%------------------------------

% tests mimetype and valuetransferencoding properties, they are part of cdmi-object and cdmi-container
% and should be changeble
mimetype_and_encoding_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {TestDirName, TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),

    %% get mimetype and valuetransferencoding of non-cdmi file
    RequestHeaders1 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code1, _Headers1, Response1} = do_request(Worker, filename:join(TestDirName, TestFileName) ++ "?mimetype;valuetransferencoding", get, RequestHeaders1, []),
    ?assertEqual(200, Code1),
    CdmiResponse1 = json_utils:decode(Response1),
    ?assertEqual(<<"application/octet-stream">>, proplists:get_value(<<"mimetype">>, CdmiResponse1)),
    ?assertEqual(<<"base64">>, proplists:get_value(<<"valuetransferencoding">>, CdmiResponse1)),
    %%------------------------------

    %%-- update mime and encoding --
    RequestHeaders2 = [?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER, user_1_token_header()],
    RawBody2 = json_utils:encode([{<<"valuetransferencoding">>, <<"utf-8">>}, {<<"mimetype">>, <<"application/binary">>}]),
    {ok, Code2, _Headers2, _Response2} = do_request(Worker, filename:join(TestDirName, TestFileName), put, RequestHeaders2, RawBody2),
    ?assertEqual(204, Code2),

    {ok, Code3, _Headers3, Response3} = do_request(Worker, filename:join(TestDirName, TestFileName) ++ "?mimetype;valuetransferencoding", get, RequestHeaders2, []),
    ?assertEqual(200, Code3),
    CdmiResponse3 = json_utils:decode(Response3),
    ?assertEqual(<<"application/binary">>, proplists:get_value(<<"mimetype">>, CdmiResponse3)),
    ?assertEqual(<<"utf-8">>, proplists:get_value(<<"valuetransferencoding">>, CdmiResponse3)),
    %%------------------------------

    %% create file with given mime and encoding
    FileName4 = "mime_file.txt",
    FileContent4 = <<"some content">>,
    RequestHeaders4 = [?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER, user_1_token_header()],
    RawBody4 = json_utils:encode([{<<"valuetransferencoding">>, <<"utf-8">>}, {<<"mimetype">>, <<"text/plain">>}, {<<"value">>, FileContent4}]),
    {ok, Code4, _Headers4, Response4} = do_request(Worker, FileName4, put, RequestHeaders4, RawBody4),
    ?assertEqual(201, Code4),
    CdmiResponse4 = json_utils:decode(Response4),
    ?assertEqual(<<"text/plain">>, proplists:get_value(<<"mimetype">>, CdmiResponse4)),

    RequestHeaders5 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code5, _Headers5, Response5} = do_request(Worker, FileName4 ++ "?value;mimetype;valuetransferencoding", get, RequestHeaders5, []),
    ?assertEqual(200, Code5),
    CdmiResponse5 = json_utils:decode(Response5),
    ?assertEqual(<<"text/plain">>, proplists:get_value(<<"mimetype">>, CdmiResponse5)),
    ?assertEqual(<<"utf-8">>, proplists:get_value(<<"valuetransferencoding">>, CdmiResponse5)), %todo what do we return here if file contains valid utf-8 string and we read byte range?
    ?assertEqual(FileContent4, proplists:get_value(<<"value">>, CdmiResponse5)),
    %%------------------------------

    %% create file with given mime and encoding using non-cdmi request
    FileName6 = "mime_file_noncdmi.txt",
    FileContent6 = <<"some content">>,
    RequestHeaders6 = [{<<"Content-Type">>, <<"text/plain; charset=utf-8">>}, user_1_token_header()],
    {ok, Code6, _Headers6, _Response6} = do_request(Worker, FileName6, put, RequestHeaders6, FileContent6),
    ?assertEqual(201, Code6),

    RequestHeaders7 = [?CDMI_VERSION_HEADER, user_1_token_header()],
    {ok, Code7, _Headers7, Response7} = do_request(Worker, FileName6 ++ "?value;mimetype;valuetransferencoding", get, RequestHeaders7, []),
    ?assertEqual(200, Code7),
    CdmiResponse7 = json_utils:decode(Response7),
    ?assertEqual(<<"text/plain">>, proplists:get_value(<<"mimetype">>, CdmiResponse7)),
    ?assertEqual(<<"utf-8">>, proplists:get_value(<<"valuetransferencoding">>, CdmiResponse7)),
    ?assertEqual(FileContent6, proplists:get_value(<<"value">>, CdmiResponse7)).
    %%------------------------------

% tests reading&writing file at random ranges
out_of_range_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {TestDirName, _TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),
    FileName = "random_range_file.txt",
    {ok, _} = create_file(Config, FileName),

    %%---- reading out of range ---- (shuld return empty binary)
    ?assertEqual(<<>>, get_file_content(Config, FileName)),
    RequestHeaders1 = [user_1_token_header(), ?CDMI_VERSION_HEADER],

    RequestBody1 = json_utils:encode([{<<"value">>, <<"data">>}]),
    {ok, Code1, _Headers1, Response1} = do_request(Worker, FileName ++ "?value:0-3", get, RequestHeaders1, RequestBody1),
    ?assertEqual(200, Code1),
    CdmiResponse1 = json_utils:decode(Response1),
    ?assertEqual(<<>>, proplists:get_value(<<"value">>, CdmiResponse1)),
    %%------------------------------

    %%------ writing at end -------- (shuld extend file)
    ?assertEqual(<<>>, get_file_content(Config, FileName)),

    RequestHeaders2 = [user_1_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody2 = json_utils:encode([{<<"value">>, base64:encode(<<"data">>)}]),
    {ok, Code2, _Headers2, _Response2} = do_request(Worker, FileName ++ "?value:0-3", put, RequestHeaders2, RequestBody2),
    ?assertEqual(204, Code2),

    ?assertEqual(<<"data">>, get_file_content(Config, FileName)),
    %%------------------------------

    %%------ writing at random -------- (should return zero bytes in any gaps)
%%     RequestBody3 = json_utils:encode([{<<"value">>, base64:encode(<<"data">>)}]), todo fix https://jira.plgrid.pl/jira/browse/VFS-1443 and uncomment
%%     {ok, Code3, _Headers3, _Response3} = do_request(Worker, FileName ++ "?value:10-13", put, RequestHeaders2, RequestBody3),
%%     ?assertEqual(204, Code3),
%%
%%     ?assertEqual(<<100, 97, 116, 97, 0, 0, 0, 0, 0, 0, 100, 97, 116, 97>>, get_file_content(Config, FileName)), % "data(6x<0_byte>)data"
    %%------------------------------

    %%----- random childrange ------ (shuld fail)
    {ok, Code4, _Headers4, Response4} = do_request(Worker, TestDirName ++ "/?children:100-132", get, RequestHeaders2, []),
    ?assertEqual(400, Code4),
    CdmiResponse4 = json_utils:decode(Response4),

    ?assertMatch([{<<"error_invalid_childrenrange">>, _}], CdmiResponse4).
    %%------------------------------

% tests copy and move operations on dataobjects and containers
copy_move_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    FileName = "move_test_file.txt",
    DirName = "move_test_dir/",
    FileUri = list_to_binary(filename:join("/", FileName)),
    FileData = <<"data">>,
    create_file(Config, FileName),
    mkdir(Config, DirName),
    write_to_file(Config, FileName, FileData, 0),
    NewMoveFileName = "new_move_test_file",
    NewMoveDirName = "new_move_test_dir/",

    %%--- conflicting mv/cpy ------- (we cannot move and copy at the same time)
    ?assertEqual(FileData, get_file_content(Config, FileName)),

    RequestHeaders1 = [user_1_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody1 = json_utils:encode([{<<"move">>, FileUri}, {<<"copy">>, FileUri}]),
    {ok, Code1, _Headers1, Response1} = do_request(Worker, NewMoveFileName, put, RequestHeaders1, RequestBody1),
    ?assertEqual(400, Code1),
    CdmiResponse1 = json_utils:decode(Response1),
    ?assertMatch([{<<"error_conflicting_body_fields">>, _}], CdmiResponse1),

    ?assertEqual(FileData, get_file_content(Config, FileName)),
    %%------------------------------

    %%----------- dir mv -----------
    ?assert(object_exists(Config, DirName)),
    ?assert(not object_exists(Config, NewMoveDirName)),

    RequestHeaders2 = [user_1_token_header(), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER],
    RequestBody2 = json_utils:encode([{<<"move">>, list_to_binary(DirName)}]),
    {ok, Code2, _Headers2, _Response2} = do_request(Worker, NewMoveDirName, put, RequestHeaders2, RequestBody2),
    ?assertEqual(201, Code2),

    ?assert(not object_exists(Config, DirName)),
    ?assert(object_exists(Config, NewMoveDirName)),
    %%------------------------------

    %%---------- file mv -----------
    ?assert(object_exists(Config, FileName)),
    ?assert(not object_exists(Config, NewMoveFileName)),
    ?assertEqual(FileData, get_file_content(Config, FileName)),
    RequestHeaders3 = [user_1_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody3 = json_utils:encode([{<<"move">>, list_to_binary(FileName)}]),
    {ok, Code3, _Headers3, _Response3} = do_request(Worker, NewMoveFileName, put, RequestHeaders3, RequestBody3),
    ?assertEqual(201, Code3),

    ?assert(not object_exists(Config, FileName)),
    ?assert(object_exists(Config, NewMoveFileName)),
    ?assertEqual(FileData, get_file_content(Config, NewMoveFileName)),
    %%------------------------------

    %%---------- file cp ----------- (copy file, with xattrs and acl)
    % create file to copy
    FileName2 = "copy_test_file.txt",
    UserId1 = ?config({user_id, 1}, Config),
    create_file(Config, FileName2),
    FileData2 = <<"data">>,
    Acl = [#accesscontrolentity{
        acetype = ?allow_mask,
        identifier = UserId1,
        aceflags = ?no_flags_mask,
        acemask = ?all_perms_mask}],
    Xattrs = [#xattr{name = <<"key1">>, value = <<"value1">>}, #xattr{name = <<"key2">>, value = <<"value2">>}],
    set_acl(Config, FileName2, Acl),
    add_xattrs(Config, FileName2, Xattrs),
    write_to_file(Config, FileName2, FileData2, 0),

    % assert source file is created and destination does not exist
    NewFileName2 = "copy_test_file2.txt",
    ?assert(object_exists(Config, FileName2)),
    ?assert(not object_exists(Config, NewFileName2)),
    ?assertEqual(FileData2, get_file_content(Config, FileName2)),
    ?assertEqual({ok, Acl}, get_acl(Config, FileName2)),

    % copy file using cdmi
    RequestHeaders4 = [user_1_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody4 = json_utils:encode([{<<"copy">>, list_to_binary(FileName2)}]),
    {ok, Code4, _Headers4, _Response4} = do_request(Worker, NewFileName2, put, RequestHeaders4, RequestBody4),
    ?assertEqual(201, Code4),

    % assert new file is created
    ?assert(object_exists(Config, FileName2)),
    ?assert(object_exists(Config, NewFileName2)),
    ?assertEqual(FileData2, get_file_content(Config, NewFileName2)),
    ?assertEqual({ok, Xattrs}, get_xattrs(Config, NewFileName2)),
    ?assertEqual({ok, Acl}, get_acl(Config, NewFileName2)),
    %%------------------------------

    %%---------- dir cp ------------
    % create dir to copy (with some subdirs and subfiles)
    DirName2 = "copy_dir/",
    mkdir(Config, DirName2),
    ?assert(object_exists(Config, DirName2)),
    NewDirName2 = "new_copy_dir/",
    set_acl(Config, DirName2, Acl),
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
    RequestHeaders5 = [user_1_token_header(), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER],
    RequestBody5 = json_utils:encode([{<<"copy">>, list_to_binary(DirName2)}]),
    {ok, Code5, _Headers5, _Response5} = do_request(Worker, NewDirName2, put, RequestHeaders5, RequestBody5),
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
    ?assertEqual({ok, Xattrs}, get_xattrs(Config, NewDirName2)),
    ?assertEqual({ok, Acl}, get_acl(Config, NewDirName2)),
    ?assert(object_exists(Config, filename:join(NewDirName2, "dir1"))),
    ?assert(object_exists(Config, filename:join(NewDirName2, "dir2"))),
    ?assert(object_exists(Config, filename:join([NewDirName2, "dir1", "1"]))),
    ?assert(object_exists(Config, filename:join([NewDirName2, "dir1", "2"]))),
    ?assert(object_exists(Config, filename:join(NewDirName2, "3"))).
    %%------------------------------

% tests cdmi and non-cdmi partial upload feature (requests with x-cdmi-partial flag set to true)
partial_upload_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    FileName = "partial.txt",
    FileName2 = "partial2.txt",
    Chunk1 = <<"some">>,
    Chunk2 = <<"_">>,
    Chunk3 = <<"value">>,

    %%------ cdmi request partial upload ------
    ?assert(not object_exists(Config, FileName)),

    % upload first chunk of file
    RequestHeaders1 = [user_1_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER, {"X-CDMI-Partial", "true"}],
    RequestBody1 = json_utils:encode([{<<"value">>, Chunk1}]),
    {ok, Code1, _Headers1, Response1} = do_request(Worker, FileName, put, RequestHeaders1, RequestBody1),
    ?assertEqual(201, Code1),
    CdmiResponse1 = json_utils:decode(Response1),
    ?assertEqual(<<"Processing">>, proplists:get_value(<<"completionStatus">>, CdmiResponse1)),

    % upload second chunk of file
    RequestBody2 = json_utils:encode([{<<"value">>, base64:encode(Chunk2)}]),
    {ok, Code2, _Headers2, _Response2} = do_request(Worker, FileName ++ "?value:4-4", put, RequestHeaders1, RequestBody2),
    ?assertEqual(204, Code2),

    % upload third chunk of file
    RequestHeaders3 = [user_1_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    RequestBody3 = json_utils:encode([{<<"value">>, base64:encode(Chunk3)}]),
    {ok, Code3, _Headers3, _Response3} = do_request(Worker, FileName ++ "?value:5-9", put, RequestHeaders3, RequestBody3),
    ?assertEqual(204, Code3),

    % get created file and check its consistency
    RequestHeaders4 = [user_1_token_header(), ?CDMI_VERSION_HEADER],
    {ok, Code4, _Headers4, Response4} = do_request(Worker, FileName, get, RequestHeaders4, []),
    ?assertEqual(200, Code4),
    CdmiResponse4 = json_utils:decode(Response4),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>, CdmiResponse4)),
    ?assertEqual(<<"utf-8">>, proplists:get_value(<<"valuetransferencoding">>, CdmiResponse4)),
    ?assertEqual(<<Chunk1/binary, Chunk2/binary, Chunk3/binary>>, proplists:get_value(<<"value">>, CdmiResponse4)),
    %%------------------------------

    %%----- non-cdmi request partial upload -------
    ?assert(not object_exists(Config, FileName2)),

    % upload first chunk of file
    RequestHeaders5 = [user_1_token_header(), {<<"X-CDMI-Partial">>, <<"true">>}],
    {ok, Code5, _Headers5, _Response5} = do_request(Worker, FileName2, put, RequestHeaders5, Chunk1),
    ?assertEqual(201, Code5),

    % check "completionStatus", should be set to "Processing"
    {ok, Code5_1, _Headers5_1, Response5_1} = do_request(Worker, FileName2 ++ "?completionStatus", get, RequestHeaders4, Chunk1),
    CdmiResponse5_1 = json_utils:decode(Response5_1),
    ?assertEqual(200, Code5_1),
    ?assertEqual(<<"Processing">>, proplists:get_value(<<"completionStatus">>, CdmiResponse5_1)),

    % upload second chunk of file
    RequestHeaders6 = [user_1_token_header(), {<<"content-range">>, <<"4-4">>}, {<<"X-CDMI-Partial">>, <<"true">>}],
    {ok, Code6, _Headers6, _Response6} = do_request(Worker, FileName2, put, RequestHeaders6, Chunk2),
    ?assertEqual(204, Code6),

    % upload third chunk of file
    RequestHeaders7 = [user_1_token_header(), {<<"content-range">>, <<"5-9">>}, {<<"X-CDMI-Partial">>, <<"false">>}],
    {ok, Code7, _Headers7, _Response7} = do_request(Worker, FileName2, put, RequestHeaders7, Chunk3),
    ?assertEqual(204, Code7),

    % get created file and check its consistency
    RequestHeaders8 = [user_1_token_header(), ?CDMI_VERSION_HEADER],
    {ok, Code8, _Headers8, Response8} = do_request(Worker, FileName2, get, RequestHeaders8, []),
    ?assertEqual(200, Code8),
    CdmiResponse8 = json_utils:decode(Response8),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>, CdmiResponse8)),
    ?assertEqual(<<Chunk1/binary, Chunk2/binary, Chunk3/binary>>, base64:decode(proplists:get_value(<<"value">>, CdmiResponse8))).
    %%------------------------------

% tests access control lists
acl_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    Filename1 = "acl_test_file1",
    Dirname1 = "acl_test_dir1/",
    UserId1 = ?config({user_id, 1}, Config),
    UserName1 = ?config({user_name, 1}, Config),
    Identifier1 = <<UserName1/binary, "#", UserId1/binary>>,

    Read = [
        {<<"acetype">>, fslogic_acl:bitmask_to_binary(?allow_mask)},
        {<<"identifier">>, Identifier1},
        {<<"aceflags">>, fslogic_acl:bitmask_to_binary(?no_flags_mask)},
        {<<"acemask">>, fslogic_acl:bitmask_to_binary(?read_mask)}
    ],
    Write = [
        {<<"acetype">>, fslogic_acl:bitmask_to_binary(?allow_mask)},
        {<<"identifier">>, Identifier1},
        {<<"aceflags">>, fslogic_acl:bitmask_to_binary(?no_flags_mask)},
        {<<"acemask">>, fslogic_acl:bitmask_to_binary(?write_mask)}
    ],
    Execute = [
        {<<"acetype">>, fslogic_acl:bitmask_to_binary(?allow_mask)},
        {<<"identifier">>, Identifier1},
        {<<"aceflags">>, fslogic_acl:bitmask_to_binary(?no_flags_mask)},
        {<<"acemask">>, fslogic_acl:bitmask_to_binary(?execute_mask)}
    ],
    WriteAcl = [
        {<<"acetype">>, fslogic_acl:bitmask_to_binary(?allow_mask)},
        {<<"identifier">>, Identifier1},
        {<<"aceflags">>, fslogic_acl:bitmask_to_binary(?no_flags_mask)},
        {<<"acemask">>, fslogic_acl:bitmask_to_binary(?write_acl_mask)}
    ],
    Delete = [
        {<<"acetype">>, fslogic_acl:bitmask_to_binary(?allow_mask)},
        {<<"identifier">>, Identifier1},
        {<<"aceflags">>, fslogic_acl:bitmask_to_binary(?no_flags_mask)},
        {<<"acemask">>, fslogic_acl:bitmask_to_binary(?delete_mask)}
    ],

    MetadataAclRead = json_utils:encode([{<<"metadata">>, [{<<"cdmi_acl">>, [Read, WriteAcl]}]}]),
    MetadataAclReadExecute = json_utils:encode([{<<"metadata">>, [{<<"cdmi_acl">>, [Read, Execute, WriteAcl]}]}]),
    MetadataAclDelete = json_utils:encode([{<<"metadata">>, [{<<"cdmi_acl">>, [Delete]}]}]),
    MetadataAclWrite = json_utils:encode([{<<"metadata">>, [{<<"cdmi_acl">>, [Write]}]}]),
    MetadataAclReadWrite = json_utils:encode([{<<"metadata">>, [{<<"cdmi_acl">>, [Write, Read]}]}]),
    MetadataAclReadWriteExecute = json_utils:encode([{<<"metadata">>, [{<<"cdmi_acl">>, [Write, Read, Execute]}]}]),

    %%----- read file test ---------
    % create test file with dummy data
    ?assert(not object_exists(Config, Filename1)),
    create_file(Config, filename:join("/", Filename1)),
    write_to_file(Config, Filename1, <<"data">>, 0),

    % set acl to 'write' and test cdmi/non-cdmi get request (should return 403 forbidden)
    RequestHeaders1 = [user_1_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    {ok, 204, _, _} = do_request(Worker, Filename1, put, RequestHeaders1, MetadataAclWrite),
    {ok, 403, _, _} = do_request(Worker, Filename1, get, RequestHeaders1, []),
    {ok, 403, _, _} = do_request(Worker, Filename1, get, [user_1_token_header()], []),
    ?assertEqual({error, ?EACCES}, open_file(Config, Filename1, read)),

    % set acl to 'read&write' and test cdmi/non-cdmi get request (should succeed)
    {ok, 204, _, _} = do_request(Worker, Filename1, put, RequestHeaders1, MetadataAclReadWrite),
    {ok, 200, _, _} = do_request(Worker, Filename1, get, RequestHeaders1, []),
    {ok, 200, _, _} = do_request(Worker, Filename1, get, [user_1_token_header()], []),
    %%------------------------------

    %%------- write file test ------
    % set acl to 'read&write' and test cdmi/non-cdmi put request (should succeed)
    {ok, 204, _, _} = do_request(Worker, Filename1, put, RequestHeaders1, MetadataAclReadWrite),
    RequestBody4 = json_utils:encode([{<<"value">>, <<"new_data">>}]),
    {ok, 204, _, _} = do_request(Worker, Filename1, put, RequestHeaders1, RequestBody4),
    ?assertEqual(<<"new_data">>, get_file_content(Config, Filename1)),
    write_to_file(Config, Filename1, <<"1">>, 8),
    ?assertEqual(<<"new_data1">>, get_file_content(Config, Filename1)),
    {ok, 204, _, _} = do_request(Worker, Filename1, put, [user_1_token_header()], <<"new_data2">>),
    ?assertEqual(<<"new_data2">>, get_file_content(Config, Filename1)),

    % set acl to 'read' and test cdmi/non-cdmi put request (should return 403 forbidden)
    {ok, 204, _, _} = do_request(Worker, Filename1, put, RequestHeaders1, MetadataAclRead),
    RequestBody6 = json_utils:encode([{<<"value">>, <<"new_data3">>}]),
    {ok, 403, _, _} = do_request(Worker, Filename1, put, RequestHeaders1, RequestBody6),
    {ok, 403, _, _} = do_request(Worker, Filename1, put, [user_1_token_header()], <<"new_data4">>),
    ?assertEqual(<<"new_data2">>, get_file_content(Config, Filename1)),
    ?assertEqual({error, ?EACCES}, open_file(Config, Filename1, write)),
    ?assertEqual(<<"new_data2">>, get_file_content(Config, Filename1)),
    %%------------------------------

    %%------ delete file test ------
    % set acl to 'delete'
    {ok, 204, _, _} = do_request(Worker, Filename1, put, RequestHeaders1, MetadataAclDelete),

    % delete file
    {ok, 204, _, _} = do_request(Worker, Filename1, delete, [user_1_token_header()], []),
    ?assert(not object_exists(Config, Filename1)),
    %%------------------------------

    %%--- read write dir test ------
    ?assert(not object_exists(Config, Dirname1)),
    mkdir(Config, filename:join("/", Dirname1)),
    File1 = filename:join(Dirname1, "1"),
    File2 = filename:join(Dirname1, "2"),
    File3 = filename:join(Dirname1, "3"),
    File4 = filename:join(Dirname1, "4"),

    % set acl to 'read&write' and test cdmi get request (should succeed)
    RequestHeaders2 = [user_1_token_header(), ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER],
    {ok, 204, _, _} = do_request(Worker, Dirname1, put, RequestHeaders2, MetadataAclReadWriteExecute),
    {ok, 200, _, _} = do_request(Worker, Dirname1, get, RequestHeaders2, []),

    % create files in directory (should succeed)
    {ok, 201, _, _} = do_request(Worker, File1, put, [user_1_token_header()], []),
    ?assert(object_exists(Config, File1)),
    {ok, 201, _, _} = do_request(Worker, File2, put, RequestHeaders1, <<"{\"value\":\"val\"}">>),
    ?assert(object_exists(Config, File2)),
    create_file(Config, File3),
    ?assert(object_exists(Config, File3)),

    % delete files (should succeed)
    {ok, 204, _, _} = do_request(Worker, File1, delete, [user_1_token_header()], []),
    ?assert(not object_exists(Config, File1)),
    {ok, 204, _, _} = do_request(Worker, File2, delete, [user_1_token_header()], []),
    ?assert(not object_exists(Config, File2)),

    % set acl to 'write' and test cdmi get request (should return 403 forbidden)
    {ok, 204, _, _} = do_request(Worker, Dirname1, put, RequestHeaders2, MetadataAclWrite),
    {ok, 403, _, _} = do_request(Worker, Dirname1, get, RequestHeaders2, []),

    % set acl to 'read' and test cdmi put request (should return 403 forbidden)
    {ok, 204, _, _} = do_request(Worker, Dirname1, put, RequestHeaders2, MetadataAclReadExecute),
    {ok, 200, _, _} = do_request(Worker, Dirname1, get, RequestHeaders2, []),
    {ok, 403, _, _} = do_request(Worker, Dirname1, put, RequestHeaders2, json_utils:encode([{<<"metadata">>, [{<<"my_meta">>, <<"value">>}]}])),

    % create files (should return 403 forbidden)
    {ok, 403, _, _} = do_request(Worker, File1, put, [user_1_token_header()], []),
    ?assert(not object_exists(Config, File1)),
    {ok, 403, _, _} = do_request(Worker, File2, put, RequestHeaders1, <<"{\"value\":\"val\"}">>),
    ?assert(not object_exists(Config, File2)),
    ?assertEqual({error,?EACCES}, create_file(Config, File4)),
    ?assert(not object_exists(Config, File4)),

    % delete files (should return 403 forbidden)
    {ok, 403, _, _} = do_request(Worker, File3, delete, [user_1_token_header()], []),
    ?assert(object_exists(Config, File3)).
    %%------------------------------

% test error handling
errors_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {TestDirName, _TestFileName, _FullTestFileName, _TestFileContent} =
        create_test_dir_and_file(Config),

    %%---- unauthorized access -----
    {ok, Code1, _Headers1, _Response1} =
        do_request(Worker, TestDirName, get, [], []),
    ?assertEqual(401, Code1),
    %%------------------------------

    %%----- wrong create path ------
    RequestHeaders2 = [
        user_1_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code2, _Headers2, _Response2} =
        do_request(Worker, "test_dir", put, RequestHeaders2, []),
    ?assertEqual(400, Code2),
    %%------------------------------

    %%---- wrong create path 2 -----
    RequestHeaders3 = [
        user_1_token_header(),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER
    ],
    {ok, Code3, _Headers3, _Response3} =
        do_request(Worker, "test_dir/", put, RequestHeaders3, []),
    ?assertEqual(400, Code3),
    %%------------------------------

    %%-------- wrong base64 --------
    RequestHeaders4 = [
        user_1_token_header(),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER
    ],
    RequestBody4 = json_utils:encode([{<<"valuetransferencoding">>, <<"base64">>}, {<<"value">>, <<"#$%">>}]),
    {ok, Code4, _Headers4, _Response4} =
        do_request(Worker, "some_file_b64", put, RequestHeaders4, RequestBody4),
    ?assertEqual(400, Code4),
    %%------------------------------

    %%-- duplicated body fields ----
    RawBody5 = [{<<"metadata">>, [{<<"a">>, <<"a">>}]}, {<<"metadata">>, [{<<"b">>, <<"b">>}]}],
    RequestBody5 = json_utils:encode(RawBody5),
    RequestHeaders5 = [
        user_1_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code5, _Headers5, Response5} =
        do_request(Worker, "dir_dupl/", put, RequestHeaders5, RequestBody5),
    ?assertEqual(400, Code5),
    CdmiResponse5 = json_utils:decode(Response5),
    ?assertMatch([{<<"error_duplicated_body_fields">>, _}], CdmiResponse5),

    %%-- reding non-existing file --
    RequestHeaders6 = [
        user_1_token_header(),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER
    ],
    {ok, Code6, _Headers6, _Response6} =
        do_request(Worker, "nonexistent_file", get, RequestHeaders6),
    ?assertMatch(404, Code6),
    %%------------------------------

    %%--- list nonexisting dir -----
    RequestHeaders7 = [
        user_1_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code7, _Headers7, _Response7} =
        do_request(Worker, "nonexisting_dir/", get, RequestHeaders7),
    ?assertEqual(404, Code7),
    %%------------------------------

    %%--- open binary file without permission -----
    File8 = "file",
    FileContent8 = <<"File content...">>,
    create_file(Config, File8),
    ?assertEqual(object_exists(Config, File8), true),
    write_to_file(Config, File8, FileContent8, ?FILE_BEGINNING),
    ?assertEqual(get_file_content(Config, File8), FileContent8),
    RequestHeaders8 = [user_1_token_header()],

    mock_opening_file_without_perms(Config),
    {ok, Code8, _Headers8, _Response8} =
        do_request(Worker, File8, get, RequestHeaders8),
    unmock_opening_file_without_perms(Config),
    ?assertEqual(403, Code8),
    %%------------------------------

    %%--- open cdmi file without permission -----
    File9 = "file",
    FileContent9 = <<"File content...">>,
    create_file(Config, File9),
    ?assertEqual(object_exists(Config, File9), true),
    write_to_file(Config, File9, FileContent9, ?FILE_BEGINNING),
    ?assertEqual(get_file_content(Config, File9), FileContent9),
    RequestHeaders9 = [
        user_1_token_header(),
        ?CDMI_VERSION_HEADER,
        ?OBJECT_CONTENT_TYPE_HEADER
    ],

    mock_opening_file_without_perms(Config),
    {ok, Code9, _Headers9, _Response9} =
        do_request(Worker, File9, get, RequestHeaders9),
    unmock_opening_file_without_perms(Config),
    ?assertEqual(403, Code9).
    %%------------------------------

accept_header_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    AcceptHeader = {<<"Accept">>, <<"*/*">>},

    {ok, Code1, _Headers1, _Response1} =
        do_request(Worker, [], get,
            [user_1_token_header(), ?CDMI_VERSION_HEADER, AcceptHeader], []),

    ?assertEqual(200, Code1).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    test_node_starter:clean_environment(Config).

init_per_testcase(choose_adequate_handler, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [cdmi_object_handler, cdmi_container_handler]),
    init_per_testcase(default, Config);
init_per_testcase(_, Config) ->
    application:start(ssl2),
    hackney:start(),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(Config),
    mock_user_auth(ConfigWithSessionInfo),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(choose_adequate_handler, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [cdmi_object_handler, cdmi_container_handler]),
    end_per_testcase(default, Config);
end_per_testcase(_, Config) ->
    lfm_proxy:teardown(Config),
    unmock_user_auth(Config),
     %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

% Performs a single request using http_client
do_request(Node, RestSubpath, Method, Headers) ->
    do_request(Node, RestSubpath, Method, Headers, []).

% Performs a single request using http_client
do_request(Node, RestSubpath, Method, Headers, Body) ->
    http_client:request(
        Method,
        cdmi_endpoint(Node) ++ RestSubpath,
        Headers,
        Body,
        [insecure]
    ).

cdmi_endpoint(Node) ->
    Port =
        case get(port) of
            undefined ->
                {ok, P} = test_utils:get_env(Node, ?APP_NAME, rest_port),
                PStr = integer_to_list(P),
                put(port, PStr),
                PStr;
            P -> P
        end,
    string:join(["https://", utils:get_host(Node), ":", Port, "/cdmi/"], "").

mock_user_auth(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    {_, SerializedUser1Macaroon} = user_1_token_header(),
    {ok, User1Macaroon} = macaroon:deserialize(SerializedUser1Macaroon),
    test_utils:mock_new(Workers, identity),
    test_utils:mock_expect(Workers, identity, get_or_fetch,
        fun
            (#auth{macaroon = M}) when M =:= User1Macaroon ->
                UserId = ?config({user_id, 1}, Config),
                {ok, #document{value = #identity{user_id = UserId}}};
            (Auth) ->
                meck:passthrough(Auth)
        end
    ).

unmock_user_auth(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, identity).

create_test_dir_and_file(Config) ->
    TestDirName = "dir",
    TestFileName = "file.txt",
    FullTestFileName = filename:join(["/",TestDirName, TestFileName]),
    TestFileContent = <<"test_file_content">>,

    case object_exists(Config, TestDirName) of
        false ->
            {ok, _} = mkdir(Config, TestDirName),
            ?assert(object_exists(Config, TestDirName)),
            {ok, _} = create_file(Config, FullTestFileName),
            ?assert(object_exists(Config, FullTestFileName)),
            {ok, _} = write_to_file(Config, FullTestFileName, TestFileContent, 0),
            ?assertEqual(TestFileContent, get_file_content(Config, FullTestFileName));
        true -> ok
    end,

    {TestDirName, TestFileName, FullTestFileName, TestFileContent}.

object_exists(Config, Path) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),

    case lfm_proxy:stat(Worker, SessionId,
        {path, absolute_binary_path(Path)}) of
        {ok, _} ->
            true;
        {error, ?ENOENT} ->
            false
    end.

create_file(Config, Path) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),
    lfm_proxy:create(Worker, SessionId, absolute_binary_path(Path), ?DEFAULT_FILE_MODE).

open_file(Config, Path, OpenMode) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),
    lfm_proxy:open(Worker, SessionId, {path, absolute_binary_path(Path)}, OpenMode).

write_to_file(Config, Path, Data, Offset) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, FileHandle} = open_file(Config, Path, write),
    Result = lfm_proxy:write(Worker, FileHandle, Offset, Data),
    lfm_proxy:close(Worker, FileHandle),
    Result.

get_file_content(Config, Path) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, FileHandle} = open_file(Config, Path, read),
    Result = case lfm_proxy:read(Worker, FileHandle, ?FILE_BEGINNING, ?INFINITY) of
        {error, Error} -> {error, Error};
        {ok, Content} -> Content
    end,
    lfm_proxy:close(Worker, FileHandle),
    Result.

mkdir(Config, Path) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),
    lfm_proxy:mkdir(Worker, SessionId, absolute_binary_path(Path)).

set_acl(Config, Path, Acl) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),
    lfm_proxy:set_acl(Worker, SessionId, {path, absolute_binary_path(Path)}, Acl).

get_acl(Config, Path) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),
    lfm_proxy:get_acl(Worker, SessionId, {path, absolute_binary_path(Path)}).

add_xattrs(Config, Path, Xattrs) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),
    lists:foreach(fun(Xattr) ->
        ok = lfm_proxy:set_xattr(Worker, SessionId, {path, absolute_binary_path(Path)}, Xattr)
    end, Xattrs).

get_xattrs(Config, Path) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),
    {ok, Xattrs} = lfm_proxy:list_xattr(Worker, SessionId, {path, absolute_binary_path(Path)}),
    lists:filtermap(
    fun
        (<<"cdmi_", _/binary>>) ->
            false;
        (Xattr) ->
            {true, lfm_proxy:get_xattr(Worker, SessionId, {path, absolute_binary_path(Path)}, Xattr)}
    end, Xattrs).

absolute_binary_path(Path) ->
    list_to_binary(ensure_begins_with_slash(Path)).

ensure_begins_with_slash(Path) ->
    ReversedBinary = list_to_binary(lists:reverse(Path)),
    lists:reverse(binary_to_list(filepath_utils:ensure_ends_with_slash(ReversedBinary))).

mock_opening_file_without_perms(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, onedata_file_api),
    test_utils:mock_expect(
        Worker, onedata_file_api, open, fun (_, _ ,_) -> {error, ?EACCES} end).

unmock_opening_file_without_perms(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Worker, onedata_file_api).
