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
-include("modules/http_worker/rest/cdmi/cdmi_errors.hrl").
-include("modules/http_worker/rest/cdmi/cdmi_capabilities.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([get_file_test/1, metadata_test/1, delete_file_test/1, create_file_test/1, update_file_test/1,
    create_dir_test/1, capabilities_test/1, choose_adequate_handler/1,
    use_supported_cdmi_version/1, use_unsupported_cdmi_version/1, objectid_test/1]).

-performance({test_cases, []}).
all() ->
    [
        get_file_test, metadata_test, delete_file_test, create_file_test, update_file_test,
        create_dir_test, capabilities_test, objectid_test, choose_adequate_handler,
        use_supported_cdmi_version, use_unsupported_cdmi_version
    ].

-define(MACAROON, "macaroon").
-define(TIMEOUT, timer:seconds(5)).

-define(USER_1_TOKEN_HEADER, {<<"X-Auth-Token">>, <<"1">>}).
-define(CDMI_VERSION_HEADER, {<<"X-CDMI-Specification-Version">>, <<"1.1.1">>}).
-define(CONTAINER_CONTENT_TYPE_HEADER, {<<"content-type">>, <<"application/cdmi-container">>}).
-define(OBJECT_CONTENT_TYPE_HEADER, {<<"content-type">>, <<"application/cdmi-object">>}).

-define(FILE_PERMISSIONS, 8#664).

-define(FILE_BEGINNING, 0).
-define(INFINITY, 9999).


%%%===================================================================
%%% Test functions
%%%===================================================================

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
    {ok, _} = write_to_file(Config, FileName,FileContent, ?FILE_BEGINNING),
    ?assertEqual(FileContent, get_file_content(Config, FileName)),

    %%-------- basic read ----------
    RequestHeaders1 = [?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
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
    RequestHeaders2 = [?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
    {ok, Code2, _Headers2, Response2} = do_request(Worker, FileName ++ "?parentURI;completionStatus", get, RequestHeaders2, []),
    ?assertEqual(200, Code2),
    CdmiResponse2 = json_utils:decode(Response2),

    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>, CdmiResponse2)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>, CdmiResponse2)),
    ?assertEqual(2, length(CdmiResponse2)),
    %%------------------------------

    %%--- selective value read -----
    RequestHeaders3 = [?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
    {ok, Code3, _Headers3, Response3} = do_request(Worker, FileName ++ "?value:1-3;valuerange", get, RequestHeaders3, []),
    ?assertEqual(200, Code3),
    CdmiResponse3 = json_utils:decode(Response3),

    ?assertEqual(<<"1-3">>, proplists:get_value(<<"valuerange">>, CdmiResponse3)),
    ?assertEqual(<<"ome">>, base64:decode(proplists:get_value(<<"value">>, CdmiResponse3))), % 1-3 from FileContent = <<"Some content...">>
    %%------------------------------

    %%------- noncdmi read --------
    {ok, Code4, Headers4, Response4} = do_request(Worker, FileName, get, [?USER_1_TOKEN_HEADER]),
    ?assertEqual(200,Code4),

    ?assertEqual(<<"application/octet-stream">>, proplists:get_value(<<"content-type">>, Headers4)),
    ?assertEqual(FileContent, Response4),
    %%------------------------------

    %% selective value read non-cdmi
    RequestHeaders7 = [{<<"Range">>,<<"1-3,5-5,-3">>}],
    {ok, Code7, _Headers7, Response7} = do_request(Worker, FileName, get, [?USER_1_TOKEN_HEADER | RequestHeaders7]),
    ?assertEqual(206,Code7),
    ?assertEqual(<<"omec...">>, Response7), % 1-3,5-5,12-14  from FileContent = <<"Some content...">>
    %%------------------------------

    %% selective value read non-cdmi error
    RequestHeaders8 = [{<<"Range">>,<<"1-3,6-4,-3">>}],
    {ok, Code8, _Headers8, _Response8} = do_request(Worker, FileName, get, [?USER_1_TOKEN_HEADER | RequestHeaders8]),
    ?assertEqual(400,Code8).
    %%------------------------------

% Tests cdmi metadata read on object GET request.
metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    FileName = "metadataTest.txt",
    FileContent = <<"Some content...">>,
    DirName = "metadataTestDir/",
    {SessId, _UserId} = {?config({session_id, 1}, Config), ?config({user_id, 1}, Config)},

    %%-------- create file with user metadata --------
    ?assert(not object_exists(Config, FileName)),

    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
    RequestBody1 = [
        {<<"value">>, FileContent}, {<<"valuetransferencoding">>, <<"utf-8">>}, {<<"mimetype">>, <<"text/plain">>},
        {<<"metadata">>, [{<<"my_metadata">>, <<"my_value">>}, {<<"cdmi_not_allowed">>, <<"my_value">>}]}],
    RawRequestBody1 = json_utils:encode(RequestBody1),
    Before = now_in_secs(),
    {ok, Code1, _Headers1, Response1} = do_request(Worker, FileName, put, RequestHeaders1, RawRequestBody1),
    After = now_in_secs(),

    ?assertEqual(201, Code1),
    CdmiResponse1 = json_utils:decode(Response1),
    Metadata1 = proplists:get_value(<<"metadata">>, CdmiResponse1),
%%     ?assertEqual(<<"15">>, proplists:get_value(<<"cdmi_size">>, Metadata1)), todo fix wrong size (0) in attrs
    CTime1 = binary_to_integer(proplists:get_value(<<"cdmi_ctime">>, Metadata1)),
    ATime1 = binary_to_integer(proplists:get_value(<<"cdmi_atime">>, Metadata1)),
    MTime1 = binary_to_integer(proplists:get_value(<<"cdmi_mtime">>, Metadata1)),
    ?assert(Before =< CTime1),
    ?assert(CTime1 =< After),
    ?assert(CTime1 =< ATime1),
    ?assert(CTime1 =< MTime1),
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
    RequestHeaders2 =[?CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
    RequestBody11 = [{<<"metadata">>, [{<<"my_metadata">>, <<"my_dir_value">>}]}],
    RawRequestBody11 = json_utils:encode(RequestBody11),
    {ok, 201, _Headers11, Response11} = do_request(Worker, DirName, put, RequestHeaders2, RawRequestBody11),
    CdmiResponse11 = json_utils:decode(Response11),
    Metadata11 = proplists:get_value(<<"metadata">>, CdmiResponse11),
    ?assertEqual(<<"my_dir_value">>, proplists:get_value(<<"my_metadata">>, Metadata11)),

    %%------ update user metadata of a directory ----------
    RequestBody12 = [{<<"metadata">>, [{<<"my_metadata">>, <<"my_dir_value_update">>}]}],
    RawRequestBody12 = json_utils:encode(RequestBody12),
    {ok, 204, _, _} = do_request(Worker, DirName, put, RequestHeaders2, RawRequestBody12).
%%     {ok, 200, _Headers13, Response13} = do_request(Worker, DirName ++ "?metadata:my", get, RequestHeaders1, []), todo uncomment when cdmi get will be implemented
%%     CdmiResponse13 = json_utils:decode(Response13),
%%     ?assertEqual(1, length(CdmiResponse13)),
%%     Metadata13 = proplists:get_value(<<"metadata">>, CdmiResponse13),
%%     ?assertEqual(<<"my_dir_value_update">>, proplists:get_value(<<"my_metadata">>, Metadata13)),
%%     ?assertEqual(1, length(Metadata13)).
    %%------------------------------

% Tests cdmi object DELETE requests
delete_file_test(Config) ->
    FileName = "toDelete",
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, 1}, Config),
    GroupFileName = string:join(["spaces", binary_to_list(SpaceName),"groupFile"], "/"),

    %%----- basic delete -----------
    {ok, _} = create_file(Config, "/" ++ FileName),
    ?assert(object_exists(Config, FileName)),
    RequestHeaders1 = [?CDMI_VERSION_HEADER],
    {ok, Code1, _Headers1, _Response1} = do_request(Worker, FileName, delete, [?USER_1_TOKEN_HEADER | RequestHeaders1]),
    ?assertEqual(204,Code1),

    ?assert(not object_exists(Config, FileName)),
    %%------------------------------

    %%----- delete group file ------
    {ok, _} = create_file(Config, GroupFileName),
    ?assert(object_exists(Config, GroupFileName)),

    RequestHeaders2 = [?CDMI_VERSION_HEADER],
    {ok, Code2, _Headers2, _Response2} = do_request(Worker, GroupFileName, delete, [?USER_1_TOKEN_HEADER | RequestHeaders2]),
    ?assertEqual(204,Code2),

    ?assert(not object_exists(Config, GroupFileName)).
    %%------------------------------

% Tests file creation (cdmi object PUT), It can be done with cdmi header (when file data is provided as cdmi-object
% json string), or without (when we treat request body as new file content)
create_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, 1}, Config),
    ToCreate = "file.txt",
    ToCreate2 = filename:join(["spaces", binary_to_list(SpaceName), "file1.txt"]),
    ToCreate4 = "file2",
    FileContent = <<"File content!">>,

    %%-------- basic create --------
    ?assert(not object_exists(Config, ToCreate)),

    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
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

    RequestHeaders2 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
    RequestBody2 = [{<<"valuetransferencoding">>, <<"base64">>}, {<<"value">>, base64:encode(FileContent)}],
    RawRequestBody2 = json_utils:encode(RequestBody2),
    {ok, Code2, _Headers2, Response2} = do_request(Worker, ToCreate2, put, RequestHeaders2, RawRequestBody2),

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

    RequestHeaders4 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
    {ok, Code4, _Headers4, _Response4} = do_request(Worker, ToCreate4, put, RequestHeaders4, []),
    ?assertEqual(201, Code4),

    ?assert(object_exists(Config, ToCreate4)),
    ?assertEqual(<<>>, get_file_content(Config, ToCreate4)).
    %%------------------------------

% Tests cdmi object PUT requests (updating content)
update_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    TestDirName = "dir",
    TestFileName = "file.txt",
    TestFileContent = <<"test_file_content">>,
    FullName = filename:join(["/", TestDirName, TestFileName]),
    NewValue = <<"New Value!">>,
    UpdatedValue = <<"123 Value!">>,

    ok = mkdir(Config, TestDirName),
    ?assert(object_exists(Config, TestDirName)),
    {ok, _} = create_file(Config, FullName),
    ?assert(object_exists(Config, FullName)),
    {ok, _} = write_to_file(Config, FullName, TestFileContent, 0),
    ?assertEqual(TestFileContent, get_file_content(Config, FullName)),

    %%--- value replace, cdmi ------
    ?assert(object_exists(Config, FullName)),
    ?assertEqual(TestFileContent, get_file_content(Config, FullName)),

    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
    RequestBody1 = [{<<"value">>, NewValue}],
    RawRequestBody1 = json_utils:encode(RequestBody1),
    {ok, Code1, _Headers1, _Response1} = do_request(Worker, FullName, put, RequestHeaders1, RawRequestBody1),
    ?assertEqual(204, Code1),

    ?assert(object_exists(Config, FullName)),
    ?assertEqual(NewValue, get_file_content(Config, FullName)),
    %%------------------------------

    %%---- value update, cdmi ------
    UpdateValue = <<"123">>,
    RequestHeaders2 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
    RequestBody2 = [{<<"value">>, base64:encode(UpdateValue)}],
    RawRequestBody2 = json_utils:encode(RequestBody2),
    {ok, Code2, _Headers2, _Response2} = do_request(Worker, FullName ++ "?value:0-2", put, RequestHeaders2, RawRequestBody2),
    ?assertEqual(204, Code2),

    ?assert(object_exists(Config, FullName)),
    ?assertEqual(UpdatedValue, get_file_content(Config, FullName)).
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
    RequestHeaders = [?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],

    % when
    {ok, Code, _ResponseHeaders, _Response} = do_request(Worker, "/random", get, RequestHeaders),

    % then
    ?assertEqual(404, Code).

use_unsupported_cdmi_version(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    RequestHeaders = [{<<"X-CDMI-Specification-Version">>, <<"1.0.2">>}],

    % when
    {ok, Code, _ResponseHeaders, _Response} = do_request(Worker, "/random", get, RequestHeaders),

    % then
    ?assertEqual(400, Code).

% Tests dir creation (cdmi container PUT), remember that every container URI ends
% with '/'
create_dir_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    DirName = "toCreate/",
    DirName2 = "toCreate2/",
    MissingParentName="unknown/",
    DirWithoutParentName = filename:join(MissingParentName,"dir")++"/",

    %%------ non-cdmi create -------
    ?assert(not object_exists(Config, DirName)),

    {ok, Code1, _Headers1, _Response1} = do_request(Worker, DirName, put, [?USER_1_TOKEN_HEADER]),
    ?assertEqual(201,Code1),

    ?assert(object_exists(Config, DirName)),
    %%------------------------------

    %%------ basic create ----------
    ?assert(not object_exists(Config, DirName2)),

    RequestHeaders2 = [?USER_1_TOKEN_HEADER, ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER],
    {ok, Code2, _Headers2, Response2} = do_request(Worker, DirName2, put, RequestHeaders2, []),
    ?assertEqual(201,Code2),
    CdmiResponse2 = json_utils:decode(Response2),
    ?assertEqual(<<"application/cdmi-container">>, proplists:get_value(<<"objectType">>,CdmiResponse2)),
    ?assertEqual(list_to_binary(DirName2), proplists:get_value(<<"objectName">>,CdmiResponse2)),
    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>,CdmiResponse2)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>,CdmiResponse2)),
    ?assertEqual([], proplists:get_value(<<"children">>,CdmiResponse2)),
    ?assert(proplists:get_value(<<"metadata">>,CdmiResponse2) =/= <<>>),

    ?assert(object_exists(Config, DirName2)),
    %%------------------------------

    %%---------- update ------------
    ?assert(object_exists(Config, DirName)),

    RequestHeaders3 = [?USER_1_TOKEN_HEADER, ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER],
    {ok, Code3, _Headers3, _Response3} = do_request(Worker, DirName, put, RequestHeaders3, []),
    ?assertEqual(204,Code3),

    ?assert(object_exists(Config, DirName)),
    %%------------------------------

    %%----- missing parent ---------
    ?assert(not object_exists(Config, MissingParentName)),

    RequestHeaders4 = [?USER_1_TOKEN_HEADER, ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER],
    {ok, Code4, _Headers4, _Response4} = do_request(Worker, DirWithoutParentName, put, RequestHeaders4, []),
    ?assertEqual(500,Code4). %todo handle this error in lfm
    %%------------------------------

% tests access to file by objectid
objectid_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    DirName = "dir/",
    FileName = "file.txt",
    FilePath = filename:join(DirName, FileName),

    ok = mkdir(Config, DirName),
    {ok, _} = create_file(Config, FilePath),
    ?assert(object_exists(Config, FilePath)),

%%     %%-------- / objectid ----------
%%     RequestHeaders1 = [?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
%%     {ok, Code1, Headers1, Response1} = do_request(Worker, "", get, RequestHeaders1, []),
%%     ?assertEqual(200, Code1),
%%
%%     ?assertEqual(<<"application/cdmi-container">>, proplists:get_value(<<"content-type">>, Headers1)),
%%     CdmiResponse1 = json_utils:decode(Response1),
%%     ?assertEqual(<<"/">>, proplists:get_value(<<"objectName">>, CdmiResponse1)),
%%     RootId = proplists:get_value(<<"objectID">>, CdmiResponse1),
%%     ?assertNotEqual(RootId, undefined),
%%     ?assert(is_binary(RootId)),
%%     ?assertEqual(<<>>, proplists:get_value(<<"parentURI">>, CdmiResponse1)),
%%     ?assertEqual(undefined, proplists:get_value(<<"parentID">>, CdmiResponse1)),
%%     ?assertEqual(<<"cdmi_capabilities/container/">>, proplists:get_value(<<"capabilitiesURI">>, CdmiResponse1)),
%%     %%------------------------------
%%
%%     %%------ /dir objectid ---------
%%     RequestHeaders2 = [?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
%%     {ok, Code2, _Headers2, Response2} = do_request(Worker, DirName, get, RequestHeaders2, []),
%%     ?assertEqual(200, Code2),
%%
%%     CdmiResponse2 = json_utils:decode(Response2),
%%     ?assertEqual(<<"dir/">>, proplists:get_value(<<"objectName">>, CdmiResponse2)),
%%     DirId = proplists:get_value(<<"objectID">>, CdmiResponse2),
%%     ?assertNotEqual(DirId, undefined),
%%     ?assert(is_binary(DirId)),
%%     ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>, CdmiResponse2)),
%%     ?assertEqual(RootId, proplists:get_value(<<"parentID">>, CdmiResponse2)),
%%     ?assertEqual(<<"cdmi_capabilities/container/">>, proplists:get_value(<<"capabilitiesURI">>, CdmiResponse2)),
%%     %%------------------------------

    %%--- /dir/file.txt objectid ---
    RequestHeaders3 = [?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
    {ok, Code3, _Headers3, Response3} = do_request(Worker, FilePath, get, RequestHeaders3, []),
    ?assertEqual(200, Code3),

    CdmiResponse3 = json_utils:decode(Response3),
    ?assertEqual(<<"file.txt">>, proplists:get_value(<<"objectName">>, CdmiResponse3)),
    FileId = proplists:get_value(<<"objectID">>, CdmiResponse3),
    ?assertNotEqual(FileId, undefined),
    ?assert(is_binary(FileId)),
    ?assertEqual(<<"/dir/">>, proplists:get_value(<<"parentURI">>, CdmiResponse3)),
%%     ?assertEqual(DirId, proplists:get_value(<<"parentID">>, CdmiResponse3)),
    ?assertEqual(<<"cdmi_capabilities/dataobject/">>, proplists:get_value(<<"capabilitiesURI">>, CdmiResponse3)).
    %%------------------------------

%%     %%---- get / by objectid -------
%%     RequestHeaders4 = [?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
%%     {ok, Code4, _Headers4, Response4} = do_request(Worker, "cdmi_objectid/" ++ binary_to_list(RootId) ++ "/", get, RequestHeaders4, []),
%%     ?assertEqual(200, Code4),
%%
%%     CdmiResponse4 = json_utils:decode(Response4),
%%     ?assertEqual(proplists:delete(<<"metadata">>, CdmiResponse1), proplists:delete(<<"metadata">>, CdmiResponse4)), %should be the same as in 1 (except metadata)
%%     %%------------------------------
%%
%%     %%--- get /dir/ by objectid ----
%%     RequestHeaders5 = [?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
%%     {ok, Code5, _Headers5, Response5} = do_request(Worker, "cdmi_objectid/" ++ binary_to_list(DirId) ++ "/", get, RequestHeaders5, []),
%%     ?assertEqual(200, Code5),
%%
%%     CdmiResponse5 = json_utils:decode(Response5),
%%     ?assertEqual(proplists:delete(<<"metadata">>, CdmiResponse2), proplists:delete(<<"metadata">>, CdmiResponse5)), %should be the same as in 2 (except metadata)
%%     %%------------------------------

%%     %% get /dir/file.txt by objectid
%%     RequestHeaders6 = [?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
%%     {ok, Code6, _Headers6, Response6} = do_request(Worker, "cdmi_objectid/" ++ binary_to_list(DirId) ++ "/file.txt", get, RequestHeaders6, []),
%%     ?assertEqual(200, Code6),
%%     CdmiResponse6 = json_utils:decode(Response6),
%%
%%     ?assertEqual(proplists:delete(<<"metadata">>, CdmiResponse3), proplists:delete(<<"metadata">>, CdmiResponse6)), %should be the same as in 3 (except metadata)
%%
%%     {ok, Code7, _Headers7, Response7} = do_request(Worker, "cdmi_objectid/" ++ binary_to_list(FileId), get, RequestHeaders6, []),
%%     ?assertEqual(200, Code7),
%%
%%     CdmiResponse7 = json_utils:decode(Response7),
%%     ?assertEqual(proplists:delete(<<"metadata">>, CdmiResponse7), proplists:delete(<<"metadata">>, CdmiResponse6)). %should be the same as in 3 (except metadata)
%%     %%------------------------------

% tests if capabilities of objects, containers, and whole storage system are set properly
capabilities_test(Config) ->
%%   todo uncomment tests with IDs
    [Worker | _] = ?config(op_worker_nodes, Config),

    %%--- system capabilities ------
    RequestHeaders8 = [?CDMI_VERSION_HEADER],
    {ok, Code8, Headers8, Response8} = do_request(Worker, "cdmi_capabilities/", get, RequestHeaders8, []),
    ?assertEqual(200, Code8),

    ?assertEqual(<<"application/cdmi-capability">>, proplists:get_value(<<"content-type">>, Headers8)),
    CdmiResponse8 = json_utils:decode(Response8),
%%   ?assertEqual(?root_capability_id, proplists:get_value(<<"objectID">>,CdmiResponse8)),
    ?assertEqual(?root_capability_path, proplists:get_value(<<"objectName">>, CdmiResponse8)),
    ?assertEqual(<<"0-1">>, proplists:get_value(<<"childrenrange">>, CdmiResponse8)),
    ?assertEqual([<<"container/">>, <<"dataobject/">>], proplists:get_value(<<"children">>, CdmiResponse8)),
    Capabilities = proplists:get_value(<<"capabilities">>, CdmiResponse8),
    ?assertEqual(?root_capability_list, Capabilities),
    %%------------------------------

    %%-- container capabilities ----
    RequestHeaders9 = [?CDMI_VERSION_HEADER],
    {ok, Code9, _Headers9, Response9} = do_request(Worker, "cdmi_capabilities/container/", get, RequestHeaders9, []),
    ?assertEqual(200, Code9),
%%   ?assertMatch({ok, Code9, _, Response9},do_request("cdmi_objectid/"++binary_to_list(?container_capability_id)++"/", get, RequestHeaders9, [])),

    CdmiResponse9 = json_utils:decode(Response9),
    ?assertEqual(?root_capability_path, proplists:get_value(<<"parentURI">>, CdmiResponse9)),
%%   ?assertEqual(?root_capability_id, proplists:get_value(<<"parentID">>,CdmiResponse9)),
%%   ?assertEqual(?container_capability_id, proplists:get_value(<<"objectID">>,CdmiResponse9)),
    ?assertEqual(<<"container/">>, proplists:get_value(<<"objectName">>, CdmiResponse9)),
    Capabilities2 = proplists:get_value(<<"capabilities">>, CdmiResponse9),
    ?assertEqual(?container_capability_list, Capabilities2),
    %%------------------------------

    %%-- dataobject capabilities ---
    RequestHeaders10 = [?CDMI_VERSION_HEADER],
    {ok, Code10, _Headers10, Response10} = do_request(Worker, "cdmi_capabilities/dataobject/", get, RequestHeaders10, []),
    ?assertEqual(200, Code10),
%%   ?assertMatch({ok, Code10, _, Response10},do_request("cdmi_objectid/"++binary_to_list(?dataobject_capability_id)++"/", get, RequestHeaders10, [])),

    CdmiResponse10 = json_utils:decode(Response10),
    ?assertEqual(?root_capability_path, proplists:get_value(<<"parentURI">>, CdmiResponse10)),
%%   ?assertEqual(?root_capability_id, proplists:get_value(<<"parentID">>,CdmiResponse10)),
%%   ?assertEqual(?dataobject_capability_id, proplists:get_value(<<"objectID">>,CdmiResponse10)),
    ?assertEqual(<<"dataobject/">>, proplists:get_value(<<"objectName">>, CdmiResponse10)),
    Capabilities3 = proplists:get_value(<<"capabilities">>, CdmiResponse10),
    ?assertEqual(?dataobject_capability_list, Capabilities3).
%%------------------------------

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
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
    initializer:clean_test_users_and_spaces(Config),
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
                {ok, P} = test_utils:get_env(Node, ?APP_NAME, http_worker_rest_port),
                PStr = integer_to_list(P),
                put(port, PStr),
                PStr;
            P -> P
        end,
    string:join(["https://", utils:get_host(Node), ":", Port, "/cdmi/"], "").

mock_user_auth(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, identity),
    test_utils:mock_expect(Workers, identity, get_or_fetch,
        fun
            (#auth{macaroon = Token}) when size(Token) == 1 ->
                UserId = ?config({user_id, binary_to_integer(Token)}, Config),
                {ok, #document{value = #identity{user_id = UserId}}};
            (Auth) ->
                meck:passthrough(Auth)
        end
    ).

unmock_user_auth(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate_and_unload(Workers, identity).

object_exists(Config, Path) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),

    case lfm_proxy:stat(Worker, SessionId, {path, absolute_binary_path(Path)}) of
        {ok, _} ->
            true;
        {error, ?ENOENT} ->
            false
    end.

create_file(Config, Path) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),
    lfm_proxy:create(Worker, SessionId, absolute_binary_path(Path), ?FILE_PERMISSIONS).

open_file(Config, Path, OpenMode) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),
    lfm_proxy:open(Worker, SessionId, {path, absolute_binary_path(Path)}, OpenMode).

write_to_file(Config, Path, Data, Offset) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, FileHandle} = open_file(Config, Path, write),
    lfm_proxy:write(Worker, FileHandle, Offset, Data).

get_file_content(Config, Path) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    {ok, FileHandle} = open_file(Config, Path, write),
    case lfm_proxy:read(Worker, FileHandle, ?FILE_BEGINNING, ?INFINITY) of
        {error, Error} -> {error, Error};
        {ok, Content} -> Content
    end.

mkdir(Config, Path) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),
    lfm_proxy:mkdir(Worker, SessionId, absolute_binary_path(Path)).

absolute_binary_path(Path) ->
    list_to_binary(ensure_begins_with_slash(Path)).

ensure_begins_with_slash(Path) ->
    ReversedBinary = list_to_binary(lists:reverse(Path)),
    lists:reverse(binary_to_list(str_utils:ensure_ends_with_slash(ReversedBinary))).

% Returns current time in seconds
now_in_secs() ->
    {MegaSecs, Secs, _MicroSecs} = erlang:now(),
    MegaSecs * 1000000 + Secs.