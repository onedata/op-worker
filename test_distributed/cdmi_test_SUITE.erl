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

-export([list_dir_test/1, get_file_test/1, delete_file_test/1, choose_adequate_handler/1,
    use_supported_cdmi_version/1, use_unsupported_cdmi_version/1,
    create_dir_test/1, capabilities_test/1, create_file_test/1,
    update_file_test/1]).

-performance({test_cases, []}).
all() ->
    [
        list_dir_test, get_file_test, create_file_test, update_file_test,
        delete_file_test, choose_adequate_handler, use_supported_cdmi_version,
        use_unsupported_cdmi_version, create_dir_test, capabilities_test
    ].

-define(MACAROON, "macaroon").
-define(TIMEOUT, timer:seconds(5)).

-define(USER_1_TOKEN_HEADER, {"X-Auth-Token", "1"}).
-define(CDMI_VERSION_HEADER, {"X-CDMI-Specification-Version", "1.1.1"}).
-define(CONTAINER_CONTENT_TYPE_HEADER,
    {"content-type", "application/cdmi-container"}).

-define(TEST_DIR_NAME, "dir").
-define(TEST_FILE_NAME, "file.txt").
-define(TEST_FILE_CONTENT, <<"test_file_content">>).

-define(FILE_PERMISSIONS, 8#664).

-define(FILE_BEGINNING, 0).


%%%===================================================================
%%% Test functions
%%%===================================================================

% Tests cdmi container GET request (also refered as LIST)
list_dir_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    create_dir(Config, ?TEST_DIR_NAME ++ "/"),
    ?assertEqual(true, object_exists(Config, ?TEST_DIR_NAME ++ "/")),
    create_file(Config, string:join([?TEST_DIR_NAME, ?TEST_FILE_NAME],"/")),
    ?assertEqual(true,
        object_exists(Config, string:join([?TEST_DIR_NAME, ?TEST_FILE_NAME],"/"))),

    %%------ list basic dir --------
    tracer:start(Worker),
    tracer:trace_calls(cdmi_container_handler, get_cdmi),
    tracer:trace_calls(cdmi_container_answer, prepare),
    tracer:trace_calls(json, encode),
    {ok, Code1, Headers1, Response1} =
        do_request(Worker, ?TEST_DIR_NAME++"/", get,
            [?USER_1_TOKEN_HEADER, ?CDMI_VERSION_HEADER], []),
    tracer:stop(),

    ?assertEqual("200", Code1),
    ?assertEqual(proplists:get_value("content-type", Headers1),
        "application/cdmi-container"),
    {struct, CdmiResponse1} = mochijson2:decode(Response1),
    ?assertEqual(<<"application/cdmi-container">>,
        proplists:get_value(<<"objectType">>,CdmiResponse1)),
    ?assertEqual(<<"dir/">>,
        proplists:get_value(<<"objectName">>,CdmiResponse1)),
    ?assertEqual(<<"Complete">>,
        proplists:get_value(<<"completionStatus">>,CdmiResponse1)),
    ?assertEqual([<<"file.txt">>],
        proplists:get_value(<<"children">>,CdmiResponse1)),
    ?assert(proplists:get_value(<<"metadata">>,CdmiResponse1) =/= <<>>),
    %%------------------------------

    %%------ list root dir ---------
    {ok, Code2, _Headers2, Response2} =
        do_request(Worker, [], get,
            [?USER_1_TOKEN_HEADER, ?CDMI_VERSION_HEADER ], []),
    ?assertEqual("200", Code2),
    {struct,CdmiResponse2} = mochijson2:decode(Response2),
    ?assertEqual(<<"/">>, proplists:get_value(<<"objectName">>,CdmiResponse2)),
    ?assertEqual([<<"dir/">>,<<"spaces/">>],
        proplists:get_value(<<"children">>,CdmiResponse2)),
    %%------------------------------

    %%--- list nonexisting dir -----
    {ok, Code3, _Headers3, _Response3} =
        do_request(Worker, "nonexisting_dir/",
            get, [?USER_1_TOKEN_HEADER, ?CDMI_VERSION_HEADER], []),
    ?assertEqual("404",Code3),
    %%------------------------------

    %%-- selective params list -----
    {ok, Code4, _Headers4, Response4} =
        do_request(Worker, ?TEST_DIR_NAME ++ "/?children;objectName",
            get, [?USER_1_TOKEN_HEADER, ?CDMI_VERSION_HEADER ], []),
    ?assertEqual("200", Code4),
    {struct,CdmiResponse4} = mochijson2:decode(Response4),
    ?assertEqual(<<"dir/">>,
        proplists:get_value(<<"objectName">>,CdmiResponse4)),
    ?assertEqual([<<"file.txt">>],
        proplists:get_value(<<"children">>,CdmiResponse4)),
    ?assertEqual(2,length(CdmiResponse4)),
    %%------------------------------

    %%---- childrenrange list ------
    ChildrangeDir = "childrange/",
    create_dir(Worker, ChildrangeDir),
    Childs = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",
              "12", "13", "14"],
    ChildsBinaries = lists:map(fun(X) -> list_to_binary(X) end, Childs),
    lists:map(fun(FileName) ->
                create_file(Worker, filename:join(ChildrangeDir, FileName))
              end, Childs),

    {ok, Code5, _Headers5, Response5} =
        do_request(Worker, ChildrangeDir ++ "?children;childrenrange",
            get, [?USER_1_TOKEN_HEADER, ?CDMI_VERSION_HEADER ], []),
    ?assertEqual("200", Code5),
    {struct,CdmiResponse5} = mochijson2:decode(Response5),
    ChildrenResponse1 = proplists:get_value(<<"children">>, CdmiResponse5),
    ?assert(is_list(ChildrenResponse1)),
    lists:foreach(fun(Name) ->
                    ?assert(lists:member(Name, ChildrenResponse1))
                  end, ChildsBinaries),
    ?assertEqual(<<"0-14">>,
        proplists:get_value(<<"childrenrange">>, CdmiResponse5)),

    {ok, Code6, _, Response6} =
        do_request(Worker, ChildrangeDir ++ "?children:2-13;childrenrange", get,
            [?USER_1_TOKEN_HEADER, ?CDMI_VERSION_HEADER ], []),
    {ok, Code7, _, Response7} =
        do_request(Worker, ChildrangeDir ++ "?children:0-1;childrenrange", get,
            [?USER_1_TOKEN_HEADER, ?CDMI_VERSION_HEADER ], []),
    {ok, Code8, _, Response8} =
        do_request(Worker, ChildrangeDir ++ "?children:14-14;childrenrange", get,
            [?USER_1_TOKEN_HEADER, ?CDMI_VERSION_HEADER ], []),
    ?assertEqual("200", Code6),
    ?assertEqual("200", Code7),
    ?assertEqual("200", Code8),
    {struct,CdmiResponse6} = mochijson2:decode(Response6),
    {struct,CdmiResponse7} = mochijson2:decode(Response7),
    {struct,CdmiResponse8} = mochijson2:decode(Response8),
    ChildrenResponse6 = proplists:get_value(<<"children">>,CdmiResponse6),
    ChildrenResponse7 = proplists:get_value(<<"children">>,CdmiResponse7),
    ChildrenResponse8 = proplists:get_value(<<"children">>,CdmiResponse8),

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
    Size = string:len(binary_to_list(FileContent)),
    [Worker | _] = ?config(op_worker_nodes, Config),

    create_file(Config, FileName),
    ?assert(object_exists(Config, FileName)),
    write_to_file(Config, FileName,FileContent, ?FILE_BEGINNING),
    ?assertEqual(FileContent,
        get_file_content(Config, FileName, Size, ?FILE_BEGINNING)),

    %%-------- basic read ----------
    RequestHeaders1 = [?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
    {ok, Code1, _Headers1, Response1} = do_request(Worker, FileName, get, RequestHeaders1, []),
    ?assertEqual("200", Code1),
    {struct, CdmiResponse1} = mochijson2:decode(Response1),

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
    ?assertEqual("200", Code2),
    {struct, CdmiResponse2} = mochijson2:decode(Response2),

    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>, CdmiResponse2)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>, CdmiResponse2)),
    ?assertEqual(2, length(CdmiResponse2)),
    %%------------------------------

    %%--- selective value read -----
    RequestHeaders3 = [?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],
    {ok, Code3, _Headers3, Response3} = do_request(Worker, FileName ++ "?value:1-3;valuerange", get, RequestHeaders3, []),
    ?assertEqual("200", Code3),
    {struct, CdmiResponse3} = mochijson2:decode(Response3),

    ?assertEqual(<<"1-3">>, proplists:get_value(<<"valuerange">>, CdmiResponse3)),
    ?assertEqual(<<"ome">>, base64:decode(proplists:get_value(<<"value">>, CdmiResponse3))), % 1-3 from FileContent = <<"Some content...">>
    %%------------------------------

    %%------- noncdmi read --------

    {ok, Code4, Headers4, Response4} =
        do_request(Worker, FileName, get, [?USER_1_TOKEN_HEADER]),
    ?assertEqual("200",Code4),

    ?assertEqual(binary_to_list(<<"application/octet-stream">>), proplists:get_value("content-type",Headers4)),
    ?assertEqual(binary_to_list(FileContent), Response4),
    %%------------------------------

    %% selective value read non-cdmi
    RequestHeaders7 = [{"Range","1-3,5-5,-3"}],
    {ok, Code7, _Headers7, Response7} =
        do_request(Worker, FileName, get, [?USER_1_TOKEN_HEADER | RequestHeaders7]),
    ?assertEqual("206",Code7),
    ?assertEqual("omec...", Response7), % 1-3,5-5,12-14  from FileContent = <<"Some content...">>
    %%------------------------------

    %% selective value read non-cdmi error
    RequestHeaders8 = [{"Range","1-3,6-4,-3"}],
    {ok, Code8, _Headers8, _Response8} =
        do_request(Worker, FileName, get, [?USER_1_TOKEN_HEADER | RequestHeaders8]),
    ?assertEqual("400",Code8).
    %%------------------------------

% Tests file creation (cdmi object PUT), It can be done with cdmi header
% (when file data is provided as cdmi-object
% json string), or without (when we treat request body as new file content)
create_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, 1}, Config),
    GroupFileName =
        string:join(["spaces", binary_to_list(SpaceName),"groupFile"], "/"),
    ToCreate = "file.txt",
    ToCreate2 = filename:join(["spaces", GroupFileName, "file1.txt"]),
    ToCreate4 = "file2",
    ToCreate5 = "file3",
    FileContent = <<"File content!">>,
    Size = string:len(binary_to_list(FileContent)),

    %%------ create noncdmi --------
    ?assert(not object_exists(Config, ToCreate5)),

    RequestHeaders5 = [{"content-type", "application/binary"}],
    tracer:start(Worker),
    tracer:trace_calls(cdmi_object_handler),
    {ok, Code5, _Headers5, _Response5} =
        do_request(Worker, ToCreate5, put,
            [?USER_1_TOKEN_HEADER | RequestHeaders5], FileContent),
    tracer:stop(),

    ?assertEqual("201",Code5),

    ?assert(object_exists(Config, ToCreate5)),
    ?assertEqual(FileContent,
        get_file_content(Config, ToCreate5, Size, ?FILE_BEGINNING)).
%%------------------------------

% Tests cdmi object PUT requests (updating content)
update_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    FullName = filename:join(["/",?TEST_DIR_NAME,?TEST_FILE_NAME]),
    NewValue = <<"New Value!">>,
    UpdatedValue = <<"123 Value!">>,
    TestSize = get_content_size(?TEST_FILE_CONTENT),
    NewSize = get_content_size(NewValue),
    UpdatedSize = get_content_size(UpdatedValue),

    %% TODO below create and write are for compliance with testcases for cdmi
    %% TODO delete after adding tests for cdmi cases
    create_file(Config, FullName),
    ?assert(object_exists(Config, FullName)),
    write_to_file(Config, FullName, UpdatedValue, ?FILE_BEGINNING),
    ?assertEqual(UpdatedValue,
        get_file_content(Config, FullName, UpdatedSize, ?FILE_BEGINNING)),

    %%--- value replace, http ------
    RequestBody3 = ?TEST_FILE_CONTENT,
    {ok, Code3, _Headers3, _Response3} =
        do_request(Worker, FullName, put, [?USER_1_TOKEN_HEADER ], RequestBody3),
    ?assertEqual("204",Code3),

    ?assert(object_exists(Config, FullName)),
    ?assertEqual(?TEST_FILE_CONTENT,
        get_file_content(Config, FullName, TestSize, ?FILE_BEGINNING)),
    %%------------------------------

    %%---- value update, http ------
    UpdateValue = <<"123">>,
    RequestHeaders4 = [{"content-range", "0-2"}],
    {ok, Code4, _Headers4, _Response4} =
        do_request(Worker, FullName,
            put, [?USER_1_TOKEN_HEADER |RequestHeaders4], UpdateValue),
    ?assertEqual("204",Code4),

    ?assert(object_exists(Config, FullName)),
    ?assertEqual(<<"123t_file_content">>,
        get_file_content(
            Config, FullName, get_content_size(UpdateValue), ?FILE_BEGINNING)),
    %%------------------------------

    %%---- value update, http error ------
    UpdateValue = <<"123">>,
    RequestHeaders5 = [{"content-range", "0-2,3-4"}],
    {ok, Code5, _Headers5, _Response5} =
        do_request(Worker, FullName, put, RequestHeaders5, UpdateValue),
    ?assertEqual("400",Code5),

    ?assert(object_exists(Config, FullName)),
    ?assertEqual(<<"123t_file_content">>,
        get_file_content(
            Config, FullName, get_content_size(UpdateValue), ?FILE_BEGINNING)).

    %%------------------------------

% Tests cdmi object DELETE requests
delete_file_test(Config) ->
    FileName = "toDelete",
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, 1}, Config),
    GroupFileName =
        string:join(["spaces", binary_to_list(SpaceName),"groupFile"], "/"),

    %%----- basic delete -----------
    create_file(Config, "/" ++ FileName),
    ?assert(object_exists(Config, FileName)),
    RequestHeaders1 = [?CDMI_VERSION_HEADER],
    {ok, Code1, _Headers1, _Response1} =
        do_request(
            Worker, FileName, delete, [?USER_1_TOKEN_HEADER | RequestHeaders1]),
    ?assertEqual("204",Code1),

    ?assert(not object_exists(Config, FileName)),
    %%------------------------------

    %%----- delete group file ------
    create_file(Config, GroupFileName),
    ?assert(object_exists(Config, GroupFileName)),

    RequestHeaders2 = [?CDMI_VERSION_HEADER],
    {ok, Code2, _Headers2, _Response2} =
        do_request(Worker, GroupFileName, delete,
                   [?USER_1_TOKEN_HEADER | RequestHeaders2]),
    ?assertEqual("204",Code2),

    ?assert(not object_exists(Config, GroupFileName)).
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
    {ok, Code, _ResponseHeaders, _Response} =
        do_request(Worker, "/random", get, RequestHeaders),

    % then
    ?assertEqual("404", Code).

use_unsupported_cdmi_version(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    RequestHeaders = [{"X-CDMI-Specification-Version", "1.0.2"}],

    % when
    {ok, Code, _ResponseHeaders, _Response} =
        do_request(Worker, "/random", get, RequestHeaders),

    % then
    ?assertEqual("400", Code).

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

    {ok, Code1, _Headers1, _Response1} =
        do_request(Worker, DirName, put, [?USER_1_TOKEN_HEADER]),
    ?assertEqual("201",Code1),

    ?assert(object_exists(Config, DirName)),
    %%------------------------------

    %%------ basic create ----------
    ?assert(not object_exists(Config, DirName2)),

    RequestHeaders2 = [?USER_1_TOKEN_HEADER, ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER],
    {ok, Code2, _Headers2, Response2} = do_request(Worker, DirName2, put, RequestHeaders2, []),
    ?assertEqual("201",Code2),
    {struct,CdmiResponse2} = mochijson2:decode(Response2),
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

    RequestHeaders3 = [
        ?USER_1_TOKEN_HEADER, ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, Code3, _Headers3, _Response3} =
        do_request(Worker, DirName, put, RequestHeaders3, []),
    ?assertEqual("204",Code3),

    ?assert(object_exists(Config, DirName)),
    %%------------------------------

    %%----- missing parent ---------
    ?assert(not object_exists(Config, MissingParentName)),

    RequestHeaders4 = [?USER_1_TOKEN_HEADER, ?CDMI_VERSION_HEADER, ?CONTAINER_CONTENT_TYPE_HEADER],
    {ok, Code4, _Headers4, _Response4} = do_request(Worker, DirWithoutParentName, put, RequestHeaders4, []),
    ?assertEqual("500",Code4). %todo handle this error in lfm
    %%------------------------------

% tests if capabilities of objects, containers, and whole storage system are set properly
capabilities_test(Config) ->
%%   todo uncomment tests with IDs
    [Worker | _] = ?config(op_worker_nodes, Config),

    %%--- system capabilities ------
    RequestHeaders8 = [{"X-CDMI-Specification-Version", "1.1.1"}],
    {ok, Code8, Headers8, Response8} = do_request(Worker, "cdmi_capabilities/",
        get, RequestHeaders8, []),
    ?assertEqual("200", Code8),

    ?assertEqual("application/cdmi-capability",
        proplists:get_value("content-type", Headers8)),
    CdmiResponse8 = mochijson2:decode(Response8),
%%   ?assertEqual(?root_capability_id, proplists:get_value(<<"objectID">>,CdmiResponse8)),
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
    RequestHeaders9 = [{"X-CDMI-Specification-Version", "1.1.1"}],
    {ok, Code9, _Headers9, Response9} =
        do_request(
            Worker, "cdmi_capabilities/container/", get, RequestHeaders9, []),
    ?assertEqual("200", Code9),
%%   ?assertMatch({Code9, _, Response9},do_request("cdmi_objectid/"++binary_to_list(?container_capability_id)++"/", get, RequestHeaders9, [])),

    CdmiResponse9 = mochijson2:decode(Response9),
    ?assertEqual(?root_capability_path,
        proplists:get_value(<<"parentURI">>, CdmiResponse9)),
%%   ?assertEqual(?root_capability_id, proplists:get_value(<<"parentID">>,CdmiResponse9)),
%%   ?assertEqual(?container_capability_id, proplists:get_value(<<"objectID">>,CdmiResponse9)),
    ?assertEqual(<<"container/">>,
        proplists:get_value(<<"objectName">>, CdmiResponse9)),
    Capabilities2 = proplists:get_value(<<"capabilities">>, CdmiResponse9),
    ?assertEqual(?container_capability_list, Capabilities2),
    %%------------------------------

    %%-- dataobject capabilities ---
    RequestHeaders10 = [{"X-CDMI-Specification-Version", "1.1.1"}],
    {ok, Code10, _Headers10, Response10} =
        do_request(Worker, "cdmi_capabilities/dataobject/", get, RequestHeaders10, []),
    ?assertEqual("200", Code10),
%%   ?assertMatch({Code10, _, Response10},do_request("cdmi_objectid/"++binary_to_list(?dataobject_capability_id)++"/", get, RequestHeaders10, [])),

    CdmiResponse10 = mochijson2:decode(Response10),
    ?assertEqual(?root_capability_path,
        proplists:get_value(<<"parentURI">>, CdmiResponse10)),
%%   ?assertEqual(?root_capability_id, proplists:get_value(<<"parentID">>,CdmiResponse10)),
%%   ?assertEqual(?dataobject_capability_id, proplists:get_value(<<"objectID">>,CdmiResponse10)),
    ?assertEqual(<<"dataobject/">>,
        proplists:get_value(<<"objectName">>, CdmiResponse10)),
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
    ssl:start(),
    ibrowse:start(),
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
    ibrowse:stop(),
    ssl:stop().

%%%===================================================================
%%% Internal functions
%%%===================================================================

% Performs a single request using ibrowse
do_request(Node, RestSubpath, Method, Headers) ->
    do_request(Node, RestSubpath, Method, Headers, []).

do_request(Node, RestSubpath, Method, Headers, Body) ->
    ibrowse:send_req(
            cdmi_endpoint(Node) ++ RestSubpath,
            Headers,
            Method,
            Body,
            [{ssl_options, [{reuse_sessions, false}]}]
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
    test_utils:mock_validate(Workers, identity),
    test_utils:mock_unload(Workers, identity).

object_exists(Config, Path) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),

    case lfm_proxy:stat(Worker, SessionId,
            {path, utils:ensure_unicode_binary("/" ++ Path)}) of
        {ok, _} ->
            true;
        {error, ?ENOENT} ->
            false
    end.

create_file(Config, Path) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),

    case lfm_proxy:create(Worker, SessionId,
            utils:ensure_unicode_binary("/" ++ Path), ?FILE_PERMISSIONS) of
        {ok, UUID} -> UUID;
        {error, Code} -> {error, Code}
    end.

open_file(Config, Path, OpenMode) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),

    case lfm_proxy:open(Worker, SessionId,
            {path, utils:ensure_unicode_binary("/" ++ Path)}, OpenMode) of
        {error, Error} -> {error, Error};
        FileHandle -> FileHandle
    end.

write_to_file(Config, Path, Data, Offset) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {ok, FileHandle} = open_file(Config, Path, write),
    case lfm_proxy:write(Worker, FileHandle, Offset, Data) of
        {error, Error} -> {error, Error};
        {ok, Bytes} -> Bytes
    end.

get_file_content(Config, Path, Size, Offset) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    {ok, FileHandle} = open_file(Config, Path, write),
    case lfm_proxy:read(Worker, FileHandle, Offset, Size) of
        {error, Error} -> {error, Error};
        {ok, Content} -> Content
    end.

get_content_size(Content) ->
    string:len(binary_to_list(Content)).

create_dir(Config, Path) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),
    lfm_proxy:mkdir(Worker, SessionId, utils:ensure_unicode_binary("/" ++ Path)).
        