%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This file contains cdmi protocol tests
%% @end
%% ===================================================================

-module(cdmi_test_SUITE).
-include("test_utils.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/control_panel/cdmi_capabilities.hrl").

-define(SH, "DirectIO").
-define(Test_dir_name, "dir").
-define(Test_file_name, "file.txt").
-define(Test_file_content, <<"test_file_content">>).

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([list_dir_test/1, get_file_test/1, metadata_test/1, create_dir_test/1, create_file_test/1, update_file_test/1,
    delete_dir_test/1, delete_file_test/1, version_test/1, request_format_check_test/1, objectid_and_capabilities_test/1,
    mimetype_and_encoding_test/1, moved_pemanently_test/1, errors_test/1, token_test/1, out_of_range_test/1]).


all() -> [list_dir_test, get_file_test, metadata_test, create_dir_test, create_file_test, update_file_test,
    delete_dir_test, delete_file_test, version_test, request_format_check_test, objectid_and_capabilities_test,
    mimetype_and_encoding_test, moved_pemanently_test, errors_test, token_test, out_of_range_test].

%% ====================================================================
%% Test functions
%% ====================================================================

% Tests cdmi container GET request (also refered as LIST)
list_dir_test(_Config) ->

    %%------ list basic dir --------
    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code1, Headers1, Response1} = do_request(?Test_dir_name++"/", get, RequestHeaders1, []),
    ?assertEqual("200", Code1),
    ?assertEqual(proplists:get_value("content-type", Headers1), "application/cdmi-container"),
    {struct,CdmiResponse1} = mochijson2:decode(Response1),
    ?assertEqual(<<"application/cdmi-container">>, proplists:get_value(<<"objectType">>,CdmiResponse1)),
    ?assertEqual(<<"dir/">>, proplists:get_value(<<"objectName">>,CdmiResponse1)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>,CdmiResponse1)),
    ?assertEqual([<<"file.txt">>], proplists:get_value(<<"children">>,CdmiResponse1)),
    ?assert(proplists:get_value(<<"metadata">>,CdmiResponse1) =/= <<>>),
    %%------------------------------

    %%------ list root dir ---------
    RequestHeaders2 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code2, _Headers2, Response2} = do_request([], get, RequestHeaders2, []),
    ?assertEqual("200", Code2),
    {struct,CdmiResponse2} = mochijson2:decode(Response2),
    ?assertEqual(<<"/">>, proplists:get_value(<<"objectName">>,CdmiResponse2)),
    ?assertEqual([<<"dir/">>,<<"spaces/">>], proplists:get_value(<<"children">>,CdmiResponse2)),
    %%------------------------------

    %%--- list nonexisting dir -----
    RequestHeaders3 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code3, _Headers3, _Response3} = do_request("nonexisting_dir/", get, RequestHeaders3, []),
    ?assertEqual("404",Code3),
    %%------------------------------

    %%-- selective params list -----
    RequestHeaders4 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code4, _Headers4, Response4} = do_request(?Test_dir_name ++ "/?children;objectName", get, RequestHeaders4, []),
    ?assertEqual("200", Code4),
    {struct,CdmiResponse4} = mochijson2:decode(Response4),
    ?assertEqual(<<"dir/">>, proplists:get_value(<<"objectName">>,CdmiResponse4)),
    ?assertEqual([<<"file.txt">>], proplists:get_value(<<"children">>,CdmiResponse4)),
    ?assertEqual(2,length(CdmiResponse4)).
    %%------------------------------

% Tests cdmi object GET request. Request can be done without cdmi header (in that case
% file conent is returned as response body), or with cdmi header (the response
% contains json string of type: application/cdmi-object, and we can specify what
% parameters we need by listing then as ';' separated list after '?' in URL ),
%  )
get_file_test(_Config) ->
    FileName = "/toRead.txt",
    FileContent = <<"Some content...">>,

    create_file(FileName),
    ?assert(object_exists(FileName)),
    write_to_file(FileName,FileContent),
    ?assertEqual(FileContent,get_file_content(FileName)),

    %%-------- basic read ----------
    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code1, _Headers1, Response1} = do_request(FileName, get, RequestHeaders1, []),
    ?assertEqual("200",Code1),
    {struct,CdmiResponse1} = mochijson2:decode(Response1),

    ?assertEqual(<<"application/cdmi-object">>, proplists:get_value(<<"objectType">>,CdmiResponse1)),
    ?assertEqual(<<"toRead.txt">>, proplists:get_value(<<"objectName">>,CdmiResponse1)),
    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>,CdmiResponse1)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>,CdmiResponse1)),
    ?assertEqual(<<"base64">>, proplists:get_value(<<"valuetransferencoding">>,CdmiResponse1)),
    ?assertEqual(<<"application/octet-stream">>, proplists:get_value(<<"mimetype">>,CdmiResponse1)),
    ?assertEqual(<<"0-14">>, proplists:get_value(<<"valuerange">>,CdmiResponse1)),
    ?assert(proplists:get_value(<<"metadata">>,CdmiResponse1) =/= <<>>),
    ?assertEqual(FileContent, base64:decode(proplists:get_value(<<"value">>,CdmiResponse1))),
    %%------------------------------

    %%-- selective params read -----
    RequestHeaders2 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code2, _Headers2, Response2} = do_request(FileName++"?parentURI;completionStatus", get, RequestHeaders2, []),
    ?assertEqual("200",Code2),
    {struct,CdmiResponse2} = mochijson2:decode(Response2),

    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>,CdmiResponse2)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>,CdmiResponse2)),
    ?assertEqual(2, length(CdmiResponse2)),
    %%------------------------------

    %%--- selective value read -----
    RequestHeaders3 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code3, _Headers3, Response3} = do_request(FileName++"?value:1-3;valuerange", get, RequestHeaders3, []),
    ?assertEqual("200",Code3),
    {struct,CdmiResponse3} = mochijson2:decode(Response3),

    ?assertEqual(<<"1-3">>, proplists:get_value(<<"valuerange">>,CdmiResponse3)),
    ?assertEqual(<<"ome">>, base64:decode(proplists:get_value(<<"value">>,CdmiResponse3))), % 1-3 from FileContent = <<"Some content...">>
    %%------------------------------

    %%------- noncdmi read --------
    {Code4, Headers4, Response4} = do_request(FileName, get, [], []),
    ?assertEqual("200",Code4),

    ?assertEqual("application/octet-stream", proplists:get_value("content-type",Headers4)),
    ?assertEqual(binary_to_list(FileContent), Response4),
    %%------------------------------

    %%------- objectid read --------
    RequestHeaders5 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code5, _Headers5, Response5} = do_request(FileName ++ "?objectID", get, RequestHeaders5, []),
    ?assertEqual("200",Code5),
    {struct,CdmiResponse5} = mochijson2:decode(Response5),
    ObjectID = proplists:get_value(<<"objectID">>,CdmiResponse5),
    ?assert(is_binary(ObjectID)),
    %%------------------------------

    %%-------- read by id ----------
    RequestHeaders6 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code6, _Headers6, Response6} = do_request("cdmi_objectid/"++binary_to_list(ObjectID), get, RequestHeaders6, []),
    ?assertEqual("200",Code6),
    {struct,CdmiResponse6} = mochijson2:decode(Response6),
    ?assertEqual(FileContent,base64:decode(proplists:get_value(<<"value">>,CdmiResponse6))),
    %%------------------------------

    %% selective value read non-cdmi
    RequestHeaders7 = [{"Range","1-3,5-5,-3"}],
    {Code7, _Headers7, Response7} = do_request(FileName, get, RequestHeaders7, []),
    ?assertEqual("206",Code7),
    ?assertEqual("omec...", Response7), % 1-3,5-5,12-14  from FileContent = <<"Some content...">>
    %%------------------------------

    %% selective value read non-cdmi error
    RequestHeaders8 = [{"Range","1-3,6-4,-3"}],
    {Code8, _Headers8, _Response8} = do_request(FileName, get, RequestHeaders8, []),
    ?assertEqual("400",Code8).
    %%------------------------------

% Tests cdmi metadata read on object GET request.
metadata_test(_Config) ->
    FileName = "/metadataTest.txt",
    FileContent = <<"Some content...">>,
    Before = now_in_secs(),

    create_file(FileName),
    ?assert(object_exists(FileName)),
    write_to_file(FileName,FileContent),
    ?assertEqual(FileContent,get_file_content(FileName)),
    After = now_in_secs(),

    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code1, _Headers1, Response1} = do_request(FileName, get, RequestHeaders1, []),
    ?assertEqual("200",Code1),
    {struct,CdmiResponse1} = mochijson2:decode(Response1),
    {struct, Metadata1} = proplists:get_value(<<"metadata">>,CdmiResponse1),
    ?assertEqual(<<"15">>, proplists:get_value(<<"cdmi_size">>, Metadata1)),
    CTime1 = binary_to_integer(proplists:get_value(<<"cdmi_ctime">>, Metadata1)),
    ATime1 = binary_to_integer(proplists:get_value(<<"cdmi_atime">>, Metadata1)),
    MTime1 = binary_to_integer(proplists:get_value(<<"cdmi_mtime">>, Metadata1)),
    ?assert(Before =< CTime1),
    ?assert(CTime1 =< After),
    ?assert(CTime1 =< ATime1),
    ?assert(CTime1 =< MTime1),
    ?assertEqual(<<"veilfstestuser">>, proplists:get_value(<<"cdmi_owner">>, Metadata1)),

    %%-- selective metadata read -----
    {_Code2, _Headers2, Response2} = do_request(FileName++"?metadata:", get, RequestHeaders1, []),
    {struct,CdmiResponse2} = mochijson2:decode(Response2),
    ?assertEqual(1, length(CdmiResponse2)),
    {struct, Metadata2} = proplists:get_value(<<"metadata">>,CdmiResponse2),
    ?assertEqual(5, length(Metadata2)),

    %%-- selective metadata read with prefix -----
    {Code3, _Headers3, Response3} = do_request(FileName++"?metadata:cdmi_", get, RequestHeaders1, []),
    ?assertEqual("200",Code3),
    {struct,CdmiResponse3} = mochijson2:decode(Response3),
    ?assertEqual(1, length(CdmiResponse3)),
    {struct, Metadata3} = proplists:get_value(<<"metadata">>,CdmiResponse3),
    ?assertEqual(5, length(Metadata3)),

    {_Code4, _Headers4, Response4} = do_request(FileName++"?metadata:cdmi_o", get, RequestHeaders1, []),
    {struct,CdmiResponse4} = mochijson2:decode(Response4),
    ?assertEqual(1, length(CdmiResponse4)),
    {struct, Metadata4} = proplists:get_value(<<"metadata">>,CdmiResponse4),
    ?assertEqual(<<"veilfstestuser">>, proplists:get_value(<<"cdmi_owner">>, Metadata4)),
    ?assertEqual(1, length(Metadata4)),

    {_Code5, _Headers5, Response5} = do_request(FileName++"?metadata:cdmi_size", get, RequestHeaders1, []),
    {struct,CdmiResponse5} = mochijson2:decode(Response5),
    ?assertEqual(1, length(CdmiResponse5)),
    {struct, Metadata5} = proplists:get_value(<<"metadata">>,CdmiResponse5),
    ?assertEqual(<<"15">>, proplists:get_value(<<"cdmi_size">>, Metadata5)),
    ?assertEqual(1, length(Metadata5)),

    {_Code6, _Headers6, Response6} = do_request(FileName++"?metadata:cdmi_no_such_metadata", get, RequestHeaders1, []),
    {struct,CdmiResponse6} = mochijson2:decode(Response6),
    ?assertEqual(1, length(CdmiResponse6)),
    ?assertEqual([], proplists:get_value(<<"metadata">>,CdmiResponse6)).
    %%------------------------------

% Tests dir creation (cdmi container PUT), remember that every container URI ends
% with '/'
create_dir_test(_Config) ->
    DirName = "toCreate/",
    DirName2 = "toCreate2/",
    MissingParentName="unknown/",
    DirWithoutParentName = filename:join(MissingParentName,"dir")++"/",

    %%------ non-cdmi create -------
    ?assert(not object_exists(DirName)),

    {Code1, _Headers1, _Response1} = do_request(DirName, put, [], []),
    ?assertEqual("201",Code1),

    ?assert(object_exists(DirName)),
    %%------------------------------

    %%------ basic create ----------
    ?assert(not object_exists(DirName2)),

    RequestHeaders2 = [{"content-type", "application/cdmi-container"},{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code2, _Headers2, Response2} = do_request(DirName2, put, RequestHeaders2, []),
    ?assertEqual("201",Code2),
    {struct,CdmiResponse2} = mochijson2:decode(Response2),
    ?assertEqual(<<"application/cdmi-container">>, proplists:get_value(<<"objectType">>,CdmiResponse2)),
    ?assertEqual(list_to_binary(DirName2), proplists:get_value(<<"objectName">>,CdmiResponse2)),
    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>,CdmiResponse2)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>,CdmiResponse2)),
    ?assertEqual([], proplists:get_value(<<"children">>,CdmiResponse2)),
    ?assert(proplists:get_value(<<"metadata">>,CdmiResponse2) =/= <<>>),

    ?assert(object_exists(DirName2)),
    %%------------------------------

    %%----- creation conflict ------
    ?assert(object_exists(DirName)),

    RequestHeaders3 = [{"content-type", "application/cdmi-container"},{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code3, _Headers3, _Response3} = do_request(DirName, put, RequestHeaders3, []),
    ?assertEqual("409",Code3),

    ?assert(object_exists(DirName)),
    %%------------------------------

    %%----- missing parent ---------
    ?assert(not object_exists(MissingParentName)),

    RequestHeaders4 = [{"content-type", "application/cdmi-container"},{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code4, _Headers4, _Response4} = do_request(DirWithoutParentName, put, RequestHeaders4, []),
    ?assertEqual("404",Code4).
    %%------------------------------

% Tests file creation (cdmi object PUT), It can be done with cdmi header (when file data is provided as cdmi-object
% json string), or without (when we treat request body as new file content)
create_file_test(_Config) ->
    ToCreate = "file.txt",
    ToCreate2 = filename:join(["spaces",?TEST_GROUP,"file1.txt"]),
    ToCreate4 = "file2",
    ToCreate5 = "file3",
    FileContent = <<"File content!">>,

    %%-------- basic create --------
    ?assert(not object_exists(ToCreate)),

    RequestHeaders1 = [{"content-type", "application/cdmi-object"},{"X-CDMI-Specification-Version", "1.0.2"}],
    RequestBody1 = [{<<"value">>, FileContent}],
    RawRequestBody1 = rest_utils:encode_to_json(RequestBody1),
    {Code1, _Headers1, Response1} = do_request(ToCreate, put, RequestHeaders1, RawRequestBody1),

    ?assertEqual("201",Code1),
    {struct,CdmiResponse1} = mochijson2:decode(Response1),
    ?assertEqual(<<"application/cdmi-object">>, proplists:get_value(<<"objectType">>,CdmiResponse1)),
    ?assertEqual(<<"file.txt">>, proplists:get_value(<<"objectName">>,CdmiResponse1)),
    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>,CdmiResponse1)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>,CdmiResponse1)),
    ?assert(proplists:get_value(<<"metadata">>,CdmiResponse1) =/= <<>>),

    ?assert(object_exists(ToCreate)),
    ?assertEqual(FileContent,get_file_content(ToCreate)),
    %%------------------------------

    %%------ base64 create ---------
    ?assert(not object_exists(ToCreate2)),

    RequestHeaders2 = [{"content-type", "application/cdmi-object"},{"X-CDMI-Specification-Version", "1.0.2"}],
    RequestBody2 = [{<<"valuetransferencoding">>,<<"base64">>},{<<"value">>, base64:encode(FileContent)}],
    RawRequestBody2 = rest_utils:encode_to_json(RequestBody2),
    {Code2, _Headers2, Response2} = do_request(ToCreate2, put, RequestHeaders2, RawRequestBody2),

    ?assertEqual("201",Code2),
    {struct,CdmiResponse2} = mochijson2:decode(Response2),
    ?assertEqual(<<"application/cdmi-object">>, proplists:get_value(<<"objectType">>,CdmiResponse2)),
    ?assertEqual(<<"file1.txt">>, proplists:get_value(<<"objectName">>,CdmiResponse2)),
    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>,CdmiResponse2)),
    ?assertEqual(<<"Complete">>, proplists:get_value(<<"completionStatus">>,CdmiResponse2)),
    ?assert(proplists:get_value(<<"metadata">>,CdmiResponse2) =/= <<>>),

    ?assert(object_exists(ToCreate2)),
    ?assertEqual(FileContent,get_file_content(ToCreate2)),
    %%------------------------------

    %%------- create empty ---------
    ?assert(not object_exists(ToCreate4)),

    RequestHeaders4 = [{"content-type", "application/cdmi-object"},{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code4, _Headers4, _Response4} = do_request(ToCreate4, put, RequestHeaders4, []),
    ?assertEqual("201",Code4),

    ?assert(object_exists(ToCreate4)),
    ?assertEqual(<<>>,get_file_content(ToCreate4)),
    %%------------------------------

    %%------ create noncdmi --------
    ?assert(not object_exists(ToCreate5)),

    RequestHeaders5 = [{"content-type", "application/binary"}],
    {Code5, _Headers5, _Response5} = do_request(ToCreate5, put, RequestHeaders5, FileContent),
    ?assertEqual("201",Code5),

    ?assert(object_exists(ToCreate5)),
    ?assertEqual(FileContent,get_file_content(ToCreate5)).
    %%------------------------------

% Tests cdmi object PUT requests (updating content)
update_file_test(_Config) ->
    FullName = filename:join(["/",?Test_dir_name,?Test_file_name]),
    NewValue = <<"New Value!">>,
    UpdatedValue = <<"123 Value!">>,

    %%--- value replace, cdmi ------
    ?assert(object_exists(FullName)),
    ?assertEqual(?Test_file_content,get_file_content(FullName)),

    RequestHeaders1 = [{"content-type", "application/cdmi-object"},{"X-CDMI-Specification-Version", "1.0.2"}],
    RequestBody1 = [{<<"value">>, NewValue}],
    RawRequestBody1 = rest_utils:encode_to_json(RequestBody1),
    {Code1, _Headers1, _Response1} = do_request(FullName, put, RequestHeaders1, RawRequestBody1),
    ?assertEqual("204",Code1),

    ?assert(object_exists(FullName)),
    ?assertEqual(NewValue,get_file_content(FullName)),
    %%------------------------------

    %%---- value update, cdmi ------
    UpdateValue = <<"123">>,
    RequestHeaders2 = [{"content-type", "application/cdmi-object"},{"X-CDMI-Specification-Version", "1.0.2"}],
    RequestBody2 = [{<<"value">>, UpdateValue}],
    RawRequestBody2 = rest_utils:encode_to_json(RequestBody2),
    {Code2, _Headers2, _Response2} = do_request(FullName ++ "?value:0-2", put, RequestHeaders2, RawRequestBody2),
    ?assertEqual("204",Code2),

    ?assert(object_exists(FullName)),
    ?assertEqual(UpdatedValue,get_file_content(FullName)),
    %%------------------------------

    %%--- value replace, http ------
    RequestBody3 = ?Test_file_content,
    {Code3, _Headers3, _Response3} = do_request(FullName, put, [], RequestBody3),
    ?assertEqual("204",Code3),

    ?assert(object_exists(FullName)),
    ?assertEqual(?Test_file_content,get_file_content(FullName)),
    %%------------------------------

    %%---- value update, http ------
    UpdateValue = <<"123">>,
    RequestHeaders4 = [{"content-range", "0-2"}],
    {Code4, _Headers4, _Response4} = do_request(FullName, put, RequestHeaders4, UpdateValue),
    ?assertEqual("204",Code4),

    ?assert(object_exists(FullName)),
    ?assertEqual(<<"123t_file_content">>,get_file_content(FullName)),
    %%------------------------------

    %%---- value update, http error ------
    UpdateValue = <<"123">>,
    RequestHeaders5 = [{"content-range", "0-2,3-4"}],
    {Code5, _Headers5, _Response5} = do_request(FullName, put, RequestHeaders5, UpdateValue),
    ?assertEqual("400",Code5),

    ?assert(object_exists(FullName)),
    ?assertEqual(<<"123t_file_content">>,get_file_content(FullName)).
    %%------------------------------

% Tests cdmi container DELETE requests
delete_dir_test(_Config) ->
    DirName = "/toDelete/",
    ChildDirName = "/toDelete/child/",
    SpacesDirName = "/spaces/",

    %%----- basic delete -----------
    create_dir(DirName),
    ?assert(object_exists(DirName)),

    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code1, _Headers1, _Response1} = do_request(DirName, delete, RequestHeaders1, []),
    ?assertEqual("204",Code1),

    ?assert(not object_exists(DirName)),
    %%------------------------------

    %%------ recursive delete ------
    create_dir(DirName),
    ?assert(object_exists(DirName)),
    create_dir(ChildDirName),
    ?assert(object_exists(ChildDirName)),

    RequestHeaders2 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code2, _Headers2, _Response2} = do_request(DirName, delete, RequestHeaders2, []),
    ?assertEqual("204",Code2),

    ?assert(not object_exists(DirName)),
    ?assert(not object_exists(ChildDirName)),
    %%------------------------------

    %%----- delete group dir -------
    ?assert(object_exists(SpacesDirName)),

    RequestHeaders3 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code3, _Headers3, _Response3} = do_request(SpacesDirName, delete, RequestHeaders3, []),
    ?assertEqual("403",Code3),

    ?assert(object_exists(SpacesDirName)).
    %%------------------------------

% Tests cdmi object DELETE requests
delete_file_test(_Config) ->
    FileName = "/toDelete",
    GroupFileName = "/spaces/veilfstestgroup/groupFile",

    %%----- basic delete -----------
    create_file(FileName),
    ?assert(object_exists(FileName)),

    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code1, _Headers1, _Response1} = do_request(FileName, delete, RequestHeaders1, []),
    ?assertEqual("204",Code1),

    ?assert(not object_exists(FileName)),
    %%------------------------------

    %%----- delete group file ------
    create_file(GroupFileName),
    ?assert(object_exists(GroupFileName)),

    RequestHeaders2 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code2, _Headers2, _Response2} = do_request(GroupFileName, delete, RequestHeaders2, []),
    ?assertEqual("204",Code2),

    ?assert(not object_exists(GroupFileName)).
    %%------------------------------

% tests version checking (X-CDMI-Specification-Version header)
version_test(_Config) ->
    %%----- version supported ------
    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2, 1.0.1, 1.0.0"}],
    {Code1, Headers1, _Response1} = do_request([], get, RequestHeaders1, []),
    ?assertEqual("200",Code1),
    ?assertEqual(proplists:get_value("x-cdmi-specification-version", Headers1), "1.0.2"),
    %%------------------------------

    %%--- version not supported ----
    RequestHeaders2 = [{"X-CDMI-Specification-Version", "1.0.0, 2.0.1"}],
    {Code2, Headers2, _Response2} = do_request([], get, RequestHeaders2, []),
    ?assertEqual("400",Code2),
    ?assertEqual(proplists:get_value("x-cdmi-specification-version", Headers2), undefined),
    %%------------------------------

    %%--------- non cdmi -----------
    {Code3, Headers3, _Response3} = do_request(filename:join(?Test_dir_name,?Test_file_name), get, [], []),
    ?assertEqual("200",Code3),
    ?assertEqual(proplists:get_value("x-cdmi-specification-version", Headers3), undefined).
    %%------------------------------

% tests req format checking
request_format_check_test(_Config) ->
    FileToCreate = "file.txt",
    DirToCreate = "dir/",
    FileContent = <<"File content!">>,

    %%-- obj missing content-type --
    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    RequestBody1 = [{<<"value">>, FileContent}],
    RawRequestBody1 = rest_utils:encode_to_json(RequestBody1),
    {Code1, _Headers1, _Response1} = do_request(FileToCreate, put, RequestHeaders1, RawRequestBody1),
    ?assertEqual("400",Code1),
    %%------------------------------

    %%-- dir missing content-type --
    RequestHeaders3 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    RequestBody3 = [{<<"metadata">>, <<"">>}],
    RawRequestBody3 = rest_utils:encode_to_json(RequestBody3),
    {Code3, _Headers3, _Response3} = do_request(DirToCreate, put, RequestHeaders3, RawRequestBody3),
    ?assertEqual("400",Code3).
    %%------------------------------

objectid_and_capabilities_test(_Config) ->
    %%-------- / objectid ----------
    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code1, Headers1, Response1} = do_request("", get, RequestHeaders1, []),
    ?assertEqual("200",Code1),

    ?assertEqual("application/cdmi-container",proplists:get_value("content-type",Headers1)),
    {struct,CdmiResponse1} = mochijson2:decode(Response1),
    ?assertEqual(<<"/">>, proplists:get_value(<<"objectName">>,CdmiResponse1)),
    RootId = proplists:get_value(<<"objectID">>,CdmiResponse1),
    ?assertNotEqual(RootId,undefined),
    ?assert(is_binary(RootId)),
    ?assertEqual(<<>>, proplists:get_value(<<"parentURI">>,CdmiResponse1)),
    ?assertEqual(undefined, proplists:get_value(<<"parentID">>,CdmiResponse1)),
    ?assertEqual(<<"cdmi_capabilities/container/">>,proplists:get_value(<<"capabilitiesURI">>,CdmiResponse1)),
    %%------------------------------

    %%------ /dir objectid ---------
    RequestHeaders2 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code2, _Headers2, Response2} = do_request("dir/", get, RequestHeaders2, []),
    ?assertEqual("200",Code2),

    {struct,CdmiResponse2} = mochijson2:decode(Response2),
    ?assertEqual(<<"dir/">>, proplists:get_value(<<"objectName">>,CdmiResponse2)),
    DirId = proplists:get_value(<<"objectID">>,CdmiResponse2),
    ?assertNotEqual(DirId,undefined),
    ?assert(is_binary(DirId)),
    ?assertEqual(<<"/">>, proplists:get_value(<<"parentURI">>,CdmiResponse2)),
    ?assertEqual(RootId, proplists:get_value(<<"parentID">>,CdmiResponse2)),
    ?assertEqual(<<"cdmi_capabilities/container/">>,proplists:get_value(<<"capabilitiesURI">>,CdmiResponse2)),
    %%------------------------------

    %%--- /dir/file.txt objectid ---
    RequestHeaders3 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code3, _Headers3, Response3} = do_request("dir/file.txt", get, RequestHeaders3, []),
    ?assertEqual("200",Code3),

    {struct,CdmiResponse3} = mochijson2:decode(Response3),
    ?assertEqual(<<"file.txt">>, proplists:get_value(<<"objectName">>,CdmiResponse3)),
    FileId = proplists:get_value(<<"objectID">>,CdmiResponse3),
    ?assertNotEqual(FileId,undefined),
    ?assert(is_binary(FileId)),
    ?assertEqual(<<"/dir/">>, proplists:get_value(<<"parentURI">>,CdmiResponse3)),
    ?assertEqual(DirId, proplists:get_value(<<"parentID">>,CdmiResponse3)),
    ?assertEqual(<<"cdmi_capabilities/dataobject/">>,proplists:get_value(<<"capabilitiesURI">>,CdmiResponse3)),
    %%------------------------------

    %%---- get / by objectid -------
    RequestHeaders4 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code4, _Headers4, Response4} = do_request("cdmi_objectid/"++binary_to_list(RootId)++"/", get, RequestHeaders4, []),
    ?assertEqual("200",Code4),

    {struct,CdmiResponse4} = mochijson2:decode(Response4),
    ?assertEqual(proplists:delete(<<"metadata">>,CdmiResponse1), proplists:delete(<<"metadata">>,CdmiResponse4)), %should be the same as in 1 (except metadata)
    %%------------------------------

    %%--- get /dir/ by objectid ----
    RequestHeaders5 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code5, _Headers5, Response5} = do_request("cdmi_objectid/"++binary_to_list(DirId)++"/", get, RequestHeaders5, []),
    ?assertEqual("200",Code5),

    {struct,CdmiResponse5} = mochijson2:decode(Response5),
    ?assertEqual(proplists:delete(<<"metadata">>,CdmiResponse2), proplists:delete(<<"metadata">>,CdmiResponse5)), %should be the same as in 2 (except metadata)
    %%------------------------------

    %% get /dir/file.txt by objectid
    RequestHeaders6 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code6, _Headers6, Response6} = do_request("cdmi_objectid/"++binary_to_list(DirId)++"/file.txt", get, RequestHeaders6, []),
    ?assertEqual("200",Code6),
    {struct,CdmiResponse6} = mochijson2:decode(Response6),

    ?assertEqual(proplists:delete(<<"metadata">>,CdmiResponse3), proplists:delete(<<"metadata">>,CdmiResponse6)), %should be the same as in 3 (except metadata)

    {Code7, _Headers7, Response7} = do_request("cdmi_objectid/"++binary_to_list(FileId), get, RequestHeaders6, []),
    ?assertEqual("200",Code7),

    {struct,CdmiResponse7} = mochijson2:decode(Response7),
    ?assertEqual(proplists:delete(<<"metadata">>,CdmiResponse7), proplists:delete(<<"metadata">>,CdmiResponse6)), %should be the same as in 3 (except metadata)
    %%------------------------------

    %%--- system capabilities ------
    RequestHeaders8 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code8, Headers8, Response8} = do_request("cdmi_capabilities/", get, RequestHeaders8, []),
    ?assertEqual("200",Code8),

    ?assertEqual("application/cdmi-capability",proplists:get_value("content-type",Headers8)),
    {struct,CdmiResponse8} = mochijson2:decode(Response8),
    ?assertEqual(?root_capability_id, proplists:get_value(<<"objectID">>,CdmiResponse8)),
    ?assertEqual(list_to_binary(?root_capability_path), proplists:get_value(<<"objectName">>,CdmiResponse8)),
    ?assertEqual(<<"0-1">>, proplists:get_value(<<"childrenrange">>,CdmiResponse8)),
    ?assertEqual([<<"container/">>,<<"dataobject/">>], proplists:get_value(<<"children">>,CdmiResponse8)),
    ?assertMatch({struct,_}, proplists:get_value(<<"capabilities">>,CdmiResponse8)),
    {struct,Capabilities} = proplists:get_value(<<"capabilities">>,CdmiResponse8),
    ?assertEqual(?root_capability_list,Capabilities),
    %%------------------------------

    %%-- container capabilities ----
    RequestHeaders9 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code9, _Headers9, Response9} = do_request("cdmi_capabilities/container/", get, RequestHeaders9, []),
    ?assertEqual("200",Code9),
    ?assertMatch({Code9, _, Response9},do_request("cdmi_objectid/"++binary_to_list(?container_capability_id)++"/", get, RequestHeaders9, [])),

    {struct,CdmiResponse9} = mochijson2:decode(Response9),
    ?assertEqual(list_to_binary(?root_capability_path), proplists:get_value(<<"parentURI">>,CdmiResponse9)),
    ?assertEqual(?root_capability_id, proplists:get_value(<<"parentID">>,CdmiResponse9)),
    ?assertEqual(?container_capability_id, proplists:get_value(<<"objectID">>,CdmiResponse9)),
    ?assertEqual(<<"container/">>, proplists:get_value(<<"objectName">>,CdmiResponse9)),
    ?assertMatch({struct,_}, proplists:get_value(<<"capabilities">>,CdmiResponse9)),
    {struct,Capabilities2} = proplists:get_value(<<"capabilities">>,CdmiResponse9),
    ?assertEqual(?container_capability_list,Capabilities2),
    %%------------------------------

    %%-- dataobject capabilities ---
    RequestHeaders10 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code10, _Headers10, Response10} = do_request("cdmi_capabilities/dataobject/", get, RequestHeaders10, []),
    ?assertEqual("200",Code10),
    ?assertMatch({Code10, _, Response10},do_request("cdmi_objectid/"++binary_to_list(?dataobject_capability_id)++"/", get, RequestHeaders10, [])),

    {struct,CdmiResponse10} = mochijson2:decode(Response10),
    ?assertEqual(list_to_binary(?root_capability_path), proplists:get_value(<<"parentURI">>,CdmiResponse10)),
    ?assertEqual(?root_capability_id, proplists:get_value(<<"parentID">>,CdmiResponse10)),
    ?assertEqual(?dataobject_capability_id, proplists:get_value(<<"objectID">>,CdmiResponse10)),
    ?assertEqual(<<"dataobject/">>, proplists:get_value(<<"objectName">>,CdmiResponse10)),
    ?assertMatch({struct,_}, proplists:get_value(<<"capabilities">>,CdmiResponse10)),
    {struct,Capabilities3} = proplists:get_value(<<"capabilities">>,CdmiResponse10),
    ?assertEqual(?dataobject_capability_list,Capabilities3).
    %%------------------------------

% tests mimetype and valuetransferencoding properties
mimetype_and_encoding_test(_Config) ->
    %% get mimetype and valuetransferencoding of non-cdmi file
    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code1, _Headers1, Response1} = do_request(filename:join(?Test_dir_name,?Test_file_name) ++ "?mimetype;valuetransferencoding", get, RequestHeaders1, []),
    ?assertEqual("200",Code1),
    {struct,CdmiResponse1} = mochijson2:decode(Response1),
    ?assertEqual(<<"application/octet-stream">>,proplists:get_value(<<"mimetype">>,CdmiResponse1)),
    ?assertEqual(<<"base64">>,proplists:get_value(<<"valuetransferencoding">>,CdmiResponse1)),
    %%------------------------------

    %%-- update mime and encoding --
    RequestHeaders2 = [{"X-CDMI-Specification-Version", "1.0.2"}, {"Content-Type", "application/cdmi-object"}],
    RawBody2 = rest_utils:encode_to_json([{<<"valuetransferencoding">>, <<"utf-8">>}, {<<"mimetype">>, <<"application/binary">>}]),
    {Code2, _Headers2, _Response2} = do_request(filename:join(?Test_dir_name,?Test_file_name), put, RequestHeaders2, RawBody2),
    ?assertEqual("204",Code2),

    {Code3, _Headers3, Response3} = do_request(filename:join(?Test_dir_name,?Test_file_name) ++ "?mimetype;valuetransferencoding", get, RequestHeaders2, []),
    ?assertEqual("200",Code3),
    {struct,CdmiResponse3} = mochijson2:decode(Response3),
    ?assertEqual(<<"application/binary">>,proplists:get_value(<<"mimetype">>,CdmiResponse3)),
    ?assertEqual(<<"utf-8">>,proplists:get_value(<<"valuetransferencoding">>,CdmiResponse3)),
    %%------------------------------

    %% create file with given mime and encoding
    FileName4 = "mime_file.txt",
    FileContent4 = <<"some content">>,
    RequestHeaders4 = [{"X-CDMI-Specification-Version", "1.0.2"}, {"Content-Type", "application/cdmi-object"}],
    RawBody4 = rest_utils:encode_to_json([{<<"valuetransferencoding">>, <<"utf-8">>}, {<<"mimetype">>, <<"text/plain">>}, {<<"value">>, FileContent4}]),
    {Code4, _Headers4, Response4} = do_request(FileName4, put, RequestHeaders4, RawBody4),
    ?assertEqual("201",Code4),
    {struct,CdmiResponse4} = mochijson2:decode(Response4),
    ?assertEqual(<<"text/plain">>,proplists:get_value(<<"mimetype">>,CdmiResponse4)),

    RequestHeaders5 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code5, _Headers5, Response5} = do_request(FileName4 ++ "?value;mimetype;valuetransferencoding", get, RequestHeaders5, []),
    ?assertEqual("200",Code5),
    {struct,CdmiResponse5} = mochijson2:decode(Response5),
    ?assertEqual(<<"text/plain">>,proplists:get_value(<<"mimetype">>,CdmiResponse5)),
    ?assertEqual(<<"utf-8">>,proplists:get_value(<<"valuetransferencoding">>,CdmiResponse5)), %todo what do we return here if file contains valid utf-8 string and we read byte range?
    ?assertEqual(FileContent4,proplists:get_value(<<"value">>,CdmiResponse5)),
    %%------------------------------

    %% create file with given mime and encoding using non-cdmi request
    FileName6 = "mime_file_noncdmi.txt",
    FileContent6 = <<"some content">>,
    RequestHeaders6 = [{"Content-Type", "text/plain; charset=utf-8"}],
    {Code6, _Headers6, _Response6} = do_request(FileName6, put, RequestHeaders6, FileContent6),
    ?assertEqual("201",Code6),

    RequestHeaders7 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code7, _Headers7, Response7} = do_request(FileName6 ++ "?value;mimetype;valuetransferencoding", get, RequestHeaders7, []),
    ?assertEqual("200",Code7),
    {struct,CdmiResponse7} = mochijson2:decode(Response7),
    ?assertEqual(<<"text/plain">>,proplists:get_value(<<"mimetype">>,CdmiResponse7)),
    ?assertEqual(<<"utf-8">>,proplists:get_value(<<"valuetransferencoding">>,CdmiResponse7)),
    ?assertEqual(FileContent6,proplists:get_value(<<"value">>,CdmiResponse7)).
    %%------------------------------

% tests if cdmi returns 'moved pemanently' code when we forget about '/' in path
moved_pemanently_test(_Config) ->
    %%--------- dir test -----------
    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code1, Headers1, _Response1} = do_request(?Test_dir_name, get, RequestHeaders1, []),
    ?assertEqual("301",Code1),
    ?assertEqual("/"++?Test_dir_name++"/",proplists:get_value("Location",Headers1)),
    %%------------------------------

    %%--------- file test ----------
    RequestHeaders2 = [{"X-CDMI-Specification-Version", "1.0.2"}],
    {Code2, Headers2, _Response2} = do_request(filename:join(?Test_dir_name,?Test_file_name)++"/", get, RequestHeaders2, []),
    ?assertEqual("301",Code2),
    ?assertEqual("/"++filename:join(?Test_dir_name,?Test_file_name),proplists:get_value("Location",Headers2)).
    %%------------------------------

% test error handling
errors_test(_Config) ->
    %%---- unauthorized access -----
    {Code1, _Headers1, _Response1} = do_request(?Test_dir_name, get, [], [], false),
    ?assertEqual("401", Code1),
    %%------------------------------

    %%----- wrong create path ------
    RequestHeaders2 = [{"X-CDMI-Specification-Version", "1.0.2"}, {"Content-Type","application/cdmi-container"}],
    {Code2, _Headers2, _Response2} = do_request("dir", put, RequestHeaders2, []),
    ?assertEqual("400", Code2),
    %%------------------------------

    %%---- wrong create path 2 -----
    RequestHeaders3 = [{"X-CDMI-Specification-Version", "1.0.2"}, {"Content-Type","application/cdmi-object"}],
    {Code3, _Headers3, _Response3} = do_request("dir/", put, RequestHeaders3, []),
    ?assertEqual("400", Code3),
    %%------------------------------

    %%-------- wrong base64 --------
    RequestHeaders4 = [{"X-CDMI-Specification-Version", "1.0.2"}, {"Content-Type","application/cdmi-object"}],
    RequestBody4 = rest_utils:encode_to_json([{<<"valuetransferencoding">>, <<"base64">>}, {<<"value">>, <<"#$%">>}]),
    {Code4, _Headers4, _Response4} = do_request("some_file_b64", put, RequestHeaders4, RequestBody4),
    ?assertEqual("400", Code4).
    %%------------------------------

% tests authentication by token
token_test(_Config) ->
    AccessToken = "access_token",
    GRUID = "global_id_for_" ++ ?TEST_USER,
    Token = base64:encode(AccessToken ++ ";" ++ GRUID),

    %%--------- read root ----------
    RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}, {"X-Auth-Token", binary_to_list(Token)}],
    {Code1, _Headers1, _Response1} = do_request([], get, RequestHeaders1, [], false),
    ?assertEqual("200",Code1).
    %%------------------------------

% tests writing to file at random ranges
out_of_range_test(_Config) ->
    FileName = "random_range_file.txt",
    create_file(FileName),

%%     %%---- reading out of range ---- todo handle this somehow
%%     ?assertEqual(<<>>, get_file_content(FileName)),
%%     RequestHeaders1 = [{"X-CDMI-Specification-Version", "1.0.2"}],
%%
%%     RequestBody1 = rest_utils:encode_to_json([{<<"value">>, <<"data">>}]),
%%     {Code1, _Headers1, _Response1} = do_request(FileName ++ "?value:0-3", get, RequestHeaders1, RequestBody1),
%%     ?assertEqual("400",Code1). %?
%%     %%------------------------------

    %%------ writing at end --------
    ?assertEqual(<<>>, get_file_content(FileName)),

    RequestHeaders2 = [{"X-CDMI-Specification-Version", "1.0.2"}, {"Content-Type", "application/cdmi-object"}],
    RequestBody2 = rest_utils:encode_to_json([{<<"value">>, <<"data">>}]),
    {Code2, _Headers2, _Response2} = do_request(FileName ++ "?value:0-3", put, RequestHeaders2, RequestBody2),
    ?assertEqual("204",Code2),

    ?assertEqual(<<"data">>, get_file_content(FileName)),
    %%------------------------------

    %%------ writing at random --------
    RequestBody3 = rest_utils:encode_to_json([{<<"value">>, <<"data">>}]),
    {Code3, _Headers3, _Response3} = do_request(FileName ++ "?value:10-13", put, RequestHeaders2, RequestBody3),
    ?assertEqual("204",Code3),

    ?assertEqual(<<100,97,116,97,0,0,0,0,0,0,100,97,116,97>>, get_file_content(FileName)). % "data(6x<0_byte>)data"
    %%------------------------------

%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(_,Config) ->
    ?INIT_CODE_PATH,
    DN = ?config(dn,Config),
    [CCM] = ?config(nodes,Config),
    Cert = ?config(cert,Config),
    StorageUUID = ?config(storage_uuid,Config),

    put(ccm,CCM),
    put(dn,DN),
    put(cert,Cert),
    put(storage_uuid, StorageUUID),

    ibrowse:start(),

    Config.

end_per_testcase(_,_Config) ->
    ibrowse:stop().

init_per_suite(Config) ->
    ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
    test_node_starter:start_deps_for_tester_node(),

    [CCM] = Nodes = test_node_starter:start_test_nodes(1),

    test_node_starter:start_app_on_nodes(?APP_Name, ?VEIL_DEPS, Nodes,
        [[{node_type, ccm_test},
            {initialization_time, 1},
            {dispatcher_port, 5055},
            {ccm_nodes, [CCM]},
            {dns_port, 1308},
            {db_nodes, [?DB_NODE]},
            {heart_beat, 1},
            {nif_prefix, './'},
            {ca_dir, './cacerts/'},
            {control_panel_download_buffer,4}
        ]]),

    gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
    gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    test_utils:wait_for_cluster_cast(),
    gen_server:cast({global, ?CCM}, init_cluster),
    test_utils:wait_for_cluster_init(),

    ibrowse:start(),
    Cert = ?COMMON_FILE("peer.pem"),
    DN = get_dn_from_cert(Cert,CCM),

    Config1 = lists:append([{nodes, Nodes}], Config),

    ?ENABLE_PROVIDER(Config1),

    StorageUUID = setup_user_in_db(Cert, Config1),

    lists:append([{dn,DN}, {cert,Cert}, {storage_uuid, StorageUUID}], Config1).

end_per_suite(Config) ->
    Nodes = ?config(nodes, Config),
    test_node_starter:stop_app_on_nodes(?APP_Name, ?VEIL_DEPS, Nodes),
    test_node_starter:stop_test_nodes(Nodes).

%% ====================================================================
%% Internal functions
%% ====================================================================

object_exists(Path) ->
    DN=get(dn),
    Ans = rpc_call_node(fun() ->
        fslogic_context:set_user_dn(DN),
        logical_files_manager:getfileattr(Path)
    end),
    case Ans of
        {ok,_} -> true;
        _ -> false
    end.

create_dir(Path) ->
    DN=get(dn),
    Ans = rpc_call_node(fun() ->
        fslogic_context:set_user_dn(DN),
        logical_files_manager:mkdir(Path)
    end),
    ?assertEqual(ok, Ans).

create_file(Path) ->
    DN=get(dn),
    Ans = rpc_call_node(fun() ->
        fslogic_context:set_user_dn(DN),
        logical_files_manager:create(Path)
    end),
    ?assertEqual(ok, Ans).

get_file_content(Path) ->
    DN=get(dn),

    rpc_call_node(fun() ->
        GetFile = fun F(Filename, Size, BytesSent, BufferSize, Ans) ->
            {ok, BytesRead} = logical_files_manager:read(Filename, BytesSent, BufferSize),
            NewSent = BytesSent + size(BytesRead),
            if
                NewSent =:= Size -> <<Ans/binary,BytesRead/binary>>;
                true -> F(Filename, Size, NewSent, BufferSize,<<Ans/binary,BytesRead/binary>>)
            end
        end,

        fslogic_context:set_user_dn(DN),
        {ok,Attr} = logical_files_manager:getfileattr(Path),
        GetFile(Path,Attr#fileattributes.size,0,10,<<>>)
    end).

write_to_file(Path,Data) ->
    DN=get(dn),

    rpc_call_node(fun() ->
        fslogic_context:set_user_dn(DN),
        logical_files_manager:write(Path,Data)
    end).

rpc_call_node(F) ->
    rpc:call(get(ccm), erlang, apply, [F, [] ]).

get_dn_from_cert(Cert,CCM) ->
    {Ans2, PemBin} = file:read_file(Cert),
    ?assertEqual(ok, Ans2),

    {Ans3, RDNSequence} = rpc:call(CCM, user_logic, extract_dn_from_cert, [PemBin]),
    ?assertEqual(rdnSequence, Ans3),

    {Ans4, DN} = rpc:call(CCM, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
    ?assertEqual(ok, Ans4),
    DN.

% Populates the database with one user and some files
setup_user_in_db(Cert, Config) ->
    [CCM | _] = ?config(nodes, Config),

    UserDoc = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_GROUP]),
    [DN | _] = UserDoc#veil_document.record#user.dn_list,

    {Ans1, StorageUUID} = rpc:call(CCM, fslogic_storage, insert_storage, [?SH, ?ARG_TEST_ROOT]),
    ?assertEqual(ok, Ans1),

    fslogic_context:set_user_dn(DN),
    Ans6 = rpc:call(CCM, erlang, apply, [
        fun() ->
            fslogic_context:set_user_dn(DN),
            logical_files_manager:mkdir(filename:join("/",?Test_dir_name))
        end, [] ]),
    ?assertEqual(ok, Ans6),


    Ans7 = rpc:call(CCM, erlang, apply, [
        fun() ->
            fslogic_context:set_user_dn(DN),
            FullName = filename:join(["/",?Test_dir_name,?Test_file_name]),
            logical_files_manager:create(FullName),
            logical_files_manager:write(FullName, ?Test_file_content)
        end, [] ]),
    ?assert(is_integer(Ans7)),

    StorageUUID.

% Performs a single request using ibrowse
do_request(RestSubpath, Method, Headers, Body) ->
    do_request(RestSubpath, Method, Headers, Body, true).

do_request(RestSubpath, Method, Headers, Body, UseCert) ->
    Cert = get(cert),
    CCM = get(ccm),

    {ok, Port} = rpc:call(CCM, application, get_env, [veil_cluster_node, rest_port]),
    Hostname = case (Port =:= 80) or (Port =:= 443) of
                   true -> "https://localhost";
                   false -> "https://localhost:" ++ integer_to_list(Port)
               end,
    Opts = case UseCert of
               true -> [{ssl_options, [{certfile, Cert}, {reuse_sessions, false}]}];
               false -> [{ssl_options, [{reuse_sessions, false}]}]
           end,
    {ok, Code, RespHeaders, Response} =
        ibrowse:send_req(
            Hostname ++ "/cdmi/" ++ RestSubpath,
            Headers,
            Method,
            Body,
            Opts
        ),
    {Code, RespHeaders, Response}.

% Returns current time in seconds
now_in_secs() ->
    {MegaSecs, Secs, _MicroSecs} = erlang:now(),
    MegaSecs * 1000000 + Secs.
