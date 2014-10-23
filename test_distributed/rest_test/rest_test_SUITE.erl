%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module test the REST handler by performing requests
%% and asserting if returned reponses are as expected.
%% @end
%% ===================================================================

-module(rest_test_SUITE).
-include("test_utils.hrl").
-include("oneprovider_modules/control_panel/common.hrl").
-include("oneprovider_modules/control_panel/rest_messages.hrl").
-include("err.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").
-include("oneprovider_modules/control_panel/global_registry_interfacing.hrl").

-define(ProtocolVersion, 1).

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([main_test/1]).


-define(SH, "DirectIO").

-define(REST_FILES_SUBPATH, "files/").
-define(REST_ATTRS_SUBPATH, "attrs/").
-define(REST_SHARE_SUBPATH, "shares/").


all() -> [main_test].

%% ====================================================================
%% Test functions
%% ====================================================================

%% Main test, sets up a single-node cluster, initializes database with single user
%% having one directory and one file in filesystem and performs REST requests.
main_test(Config) ->
    NodesUp = ?config(nodes, Config),
    [CCM | _] = NodesUp,
    put(ccm, CCM),

    gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
    gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    test_utils:wait_for_cluster_cast(),
    gen_server:cast({global, ?CCM}, init_cluster),
    test_utils:wait_for_cluster_init(),

    ?ENABLE_PROVIDER(Config),

    put(dn, get_dn_from_cert(Config)),

    ibrowse:start(),
    rest_test_user_unknown(get_dn_from_cert(Config)),

    % Create a user in db with some files
    {DN, StorageUUID} = setup_user_in_db(Config),

    % Test if REST requests return what is expected
    test_rest_error_messages(),
    test_rest_files_dirs(),
    test_rest_files_regulars(),
    test_rest_upload(),
    test_rest_attrs(),
    test_rest_shares(),
    test_rest_connection_check(),

    ibrowse:stop(),
    % DB cleanup
    RemoveStorageAns = rpc:call(CCM, dao_lib, apply, [dao_vfs, remove_storage, [{uuid, StorageUUID}], ?ProtocolVersion]),
    ?assertEqual(ok, RemoveStorageAns),

    ?assertEqual(ok, rpc:call(CCM, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ ?TEST_GROUP], ?ProtocolVersion])),
    ?assertEqual(ok, rpc:call(CCM, dao_lib, apply, [dao_vfs, remove_file, ["spaces/" ++ ?TEST_USER], ?ProtocolVersion])),
    ?assertEqual(ok, rpc:call(CCM, dao_lib, apply, [dao_vfs, remove_file, ["spaces"], ?ProtocolVersion])),

    rpc:call(CCM, user_logic, remove_user, [{dn, DN}]).


%% ====================================================================
%% Internal functions
%% ====================================================================

% Tests if proper error message is returned when user doesn't exist in the database
rest_test_user_unknown(DN) ->
    {Code1, Headers1, Response1} = do_request(?REST_FILES_SUBPATH, get, [], []),
    ?assertEqual("500", Code1),
    ?assertEqual("application/json", proplists:get_value("content-type", Headers1)),
    ?assertEqual(rest_utils:error_reply(?report_error(?error_user_unknown, [DN])), list_to_binary(Response1)).


% Tests if version matching mechanism works correctly and if proper errors are returned
% when requested path doesn't point to anything.
test_rest_error_messages() ->
    {Code1, Headers1, Response1} = do_request("0.666", ?REST_FILES_SUBPATH, get, [], []),
    ?assertEqual(Code1, "500"),
    ?assertEqual(proplists:get_value("content-type", Headers1), "application/json"),
    ?assertEqual(list_to_binary(Response1), rest_utils:error_reply(?report_warning(?error_version_unsupported, ["0.666"]))),

    {Code2, Headers2, Response2} = do_request("latest", ?REST_FILES_SUBPATH, get, [], []),
    ?assertEqual(Code2, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers2), "application/json"),
    ?assertEqual(Response2, "[\"dir\",\"spaces\"]"),

    {Code3, Headers3, Response3} = do_request("asdfgadrfg", get, [], []),
    ?assertEqual(Code3, "500"),
    ?assertEqual(proplists:get_value("content-type", Headers3), "application/json"),
    ?assertEqual(list_to_binary(Response3), rest_utils:error_reply(?report_warning(?error_path_invalid))),

    {Code4, Headers4, Response4} = do_request("latest", ?REST_FILES_SUBPATH, post, [], []),
    ?assertEqual(Code4, "500"),
    ?assertEqual(proplists:get_value("content-type", Headers4), "application/json"),
    ?assertEqual(list_to_binary(Response4), rest_utils:error_reply(?report_warning(?error_media_type_unsupported))).


% Tests the functionality of rest_files module, concerning dirs as resources
test_rest_files_dirs() ->
    {Code1, Headers1, Response1} = do_request(?REST_FILES_SUBPATH, get, [], []),
    ?assertEqual(Code1, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers1), "application/json"),
    ?assertEqual(Response1, "[\"dir\",\"spaces\"]"),

    {Code2, Headers2, Response2} = do_request(?REST_FILES_SUBPATH ++ "dir", get, [], []),
    ?assertEqual(Code2, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers2), "application/json"),
    ?assertEqual(Response2, "[\"file.txt\"]"),

    {Code3, Headers3, Response3} = do_request(?REST_FILES_SUBPATH ++ "dirgfhdfgh", get, [], []),
    ?assertEqual(Code3, "404"),
    ?assertEqual(proplists:get_value("content-type", Headers3), "application/json"),
    ?assertEqual(list_to_binary(Response3), rest_utils:error_reply(?report_warning(?error_not_found, ["dirgfhdfgh"]))),

    {Code4, _Headers4, Response4} = do_request(?REST_FILES_SUBPATH ++ "dir", put, [], []),
    ?assertEqual(Code4, "500"),
    ?assertEqual(list_to_binary(Response4), rest_utils:error_reply(?report_warning(?error_media_type_unsupported))),

    {Code5, _Headers5, Response5} = do_request(?REST_FILES_SUBPATH ++ "dir", post, [], []),
    ?assertEqual(Code5, "500"),
    ?assertEqual(list_to_binary(Response5), rest_utils:error_reply(?report_warning(?error_media_type_unsupported))),

    {Code6, _Headers6, Response6} = do_request(?REST_FILES_SUBPATH ++ "dir", delete, [], []),
    ?assertEqual(Code6, "500"),
    ?assertEqual(list_to_binary(Response6), rest_utils:error_reply(?report_warning(?error_dir_cannot_delete))).


% Tests the functionality of rest_files module, concerning regular files as resources
test_rest_files_regulars() ->
    {Code3, _Headers3, Response3} = do_request(?REST_FILES_SUBPATH ++ "somefile.txt", get, [], []),
    ?assertEqual(Code3, "404"),
    ?assertEqual(list_to_binary(Response3), rest_utils:error_reply(?report_warning(?error_not_found, ["somefile.txt"]))),

    {Code4, Headers4, Response4} = do_request(?REST_FILES_SUBPATH ++ "dir/file.txt", get, [], []),
    ?assertEqual(Code4, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers4), "text/plain"),
    ?assert(proplists:get_value("content-disposition", Headers4) /= undefined),
    ?assertEqual(Response4, ""),

    {Code5, _Headers5, Response5} = do_request(?REST_FILES_SUBPATH ++ "dir/file.txt", post, [{"content-type", "multipart/form-data"}], []),
    ?assertEqual(Code5, "400"),
    ?assertEqual(list_to_binary(Response5), rest_utils:error_reply(?report_error(?error_upload_cannot_create))),

    {Code6, _Headers6, Response6} = do_request(?REST_FILES_SUBPATH ++ "dir/file.txt", put, [{"content-type", "multipart/form-data"}], []),
    ?assertEqual(Code6, "400"),
    ?assertEqual(list_to_binary(Response6), rest_utils:error_reply(?report_error(?error_upload_unprocessable))),

    {Code7, _Headers7, Response7} = do_request(?REST_FILES_SUBPATH ++ "dir/file.txt", delete, [], []),
    ?assertEqual(Code7, "200"),
    ?assertEqual(list_to_binary(Response7), rest_utils:success_reply(?success_file_deleted)),

    {Code8, _Headers8, Response8} = do_request(?REST_FILES_SUBPATH ++ "dir/file.txt", delete, [], []),
    ?assertEqual(Code8, "404"),
    ?assertEqual(list_to_binary(Response8), rest_utils:error_reply(?report_warning(?error_not_found, ["dir/file.txt"]))).

% Tests the functionality of rest_files module, concerning files upload
test_rest_upload() ->
    Data1 = "123456789",
    {Header1, Body1} = format_multipart_request(Data1),
    {Code1, _Headers1, Response1} = do_request(?REST_FILES_SUBPATH ++ "dir/file.txt", post, [Header1], [Body1]),
    ?assertEqual(Code1, "200"),
    ?assertEqual(list_to_binary(Response1), rest_utils:success_reply(?success_file_uploaded)),
    File1 = rpc:call(get(ccm), logical_files_manager, read, ["/spaces/" ++ ?TEST_USER ++ "/dir/file.txt", 0, 9]),
    ?assertEqual(File1, {ok, list_to_binary(Data1)}),

    {Header2, Body2} = format_multipart_request(Data1),
    {Code2, _Headers2, Response2} = do_request(?REST_FILES_SUBPATH ++ "dir/file.txt", post, [Header2], [Body2]),
    ?assertEqual(Code2, "400"),
    ?assertEqual(list_to_binary(Response2), rest_utils:error_reply(?report_error(?error_upload_cannot_create))),

    Data2 = "00000000",
    {Header3, Body3} = format_multipart_request(Data2),
    {Code3, _Headers3, Response3} = do_request(?REST_FILES_SUBPATH ++ "dir/file.txt", put, [Header3], [Body3]),
    ?assertEqual(Code3, "200"),
    ?assertEqual(list_to_binary(Response3), rest_utils:success_reply(?success_file_uploaded)),
    File3 = rpc:call(get(ccm), logical_files_manager, read, ["/spaces/" ++ ?TEST_USER ++ "/dir/file.txt", 0, 9]),
    ?assertEqual(File3, {ok, list_to_binary(Data2)}),

    {Header4, Body4} = format_multipart_request(Data2),
    {Code4, _Headers4, Response4} = do_request(?REST_FILES_SUBPATH ++ "dir/file.txt/file1.txt", put, [Header4], [Body4]),
    ?assertEqual(Code4, "400"),
    ?assertEqual(list_to_binary(Response4), rest_utils:error_reply(?report_error(?error_upload_cannot_create))).


% Tests the functionality of rest_attr module
test_rest_attrs() ->
    {Code1, _Headers1, Response1} = do_request(?REST_ATTRS_SUBPATH, get, [], []),
    ?assertEqual(Code1, "500"),
    ?assertEqual(list_to_binary(Response1), rest_utils:error_reply(?report_warning(?error_no_id_in_uri))),

    {Code2, Headers2, Response2} = do_request(?REST_ATTRS_SUBPATH ++ "dir", get, [], []),
    ?assertEqual(Code2, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers2), "application/json"),
    ?assert(Response2 /= []),

    {Code3, Headers3, Response3} = do_request(?REST_ATTRS_SUBPATH ++ "dirgfhdfgh", get, [], []),
    ?assertEqual(Code3, "404"),
    ?assertEqual(proplists:get_value("content-type", Headers3), "application/json"),
    ?assertEqual(list_to_binary(Response3), rest_utils:error_reply(?report_warning(?error_not_found, ["dirgfhdfgh"]))),

    {Code4, _Headers4, Response4} = do_request(?REST_ATTRS_SUBPATH ++ "dir", put, [], []),
    ?assertEqual(Code4, "405"),
    ?assertEqual(list_to_binary(Response4), rest_utils:error_reply(?report_warning(?error_method_unsupported, ["PUT"]))),

    {Code5, _Headers5, Response5} = do_request(?REST_ATTRS_SUBPATH, post, [], []),
    ?assertEqual(Code5, "405"),
    ?assertEqual(list_to_binary(Response5), rest_utils:error_reply(?report_warning(?error_method_unsupported, ["POST"]))),

    {Code6, _Headers6, Response6} = do_request(?REST_ATTRS_SUBPATH ++ "dir", delete, [], []),
    ?assertEqual(Code6, "405"),
    ?assertEqual(list_to_binary(Response6), rest_utils:error_reply(?report_warning(?error_method_unsupported, ["DELETE"]))).


% Tests the functionality of rest_shares module
test_rest_shares() ->
    {Code1, Headers1, Response1} = do_request(?REST_SHARE_SUBPATH, get, [], []),
    ?assertEqual(Code1, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers1), "application/json"),
    ?assertEqual(Response1, "[]"),

    {Code2, _Headers2, Response2} = do_request(?REST_SHARE_SUBPATH, post, [{"content-type", "application/json"}], [<<"\"dir/file.txt\"">>]),
    ?assertEqual(Code2, "200"),
    ?assert(Response2 /= []),

    {Code3, Headers3, Response3} = do_request(?REST_SHARE_SUBPATH, get, [], []),
    ?assertEqual(Code3, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers3), "application/json"),
    ?assert(Response3 /= []),

        "[\"" ++ ShareIDWithBracket = Response3,
        "]\"" ++ ReversedShareID = lists:reverse(ShareIDWithBracket),
    ShareID = lists:reverse(ReversedShareID),

    {Code4, Headers4, Response4} = do_request(?REST_SHARE_SUBPATH ++ ShareID, get, [], []),
    ?assertEqual(Code4, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers4), "application/json"),
    {ok, Port} = rpc:call(get(ccm), application, get_env, [oneprovider_node, control_panel_port]),
    Hostname = case (Port =:= 80) or (Port =:= 443) of
                   true -> "https://localhost";
                   false -> "https://localhost:" ++ integer_to_list(Port)
               end,
    DlPath = Hostname ++ ?shared_files_download_path ++ ShareID,
    ?assertEqual(Response4, "{\"file_path\":\"spaces/" ++ ?TEST_USER ++ "/dir/file.txt\",\"download_path\":\"" ++ DlPath ++ "\"}"),

    {Code5, Headers5, Response5} = do_request(?REST_SHARE_SUBPATH ++ ShareID, delete, [], []),
    ?assertEqual(Code5, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers5), "application/json"),
    ?assertEqual(list_to_binary(Response5), rest_utils:success_reply(?success_share_deleted)),

    {Code6, Headers6, Response6} = do_request(?REST_SHARE_SUBPATH ++ ShareID, delete, [], []),
    ?assertEqual(Code6, "404"),
    ?assertEqual(proplists:get_value("content-type", Headers6), "application/json"),
    ?assertEqual(list_to_binary(Response6), rest_utils:error_reply(?report_warning(?error_not_found, [ShareID]))),

    {Code7, _Headers7, Response7} = do_request(?REST_SHARE_SUBPATH ++ "dir", put, [], []),
    ?assertEqual(Code7, "405"),
    ?assertEqual(list_to_binary(Response7), rest_utils:error_reply(?report_warning(?error_method_unsupported, ["PUT"]))),

{Code8, _Headers8, Response8} = do_request(?REST_SHARE_SUBPATH, post, [{"content-type", "application/json"}], [<<"\"somepath\"">>]),
    ?assertEqual(Code8, "400"),
    ?assertEqual(list_to_binary(Response8), rest_utils:error_reply(?report_warning(?error_share_cannot_create, ["somepath"]))).

test_rest_connection_check() ->
    {Code, _Headers, Response} = do_request(binary_to_list(?connection_check_path), get, [], []),
    ?assertEqual("200", Code),
    ?assertEqual(binary_to_list(?rest_connection_check_value), Response).

do_request(RestSubpath, Method, Headers, Body) ->
    do_request("latest", RestSubpath, Method, Headers, Body).

% Performs a single request using ibrowse
do_request(APIVersion, RestSubpath, Method, Headers, Body) ->
    Cert = ?COMMON_FILE("peer.pem"),
    CCM = get(ccm),

    {ok, Port} = rpc:call(CCM, application, get_env, [oneprovider_node, rest_port]),
    Hostname = case (Port =:= 80) or (Port =:= 443) of
                   true -> "https://localhost";
                   false -> "https://localhost:" ++ integer_to_list(Port)
               end,
    {ok, Code, RespHeaders, Response} = rpc:call(CCM,
        ibrowse,
        send_req, [
                Hostname ++ "/rest/" ++ APIVersion ++ "/" ++ RestSubpath,
            Headers,
            Method,
            Body,
            [{ssl_options, [{certfile, Cert}, {reuse_sessions, false}]}]
        ]),
    {Code, RespHeaders, Response}.


format_multipart_request(Data) ->
    Boundary = "------------a450glvjfEoqerAc1p431paQlfDac152cadADfd",
    Body = format_multipart_formdata(Boundary, [], [{file, "file", Data}]),
    Header = {"content-type", lists:concat(["multipart/form-data; boundary=", Boundary])},
    {Header, Body}.


format_multipart_formdata(Boundary, Fields, Files) ->
    FieldParts = lists:map(fun({FieldName, FieldContent}) ->
        [lists:concat(["--", Boundary]),
            lists:concat(["Content-Disposition: form-data; name=\"", atom_to_list(FieldName), "\""]),
            "",
            FieldContent]
    end, Fields),
    FieldParts2 = lists:append(FieldParts),
    FileParts = lists:map(fun({FieldName, FileName, FileContent}) ->
        [lists:concat(["--", Boundary]),
            lists:concat(["Content-Disposition: form-data; name=\"", atom_to_list(FieldName), "\"; filename=\"", FileName, "\""]),
            lists:concat(["Content-Type: ", "application/octet-stream"]),
            "",
            FileContent]
    end, Files),
    FileParts2 = lists:append(FileParts),
    EndingParts = [lists:concat(["--", Boundary, "--"]), ""],
    Parts = lists:append([FieldParts2, FileParts2, EndingParts]),
    string:join(Parts, "\r\n").


get_dn_from_cert(Config) ->
    [CCM | _] = ?config(nodes, Config),
    Cert = ?config(cert, Config),

    {Ans2, PemBin} = file:read_file(Cert),
    ?assertEqual(ok, Ans2),

    {Ans3, RDNSequence} = rpc:call(CCM, user_logic, extract_dn_from_cert, [PemBin]),
    ?assertEqual(rdnSequence, Ans3),

    {Ans4, DN} = rpc:call(CCM, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
    ?assertEqual(ok, Ans4),
    DN.


% Populates the database with one user and some files
setup_user_in_db(Config) ->
    [CCM | _] = ?config(nodes, Config),

    {Ans1, StorageUUID} = rpc:call(CCM, fslogic_storage, insert_storage, [?SH, ?ARG_TEST_ROOT]),
    ?assertEqual(ok, Ans1),

    Cert = ?config(cert, Config),
    UserDoc = test_utils:add_user(Config, ?TEST_USER, Cert, [?TEST_USER, ?TEST_GROUP]),
    [DN | _] = user_logic:get_dn_list(UserDoc),

    fslogic_context:set_user_dn(DN),
    Ans6 = rpc:call(CCM, erlang, apply, [
        fun() ->
            fslogic_context:set_user_dn(DN),
            logical_files_manager:mkdir("/dir")
        end, [] ]),
    ?assertEqual(ok, Ans6),


    Ans7 = rpc:call(CCM, erlang, apply, [
        fun() ->
            fslogic_context:set_user_dn(DN),
            logical_files_manager:create("/dir/file.txt")
        end, [] ]),
    ?assertEqual(ok, Ans7),

    {DN, StorageUUID}.


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================
init_per_testcase(main_test, Config) ->
    ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
    test_node_starter:start_deps_for_tester_node(),

    Nodes = test_node_starter:start_test_nodes(1),
    [Node1 | _] = Nodes,


    DB_Node = ?DB_NODE,

    test_node_starter:start_app_on_nodes(?APP_Name, ?ONEPROVIDER_DEPS, Nodes,
        [[{node_type, ccm_test},
            {initialization_time, 1},
            {dispatcher_port, 5055},
            {ccm_nodes, [Node1]},
            {dns_port, 1308},
            {db_nodes, [DB_Node]},
            {heart_beat, 1},
            {nif_prefix, './'},
            {ca_dir, './cacerts/'}
        ]]),

    lists:append([{nodes, Nodes}, {cert, ?COMMON_FILE("peer.pem")}], Config).


end_per_testcase(main_test, Config) ->
    Nodes = ?config(nodes, Config),
    test_node_starter:stop_app_on_nodes(?APP_Name,?ONEPROVIDER_DEPS,Nodes),
    test_node_starter:stop_test_nodes(Nodes).
