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
-include("nodes_manager.hrl").
-include("veil_modules/control_panel/common.hrl").
-include("registered_names.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([main_test/1]).


-define(SH, "DirectIO").
-define(TEST_ROOT, ["/tmp/veilfs"]). %% Root of test filesystem


all() -> [main_test].

%% ====================================================================
%% Test functions
%% ====================================================================

%% Main test, sets up a single-node cluster, initializes database with single user
%% having one directory and one file in filesystem and performs REST requests.
main_test(Config) ->
    nodes_manager:check_start_assertions(Config),

    NodesUp = ?config(nodes, Config),
    [CCM | _] = NodesUp,
    put(ccm, CCM),

    gen_server:cast({?Node_Manager_Name, CCM}, do_heart_beat),
    gen_server:cast({global, ?CCM}, {set_monitoring, on}),
    gen_server:cast({global, ?CCM}, init_cluster),
    timer:sleep(2500),


    % Create a user in db with some files
    DN = setup_user_in_db(),    
    ibrowse:start(),

    % Test if REST requests return what is expected
    test_rest_files_content_dirs(),
    test_rest_files_content_regulars(),
    test_rest_files_attr(),
    test_rest_shares(),


    % DB cleanup
    rpc:call(CCM, user_logic, remove_user, [{dn, DN}]).


%% ====================================================================
%% Internal functions
%% ====================================================================

% Tests the functionality of rest_files_content module, concerning dirs as resources
test_rest_files_content_dirs() ->
    {Code1, Headers1, Response1} = do_request("files/content/", get, [], []),
    ?assertEqual(Code1, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers1), "application/json"),
    ?assertEqual(Response1, "[\"dir1\",\"groups\"]"),

    {Code2, Headers2, Response2} = do_request("files/content/dir1", get, [], []),
    ?assertEqual(Code2, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers2), "application/json"),
    ?assertEqual(Response2, "[\"file.txt\"]"),

    {Code3, Headers3, Response3} = do_request("files/content/dir1gfhdfgh", get, [], []),
    ?assertEqual(Code3, "404"),
    ?assertEqual(proplists:get_value("content-type", Headers3), "application/json"),
    ?assertEqual(Response3, []),

    {Code4, _Headers4, Response4} = do_request("files/content/dir1", put, [], []),
    ?assertEqual(Code4, "405"),
    ?assertEqual(Response4, []),

    {Code5, _Headers5, Response5} = do_request("files/content/", post, [], []),
    ?assertEqual(Code5, "405"),
    ?assertEqual(Response5, []),

    {Code6, _Headers6, Response6} = do_request("files/content/dir1", delete, [], []),
    ?assertEqual(Code6, "405"),
    ?assertEqual(Response6, []).


% Tests the functionality of rest_files_content module, concerning regular files as resources
test_rest_files_content_regulars() ->
    {Code3, _Headers3, Response3} = do_request("files/content/somefile.txt", get, [], []),
    ?assertEqual(Code3, "404"),
    ?assertEqual(Response3, []),

    {Code4, Headers4, Response4} = do_request("files/content/dir1%2Ffile.txt", get, [], []),
    ?assertEqual(Code4, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers4), "text/plain"),
    ?assert(proplists:get_value("content-disposition", Headers4) /= undefined),
    ?assertEqual(Response4, ""),

    {Code5, _Headers5, Response5} = do_request("files/content/dir1%2Ffile.txt", post, [], []),
    ?assertEqual(Code5, "405"),
    ?assertEqual(Response5, []),

    {Code6, _Headers6, Response6} = do_request("files/content/dir1%2Ffile.txt", delete, [], []),
    ?assertEqual(Code6, "405"),
    ?assertEqual(Response6, []),

    {Code7, _Headers7, Response7} = do_request("files/content/dir1%2Ffile.txt", put, [], []),
    ?assertEqual(Code7, "405"),
    ?assertEqual(Response7, []).


% Tests the functionality of rest_attr module
test_rest_files_attr() ->
    {Code1, _Headers1, Response1} = do_request("files/attr/", get, [], []),
    ?assertEqual(Code1, "405"),
    ?assertEqual(Response1, []),

    {Code2, Headers2, Response2} = do_request("files/attr/dir1", get, [], []),
    ?assertEqual(Code2, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers2), "application/json"),
    ?assert(Response2 /= []),

    {Code3, Headers3, Response3} = do_request("files/attr/dir1gfhdfgh", get, [], []),
    ?assertEqual(Code3, "404"),
    ?assertEqual(proplists:get_value("content-type", Headers3), "application/json"),
    ?assertEqual(Response3, []),

    {Code4, _Headers4, Response4} = do_request("files/attr/dir1", put, [], []),
    ?assertEqual(Code4, "405"),
    ?assertEqual(Response4, []),

    {Code5, _Headers5, Response5} = do_request("files/attr/", post, [], []),
    ?assertEqual(Code5, "405"),
    ?assertEqual(Response5, []),

    {Code6, _Headers6, Response6} = do_request("files/attr/dir1", delete, [], []),
    ?assertEqual(Code6, "405"),
    ?assertEqual(Response6, []).
    

% Tests the functionality of rest_shares module
test_rest_shares() ->
    {Code1, Headers1, Response1} = do_request("shares/", get, [], []),
    ?assertEqual(Code1, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers1), "application/json"),
    ?assertEqual(Response1, "[]"),

    {Code2, _Headers2, Response2} = do_request("shares/", post, [{"content-type", "application/json"}], [<<"\"dir1/file.txt\"">>]),
    ?assertEqual(Code2, "303"),
    ?assertEqual(Response2, []),

    {Code3, Headers3, Response3} = do_request("shares/", get, [], []),
    ?assertEqual(Code3, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers3), "application/json"),
    ?assert(Response3 /= []),

    "[\"" ++ ShareIDWithBracket = Response3,
    "]\"" ++ ReversedShareID = lists:reverse(ShareIDWithBracket), 
    ShareID = lists:reverse(ReversedShareID),

    {Code4, Headers4, Response4} = do_request("shares/" ++ ShareID, get, [], []),
    ?assertEqual(Code4, "200"),
    ?assertEqual(proplists:get_value("content-type", Headers4), "application/json"),
    {ok, Port} = rpc:call(get(ccm), application, get_env, [veil_cluster_node, control_panel_port]),
    Hostname = case (Port =:= 80) or (Port =:= 443) of
        true -> "https://localhost";
        false -> "https://localhost:" ++ integer_to_list(Port)
    end,
    DlPath = Hostname ++ ?shared_files_download_path ++ ShareID,
    ?assertEqual(Response4, "{\"file_path\":\"dir1/file.txt\",\"download_path\":\"" ++ DlPath ++ "\"}"),

    {Code5, Headers5, Response5} = do_request("shares/" ++ ShareID, delete, [], []),
    ?assertEqual(Code5, "204"),
    ?assertEqual(proplists:get_value("content-type", Headers5), "application/json"),
    ?assertEqual(Response5, []),

    {Code6, Headers6, Response6} = do_request("shares/" ++ ShareID, delete, [], []),
    ?assertEqual(Code6, "404"),
    ?assertEqual(proplists:get_value("content-type", Headers6), "application/json"),
    ?assertEqual(Response6, []),

    {Code7, _Headers7, Response7} = do_request("shares/dir1", put, [], []),
    ?assertEqual(Code7, "405"),
    ?assertEqual(Response7, []).
    

% Performs a single request using ibrowse
do_request(RestSubpath, Method, Headers, Body) ->
    Cert = ?COMMON_FILE("peer.pem"),
    CCM = get(ccm),

    {ok, Port} = rpc:call(CCM, application, get_env, [veil_cluster_node, control_panel_port]),
    Hostname = case (Port =:= 80) or (Port =:= 443) of
        true -> "https://localhost";
        false -> "https://localhost:" ++ integer_to_list(Port)
    end,
    {ok, Code, RespHeaders, Response} = rpc:call(CCM, 
        ibrowse, 
        send_req, [
            Hostname ++ "/rest/latest/" ++ RestSubpath, 
            Headers,
            Method, 
            Body, 
            [{ssl_options, [{certfile, Cert}, {reuse_sessions, false}]}]
    ]),
    {Code, RespHeaders, Response}.


% Populates the database with one user and some files
setup_user_in_db() ->
    CCM = get(ccm), 

    Cert = ?COMMON_FILE("peer.pem"),
    {Ans2, PemBin} = file:read_file(Cert),
    ?assertEqual(ok, Ans2),

    {Ans3, RDNSequence} = rpc:call(CCM, user_logic, extract_dn_from_cert, [PemBin]),
    ?assertEqual(rdnSequence, Ans3),

    {Ans4, DN} = rpc:call(CCM, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
    ?assertEqual(ok, Ans4),

    DnList = [DN],
    Login = "veilfstestuser",
    Name = "user user",
    Teams = "veilfstestgroup",
    Email = "user@email.net",

    % Cleanup data from other tests
    % TODO Usunac jak wreszcie baza bedzie czyszczona miedzy testami
    rpc:call(CCM, user_logic, remove_user, [{dn, DN}]),

    {Ans1, _} = rpc:call(CCM, fslogic_storage, insert_storage, [?SH, ?TEST_ROOT]),
    ?assertEqual(ok, Ans1),
    {Ans5, _} = rpc:call(CCM, user_logic, create_user, [Login, Name, Teams, Email, DnList]),
    ?assertEqual(ok, Ans5),


    put(user_id, DN),
    Ans6 = rpc:call(CCM, logical_files_manager, mkdir, ["veilfstestuser/dir1"]), 
    ?assertEqual(ok, Ans6),


    Ans7 = rpc:call(CCM, logical_files_manager, create, ["veilfstestuser/dir1/file.txt"]), 
    ?assertEqual(ok, Ans7),

    DN.


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================
init_per_testcase(main_test, Config) ->
    ?INIT_DIST_TEST,
    nodes_manager:start_deps_for_tester_node(),

    Nodes = nodes_manager:start_test_on_nodes(1),
    [Node1 | _] = Nodes,


    DB_Node = nodes_manager:get_db_node(),

    StartLog = nodes_manager:start_app_on_nodes(Nodes, 
        [[{node_type, ccm_test}, 
            {dispatcher_port, 5055}, 
            {ccm_nodes, [Node1]}, 
            {dns_port, 1308},
            {db_nodes, [DB_Node]}]]),

    Assertions = [{false, lists:member(error, Nodes)}, {false, lists:member(error, StartLog)}],
    lists:append([{nodes, Nodes}, {assertions, Assertions}], Config).


end_per_testcase(main_test, Config) ->
    Nodes = ?config(nodes, Config),
    StopLog = nodes_manager:stop_app_on_nodes(Nodes),
    StopAns = nodes_manager:stop_nodes(Nodes),
    ?assertEqual(false, lists:member(error, StopLog)),
    ?assertEqual(ok, StopAns).