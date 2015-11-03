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
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([choose_adequate_handler/1, list_basic_dir_test/1, list_root_dir_test/1,
    list_nonexisting_dir_test/1, get_selective_params_of_dir_test/1]).

-performance({test_cases, []}).
all() -> [choose_adequate_handler].

-define(MACAROON, "macaroon").
-define(TIMEOUT, timer:seconds(5)).

%%%===================================================================
%%% Test functions
%%%===================================================================

choose_adequate_handler(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    File = "file",
    Dir = "dir/",

    % when
    do_request(Worker, File, get, [], []),
    % then
    ?assert(rpc:call(Worker, meck, called, [cdmi_object_handler, rest_init, '_'])),

    % when
    do_request(Worker, Dir, get, [], []),
    % then
    ?assert(rpc:call(Worker, meck, called, [cdmi_container_handler, rest_init, '_'])).

list_basic_dir_test(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    RequestHeaders = [{"X-CDMI-Specification-Version", "1.0.2"}],
    TestDir="dir",
    %todo create TestDir

    % when
    {Code, ResponseHeaders, Response} = do_request(Worker, TestDir ++ "/", get, RequestHeaders, []),

    % then
    ?assertEqual("200", Code),
    ContentType = proplists:get_value("content-type", ResponseHeaders),
    {struct, CdmiResponse} = mochijson2:decode(Response),
    ObjectType = proplists:get_value(<<"objectType">>, CdmiResponse),
    ObjectName = proplists:get_value(<<"objectName">>, CdmiResponse),
    CompletionStatus = proplists:get_value(<<"completionStatus">>, CdmiResponse),
    Children = proplists:get_value(<<"children">>, CdmiResponse),
    Metadata = proplists:get_value(<<"metadata">>, CdmiResponse),
    ?assertEqual("application/cdmi-container", ContentType),
    ?assertEqual(<<"application/cdmi-container">>, ObjectType),
    ?assertEqual(<<"dir/">>, ObjectName),
    ?assertEqual(<<"Complete">>, CompletionStatus),
    ?assertEqual([], Children),
    ?assertNotEqual(<<>>, Metadata).

list_root_dir_test(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    RequestHeaders = [{"X-CDMI-Specification-Version", "1.0.2"}],

    % when
    {Code, _, Response} = do_request(Worker, [], get, RequestHeaders, []),

    % then
    ?assertEqual("200", Code),
    {struct, CdmiResponse} = mochijson2:decode(Response),
    ObjectName = proplists:get_value(<<"objectName">>, CdmiResponse),
    Children = proplists:get_value(<<"children">>, CdmiResponse),
    ?assertEqual(<<"/">>, ObjectName),
    ?assertEqual([<<"spaces/">>], Children).

list_nonexisting_dir_test(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    RequestHeaders = [{"X-CDMI-Specification-Version", "1.0.2"}],

    % when
    {Code, _, _} = do_request(Worker, "nonexisting_dir/", get, RequestHeaders, []),

    % then
    ?assertEqual("404", Code).

get_selective_params_of_dir_test(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    RequestHeaders = [{"X-CDMI-Specification-Version", "1.0.2"}],

    % when
    {Code, _, Response} = do_request(Worker, "spaces/?children;objectName", get, RequestHeaders, []),

    % then
    ?assertEqual("200", Code),
    {struct,CdmiResponse4} = mochijson2:decode(Response),
    ?assertEqual(<<"dir/">>, proplists:get_value(<<"objectName">>,CdmiResponse4)),
    ?assertEqual([<<"file.txt">>], proplists:get_value(<<"children">>,CdmiResponse4)),
    ?assertEqual(2,length(CdmiResponse4)).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(choose_adequate_handler, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [cdmi_object_handler, cdmi_container_handler]),
    init_per_testcase(default, Config);
init_per_testcase(_, Config) ->
    ssl:start(),
    ibrowse:start(),
    Config.

end_per_testcase(choose_adequate_handler, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, [cdmi_object_handler, cdmi_container_handler]),
    test_utils:mock_unload(Workers, [cdmi_object_handler, cdmi_container_handler]),
    end_per_testcase(default, Config);
end_per_testcase(_, _Config) ->
    ibrowse:stop(),
    ssl:stop().

%%%===================================================================
%%% Internal functions
%%%===================================================================

% Performs a single request using ibrowse
do_request(Node, RestSubpath, Method, Headers, Body) ->
    {ok, Code, RespHeaders, Response} =
        ibrowse:send_req(
            cdmi_endpoint(Node) ++ RestSubpath,
            Headers,
            Method,
            Body,
            [{ssl_options, [{reuse_sessions, false}]}]
        ),
    {Code, RespHeaders, Response}.

cdmi_endpoint(Node) ->
    Port =
        case get(port) of
            undefined ->
                {ok, P} = rpc:call(Node, application, get_env, [?APP_NAME, http_worker_rest_port]),
                PStr = integer_to_list(P),
                put(port, PStr),
                PStr;
            P -> P
        end,
    string:join(["https://", utils:get_host(Node), ":", Port, "/cdmi/"], "").
