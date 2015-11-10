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
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([choose_adequate_handler/1, list_basic_dir_test/1, list_root_dir_test/1,
    list_nonexisting_dir_test/1, get_selective_params_of_dir_test/1,
    use_supported_cdmi_version/1, use_unsupported_cdmi_version/1]).

-performance({test_cases, []}).
all() -> [choose_adequate_handler, use_supported_cdmi_version, use_unsupported_cdmi_version].

-define(MACAROON, "macaroon").
-define(TIMEOUT, timer:seconds(5)).
-define(USER_1_TOKEN, {"X-Auth-Token", "1"}).
-define(USER_2_TOKEN, {"X-Auth-Token", "2"}).
-define(USER_3_TOKEN, {"X-Auth-Token", "3"}).
-define(USER_4_TOKEN, {"X-Auth-Token", "4"}).
-define(USER_5_TOKEN, {"X-Auth-Token", "5"}).

%%%===================================================================
%%% Test functions
%%%===================================================================

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

list_basic_dir_test(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    RequestHeaders = [{"X-CDMI-Specification-Version", "1.0.2"}],
    TestDir="dir",
    %todo create TestDir

    % when
    {ok, Code, ResponseHeaders, Response} = do_request(Worker, TestDir ++ "/", get, RequestHeaders, []),

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
    {ok, Code, _, Response} = do_request(Worker, [], get, RequestHeaders, []),

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
    {ok, Code, _, _} = do_request(Worker, "nonexisting_dir/", get, RequestHeaders, []),

    % then
    ?assertEqual("404", Code).

get_selective_params_of_dir_test(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    RequestHeaders = [{"X-CDMI-Specification-Version", "1.0.2"}],

    % when
    {ok, Code, _, Response} = do_request(Worker, "spaces/?children;objectName", get, RequestHeaders, []),

    % then
    ?assertEqual("200", Code),
    {struct,CdmiResponse4} = mochijson2:decode(Response),
    ?assertEqual(<<"dir/">>, proplists:get_value(<<"objectName">>,CdmiResponse4)),
    ?assertEqual([<<"file.txt">>], proplists:get_value(<<"children">>,CdmiResponse4)),
    ?assertEqual(2,length(CdmiResponse4)).

use_supported_cdmi_version(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    RequestHeaders = [{"X-CDMI-Specification-Version", "1.1.1"}, ?USER_1_TOKEN],

    % when
    {ok, Code, _ResponseHeaders, _Response} = do_request(Worker, "/random", get, RequestHeaders, []),

    % then
    ?assertEqual("404", Code).

use_unsupported_cdmi_version(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    RequestHeaders = [{"X-CDMI-Specification-Version", "1.0.2"}],

    % when
    {ok, Code, _ResponseHeaders, _Response} = do_request(Worker, "/random", get, RequestHeaders, []),

    % then
    ?assertEqual("400", Code).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
    initializer:setup_storage(ConfigWithNodes).
end_per_suite(Config) ->
    tracer:stop(),
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
    test_utils:mock_validate(Workers, [cdmi_object_handler, cdmi_container_handler]),
    test_utils:mock_unload(Workers, [cdmi_object_handler, cdmi_container_handler]),
    end_per_testcase(default, Config);
end_per_testcase(_, Config) ->
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces(Config),
    unmock_user_auth(Config),
    ibrowse:stop(),
    ssl:stop().

%%%===================================================================
%%% Internal functions
%%%===================================================================

% Performs a single request using ibrowse
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
                {ok, P} = rpc:call(Node, application, get_env, [?APP_NAME, http_worker_rest_port]),
                PStr = integer_to_list(P),
                put(port, PStr),
                PStr;
            P -> P
        end,
    string:join(["https://", utils:get_host(Node), ":", Port, "/cdmi/"], "").

mock_user_auth(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, rest_auth),
    test_utils:mock_expect(Workers, rest_auth, is_authorized,
        fun(Req, State) ->
            case cowboy_req:header(<<"x-auth-token">>, Req) of
                {undefined, NewReq} ->
                    {{false, <<"authentication_error">>}, NewReq, State};
                {Token, NewReq} ->
                    UserId = ?config({user_id, binary_to_integer(Token)}, Config),
                    {true, NewReq, State#{identity => #identity{user_id = UserId}}}
            end
        end
    ).

unmock_user_auth(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, rest_auth),
    test_utils:mock_unload(Workers, rest_auth).