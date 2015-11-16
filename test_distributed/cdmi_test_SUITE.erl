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

-include("modules/http_worker/rest/cdmi/cdmi_capabilities.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
  end_per_testcase/2]).

-export([choose_adequate_handler/1, list_basic_dir_test/1, list_root_dir_test/1,
  list_nonexisting_dir_test/1, get_selective_params_of_dir_test/1,
  use_supported_cdmi_version/1, use_unsupported_cdmi_version/1, capabilities_test/1]).

-performance({test_cases, []}).
all() -> [choose_adequate_handler, use_supported_cdmi_version, use_unsupported_cdmi_version, capabilities_test].

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

use_supported_cdmi_version(Config) ->
  % given
  [Worker | _] = ?config(op_worker_nodes, Config),
  RequestHeaders = [{"X-CDMI-Specification-Version", "1.1.1"}],

  % when
  {Code, _ResponseHeaders, _Response} = do_request(Worker, "/", get, RequestHeaders, []),

  % then
  %% we are to get 404 because path "/" doesn't exist.
  ?assertEqual("404", Code).

use_unsupported_cdmi_version(Config) ->
  % given
  [Worker | _] = ?config(op_worker_nodes, Config),
  RequestHeaders = [{"X-CDMI-Specification-Version", "1.0.2"}],

  % when
  {Code, _ResponseHeaders, _Response} = do_request(Worker, "/", get, RequestHeaders, []),

  % then
  ?assertEqual("400", Code).

% tests if capabilities of objects, containers, and whole storage system are set properly
capabilities_test(Config) ->
  [Worker | _] = ?config(op_worker_nodes, Config),

  %%--- system capabilities ------
  RequestHeaders8 = [{"X-CDMI-Specification-Version", "1.1.1"}],
  {Code8, Headers8, Response8} = do_request(Worker, "cdmi_capabilities/", get, RequestHeaders8, []),
  ?assertEqual("200",Code8),

  ?assertEqual("application/cdmi-capability",proplists:get_value("content-type",Headers8)),
  {struct,CdmiResponse8} = mochijson2:decode(Response8),
%%   ?assertEqual(?root_capability_id, proplists:get_value(<<"objectID">>,CdmiResponse8)),
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
%%   ?assertMatch({Code9, _, Response9},do_request("cdmi_objectid/"++binary_to_list(?container_capability_id)++"/", get, RequestHeaders9, [])),

  {struct,CdmiResponse9} = mochijson2:decode(Response9),
  ?assertEqual(list_to_binary(?root_capability_path), proplists:get_value(<<"parentURI">>,CdmiResponse9)),
%%   ?assertEqual(?root_capability_id, proplists:get_value(<<"parentID">>,CdmiResponse9)),
%%   ?assertEqual(?container_capability_id, proplists:get_value(<<"objectID">>,CdmiResponse9)),
  ?assertEqual(<<"container/">>, proplists:get_value(<<"objectName">>,CdmiResponse9)),
  ?assertMatch({struct,_}, proplists:get_value(<<"capabilities">>,CdmiResponse9)),
  {struct,Capabilities2} = proplists:get_value(<<"capabilities">>,CdmiResponse9),
  ?assertEqual(?container_capability_list,Capabilities2),
  %%------------------------------

  %%-- dataobject capabilities ---
  RequestHeaders10 = [{"X-CDMI-Specification-Version", "1.0.2"}],
  {Code10, _Headers10, Response10} = do_request("cdmi_capabilities/dataobject/", get, RequestHeaders10, []),
  ?assertEqual("200",Code10),
%%   ?assertMatch({Code10, _, Response10},do_request("cdmi_objectid/"++binary_to_list(?dataobject_capability_id)++"/", get, RequestHeaders10, [])),

  {struct,CdmiResponse10} = mochijson2:decode(Response10),
  ?assertEqual(list_to_binary(?root_capability_path), proplists:get_value(<<"parentURI">>,CdmiResponse10)),
%%   ?assertEqual(?root_capability_id, proplists:get_value(<<"parentID">>,CdmiResponse10)),
%%   ?assertEqual(?dataobject_capability_id, proplists:get_value(<<"objectID">>,CdmiResponse10)),
  ?assertEqual(<<"dataobject/">>, proplists:get_value(<<"objectName">>,CdmiResponse10)),
  ?assertMatch({struct,_}, proplists:get_value(<<"capabilities">>,CdmiResponse10)),
  {struct,Capabilities3} = proplists:get_value(<<"capabilities">>,CdmiResponse10),
  ?assertEqual(?dataobject_capability_list,Capabilities3).
%%------------------------------

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
do_request(RestSubpath, Method, Headers, Body) ->
  do_request(RestSubpath, Method, Headers, Body, true).

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