%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of the op-worker circuit breaker mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(op_worker_circuit_breaker_test_SUITE).
-author("Katarzyna Such").

-include("cdmi_test.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("onenv_test_utils.hrl").
-include("api_file_test_utils.hrl").

-define(PROVIDER_SELECTOR, krakow).

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    rest_handler_circuit_breaker_test/1,
    cdmi_handler_circuit_breaker_test/1,
    gs_circuit_breaker_test/1
]).

all() -> ?ALL([
    rest_handler_circuit_breaker_test,
    cdmi_handler_circuit_breaker_test,
    gs_circuit_breaker_test
]).

%%%===================================================================
%%% Tests
%%%===================================================================


rest_handler_circuit_breaker_test(_Config) ->
    set_circuit_breaker_state(closed),
    ?assertMatch(ok, get_rest_response()),

    set_circuit_breaker_state(open),
    ?assertMatch(?ERROR_SERVICE_UNAVAILABLE, get_rest_response()),

    set_circuit_breaker_state(closed),
    ?assertMatch(ok, get_rest_response()).


cdmi_handler_circuit_breaker_test(_Config) ->
    set_circuit_breaker_state(closed),
    ?assertMatch(ok, get_cdmi_response()),

    set_circuit_breaker_state(open),
    ?assertMatch(?ERROR_SERVICE_UNAVAILABLE, get_cdmi_response()),

    set_circuit_breaker_state(closed),
    ?assertMatch(ok, get_cdmi_response()).


gs_circuit_breaker_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    GsArgs = #gs_args{
        operation = get,
        gri = #gri{type = op_provider, aspect = configuration, scope = public},
        data = #{}
    },

    set_circuit_breaker_state(closed),
    {ok, GsClient} = ?assertMatch({ok, _}, onenv_api_test_runner:connect_via_gs(Node, ?NOBODY)),
    ?assertMatch({ok, _}, gs_test_utils:gs_request(GsClient, GsArgs)),

    set_circuit_breaker_state(open),
    ?assertMatch(?ERROR_SERVICE_UNAVAILABLE, gs_test_utils:gs_request(GsClient, GsArgs)),
    ?assertMatch(?ERROR_SERVICE_UNAVAILABLE, onenv_api_test_runner:connect_via_gs(Node, ?NOBODY)),

    set_circuit_breaker_state(closed),
    ?assertMatch({ok, _}, gs_test_utils:gs_request(GsClient, GsArgs)),
    ?assertMatch({ok, _}, onenv_api_test_runner:connect_via_gs(Node, ?NOBODY)).


%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op"
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_, Config) ->
    Config.


end_per_testcase(_, Config) ->
    Config.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
get_rest_response() ->
    Node = oct_background:get_random_provider_node(?PROVIDER_SELECTOR),
    process_http_response(rest_test_utils:request(Node, <<"configuration">>, get, #{}, <<>>)).


%% @private
get_cdmi_response() ->
    Nodes = oct_background:get_provider_nodes(?PROVIDER_SELECTOR),
    process_http_response(cdmi_test_utils:do_request(Nodes, "cdmi_capabilities/", get, [?CDMI_VERSION_HEADER], [])).


%% @private
set_circuit_breaker_state(State) ->
    ok = ?rpc(?PROVIDER_SELECTOR, op_worker:set_env(service_circuit_breaker_state, State)).


%% @private
process_http_response(Response) ->
    case Response of
        {ok, 200, _, _} ->
            ok;
        {ok, _, _, ErrorBody} ->
            #{<<"error">> := ErrorJson} = json_utils:decode(ErrorBody),
            errors:from_json(ErrorJson)
    end.
