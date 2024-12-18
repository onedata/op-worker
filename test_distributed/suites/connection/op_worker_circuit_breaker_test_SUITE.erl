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

-include("api_file_test_utils.hrl").
-include("cdmi_test.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/http/codes.hrl").

-define(PROVIDER_SELECTOR, krakow).
-define(LOW_GUI_UPLOAD_RETRY_INTERVAL_SECONDS, 2).

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    rest_handler_circuit_breaker_test/1,
    cdmi_handler_circuit_breaker_test/1,
    gs_circuit_breaker_test/1,
    gui_upload_circuit_breaker_test/1
]).

all() -> ?ALL([
    rest_handler_circuit_breaker_test,
    cdmi_handler_circuit_breaker_test,
    gs_circuit_breaker_test,
    gui_upload_circuit_breaker_test
]).

%%%===================================================================
%%% Tests
%%%===================================================================


rest_handler_circuit_breaker_test(_Config) ->
    set_circuit_breaker_state(op_worker, closed),
    ?assertMatch(ok, get_rest_response()),

    set_circuit_breaker_state(op_worker, open),
    ?assertMatch(?ERROR_SERVICE_UNAVAILABLE, get_rest_response()),

    set_circuit_breaker_state(op_worker, closed),
    ?assertMatch(ok, get_rest_response()).


cdmi_handler_circuit_breaker_test(_Config) ->
    set_circuit_breaker_state(op_worker, closed),
    ?assertMatch(ok, get_cdmi_response()),

    set_circuit_breaker_state(op_worker, open),
    ?assertMatch(?ERROR_SERVICE_UNAVAILABLE, get_cdmi_response()),

    set_circuit_breaker_state(op_worker, closed),
    ?assertMatch(ok, get_cdmi_response()).


gs_circuit_breaker_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    GsArgs = #gs_args{
        operation = get,
        gri = #gri{type = op_provider, aspect = configuration, scope = public},
        data = #{}
    },

    set_circuit_breaker_state(op_worker, closed),
    {ok, GsClient} = ?assertMatch({ok, _}, gs_test_utils:connect_via_gs(Node, ?NOBODY)),
    ?assertMatch({ok, _}, gs_test_utils:gs_request(GsClient, GsArgs)),

    set_circuit_breaker_state(op_worker, open),
    ?assertMatch(?ERROR_SERVICE_UNAVAILABLE, gs_test_utils:gs_request(GsClient, GsArgs)),
    ?assertMatch(?ERROR_SERVICE_UNAVAILABLE, gs_test_utils:connect_via_gs(Node, ?NOBODY)),

    set_circuit_breaker_state(op_worker, closed),
    ?assertMatch({ok, _}, gs_test_utils:gs_request(GsClient, GsArgs)),
    ?assertMatch({ok, _}, gs_test_utils:connect_via_gs(Node, ?NOBODY)).


gui_upload_circuit_breaker_test(_Config) ->
    OpWorkerNode = oct_background:get_random_provider_node(?PROVIDER_SELECTOR),

    set_circuit_breaker_state(oz_worker, open),
    DummyGuiHash = create_dummy_gui_package(),

    %% since the oz is blocked op will not upload gui
    ?assertNotEqual(DummyGuiHash, get_gui_hash_set_up_in_oz(OpWorkerNode)),

    set_circuit_breaker_state(oz_worker, closed),

    %% after setting service_circuit_breaker_state to closed in oz,
    %% gui will upload automatically within gui_upload_interval time
    %% (low time interval is set in init_per_suite)
    ?assertEqual(DummyGuiHash, get_gui_hash_set_up_in_oz(OpWorkerNode), ?ATTEMPTS).


%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [
            {gui_upload_retry_interval_seconds, ?LOW_GUI_UPLOAD_RETRY_INTERVAL_SECONDS}
        ]}]
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
set_circuit_breaker_state(op_worker, State) ->
    ok = opw_test_rpc:set_env(?PROVIDER_SELECTOR, service_circuit_breaker_state, State);
set_circuit_breaker_state(oz_worker, State) ->
    ok = ozw_test_rpc:set_env(service_circuit_breaker_state, State).


%% @private
process_http_response(Response) ->
    case Response of
        {ok, ?HTTP_200_OK, _, _} ->
            ok;
        {ok, _, _, ErrorBody} ->
            #{<<"error">> := ErrorJson} = json_utils:decode(ErrorBody),
            errors:from_json(ErrorJson)
    end.


%% @private
create_dummy_gui_package() ->
    Path = opw_test_rpc:get_env(?PROVIDER_SELECTOR, gui_package_path),
    DirName = filename:dirname(?rpc(?PROVIDER_SELECTOR, filename:absname(Path))),
    DummyGuiRoot = filename:join(DirName, "gui_static"),
    case filelib:is_dir(DummyGuiRoot) of
        true -> ok = file:del_dir_r(DummyGuiRoot);
        false -> ok
    end,
    ok = file:make_dir(DummyGuiRoot),

    DummyIndex = filename:join(DummyGuiRoot, "index.html"),
    IndexContent = datastore_key:new(),
    ok = file:write_file(DummyIndex, IndexContent),
    DummyPackage = filename:join(DirName, "gui_static.tar.gz"),

    % Use tar to create archive as erl_tar is limited when it comes to tarring directories
    [] = os:cmd(str_utils:format("tar -C ~ts -czf ~ts ~ts", [DirName, DummyPackage, "gui_static"])),
    {ok, Content} = file:read_file(DummyPackage),
    ok = ?rpc(?PROVIDER_SELECTOR, file:write_file(DummyPackage, Content)),

    OpWorkerNode = oct_background:get_random_provider_node(?PROVIDER_SELECTOR),
    {ok, DummyGuiHash} = ?rpc(OpWorkerNode, gui:package_hash(DummyPackage)),
    DummyGuiHash.


%% @private
get_gui_hash_set_up_in_oz(OpWorkerNode) ->
    {ok, #document{value = #od_cluster{worker_gui_hash = WorkerGuiHash}}} =
        ?rpc(OpWorkerNode, cluster_logic:get()),
    WorkerGuiHash.
