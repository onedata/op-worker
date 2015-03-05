%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% REST and CDMI tests
%%% @end
%%%-------------------------------------------------------------------
-module(rest_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([rest_token_auth/1]).

all() -> [rest_token_auth].

-define(TOKEN, "TOKEN").

%%%===================================================================
%%% API
%%%===================================================================

rest_token_auth(Config) ->
    timer:sleep(timer:seconds(5)), % waiting for appmock. todo integrate with nagios
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    Endpoint = rest_endpoint(Worker),

    % when
    AuthFail = ibrowse:send_req(Endpoint ++ "unknown", [{"X-Auth-Token", <<"invalid">>}], get),
    AuthSuccess = ibrowse:send_req(Endpoint ++ "unknown", [{"X-Auth-Token", ?TOKEN}], get),

    % then
    ?assertMatch({ok, "401", _, _}, AuthFail),
    ?assertMatch({ok, "404", _, _}, AuthSuccess).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_suite(Config) ->
    test_node_starter:prepare_test_environment(Config,
        ?TEST_FILE(Config, "env_desc.json"), ?MODULE).

end_per_suite(Config) ->
%%     test_node_starter:clean_environment(Config).
ok.

init_per_testcase(_, Config) ->
    ssl:start(),
    ibrowse:start(),
    mock_gr_certificates(Config),
    Config.

end_per_testcase(_, Config) ->
    unmock_gr_certificates(Config),
    ibrowse:stop(),
    ssl:stop().

%%%===================================================================
%%% Internal functions
%%%===================================================================

rest_endpoint(Node) ->
    Port =
        case get(port) of
            undefined ->
                {ok, P} = rpc:call(Node, application, get_env, [?APP_NAME, http_worker_rest_port]),
                PStr = integer_to_list(P),
                put(port, PStr),
                PStr;
            P -> P
        end,
    string:join(["https://", utils:get_host(Node), ":", Port, "/rest/latest/"], "").

%todo move to ctool
mock_gr_certificates(Config) ->
    [Worker1, _] = Workers = ?config(op_worker_nodes, Config),
    KeyPath = rpc:call(Worker1, gr_plugin, get_key_path, []),
    CertPath = rpc:call(Worker1, gr_plugin, get_cert_path, []),
    CacertPath = rpc:call(Worker1, gr_plugin, get_cacert_path, []),
    {ok, Key} = file:read_file(?TEST_FILE(Config, "grpkey.pem")),
    {ok, Cert} = file:read_file(?TEST_FILE(Config, "grpcert.pem")),
    {ok, Cacert} = file:read_file(?TEST_FILE(Config, "grpCA.pem")),
    test_utils:mock_new(Workers, [file]),
    test_utils:mock_expect(Workers, file, read_file,
        fun
            (Path) when Path =:= KeyPath -> {ok, Key};
            (Path) when Path =:= CertPath -> {ok, Cert};
            (Path) when Path =:= CacertPath -> {ok, Cacert};
            (Path) -> meck:passthrough([Path])
        end
    ).

%todo move to ctool
unmock_gr_certificates(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, [file]),
    test_utils:mock_unload(Workers, [file]).