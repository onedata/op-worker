%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Provider connection tests
%%% @end
%%%-------------------------------------------------------------------
-module(provider_connection_test_SUITE).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("http/rest/cdmi/cdmi_capabilities.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    incompatible_providers_should_not_connect/1,
    provider_should_reconnect_after_loss_of_connection/1,
    after_connection_timeout_session_is_terminated/1,
    deprecated_configuration_endpoint_is_served/1,
    configuration_endpoints_give_same_results/1,
    provider_logic_should_correctly_resolve_nodes_to_connect/1
]).

all() ->
    ?ALL([
        incompatible_providers_should_not_connect,
        provider_should_reconnect_after_loss_of_connection,
        after_connection_timeout_session_is_terminated,
        deprecated_configuration_endpoint_is_served,
        configuration_endpoints_give_same_results,
        provider_logic_should_correctly_resolve_nodes_to_connect
    ]).

-define(ATTEMPTS, 90).

%%%===================================================================
%%% Test functions
%%%===================================================================


% Providers should not connect because incorrect compatible_op_versions
% env variables are defined using in env_up_posthook
incompatible_providers_should_not_connect(Config) ->
    % providers should start connecting right after init_per_testcase
    % (spaces creation); just in case wait some time before checking
    % that connection failed
    timer:sleep(timer:seconds(30)),

    % There are 2 providers and 3 nodes:
    %   * P1 -> 1 node
    %   * P2 -> 2 nodes
    Nodes = ?config(op_worker_nodes, Config),
    [Domain1, Domain2] = lists:usort([?GET_DOMAIN(N) || N <- Nodes]),
    Nodes1 = [N || N <- Nodes, ?GET_DOMAIN(N) =:= Domain1],
    Nodes2 = [N || N <- Nodes, ?GET_DOMAIN(N) =:= Domain2],
    P1 = hd(Nodes1),
    P2 = hd(Nodes2),

    ?assertMatch(false, connection_exists(P1, P2)),
    ?assertMatch(false, connection_exists(P2, P1)),

    {_AppId, _AppName, AppVersion} = lists:keyfind(
        ?APP_NAME, 1, rpc:call(hd(Nodes), application, loaded_applications, [])
    ),
    rpc:multicall(Nodes, application, set_env, [
        ?APP_NAME, compatible_op_versions, [AppVersion]
    ]),
    ?assertMatch(true, connection_exists(P1, P2), ?ATTEMPTS),
    ?assertMatch(true, connection_exists(P2, P1), ?ATTEMPTS),

    ok.


provider_should_reconnect_after_loss_of_connection(Config) ->
    % There are 2 providers and 3 nodes:
    %   * P1 -> 1 node
    %   * P2 -> 2 nodes
    Nodes = ?config(op_worker_nodes, Config),
    [Domain1, Domain2] = lists:usort([?GET_DOMAIN(N) || N <- Nodes]),
    P1Worker = hd([N || N <- Nodes, ?GET_DOMAIN(N) =:= Domain1]),
    P2 = initializer:domain_to_provider_id(Domain2),
    OutgoingSessId = get_provider_session_id(P1Worker, outgoing, P2),

    % undo initializer mock for get_nodes and reset connections
    GetNodesFun = fun(PID) ->
        {ok, IPsAtoms} = inet:getaddrs(binary_to_list(PID), inet),
        {ok, [list_to_binary(inet:ntoa(IP)) || IP <- IPsAtoms]}
    end,
    test_utils:mock_expect(Nodes, provider_logic, get_nodes, GetNodesFun),
    {ok, Cons} = list_session_connections(P1Worker, OutgoingSessId),
    lists:foreach(fun(Conn) -> connection:close(Conn) end, Cons),

    {ok, [Conn2, Conn1]} = ?assertMatch(
        {ok, [_, _]},
        list_session_connections(P1Worker, OutgoingSessId),
        ?ATTEMPTS
    ),
    connection:close(Conn1),
    {ok, [Conn3, _]} = ?assertMatch(
        {ok, [_Conn3, Conn2]},
        list_session_connections(P1Worker, OutgoingSessId),
        ?ATTEMPTS
    ),

    ?assertNotEqual(Conn1, Conn3).


after_connection_timeout_session_is_terminated(Config) ->
    % There are 2 providers and 3 nodes:
    %   * P1 -> 1 node
    %   * P2 -> 2 nodes
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Nodes, session_connections),
    test_utils:mock_expect(Nodes, session_connections, ensure_connected, fun(_) -> ok end),

    [Domain1, Domain2] = lists:usort([?GET_DOMAIN(N) || N <- Nodes]),
    P1 = hd([N || N <- Nodes, ?GET_DOMAIN(N) =:= Domain1]),
    P2Id = initializer:domain_to_provider_id(Domain2),
    OutgoingSessId = get_provider_session_id(P1, outgoing, P2Id),

    % when one of session connection timeouts
    {ok, [Conn2, Conn1]} = ?assertMatch(
        {ok, [_, _]},
        list_session_connections(P1, OutgoingSessId),
        ?ATTEMPTS
    ),
    Conn1 ! timeout,

    % then entire session and it's connections should be terminated
    ?assertMatch(false, session_exists(P1, OutgoingSessId), ?ATTEMPTS),
    ?assertMatch(false, rpc:call(P1, erlang, is_process_alive, [Conn1])),
    ?assertMatch(false, rpc:call(P1, erlang, is_process_alive, [Conn2])),
    ?assertMatch({error, not_found}, list_session_connections(P1, OutgoingSessId), ?ATTEMPTS),

    test_utils:mock_unload(Nodes, [session_connections]).


deprecated_configuration_endpoint_is_served(Config) ->
    Nodes = ?config(op_worker_nodes, Config),

    lists:foreach(fun(Node) ->
        ExpectedConfiguration = expected_configuration(Node),
        URL = str_utils:format("https://~s/configuration/",
            [maps:get(<<"domain">>, ExpectedConfiguration)]),
        {_, _, _, Body} = ?assertMatch({ok, 200, _, _},
            http_client:get(URL, #{}, <<>>, [{ssl_options, [{secure, false}]}])),
        ?assertMatch(ExpectedConfiguration, json_utils:decode(Body))
    end, Nodes).


configuration_endpoints_give_same_results(Config) ->
    Nodes = ?config(op_worker_nodes, Config),

    lists:foreach(fun(Node) ->
        {ok, Domain} = rpc:call(Node, provider_logic, get_domain, []),
        OldURL = str_utils:format("https://~s/configuration", [Domain]),
        NewURL = str_utils:format("https://~s/api/v3/oneprovider/configuration", [Domain]),

        {_, _, _, OldBody} = ?assertMatch({ok, 200, _, _},
            http_client:get(OldURL, #{}, <<>>, [{ssl_options, [{secure, false}]}])),
        {_, _, _, NewBody} = ?assertMatch({ok, 200, _, _},
            http_client:get(NewURL, #{}, <<>>, [{ssl_options, [{secure, false}]}])),
        ?assertEqual(json_utils:decode(OldBody), json_utils:decode(NewBody))
    end, Nodes).


provider_logic_should_correctly_resolve_nodes_to_connect(Config) ->
    % There are 2 providers and 3 nodes:
    %   * P1 -> 1 node
    %   * P2 -> 2 nodes
    Nodes = ?config(op_worker_nodes, Config),
    [Domain1Atom, Domain2Atom] = lists:usort([?GET_DOMAIN(N) || N <- Nodes]),
    Domain1Bin = atom_to_binary(Domain1Atom, utf8),
    Domain2Bin = atom_to_binary(Domain2Atom, utf8),
    Nodes1 = [N || N <- Nodes, ?GET_DOMAIN(N) =:= Domain1Atom],
    Nodes2 = [N || N <- Nodes, ?GET_DOMAIN(N) =:= Domain2Atom],
    P1 = initializer:domain_to_provider_id(?GET_DOMAIN(hd(Nodes1))),
    P2 = initializer:domain_to_provider_id(?GET_DOMAIN(hd(Nodes2))),
    IPs1 = [node_ip(N) || N <- Nodes1],
    IPs2 = [node_ip(N) || N <- Nodes2],
    IPsBin1 = [ip_to_binary(IP) || IP <- IPs1],
    IPsBin2 = [ip_to_binary(IP) || IP <- IPs2],

    % I) provider is registered using an IP address
    test_utils:mock_expect(Nodes, provider_logic, get_domain, fun(_, ProviderId) ->
        case ProviderId of
            P1 -> {ok, <<"172.17.0.10">>};
            P2 -> {ok, <<"172.17.0.74">>}
        end
    end),
    ?assertMatch({ok, [<<"172.17.0.10">>]}, rpc:call(hd(Nodes2), provider_logic, get_nodes, [P1])),
    ?assertMatch({ok, [<<"172.17.0.74">>]}, rpc:call(hd(Nodes1), provider_logic, get_nodes, [P2])),

    % II) provider is registered using a domain that resolves to IPs that host
    % the same service
    test_utils:mock_expect(Nodes, provider_logic, get_domain, fun(_, ProviderId) ->
        case ProviderId of
            P1 -> {ok, Domain1Bin};
            P2 -> {ok, Domain2Bin}
        end
    end),
    test_utils:mock_expect(Nodes, inet, getaddrs, fun(Domain, inet) ->
        case list_to_atom(Domain) of
            Domain1Atom -> {ok, IPs1};
            Domain2Atom -> {ok, IPs2}
        end
    end),

    ?assertMatch({ok, IPsBin1}, rpc:call(hd(Nodes2), provider_logic, get_nodes, [P1])),
    ?assertMatch({ok, IPsBin2}, rpc:call(hd(Nodes1), provider_logic, get_nodes, [P2])),

    % III) provider is registered using a domain, but the IPs that it resolves to
    % do not point to the same service (e.g. when reverse proxy is used).
    test_utils:mock_expect(Nodes, inet, getaddrs, fun(Domain, inet) ->
        case list_to_atom(Domain) of
            Domain1Atom -> {ok, [{192, 168, 200, 200}, {192, 168, 200, 201}]};
            Domain2Atom -> {ok, [{192, 168, 200, 202}]}
        end
    end),

    % in such case, provider should fall back to using domain for connections
    ?assertMatch({ok, [Domain1Bin]}, rpc:call(hd(Nodes2), provider_logic, get_nodes, [P1])),
    ?assertMatch({ok, [Domain2Bin]}, rpc:call(hd(Nodes1), provider_logic, get_nodes, [P2])).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        Nodes = ?config(op_worker_nodes, NewConfig),
        rpc:multicall(Nodes, application, set_env, [
            ?APP_NAME, compatible_op_versions, ["16.04-rc5"]
        ]),
        NewConfig
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(_Config) ->
    ok.

init_per_testcase(provider_logic_should_correctly_resolve_nodes_to_connect, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Nodes, inet, [unstick, passthrough]),
    ssl:start(),
    hackney:start(),

    % Disable caching of resolved nodes
    rpc:multicall(Nodes, application, set_env, [?APP_NAME, provider_nodes_cache_ttl, -1]),

    init_per_testcase(default, Config),
    % get_nodes/1 is mocked in initializer - unmock
    test_utils:mock_expect(Nodes, provider_logic, get_nodes, fun(ProviderId) ->
        meck:passthrough([ProviderId])
    end),
    Config;

init_per_testcase(Case, Config) when
    Case == deprecated_configuration_endpoint_is_served;
    Case == configuration_endpoints_give_same_results ->
    ssl:start(),
    hackney:start(),
    init_per_testcase(default, Config);

init_per_testcase(_Case, Config) ->
    initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config).


end_per_testcase(provider_logic_should_correctly_resolve_nodes_to_connect, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Nodes, [inet]),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case == deprecated_configuration_endpoint_is_served;
    Case == configuration_endpoints_give_same_results ->
    hackney:stop(),
    ssl:stop(),
    end_per_testcase(default, Config);

end_per_testcase(_Case, Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


connection_exists(Provider, PeerProvider) ->
    PeerProviderId = initializer:domain_to_provider_id(?GET_DOMAIN(PeerProvider)),
    IncomingSessId = get_provider_session_id(Provider, incoming, PeerProviderId),
    OutgoingSessId = get_provider_session_id(Provider, outgoing, PeerProviderId),

    IncomingSessExists = session_exists(Provider, IncomingSessId),
    % Outgoing session is always created but will fail to make any connection
    % in case of providers incompatibilities.
    OutgoingConnExists = case session_exists(Provider, OutgoingSessId) of
        true ->
            case list_session_connections(Provider, OutgoingSessId) of
                {ok, [_ | _]} -> true;
                _ -> false
            end;
        false ->
            false
    end,

    IncomingSessExists orelse OutgoingConnExists.


get_provider_session_id(Worker, Type, ProviderId) ->
    rpc:call(Worker, session_utils, get_provider_session_id,
        [Type, ProviderId]
    ).


session_exists(Provider, SessId) ->
    rpc:call(Provider, session, exists, [SessId]).


list_session_connections(Provider, SessId) ->
    rpc:call(Provider, session_connections, list, [SessId]).


-spec expected_configuration(node()) -> #{binary() := binary() | [binary()]}.
expected_configuration(Node) ->
    {ok, CompOzVersions} = test_utils:get_env(Node, ?APP_NAME, compatible_oz_versions),
    {ok, CompOpVersions} = test_utils:get_env(Node, ?APP_NAME, compatible_op_versions),
    {ok, CompOcVersions} = test_utils:get_env(Node, ?APP_NAME, compatible_oc_versions),
    {ok, TransferConfig} = test_utils:get_env(Node, rtransfer_link, transfer),
    RtransferPort = proplists:get_value(server_port, TransferConfig),
    {ok, Name} = rpc:call(Node, provider_logic, get_name, []),
    {ok, Domain} = rpc:call(Node, provider_logic, get_domain, []),

    #{
        <<"providerId">> => rpc:call(Node, oneprovider, get_id_or_undefined, []),
        <<"name">> => Name,
        <<"domain">> => Domain,
        <<"onezoneDomain">> => rpc:call(Node, oneprovider, get_oz_domain, []),
        <<"version">> => rpc:call(Node, oneprovider, get_version, []),
        <<"build">> => rpc:call(Node, oneprovider, get_build, []),
        <<"rtransferPort">> => RtransferPort,
        <<"compatibleOnezoneVersions">> => to_binaries(CompOzVersions),
        <<"compatibleOneproviderVersions">> => to_binaries(CompOpVersions),
        <<"compatibleOneclientVersions">> => to_binaries(CompOcVersions)
    }.


-spec to_binaries([string()]) -> [binary()].
to_binaries(Strings) ->
    [list_to_binary(S) || S <- Strings].


-spec ip_to_binary(inet:ip4_address()) -> binary().
ip_to_binary(IP) ->
    list_to_binary(inet:ntoa(IP)).


-spec node_ip(node()) -> inet:ip4_address().
node_ip(Node) ->
    {ok, [IP]} = inet:getaddrs(?GET_HOSTNAME(Node), inet),
    IP.
