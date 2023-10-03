%%%-------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------
%%% @doc
%%% CDMI tests utils.
%%% @end
%%%-------------------------------------
-module(cdmi_test_utils).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("../suites/cdmi/cdmi_test.hrl").

%% API
-export([
    do_request/5, cdmi_endpoint/2, user_2_token_header/0,
    get_tests_root_path/1, get_cdmi_endpoint/1, build_test_root_path/2
]).


%%%===================================================================
%%% API
%%%===================================================================


% Performs a single request using http_client
do_request(Node, CdmiSubPath, Method, Headers, Body) ->
    CaCerts = rpc:call(Node, https_listener, get_cert_chain_ders, []),
    Domain = opw_test_rpc:get_provider_domain(Node),
    Result = http_client:request(
        Method,
        cdmi_endpoint(Node, Domain) ++ CdmiSubPath,
        maps:from_list(Headers),
        Body,
        [
            {ssl_options, [{cacerts, CaCerts}, {hostname, Domain}]},
            {connect_timeout, timer:minutes(1)},
            {recv_timeout, timer:minutes(1)}
        ]
    ),
    case Result of
        {ok, RespCode, RespHeaders, RespBody} ->
            {ok, RespCode, (RespHeaders), RespBody};
        Other ->
            Other
    end.


cdmi_endpoint(Node, Domain) ->
    Port = get_https_server_port_str(Node),
    str_utils:format("https://~s~s/cdmi/", [Domain, Port]).


user_2_token_header() ->
    rest_test_utils:user_token_header(oct_background:get_user_access_token(user2)).


get_tests_root_path(Config) ->
    SpaceName = binary_to_list(oct_background:get_space_name(
        Config#cdmi_test_config.space_selector)
    ),
    RootName = node_cache:get(root_dir_name),
    RootPath = filename:join(SpaceName, RootName) ++ "/",
    RootPath.

build_test_root_path(Config, TestName) ->
    filename:join([get_tests_root_path(Config), TestName]).


get_cdmi_endpoint(Config) ->
    WorkerP1= oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
    Domain = opw_test_rpc:get_provider_domain(WorkerP1),
    cdmi_test_utils:cdmi_endpoint(WorkerP1, Domain).


%% @private
-spec get_https_server_port_str(node()) -> PortStr :: string().
get_https_server_port_str(Node) ->
    case get(port) of
        undefined ->
            {ok, Port} = test_utils:get_env(Node, ?APP_NAME, https_server_port),
            PortStr = case Port of
                443 -> "";
                _ -> ":" ++ integer_to_list(Port)
            end,
            put(port, PortStr),
            PortStr;
        Port ->
            Port
    end.