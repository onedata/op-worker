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

%% API
-export([do_request/5, cdmi_endpoint/1]).


%%%===================================================================
%%% API
%%%===================================================================


% Performs a single request using http_client
do_request(Node, CdmiSubPath, Method, Headers, Body) ->
    CaCerts = rpc:call(Node, https_listener, get_cert_chain_pems, []),
    {ok, Domain} = test_utils:get_env(Node, ?APP_NAME, test_web_cert_domain),
    Result = http_client:request(
        Method,
        cdmi_endpoint(Node) ++ CdmiSubPath,
        maps:from_list(Headers),
        Body,
        [
            {ssl_options, [{cacerts, CaCerts}, {hostname, str_utils:to_binary(Domain)}]},
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


cdmi_endpoint(Node) ->
    Port = case get(port) of
        undefined ->
            {ok, P} = test_utils:get_env(Node, ?APP_NAME, https_server_port),
            PStr = case P of
                443 -> "";
                _ -> ":" ++ integer_to_list(P)
            end,
            put(port, PStr),
            PStr;
        P -> P
    end,
    Ip = test_utils:get_docker_ip(Node),
    string:join(["https://", str_utils:to_list(Ip), Port, "/cdmi/"], "").
