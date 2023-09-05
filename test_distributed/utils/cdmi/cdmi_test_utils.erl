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
-export([do_request/5, cdmi_endpoint/2]).


%%%===================================================================
%%% API
%%%===================================================================


% Performs a single request using http_client
do_request(Node, CdmiSubPath, Method, Headers, Body) ->
    CaCerts = rpc:call(Node, https_listener, get_cert_chain_ders, []),
    Domain = oct_background:get_provider_domain(krakow),
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
    str_utils:format("https://~s~s/cdmi/", [str_utils:to_list(Domain), str_utils:to_list(Port)]).


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