%%%-------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------
%%% @doc
%%% Graph sync tests utils.
%%% @end
%%%-------------------------------------
-module(gs_test_utils).
-author("Katarzyna Such").

-include("api_test_runner.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").
-include_lib("ctool/include/aai/aai.hrl").

-type gs_args() :: #gs_args{}.
-define(GS_RESP(Result), #gs_resp_graph{data = Result}).

%% API
-export([gs_request/2, connect_via_gs/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec gs_request(GsClient :: pid(), gs_args()) ->
    {ok, Result :: map()} | {error, term()}.
gs_request(GsClient, #gs_args{
    operation = Operation,
    gri = GRI,
    auth_hint = AuthHint,
    data = Data
}) ->
    case gs_client:graph_request(GsClient, GRI, Operation, Data, false, AuthHint) of
        {ok, ?GS_RESP(undefined)} ->
            ok;
        {ok, ?GS_RESP(Result)} ->
            {ok, Result};
        {error, _} = Error ->
            Error
    end.


-spec connect_via_gs(node(), aai:auth()) ->
    {ok, GsClient :: pid()} | errors:error().
connect_via_gs(Node, Client) ->
    {Auth, ExpIdentity} = case Client of
        ?NOBODY ->
            {undefined, ?SUB(nobody)};
        ?USER(UserId) ->
            TokenAuth = {token, oct_background:get_user_access_token(UserId)},
            {TokenAuth, ?SUB(user, UserId)}
    end,
    GsEndpoint = gs_endpoint(Node),
    GsSupportedVersions = opw_test_rpc:gs_protocol_supported_versions(Node),
    Opts = [{cacerts, opw_test_rpc:get_cert_chain_ders(krakow)}],

    case gs_client:start_link(GsEndpoint, Auth, GsSupportedVersions, fun(_) -> ok end, Opts) of
        {ok, GsClient, #gs_resp_handshake{identity = ExpIdentity}} ->
            {ok, GsClient};
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec gs_endpoint(node()) -> URL :: binary().
gs_endpoint(Node) ->
    Port = api_test_utils:get_https_server_port_str(Node),
    Domain = opw_test_rpc:get_provider_domain(Node),

    str_utils:join_as_binaries(
        ["wss://", Domain, Port, "/graph_sync/gui"],
        <<>>
    ).

