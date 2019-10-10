%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating tokens via Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(token_logic).
-author("Lukasz Opiola").

-include("graph_sync/provider_graph_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/aai/aai.hrl").

-export([
    preauthorize/1,
    verify_identity/1
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Pre-authorizes subject - asks Onezone to verify given token, and upon success,
%% returns the auth object, including subject's identity and caveats that were
%% inscribed in the token.
%% @end
%%--------------------------------------------------------------------
-spec preauthorize(#token_auth{}) -> {ok, aai:auth()} | errors:error().
preauthorize(#token_auth{token = SerializedToken, peer_ip = PeerIp}) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_token, id = undefined, aspect = preauthorize, scope = public},
        data = #{
            <<"token">> => SerializedToken,
            <<"peerIp">> => case PeerIp of
                undefined -> null;
                _ -> element(2, {ok, _} = ip_utils:to_binary(PeerIp))
            end
        }
    }),
    case Result of
        {error, _} = Error ->
            Error;
        {ok, #{<<"subject">> := Subject}} ->
            {ok, Token} = tokens:deserialize(SerializedToken),
            {ok, #auth{
                subject = aai:deserialize_subject(Subject),
                caveats = tokens:get_caveats(Token)
            }}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Verifies given provider in Onezone based on its identity token.
%% @end
%%--------------------------------------------------------------------
-spec verify_identity(tokens:serialized()) ->
    {ok, aai:subject()} | errors:error().
verify_identity(SerializedToken) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_token, id = undefined, aspect = preauthorize, scope = public},
        data = #{
            <<"token">> => SerializedToken
        }
    }),
    case Result of
        {error, _} = Error ->
            Error;
        {ok, #{<<"subject">> := Subject}} ->
            {ok, aai:deserialize_subject(Subject)}
    end.
