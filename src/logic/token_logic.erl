%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for manipulating tokens via Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(token_logic).
-author("Lukasz Opiola").

-include("graph_sync/provider_graph_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/aai/aai.hrl").

-export([
    verify_access_token/1,
    verify_identity_token/1
]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Verifies given access token in Onezone, and upon success, returns the auth
%% object, including subject's identity and caveats that were inscribed in the token.
%% @end
%%--------------------------------------------------------------------
-spec verify_access_token(#token_auth{}) -> {ok, aai:auth()} | errors:error().
verify_access_token(#token_auth{token = SerializedToken, peer_ip = PeerIp}) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_token, id = undefined, aspect = verify_access_token, scope = public},
        data = #{
            %% @TODO VFS-5914 add full auth_ctx information
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
%% Verifies given identity token in Onezone and returns the subject on success.
%% @end
%%--------------------------------------------------------------------
-spec verify_identity_token(tokens:serialized()) ->
    {ok, aai:subject()} | errors:error().
verify_identity_token(SerializedToken) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_token, id = undefined, aspect = verify_identity_token, scope = public},
        data = #{
            %% @TODO VFS-5914 add full auth_ctx information
            <<"token">> => SerializedToken
        }
    }),
    case Result of
        {error, _} = Error ->
            Error;
        {ok, #{<<"subject">> := Subject}} ->
            {ok, aai:deserialize_subject(Subject)}
    end.
