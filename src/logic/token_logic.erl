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
    verify_provider_identity_token/1
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Verifies given access token in Onezone, and upon success, returns the
%% auth object, which includes subject's identity and caveats, and its ttl.
%% Ttl is the remaining time for which token (and so auth) is valid or
%% 'undefined' if no time constraints were set for token.
%% @end
%%--------------------------------------------------------------------
-spec verify_access_token(#token_auth{}) ->
    {ok, aai:auth(), TTL :: undefined | non_neg_integer()} | errors:error().
verify_access_token(#token_auth{token = SerializedToken} = TokenAuth) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{
            type = od_token,
            id = undefined,
            aspect = verify_access_token,
            scope = public
        },
        data = build_verification_payload(TokenAuth)
    }),
    case Result of
        {error, _} = Error ->
            Error;
        {ok, #{<<"subject">> := Subject} = Ans} ->
            {ok, Token} = tokens:deserialize(SerializedToken),
            Auth = #auth{
                subject = aai:deserialize_subject(Subject),
                caveats = tokens:get_caveats(Token)
            },
            TokenTTL = gs_protocol:null_to_undefined(
                maps:get(<<"ttl">>, Ans, null)
            ),
            {ok, Auth, TokenTTL}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Verifies given provider identity token in Onezone and returns the
%% subject on success.
%% @end
%%--------------------------------------------------------------------
-spec verify_provider_identity_token(tokens:serialized()) ->
    {ok, aai:subject()} | errors:error().
verify_provider_identity_token(SerializedToken) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{
            type = od_token,
            id = undefined,
            aspect = verify_identity_token,
            scope = public
        },
        data = #{<<"token">> => SerializedToken}
    }),
    case Result of
        {error, _} = Error ->
            Error;
        {ok, #{<<"subject">> := Subject}} ->
            {ok, aai:deserialize_subject(Subject)}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_verification_payload(#token_auth{}) -> map().
build_verification_payload(#token_auth{
    token = SerializedToken,
    peer_ip = PeerIp,
    interface = Interface,
    data_access_caveats_policy = DataAccessCaveatsPolicy
}) ->
    Json = #{
        <<"token">> => SerializedToken,
        <<"peerIp">> => case PeerIp of
            undefined -> null;
            _ -> element(2, {ok, _} = ip_utils:to_binary(PeerIp))
        end,
        <<"allowDataAccessCaveats">> => case DataAccessCaveatsPolicy of
            allow_data_access_caveats -> true;
            disallow_data_access_caveats -> false
        end
    },
    case Interface of
        undefined ->
            Json;
        _ ->
            Json#{<<"interface">> => atom_to_binary(Interface, utf8)}
    end.
