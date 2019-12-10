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
-include_lib("ctool/include/errors.hrl").

-export([
    verify_access_token/4,
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
-spec verify_access_token(
    auth_manager:access_token(),
    PeerIp :: undefined | ip_utils:ip(),
    Interface :: undefined | cv_interface:interface(),
    data_access_caveats:policy()
) ->
    {ok, aai:auth(), TTL :: undefined | time_utils:seconds()} | errors:error().
verify_access_token(AccessToken, PeerIp, Interface, DataAccessCaveatsPolicy) ->
    case tokens:deserialize(AccessToken) of
        {ok, #token{subject = ?SUB(user, UserId) = Subject} = Token} ->
            case provider_logic:has_eff_user(UserId) of
                false ->
                    ?ERROR_USER_NOT_SUPPORTED;
                true ->
                    VerificationPayload = build_verification_payload(
                        AccessToken, PeerIp, Interface,
                        DataAccessCaveatsPolicy
                    ),
                    verify_access_token(
                        Subject, tokens:get_caveats(Token),
                        VerificationPayload
                    )
            end;
        {error, _} = DeserializationError ->
            DeserializationError
    end.


%%--------------------------------------------------------------------
%% @doc
%% Verifies given provider identity token in Onezone and returns the
%% subject on success.
%% @end
%%--------------------------------------------------------------------
-spec verify_provider_identity_token(tokens:serialized()) ->
    {ok, aai:subject()} | errors:error().
verify_provider_identity_token(IdentityToken) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{
            type = od_token,
            id = undefined,
            aspect = verify_identity_token,
            scope = public
        },
        data = #{<<"token">> => IdentityToken}
    }),
    case Result of
        {ok, #{<<"subject">> := Subject}} ->
            {ok, aai:deserialize_subject(Subject)};
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec verify_access_token(
    Subject :: aai:subject(),
    Caveats :: [caveats:caveat()],
    VerificationPayload :: json_utils:json_term()
) ->
    {ok, aai:auth(), TTL :: undefined | time_utils:seconds()} | errors:error().
verify_access_token(Subject, Caveats, VerificationPayload) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{
            type = od_token,
            id = undefined,
            aspect = verify_access_token,
            scope = public
        },
        data = VerificationPayload
    }),
    case Result of
        {ok, #{<<"subject">> := AnsSubject} = Ans} ->
            Auth = #auth{
                subject = Subject = aai:deserialize_subject(AnsSubject),
                caveats = Caveats
            },
            {ok, Auth, maps:get(<<"ttl">>, Ans, undefined)};
        {error, _} = Error ->
            Error
    end.


%% @private
-spec build_verification_payload(
    auth_manager:access_token(),
    PeerIp :: undefined | ip_utils:ip(),
    Interface :: undefined | cv_interface:interface(),
    data_access_caveats:policy()
) ->
    json_utils:json_term().
build_verification_payload(AccessToken, PeerIp, Interface, DataAccessCaveatsPolicy) ->
    Json = #{
        <<"token">> => AccessToken,
        <<"peerIp">> => case PeerIp of
            undefined ->
                null;
            _ ->
                element(2, {ok, _} = ip_utils:to_binary(PeerIp))
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
