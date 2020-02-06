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
-author("Bartosz Walkowicz").

-include("graph_sync/provider_graph_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").

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
%% subject's identity and token ttl.
%% Ttl is the remaining time for which token will be valid or 'undefined'
%% if no time constraints were set for token.
%% @end
%%--------------------------------------------------------------------
-spec verify_access_token(
    auth_manager:access_token(),
    PeerIp :: undefined | ip_utils:ip(),
    Interface :: undefined | cv_interface:interface(),
    data_access_caveats:policy()
) ->
    {ok, aai:subject(), TTL :: undefined | time_utils:seconds()} | errors:error().
verify_access_token(AccessToken, PeerIp, Interface, DataAccessCaveatsPolicy) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{
            type = od_token,
            id = undefined,
            aspect = verify_access_token,
            scope = public
        },
        data = build_verification_payload(
            AccessToken, PeerIp, Interface,
            DataAccessCaveatsPolicy
        )
    }),
    case Result of
        {ok, #{<<"subject">> := Subject} = Ans} ->
            TokenTTL = utils:null_to_undefined(maps:get(<<"ttl">>, Ans, null)),
            {ok, aai:deserialize_subject(Subject), TokenTTL};
        {error, _} = Error ->
            Error
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
        data = #{
            <<"token">> => IdentityToken,
            <<"consumer">> => ?SUB(?ONEPROVIDER, oneprovider:get_id())
        }
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
