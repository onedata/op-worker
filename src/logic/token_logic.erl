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
    get_shared_data/1,
    is_token_revoked/1,

    get_temporary_tokens_generation/1,

    create_identity_token/1,
    verify_access_token/5,
    verify_provider_identity_token/1
]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a new temporary identity token for this provider with given TTL.
%% @end
%%--------------------------------------------------------------------
-spec create_identity_token(ValidUntil :: time_utils:seconds()) ->
    {ok, tokens:serialized()} | errors:error().
create_identity_token(ValidUntil) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{
            type = od_token,
            id = undefined,
            aspect = {provider_temporary_token, oneprovider:get_id()},
            scope = private
        },
        data = #{
            <<"type">> => token_type:to_json(?IDENTITY_TOKEN),
            <<"caveats">> => [caveats:to_json(#cv_time{valid_until = ValidUntil})]
        }
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves token doc restricted to shared data by given TokenId.
%% @end
%%--------------------------------------------------------------------
-spec get_shared_data(od_token:id()) -> {ok, od_token:doc()} | errors:error().
get_shared_data(TokenId) ->
    gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_token, id = TokenId, aspect = instance, scope = shared},
        subscribe = true
    }).


-spec is_token_revoked(od_token:id()) -> {ok, boolean()} | errors:error().
is_token_revoked(TokenId) ->
    case get_shared_data(TokenId) of
        {ok, #document{value = #od_token{revoked = Revoked}}} ->
            {ok, Revoked};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves temporary tokens generation for specified user.
%% @end
%%--------------------------------------------------------------------
-spec get_temporary_tokens_generation(od_user:id()) ->
    {ok, temporary_token_secret:generation()} | errors:error().
get_temporary_tokens_generation(UserId) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = get,
        gri = #gri{
            type = temporary_token_secret,
            id = UserId,
            aspect = user,
            scope = shared
        },
        subscribe = true
    }),
    case Result of
        {ok, #document{value = #temporary_token_secret{generation = Generation}}} ->
            {ok, Generation};
        {error, _} = Error ->
            Error
    end.


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
    auth_manager:consumer_token(),
    PeerIp :: undefined | ip_utils:ip(),
    Interface :: undefined | cv_interface:interface(),
    data_access_caveats:policy()
) ->
    {ok, aai:subject(), TTL :: undefined | time_utils:seconds()} | errors:error().
verify_access_token(AccessToken, ConsumerToken, PeerIp, Interface, DataAccessCaveatsPolicy) ->
    Result = gs_client_worker:request(?ROOT_SESS_ID, #gs_req_graph{
        operation = create,
        gri = #gri{
            type = od_token,
            id = undefined,
            aspect = verify_access_token,
            scope = public
        },
        data = build_verification_payload(
            AccessToken, ConsumerToken,
            PeerIp, Interface, DataAccessCaveatsPolicy
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
            <<"consumerToken">> => op_worker_identity_token()
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
    auth_manager:consumer_token(),
    PeerIp :: undefined | ip_utils:ip(),
    Interface :: undefined | cv_interface:interface(),
    data_access_caveats:policy()
) ->
    json_utils:json_term().
build_verification_payload(AccessToken, ConsumerToken, PeerIp, Interface, DataAccessCaveatsPolicy) ->
    Json0 = #{
        <<"token">> => AccessToken,
        <<"peerIp">> => case PeerIp of
            undefined ->
                null;
            _ ->
                element(2, {ok, _} = ip_utils:to_binary(PeerIp))
        end,
        <<"serviceToken">> => op_worker_identity_token(),
        <<"allowDataAccessCaveats">> => case DataAccessCaveatsPolicy of
            allow_data_access_caveats -> true;
            disallow_data_access_caveats -> false
        end
    },
    Json1 = case Interface of
        undefined ->
            Json0;
        _ ->
            Json0#{<<"interface">> => atom_to_binary(Interface, utf8)}
    end,
    case ConsumerToken of
        undefined ->
            Json1;
        _ ->
            Json1#{<<"consumerToken">> => ConsumerToken}
    end.


%% @private
-spec op_worker_identity_token() -> tokens:serialized().
op_worker_identity_token() ->
    {ok, IdentityToken} = provider_auth:get_identity_token(),
    tokens:add_oneprovider_service_indication(?OP_WORKER, IdentityToken).