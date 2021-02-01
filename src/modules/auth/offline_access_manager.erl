%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Provides utility functions to manage offline sessions.
%%% @end
%%%-------------------------------------------------------------------
-module(offline_access_manager).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    init_session/2,
    get_session_id/1,
    close_session/1
]).
-export([ensure_consumer_token_up_to_date/1]).

-type offline_job_id() :: binary().

-export_type([offline_job_id/0]).

-type error() :: {error, term()}.

-define(EXPIRATION_RATIO, 0.34).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_session(offline_job_id(), auth_manager:credentials()) ->
    {ok, session:id()} | error().
init_session(OfflineJobId, TokenCredentials) ->
    Subject = get_subject(TokenCredentials),

    case offline_access_credentials:acquire(OfflineJobId, Subject, TokenCredentials) of
        {ok, #document{value = OfflineCredentials}} ->
            session_manager:reuse_or_create_offline_session(
                OfflineJobId, Subject, to_token_credentials(OfflineCredentials)
            );
        {error, _} = Error ->
            Error
    end.


-spec get_session_id(offline_job_id()) -> {ok, session:id()} | error().
get_session_id(OfflineJobId) ->
    case get_and_refresh_offline_credentials_if_near_expiration(OfflineJobId) of
        {ok, #offline_access_credentials{user_id = UserId} = OfflineCredentials} ->
            session_manager:reuse_or_create_offline_session(
                OfflineJobId, ?SUB(user, UserId), to_token_credentials(OfflineCredentials)
            );
        {error, _} = Error ->
            Error
    end.


-spec close_session(offline_job_id()) -> ok.
close_session(OfflineJobId) ->
    ok = offline_access_credentials:remove(OfflineJobId),
    ok = session_manager:terminate_session(OfflineJobId).


-spec ensure_consumer_token_up_to_date(auth_manager:credentials()) ->
    auth_manager:credentials().
ensure_consumer_token_up_to_date(TokenCredentials) ->
    AccessToken = auth_manager:get_access_token(TokenCredentials),
    {ok, ProviderIdentityToken} = provider_auth:acquire_identity_token(),

    auth_manager:update_client_tokens(
        TokenCredentials,
        AccessToken,
        ProviderIdentityToken
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_subject(auth_manager:credentials()) -> aai:subject().
get_subject(TokenCredentials) ->
    AccessToken = auth_manager:get_access_token(TokenCredentials),
    {ok, #token{subject = Subject}} = tokens:deserialize(AccessToken),
    Subject.


%% @private
-spec get_and_refresh_offline_credentials_if_near_expiration(offline_job_id()) ->
    {ok, offline_access_credentials:record()} | error().
get_and_refresh_offline_credentials_if_near_expiration(OfflineJobId) ->
    Now = global_clock:timestamp_seconds(),

    case offline_access_credentials:get(OfflineJobId) of
        {ok, #document{value = #offline_access_credentials{
            user_id = UserId,
            acquirement_timestamp = AcquiredAt,
            valid_until = ValidUntil
        } = OfflineCredentials}} when Now =< ValidUntil ->
            case Now > AcquiredAt + ?EXPIRATION_RATIO * (ValidUntil - AcquiredAt) of
                true ->
                    case offline_access_credentials:acquire(
                        OfflineJobId, ?SUB(user, UserId),
                        to_token_credentials(OfflineCredentials)
                    ) of
                        {ok, #document{value = NewOfflineCredentials}} ->
                            {ok, NewOfflineCredentials};
                        ?ERROR_TOKEN_INVALID ->
                            ?ERROR_TOKEN_INVALID;
                        {error, _} ->
                            % If refresh failed return current credentials -
                            % there is still some time left to retry later
                            {ok, OfflineCredentials}
                    end;
                false ->
                    {ok, OfflineCredentials}
            end;
        {ok, QWE} ->
            ?error("ZXC: ~p~n~p", [Now, QWE]),
            ?ERROR_TOKEN_INVALID;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec to_token_credentials(offline_access_credentials:record()) -> auth_manager:credentials().
to_token_credentials(#offline_access_credentials{
    access_token = OfflineAccessToken,
    interface = Interface,
    data_access_caveats_policy = DataAccessCaveatsPolicy
}) ->
    {ok, ProviderIdentityToken} = provider_auth:acquire_identity_token(),

    auth_manager:build_token_credentials(
        OfflineAccessToken, ProviderIdentityToken,
        undefined, Interface, DataAccessCaveatsPolicy
    ).
