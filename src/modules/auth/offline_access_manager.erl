%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Provides utility functions to manage offline sessions. Such sessions are
%%% required to perform long-lasting operations/jobs that need to progress even
%%% if the ordering client has disconnected.
%%%
%%% The lifecycle of such session is as follows:
%%% 1) it is started by calling 'init_session' with unique offline job id,
%%%    as resulting session will be associated with that job, and valid user
%%%    credentials.
%%% 2) while in use 'get_session_id' must be periodically called.
%%%    This ensures that offline credentials are appropriately refreshed
%%%    (default expiration period is 7 days).
%%% 3) after finishing its tasks it is offline job's responsibility to terminate
%%%    session and remove no longer used docs by calling 'close_session'.
%%%    It will not be done automatically even if credentials expire.
%%%
%%% @see provider_offline_access
%%% @end
%%%-------------------------------------------------------------------
-module(offline_access_manager).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    init_session/2,
    get_session_id/1,
    close_session/1
]).

% An arbitrary Id provided by the calling module used to identify an offline job.
-type offline_job_id() :: binary().

-export_type([offline_job_id/0]).

-type error() :: {error, term()}.

% Tells after what chunk of token TTL to try to refresh token
-define(TOKEN_REFRESH_THRESHOLD, 0.34).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_session(offline_job_id(), auth_manager:credentials()) ->
    {ok, session:id()} | error().
init_session(_OfflineJobId, ?ROOT_CREDENTIALS) ->
    ?ERROR_TOKEN_SUBJECT_INVALID;
init_session(_OfflineJobId, ?GUEST_CREDENTIALS) ->
    ?ERROR_TOKEN_SUBJECT_INVALID;
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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_subject(auth_manager:token_credentials()) -> aai:subject().
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
            acquired_at = AcquiredAt,
            valid_until = ValidUntil
        } = OfflineCredentials}} when Now =< ValidUntil ->
            case Now > AcquiredAt + ?TOKEN_REFRESH_THRESHOLD * (ValidUntil - AcquiredAt) of
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
        {ok, _} ->
            ?ERROR_TOKEN_INVALID;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec to_token_credentials(offline_access_credentials:record()) ->
    auth_manager:token_credentials().
to_token_credentials(#offline_access_credentials{
    access_token = OfflineAccessToken,
    interface = Interface,
    data_access_caveats_policy = DataAccessCaveatsPolicy
}) ->
    auth_manager:build_token_credentials(
        OfflineAccessToken, provider_identity_token,
        undefined, Interface, DataAccessCaveatsPolicy
    ).
