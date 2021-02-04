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
%%% 1) it is started by calling 'init_session' with a unique offline job id
%%%    (opaque to this module), as resulting session will be associated with
%%%    that job, and valid user credentials.
%%% 2) while in use 'get_session_id' must be periodically called.
%%%    This ensures that offline credentials are appropriately renewed
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

-include("modules/auth/offline_access_manager.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

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

% Tells after what chunk of token TTL to try to renew offline credentials
-define(TOKEN_RENEWAL_THRESHOLD, 0.34).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_session(offline_job_id(), auth_manager:credentials()) ->
    {ok, session:id()} | errors:error().
init_session(_OfflineJobId, ?ROOT_CREDENTIALS) ->
    ?ERROR_TOKEN_SUBJECT_INVALID;
init_session(_OfflineJobId, ?GUEST_CREDENTIALS) ->
    ?ERROR_TOKEN_SUBJECT_INVALID;
init_session(OfflineJobId, TokenCredentials) ->
    Subject = auth_manager:get_subject(TokenCredentials),

    case acquire_offline_credentials(OfflineJobId, Subject, TokenCredentials) of
        {ok, OfflineCredentials} ->
            session_manager:reuse_or_create_offline_session(
                OfflineJobId, Subject, to_token_credentials(OfflineCredentials)
            );
        {error, _} = Error ->
            Error
    end.


-spec get_session_id(offline_job_id()) -> {ok, session:id()} | errors:error().
get_session_id(OfflineJobId) ->
    case reuse_or_renew_offline_credentials(OfflineJobId) of
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
-spec reuse_or_renew_offline_credentials(offline_job_id()) ->
    {ok, offline_access_credentials:record()} | errors:error().
reuse_or_renew_offline_credentials(OfflineJobId) ->
    Now = global_clock:timestamp_seconds(),

    case offline_access_credentials:get(OfflineJobId) of
        {ok, #document{value = #offline_access_credentials{
            user_id = UserId,
            next_renewal_threshold = NextRenewalThreshold,
            valid_until = ValidUntil
        } = OfflineCredentials}} when Now =< ValidUntil ->
            case Now > NextRenewalThreshold of
                true ->
                    case acquire_offline_credentials(
                        OfflineJobId, ?SUB(user, UserId),
                        to_token_credentials(OfflineCredentials)
                    ) of
                        {ok, NewOfflineCredentials} ->
                            {ok, NewOfflineCredentials};
                        ?ERROR_TOKEN_INVALID ->
                            ?ERROR_TOKEN_INVALID;
                        {error, _} ->
                            % If renewal failed return current credentials -
                            % there is still some time left to retry later
                            update_next_renewal_backoff(OfflineJobId, Now),
                            {ok, OfflineCredentials}
                    end;
                false ->
                    {ok, OfflineCredentials}
            end;
        {ok, _} ->
            % Offline access token expired
            ?ERROR_TOKEN_INVALID;
        ?ERROR_NOT_FOUND = Error ->
            Error
    end.


%% @private
-spec update_next_renewal_backoff(offline_job_id(), time:seconds()) ->
    ok.
update_next_renewal_backoff(OfflineJobId, Now) ->
    ?extract_ok(offline_access_credentials:update(OfflineJobId, fun(#offline_access_credentials{
        next_renewal_threshold = NextRenewalThreshold,
        next_renewal_backoff = NextRenewalBackoff
    } = OfflineCredentials) ->

        {ok, OfflineCredentials#offline_access_credentials{
            next_renewal_threshold = max(
                Now + NextRenewalBackoff,
                NextRenewalThreshold
            ),
            next_renewal_backoff = min(
                NextRenewalBackoff * ?OFFLINE_TOKEN_RENEWAL_BACKOFF_RATE,
                ?MAX_OFFLINE_TOKEN_RENEWAL_INTERVAL_SEC
            )
        }}
    end)).


%% @private
-spec acquire_offline_credentials(offline_job_id(), aai:subject(), auth_manager:token_credentials()) ->
    {ok, offline_access_credentials:record()} | errors:error().
acquire_offline_credentials(OfflineJobId, ?SUB(user, UserId), TokenCredentials) ->
    case auth_manager:acquire_offline_user_access_token(UserId, TokenCredentials) of
        {ok, OfflineAccessToken} ->
            AcquiredAt = global_clock:timestamp_seconds(),
            ValidUntil = token_valid_until(OfflineAccessToken),
            TokenTTL = ValidUntil - AcquiredAt,

            {ok, Doc} = offline_access_credentials:save(OfflineJobId, #offline_access_credentials{
                user_id = UserId,
                access_token = OfflineAccessToken,
                interface = auth_manager:get_interface(TokenCredentials),
                data_access_caveats_policy = auth_manager:get_data_access_caveats_policy(
                    TokenCredentials
                ),
                valid_until = ValidUntil,
                next_renewal_threshold = AcquiredAt + ceil(?TOKEN_RENEWAL_THRESHOLD * TokenTTL),
                next_renewal_backoff = ?MIN_OFFLINE_TOKEN_RENEWAL_INTERVAL_SEC
            }),
            {ok, Doc#document.value};
        {error, _} = Error ->
            Error
    end;
acquire_offline_credentials(_, _, _) ->
    ?ERROR_TOKEN_SUBJECT_INVALID.


%% @private
-spec token_valid_until(tokens:serialized()) -> time:seconds().
token_valid_until(OfflineAccessTokenBin) ->
    {ok, OfflineAccessToken} = tokens:deserialize(OfflineAccessTokenBin),
    caveats:infer_expiration_timestamp(tokens:get_caveats(OfflineAccessToken)).


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
