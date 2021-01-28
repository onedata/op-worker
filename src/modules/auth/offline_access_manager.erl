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

-type offline_job_id() :: binary().

-export_type([offline_job_id/0]).

-type error() :: {error, term()}.

-define(CREDENTIALS_EXPIRATION_RATIO, 0.34).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_session(offline_job_id(), auth_manager:credentials()) ->
    {ok, session:id()} | error().
init_session(OfflineJobId, TokenCredentials) ->
    Subject = get_subject(TokenCredentials),

    case offline_access_credentials:acquire(OfflineJobId, Subject, TokenCredentials) of
        {ok, OfflineTokenCredentials} ->
            session_manager:reuse_or_create_offline_session(
                OfflineJobId, Subject, OfflineTokenCredentials
            );
        {error, _} = Error ->
            Error
    end.


-spec get_session_id(offline_job_id()) -> {ok, session:id()} | error().
get_session_id(OfflineJobId) ->
    case session:get(OfflineJobId) of
        {ok, #document{value = Session}} ->
            refresh_offline_token_if_near_expiration(OfflineJobId, Session);
        ?ERROR_NOT_FOUND ->
            case offline_access_credentials:get(OfflineJobId) of
                {ok, OfflineTokenCredentials} ->
                    init_session(OfflineJobId, OfflineTokenCredentials);
                {error, _} = Error ->
                    Error
            end
    end.


-spec close_session(offline_job_id()) -> ok.
close_session(OfflineAccessJobId) ->
    ok = offline_access_credentials:remove(OfflineAccessJobId),
    ok = session_manager:terminate_session(OfflineAccessJobId).


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
-spec refresh_offline_token_if_near_expiration(offline_job_id(), session:record()) ->
    {ok, session:id()} | error().
refresh_offline_token_if_near_expiration(OfflineJobId, #session{
    credentials = TokenCredentials
} = Session) ->
    Now = global_clock:timestamp_seconds(),
    CredentialsValidUntil = credentials_valid_until(TokenCredentials),

    case Now > ?CREDENTIALS_EXPIRATION_RATIO * CredentialsValidUntil of
        true -> refresh_offline_token(OfflineJobId, Session);
        false -> {ok, OfflineJobId}
    end.


%% @private
-spec credentials_valid_until(auth_manager:credentials()) -> non_neg_integer().
credentials_valid_until(TokenCredentials) ->
    AccessTokenBin = auth_manager:get_access_token(TokenCredentials),
    {ok, AccessToken} = tokens:deserialize(AccessTokenBin),

    lists:foldl(fun
        (#cv_time{valid_until = ValidUntil}, undefined) -> ValidUntil;
        (#cv_time{valid_until = ValidUntil}, Acc) -> min(ValidUntil, Acc)
    end, undefined, caveats:filter([cv_time], tokens:get_caveats(AccessToken))).


%% @private
-spec refresh_offline_token(offline_job_id(), session:record()) ->
    {ok, session:id()} | error().
refresh_offline_token(OfflineJobId, #session{
    identity = Subject,
    credentials = Credentials,
    watcher = SessionWatcher
}) ->
    case offline_access_credentials:acquire(OfflineJobId, Subject, Credentials) of
        {ok, OfflineTokenCredentials} ->
            ok = incoming_session_watcher:update_credentials(
                SessionWatcher,
                auth_manager:get_access_token(OfflineTokenCredentials),
                auth_manager:get_consumer_token(OfflineTokenCredentials)
            ),
            {ok, OfflineJobId};
        ?ERROR_TOKEN_INVALID ->
            ?ERROR_TOKEN_INVALID;
        {error, _} ->
            % Ignore other errors - token refresh will be retried on next call
            % to `get_session_id`
            {ok, OfflineJobId}
    end.
