%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper function for user authentication in REST/CDMI and gui.
%%% @end
%%%-------------------------------------------------------------------
-module(http_auth).
-author("Tomasz Lichon").
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([authenticate/1, authenticate/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec authenticate(
    cowboy_req:req(),
    cv_interface:interface(),
    data_access_caveats:policy()
) ->
    {ok, aai:auth()} | errors:unauthorized_error().
authenticate(Req, Interface, DataAccessCaveatsPolicy) ->
    case tokens:parse_access_token_header(Req) of
        undefined ->
            {ok, ?GUEST};
        SubjectAccessToken ->
            {PeerIp, _} = cowboy_req:peer(Req),
            TokenCredentials = auth_manager:build_token_credentials(
                SubjectAccessToken, tokens:parse_consumer_token_header(Req),
                PeerIp, Interface, DataAccessCaveatsPolicy
            ),
            authenticate(TokenCredentials)
    end.


-spec authenticate(auth_manager:token_credentials()) ->
    {ok, aai:auth()} | errors:unauthorized_error().
authenticate(TokenCredentials) ->
    try
        authenticate_insecure(TokenCredentials)
    catch
        throw:Error ->
            ?ERROR_UNAUTHORIZED(Error);
        Type:Message ->
            ?error_stacktrace("Unexpected error in ~p:~p - ~p:~p", [
                ?MODULE, ?FUNCTION_NAME, Type, Message
            ]),
            ?ERROR_UNAUTHORIZED(?ERROR_INTERNAL_SERVER_ERROR)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec authenticate_insecure(auth_manager:token_credentials()) ->
    {ok, aai:auth()} | errors:unauthorized_error() | no_return().
authenticate_insecure(TokenCredentials) ->
    case auth_manager:verify_credentials(TokenCredentials) of
        {ok, #auth{subject = Identity} = Auth, _TokenValidUntil} ->
            Interface = auth_manager:get_interface(TokenCredentials),
            case create_or_reuse_session(Identity, TokenCredentials, Interface) of
                {ok, SessionId} ->
                    {ok, Auth#auth{session_id = SessionId}};
                {error, {invalid_identity, _}} ->
                    %% TODO VFS-5895
                    ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_SUBJECT_INVALID)
            end;
        {error, _} = Error ->
            ?ERROR_UNAUTHORIZED(Error)
    end.


%% @private
-spec create_or_reuse_session(
    Identity :: aai:subject(),
    auth_manager:token_credentials(),
    Interface :: graphsync | rest
) ->
    {ok, session:id()} | {error, term()}.
create_or_reuse_session(Identity, TokenCredentials, graphsync) ->
    session_manager:reuse_or_create_gui_session(Identity, TokenCredentials);
create_or_reuse_session(Identity, TokenCredentials, rest) ->
    session_manager:reuse_or_create_rest_session(Identity, TokenCredentials).
