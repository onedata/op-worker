%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2023 ACK CYFRONET AGH
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

%% API
-export([authenticate/2, authenticate_by_token/1]).

-type ctx() :: #{
    interface := cv_interface:interface(),
    data_access_caveats_policy := data_access_caveats:policy(),
    % TODO WRITEME
    allow_session_cookie => boolean()
}.
-export_type([ctx/0]).


-define(catch_auth_exceptions(__EXPR),
    try
        __EXPR
    catch Class:Reason:Stacktrace ->
        ?ERROR_UNAUTHORIZED(?examine_exception(Class, Reason, Stacktrace))
    end
).


%%%===================================================================
%%% API
%%%===================================================================


-spec authenticate(cowboy_req:req(), ctx()) ->
    {ok, aai:auth()} | errors:unauthorized_error().
authenticate(Req, AuthCtx) ->
    AuthResult = lists_utils:foldl_while(fun(AuthFun, Acc) ->
        case AuthFun(Req, AuthCtx) of
            false -> {cont, Acc};
            AuthResult -> {halt, AuthResult}
        end
    end, {ok, ?GUEST}, [
        fun try_authenticate_by_token/2,
        fun try_authenticate_by_session_cookie/2
    ]).


-spec authenticate_by_token(auth_manager:token_credentials()) ->
    {ok, aai:auth()} | errors:unauthorized_error().
authenticate_by_token(TokenCredentials) ->
    ?catch_auth_exceptions(do_authenticate_by_token(TokenCredentials)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec try_authenticate_by_token(cowboy_req:req(), ctx()) ->
    false | {ok, aai:auth()} | errors:unauthorized_error().
try_authenticate_by_token(Req, #{
    interface := Interface,
    data_access_caveats_policy := DataAccessCaveatsPolicy
}) ->
    case tokens:parse_access_token_header(Req) of
        undefined ->
            false;
        SubjectAccessToken ->
            {PeerIp, _} = cowboy_req:peer(Req),
            TokenCredentials = auth_manager:build_token_credentials(
                SubjectAccessToken, tokens:parse_consumer_token_header(Req),
                PeerIp, Interface, DataAccessCaveatsPolicy
            ),
            authenticate_by_token(TokenCredentials)
    end.


%% @private
-spec try_authenticate_by_session_cookie(cowboy_req:req(), ctx()) ->
    false | {ok, aai:auth()} | errors:unauthorized_error().
try_authenticate_by_session_cookie(Req, #{allow_session_cookie := true}) ->
    case page_acquire_session:get_session_cookie(Req) of
        undefined ->
            false;
        SessionId ->
            ?catch_auth_exceptions(do_authenticate_by_session_cookie(Req, SessionId))
    end;

try_authenticate_by_session_cookie(_Req, _AuthCtx) ->
    false.


%% @private
-spec do_authenticate_by_session_cookie(cowboy_req:req(), session:id()) ->
    {ok, aai:auth()} | errors:unauthorized_error() | no_return().
do_authenticate_by_session_cookie(Req, SessionId) ->
    case session:get(SessionId) of
        {ok, #document{value = #session{identity = Identity, credentials = TokenCredentials}}} ->
            {PeerIp, _} = cowboy_req:peer(Req),

            {ok, #auth{
                subject = Identity,
                caveats = ?check(auth_manager:get_caveats(TokenCredentials)),
                peer_ip = PeerIp,
                session_id = SessionId
            }};

        {error, not_found} ->
            ?ERROR_UNAUTHORIZED
    end.


%% @private
-spec do_authenticate_by_token(auth_manager:token_credentials()) ->
    {ok, aai:auth()} | errors:unauthorized_error() | no_return().
do_authenticate_by_token(TokenCredentials) ->
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
