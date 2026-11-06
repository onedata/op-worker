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

-include("http/http_auth.hrl").
-include("middleware/middleware.hrl").

%% API
-export([authenticate/2, authenticate_by_token/1]).

-type ctx() :: #http_auth_ctx{}.

-export_type([ctx/0]).


-define(catch_auth_exceptions(__EXPR),
    try
        __EXPR
    catch Class:Reason:Stacktrace ->
        ?ERR_UNAUTHORIZED(?err_ctx(), ?examine_exception(Class, Reason, Stacktrace))
    end
).


%%%===================================================================
%%% API
%%%===================================================================


-spec authenticate(cowboy_req:req(), ctx()) ->
    {ok, aai:auth()} | od_error:auth_error().
authenticate(Req, #http_auth_ctx{
    interface = Interface,
    data_access_caveats_policy = DataAccessCaveatsPolicy
}) ->
    case tokens:parse_access_token_header(Req) of
        undefined ->
            {ok, ?GUEST};
        SubjectAccessToken ->
            {PeerIp, _} = cowboy_req:peer(Req),
            TokenCredentials = auth_manager:build_token_credentials(
                SubjectAccessToken, tokens:parse_consumer_token_header(Req),
                PeerIp, Interface, DataAccessCaveatsPolicy
            ),
            authenticate_by_token(TokenCredentials)
    end.


-spec authenticate_by_token(auth_manager:token_credentials()) ->
    {ok, aai:auth()} | od_error:auth_error().
authenticate_by_token(TokenCredentials) ->
    ?catch_auth_exceptions(do_authenticate_by_token(TokenCredentials)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec do_authenticate_by_token(auth_manager:token_credentials()) ->
    {ok, aai:auth()} | od_error:auth_error() | no_return().
do_authenticate_by_token(TokenCredentials) ->
    case auth_manager:verify_credentials(TokenCredentials) of
        {ok, #auth{subject = Identity} = Auth, _TokenValidUntil} ->
            Interface = auth_manager:get_interface(TokenCredentials),
            case create_or_reuse_session(Identity, TokenCredentials, Interface) of
                {ok, SessionId} ->
                    {ok, Auth#auth{session_id = SessionId}};
                {error, {invalid_identity, _}} ->
                    %% TODO VFS-5895
                    ?ERR_UNAUTHORIZED(?err_ctx(), ?ERR_TOKEN_SUBJECT_INVALID(?err_ctx()))
            end;
        {error, _} = Error ->
            ?ERR_UNAUTHORIZED(?err_ctx(), Error)
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
