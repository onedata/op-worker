%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2015-2019 ACK CYFRONET AGH
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


-spec authenticate(#token_auth{}) -> {ok, aai:auth()} | errors:error().
authenticate(TokenAuth) ->
    try
        authenticate_insecure(TokenAuth)
    catch
        throw:Error ->
            Error;
        Type:Message ->
            ?error_stacktrace("Unexpected error in ~p:~p - ~p:~p", [
                ?MODULE, ?FUNCTION_NAME, Type, Message
            ]),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


-spec authenticate(cowboy_req:req(), cv_interface:interface(),
    data_access_caveats:policy()) -> {ok, aai:auth()} | errors:error().
authenticate(Req, Interface, DataCaveatsPolicy) ->
    case tokens:parse_access_token_header(Req) of
        undefined ->
            {ok, ?NOBODY};
        AccessToken ->
            {PeerIp, _} = cowboy_req:peer(Req),
            authenticate(#token_auth{
                token = AccessToken,
                peer_ip = PeerIp,
                interface = Interface,
                data_access_caveats_policy = DataCaveatsPolicy
            })
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec authenticate_insecure(#token_auth{}) -> {ok, aai:auth()} | no_return().
authenticate_insecure(TokenAuth) ->
    case auth_manager:verify(TokenAuth) of
        {ok, ?USER(UserId) = Auth, _TTL} ->
            Identity = #user_identity{user_id = UserId},
            case create_or_reuse_session(Identity, TokenAuth) of
                {ok, SessionId} ->
                    {ok, Auth#auth{session_id = SessionId}};
                {error, {invalid_identity, _}} ->
                    ?ERROR_UNAUTHORIZED
            end;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec create_or_reuse_session(session:identity(), #token_auth{}) ->
    {ok, session:id()} | {error, term()}.
create_or_reuse_session(Identity, #token_auth{interface = graphsync} = TokenAuth) ->
    session_manager:reuse_or_create_gui_session(Identity, TokenAuth);
create_or_reuse_session(Identity, #token_auth{interface = rest} = TokenAuth) ->
    session_manager:reuse_or_create_rest_session(Identity, TokenAuth).
