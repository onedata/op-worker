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

-include("op_logic.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([authenticate/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec authenticate(#token_auth{} | cowboy_req:req(), rest | gui,
    AcceptDataCaveats :: boolean()) -> {ok, aai:auth()} | errors:error().
authenticate(ReqOrTokenAuth, Interface, AcceptDataCaveats) ->
    try
        authenticate_insecure(ReqOrTokenAuth, Interface, AcceptDataCaveats)
    catch
        throw:Error ->
            Error;
        Type:Message ->
            ?error_stacktrace("Unexpected error in ~p:~p - ~p:~p", [
                ?MODULE, ?FUNCTION_NAME, Type, Message
            ]),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec authenticate_insecure(#token_auth{} | cowboy_req:req(), rest | gui,
    AcceptDataCaveats :: boolean()) -> {ok, aai:auth()} | no_return().
authenticate_insecure(#token_auth{
    token = SerializedToken,
    peer_ip = PeerIp
} = Credentials, Interface, AcceptDataCaveats) ->
    Caveats = get_caveats(SerializedToken),
    ensure_valid_caveats(Caveats, Interface, AcceptDataCaveats),

    % TODO VFS-5895 - return api errors from user_identity
    {ok, #document{value = Iden}} = user_identity:get_or_fetch(Credentials),
    case create_or_reuse_session(Iden, Credentials, Interface) of
        {ok, SessionId} ->
            {ok, #auth{
                subject = ?SUB(user, Iden#user_identity.user_id),
                caveats = Caveats,
                peer_ip = PeerIp,
                session_id = SessionId
            }};
        {error, {invalid_identity, _}} ->
            user_identity:delete(Credentials),
            authenticate_insecure(Credentials, Interface, AcceptDataCaveats)
    end;
authenticate_insecure(Req, Interface, AcceptDataCaveats) ->
    case tokens:parse_access_token_header(Req) of
        undefined ->
            {ok, ?NOBODY};
        AccessToken ->
            {PeerIp, _} = cowboy_req:peer(Req),
            TokenAuth = #token_auth{
                token = AccessToken,
                peer_ip = PeerIp
            },
            authenticate_insecure(TokenAuth, Interface, AcceptDataCaveats)
    end.


%% @private
-spec get_caveats(tokens:serialized()) ->
    [caveats:caveat()] | no_return().
get_caveats(SerializedToken) ->
    case tokens:deserialize(SerializedToken) of
        {ok, Token} ->
            tokens:get_caveats(Token);
        {error, _} = Error ->
            throw(Error)
    end.


%% @private
-spec ensure_valid_caveats([caveats:caveat()], gui | rest, boolean()) ->
    ok | no_return().
ensure_valid_caveats(Caveats, Interface, AcceptDataCaveats) ->
    token_utils:assert_interface_allowed(Caveats, Interface),
    case AcceptDataCaveats of
        true -> ok;
        false -> token_utils:assert_no_data_caveats(Caveats)
    end.


%% @private
-spec create_or_reuse_session(session:identity(), session:auth(), gui | rest) ->
    {ok, session:id()} | {error, term()}.
create_or_reuse_session(Identity, Credentials, gui) ->
    session_manager:reuse_or_create_gui_session(Identity, Credentials);
create_or_reuse_session(Identity, Credentials, rest) ->
    session_manager:reuse_or_create_rest_session(Identity, Credentials).
