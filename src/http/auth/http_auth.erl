%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper function for user authentication in REST/CDMI
%%% @end
%%%-------------------------------------------------------------------
-module(http_auth).
-author("Tomasz Lichon").

-include("op_logic.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([authenticate/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec authenticate(#token_auth{} | cowboy_req:req(), rest | gui,
    AcceptDataCaveats :: boolean()) -> {ok, aai:auth()} | {error, term()}.
authenticate(#token_auth{
    token = SerializedToken,
    peer_ip = PeerIp
} = Credentials, Interface, AcceptDataCaveats) ->
    Caveats = get_caveats(SerializedToken),
    ensure_valid_caveats(Caveats, Interface, AcceptDataCaveats),

    case user_identity:get_or_fetch(Credentials) of
        {ok, #document{value = #user_identity{user_id = UserId} = Identity}} ->
            case create_or_reuse_session(Identity, Credentials, Interface) of
                {ok, SessionId} ->
                    {ok, #auth{
                        subject = ?SUB(user, UserId),
                        caveats = Caveats,
                        peer_ip = PeerIp,
                        session_id = SessionId
                    }};
                {error, {invalid_identity, _}} ->
                    user_identity:delete(Credentials),
                    authenticate(Credentials, Interface, AcceptDataCaveats)
            end;
        {error, _} = Error ->
            Error
    end;
authenticate(Req, Type, AcceptDataCaveats) ->
    case tokens:parse_access_token_header(Req) of
        undefined ->
            {ok, ?NOBODY};
        AccessToken ->
            {PeerIp, _} = cowboy_req:peer(Req),
            TokenAuth = #token_auth{
                token = AccessToken,
                peer_ip = PeerIp
            },
            authenticate(TokenAuth, Type, AcceptDataCaveats)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_caveats(tokens:serialized()) ->
    [caveats:caveat()] | no_return().
get_caveats(SerializedToken) ->
    case tokens:deserialize(SerializedToken) of
        {ok, Token} ->
            tokens:get_caveats(Token);
        {error, _} ->
            throw(?ERROR_TOKEN_INVALID)
    end.


%% @private
-spec ensure_valid_caveats([caveats:caveat()], gui | rest, boolean()) ->
    ok | no_return().
ensure_valid_caveats(Caveats, Interface, AcceptDataCaveats) ->
    case token_caveats:is_interface_allowed(Caveats, Interface) of
        true -> ok;
        false -> throw(?ERROR_TOKEN_INVALID)
    end,
    case AcceptDataCaveats of
        true -> ok;
        false -> fslogic_caveats:assert_none_data_caveats(Caveats)
    end.


%% @private
-spec create_or_reuse_session(session:identity(), session:auth(), gui | rest) ->
    {ok, session:id()} | {error, term()}.
create_or_reuse_session(Identity, Credentials, gui) ->
    session_manager:reuse_or_create_gui_session(Identity, Credentials);
create_or_reuse_session(Identity, Credentials, rest) ->
    session_manager:reuse_or_create_rest_session(Identity, Credentials).
