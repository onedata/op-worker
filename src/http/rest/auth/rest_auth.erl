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
-module(rest_auth).
-author("Tomasz Lichon").

-include("http/http_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([is_authorized/2, authenticate/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function authorizes user and inserts 'auth' field to
%% request's State
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | halt, req(), maps:map()}.
is_authorized(Req, State) ->
    case authenticate(Req) of
        {ok, Auth} ->
            {true, Req, State#{auth => Auth}};
        {error, not_found} ->
            {ok, Req2} = cowboy_req:reply(401, [], <<"">>, Req),
            {halt, Req2, State};
        {error, Error} ->
            ?debug("Authentication error ~p", [Error]),
            {{false, <<"authentication_error">>}, Req, State}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Authenticates user based on request headers.
%% @end
%%--------------------------------------------------------------------
-spec authenticate(Req :: req()) -> {ok, session:id()} | {error, term()}.
authenticate(Req) ->
    case resolve_auth(Req) of
        {error, Reason} ->
            {error, Reason};
        Auth ->
            case user_identity:get_or_fetch(Auth) of
                {ok, #document{value = Iden}} ->
                    case session_manager:reuse_or_create_rest_session(Iden, Auth) of
                        {ok, SessId} ->
                            {ok, SessId};
                        {error, {invalid_identity, _}} ->
                            user_identity:delete(Auth),
                            authenticate(Req)
                    end;
                Error ->
                    Error
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolves authorization carried by request (if any).
%% @end
%%--------------------------------------------------------------------
-spec resolve_auth(Req :: req()) -> user_identity:credentials() | {error, term()}.
resolve_auth(Req) ->
    case parse_macaroon_from_header(Req) of
        undefined -> {error, not_found};
        Macaroon -> #macaroon_auth{macaroon = Macaroon}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Parses macaroon from request headers, accepted headers:
%%  * Macaroon
%%  * X-Auth-Token
%% @end
%%--------------------------------------------------------------------
-spec parse_macaroon_from_header(Req :: req()) -> undefined | binary().
parse_macaroon_from_header(Req) ->
    case cowboy_req:header(<<"macaroon">>, Req) of
        {undefined, _} ->
            {ValueOrUndef, _} = cowboy_req:header(<<"x-auth-token">>, Req),
            ValueOrUndef;
        {Value, _} ->
            Value
    end.

