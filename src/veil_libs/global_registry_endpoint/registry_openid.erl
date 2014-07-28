%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Jul 2014 18:18
%%%-------------------------------------------------------------------
-module(registry_openid).
-author("Rafal Slota").

-include_lib("ctool/include/logging.hrl").

%% API
-export([client_verify/2]).


-spec client_verify(UserGID :: binary(), TokenHash :: binary()) -> boolean() | no_return().
client_verify(_, _) ->
    true;
client_verify(undefined, _) ->
    false;
client_verify(_, undefined) ->
    false;
client_verify(UserGID, TokenHash) when is_binary(UserGID), is_binary(TokenHash) ->
    case global_registry:provider_request(post, "openid/client/verify",
            #{<<"userId">> => vcn_utils:ensure_binary(UserGID), <<"secret">> => base64:encode(vcn_utils:ensure_binary(TokenHash))}) of
        {ok, #{<<"verified">> := VerifyStatus}} ->
            VerifyStatus;
        {error, Reason} ->
            ?error("Cannot verify user (~p) authentication due to: ~p", [UserGID, Reason]),
            throw({unable_to_authenticate, Reason})
    end.