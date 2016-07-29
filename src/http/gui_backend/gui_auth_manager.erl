%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles authentication of users redirected for login from GR.
%%% @end
%%%-------------------------------------------------------------------
-module(gui_auth_manager).
-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").
-include("proto/common/credentials.hrl").

-export([authenticate/1]).

%% ====================================================================
%% API
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Authorizes a user via Global Registry. Upon success, returns
%% the #token_auth{} record that can be used to perform operations
%% on behalf of the user.
%% @end
%%--------------------------------------------------------------------
-spec authenticate(Macaroon :: macaroon:macaroon()) ->
    {ok, #token_auth{}} | {error, term()}.
authenticate(Macaroon) ->
    try
        Caveats = macaroon:third_party_caveats(Macaroon),
        DischMacaroons = lists:map(
            fun({_, CaveatId}) ->
                {ok, SrlzdDM} = oz_users:authorize(CaveatId),
                {ok, DM} = token_utils:deserialize(SrlzdDM),
                DM
            end, Caveats),
        {ok, #token_auth{
            macaroon = Macaroon,
            disch_macaroons = DischMacaroons}}
    catch
        T:M ->
            ?error_stacktrace("Cannot authorize user with macaroon ~p - ~p:~p",
                [Macaroon, T, M]),
            {error, M}
    end.
