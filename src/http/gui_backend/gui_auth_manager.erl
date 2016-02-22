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
%% Authorizes a user via Global Registry. Upon success, returns the #auth{}
%% record that can be used to perform operations on behalf of the user.
%% @end
%%--------------------------------------------------------------------
-spec authenticate(SrlzdMcroon :: binary()) -> {ok, #auth{}} | {error, term()}.
authenticate(SrlzdMacaroon) ->
    try
        {ok, Macaroon} = macaroon:deserialize(SrlzdMacaroon),
        {ok, Caveats} = macaroon:third_party_caveats(Macaroon),
        DischMacaroons = lists:map(
            fun({_, CaveatId}) ->
                {ok, DM} = oz_users:authorize(CaveatId),
                DM
            end, Caveats),
        {ok, #auth{
            macaroon = SrlzdMacaroon,
            disch_macaroons = DischMacaroons}}
    catch
        T:M ->
            ?error_stacktrace("Cannot authorize user with macaroon ~p - ~p:~p",
                [SrlzdMacaroon, T, M]),
            {error, M}
    end.
