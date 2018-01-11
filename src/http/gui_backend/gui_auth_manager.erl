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
%% the #macaroon_auth{} record that can be used to perform operations
%% on behalf of the user.
%% @end
%%--------------------------------------------------------------------
-spec authenticate(MacaroonBin :: binary()) ->
    {ok, #macaroon_auth{}} | {error, term()}.
authenticate(MacaroonBin) ->
    try
        {ok, Macaroon} = onedata_macaroons:deserialize(MacaroonBin),
        Caveats = macaroon:third_party_caveats(Macaroon),
        DischMacaroonsBin = lists:map(
            fun({_, CaveatId}) ->
                {ok, DischMacaroonBin} = user_logic:authorize(CaveatId),
                DischMacaroonBin
            end, Caveats),
        {ok, #macaroon_auth{
            macaroon = MacaroonBin,
            disch_macaroons = DischMacaroonsBin}}
    catch
        T:M ->
            ?error_stacktrace("Cannot authorize user with macaroon ~p - ~p:~p",
                [MacaroonBin, T, M]),
            {error, M}
    end.
