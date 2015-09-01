%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2015 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This library is used to authenticate users that have been redirected
%% from global registry.
%% @end
%% ===================================================================
-module(auth_utils).

-include_lib("ctool/include/logging.hrl").


%% ====================================================================
%% API functions
%% ====================================================================
-export([authorize/1]).

%%--------------------------------------------------------------------
%% @doc
%% Authorizes a user via Global Registry. Upon success, returns the root
%% macaroon and discharge macaroons that can be used to perform operations
%% on behalf of the user.
%% @end
%%--------------------------------------------------------------------
-spec authorize(RootMacaroon) ->
    {ok, RootMacaroon, DischargeMacaroons} | {error, term()} when
    RootMacaroon :: binary(), DischargeMacaroons :: [binary()].
authorize(SerializedMacaroon) ->
    try
        {ok, Macaroon} = macaroon:deserialize(SerializedMacaroon),
        {ok, Caveats} = macaroon:third_party_caveats(Macaroon),
        DischMacaroons = lists:map(
            fun({_, CaveatId}) ->
                {ok, DM} = gr_users:authorize(CaveatId),
                DM
            end, Caveats),
        {ok, SerializedMacaroon, DischMacaroons}
    catch
        T:M ->
            ?error_stacktrace("Cannot authorize user with macaroon ~p - ~p:~p",
                [SerializedMacaroon, T, M]),
            {error, M}
    end.
