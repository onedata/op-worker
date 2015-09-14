%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2015 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Unit tests for gui_auth_manager module.
%% @end
%% ===================================================================
-module(gui_auth_manager_tests).
-author("Lukasz Opiola").

-include_lib("eunit/include/eunit.hrl").
-include("proto/common/credentials.hrl").

% Root macaroon (serialized)
-define(SRLZD_MACAROON, <<"srlzd_macaroon">>).
% Root macaroon
-define(MACAROON, <<"macaroon">>).
% List of third party caveats
-define(THIRD_PARTY_CAVEATS, [
    {<<"Location1">>, <<"TPCaveat1">>},
    {<<"Location2">>, <<"TPCaveat2">>},
    {<<"Location3">>, <<"TPCaveat3">>},
    {<<"Location4">>, <<"TPCaveat4">>}
]).
% All disch macaroons
-define(ALL_DISCH_MACAROONS, [
    <<"DischMacaroon1">>,
    <<"DischMacaroon2">>,
    <<"DischMacaroon3">>,
    <<"DischMacaroon4">>
]).


get_disch_macaroon(CaveatID) ->
    case CaveatID of
        <<"TPCaveat1">> -> <<"DischMacaroon1">>;
        <<"TPCaveat2">> -> <<"DischMacaroon2">>;
        <<"TPCaveat3">> -> <<"DischMacaroon3">>;
        <<"TPCaveat4">> -> <<"DischMacaroon4">>
    end.


authorize_test_() ->
    {setup,
        % Setup fun
        fun() ->
            % Set up the mocks
            meck:new(macaroon),
            meck:expect(macaroon, deserialize,
                fun(?SRLZD_MACAROON) -> {ok, ?MACAROON} end),
            meck:expect(macaroon, third_party_caveats,
                fun(?MACAROON) -> {ok, ?THIRD_PARTY_CAVEATS} end),
            meck:new(gr_users),
            meck:expect(gr_users, authorize,
                fun(CaveatID) -> {ok, get_disch_macaroon(CaveatID)} end)
        end,
        % Teardown fun
        fun(_) ->
            % Validate mocks
            ?assert(meck:validate(gr_users)),
            ok = meck:unload(gr_users),
            ?assert(meck:validate(macaroon)),
            ok = meck:unload(macaroon)
        end,
        % Test funs
        fun() ->
            CorrectAuth = #auth{
                macaroon = ?SRLZD_MACAROON, disch_macaroons = ?ALL_DISCH_MACAROONS},
            ?assertEqual({ok, CorrectAuth}, gui_auth_manager:authorize(?SRLZD_MACAROON))
        end
    }.
