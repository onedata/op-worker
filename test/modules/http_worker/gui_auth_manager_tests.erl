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

get_disch_macaroon(M, CaveatID) ->
    Key =
        case CaveatID of
            <<"TPCaveat1">> -> "Key1";
            <<"TPCaveat2">> -> "Key2";
            <<"TPCaveat3">> -> "Key3";
            <<"TPCaveat4">> -> "Key4"
        end,

    DM = macaroon:create("L", Key, CaveatID),
    macaroon:prepare_for_request(M, DM).

authorize_test() ->
    M = macaroon:create("a", "Key", "c"),
    M1 = macaroon:add_third_party_caveat(M, "Location1", "Key1", "TPCaveat1"),
    M2 = macaroon:add_third_party_caveat(M1, "Location2", "Key2", "TPCaveat2"),
    M3 = macaroon:add_third_party_caveat(M2, "Location3", "Key3", "TPCaveat3"),
    M4 = macaroon:add_third_party_caveat(M3, "Location4", "Key4", "TPCaveat4"),

    meck:new(oz_users),
    meck:expect(oz_users, authorize, fun(CaveatID) ->
        Macaroon = get_disch_macaroon(M4, CaveatID),
        {ok, Token} = macaroon:serialize(Macaroon),
        {ok, Token}
    end),

    {ok, #token_auth{macaroon = M4, disch_macaroons = DischMacaroons}} =
        gui_auth_manager:authenticate(M4),

    V = macaroon_verifier:create(),
    ?assertEqual(ok, macaroon_verifier:verify(V, M4, "Key", DischMacaroons)),

    ?assert(meck:validate(oz_users)),
    ok = meck:unload(oz_users).
