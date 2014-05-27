%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of openid_utils, using eunit tests.
%% @end
%% ===================================================================
-module(openid_utils_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("include/veil_modules/control_panel/openid_utils.hrl").
-include_lib("include/logging.hrl").

-define(mock_xrds_file,
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
    <xrds:XRDS xmlns:xrds=\"xri://$xrds\" xmlns=\"xri://$xrd*($v*2.0)\">
        <XRD>
            <Service priority=\"0\">
                <Type>http://specs.openid.net/auth/2.0/server</Type>
                <URI>https://openid.plgrid.pl/server</URI>
            </Service>
        </XRD>
    </xrds:XRDS>").

-define(hostname, "some.host.name.com").
-define(redirect_params, "?x=12433425jdfg").

-define(correct_request_url,
    <<"https://openid.plgrid.pl/server?openid.mode=checkid_setup&openid.ns=http://specs.openid.net/auth/2.0&",
    "openid.return_to=https://", ?hostname, "/validate_login", ?redirect_params,
    "&openid.claimed_id=http://specs.openid.net/auth/2.0/identifier_select&",
    "openid.identity=http://specs.openid.net/auth/2.0/identifier_select&openid.realm=https://", ?hostname, "&",
    "openid.sreg.required=nickname,email,fullname&openid.ns.ext1=http://openid.net/srv/ax/1.0&openid.ext1.mode=fetch_request&",
    "openid.ext1.type.dn1=http://openid.plgrid.pl/certificate/dn1&openid.ext1.type.dn2=http://openid.plgrid.pl/certificate/dn2&",
    "openid.ext1.type.dn3=http://openid.plgrid.pl/certificate/dn3&openid.ext1.type.teams=http://openid.plgrid.pl/userTeamsXML&",
    "openid.ext1.if_available=dn1,dn2,dn3,teams">>).


% These tests check reactions to theoretical responses when requesting 
% endpoint information from OpenID provider.
get_url_test_() ->
    {foreach,
        fun() ->
            meck:new(gui_utils),
            meck:new(lager)
        end,
        fun(_) ->
            ok = meck:unload(gui_utils),
            ok = meck:unload(lager)
        end,
        [
            {"URL correctness",
                fun() ->
                    meck:expect(gui_utils, https_get, fun(_, _) -> {ok, <<?mock_xrds_file>>} end),
                    ?assertEqual(?correct_request_url, openid_utils:get_login_url(<<?hostname>>, <<?redirect_params>>)),
                    ?assert(meck:validate(gui_utils))
                end},

            {"No 200 code case",
                fun() ->
                    meck:expect(gui_utils, https_get, fun(_, _) -> {error, nxdomain} end),
                    meck:expect(lager, log, fun(error, _, _) -> ok end),
                    ?assertEqual({error, endpoint_unavailable}, openid_utils:get_login_url(<<?hostname>>, <<?redirect_params>>)),
                    ?assert(meck:validate(gui_utils)),
                    ?assert(meck:validate(lager))
                end},

            {"Connection refused case",
                fun() ->
                    meck:expect(gui_utils, https_get, fun(_, _) -> {error, econnrefused} end),
                    meck:expect(lager, log, fun(error, _, _) -> ok end),
                    ?assertEqual({error, endpoint_unavailable}, openid_utils:get_login_url(<<?hostname>>, <<?redirect_params>>)),
                    ?assert(meck:validate(gui_utils)),
                    ?assert(meck:validate(lager))
                end}
        ]}.


% These tests check reactions to theoretical responses from OpenID provider when validating login.
validate_login_test_() ->
    {foreach,
        fun() ->
            meck:new(gui_utils),
            meck:new(lager)
        end,
        fun(_) ->
            meck:unload(gui_utils),
            meck:unload(lager)
        end,
        [
            {"Login valid case",
                fun() ->
                    meck:expect(gui_utils, https_post, fun("mock", _, "me") -> {ok, <<?valid_auth_info>>} end),
                    ?assertEqual(ok, openid_utils:validate_openid_login({"mock", "me"})),
                    ?assert(meck:validate(gui_utils))
                end},

            {"Login invalid case",
                fun() ->
                    meck:expect(gui_utils, https_post, fun("mock", _, "me") -> {ok, <<"is_valid: false\n">>} end),
                    meck:expect(lager, log, fun(alert, _, _) -> ok end),
                    ?assertEqual({error, auth_invalid}, openid_utils:validate_openid_login({"mock", "me"})),
                    ?assert(meck:validate(gui_utils)),
                    ?assert(meck:validate(lager))
                end},

            {"No 200 code case",
                fun() ->
                    meck:expect(gui_utils, https_post, fun("mock", _, "me") -> {error, nxdomain} end),
                    meck:expect(lager, log, fun(error, _, _) -> ok end),
                    ?assertEqual({error, no_connection}, openid_utils:validate_openid_login({"mock", "me"})),
                    ?assert(meck:validate(gui_utils)),
                    ?assert(meck:validate(lager))
                end},

            {"Connection refused case",
                fun() ->
                    meck:expect(gui_utils, https_post, fun("mock", _, "me") -> {error, econnrefused} end),
                    meck:expect(lager, log, fun(error, _, _) -> ok end),
                    ?assertEqual({error, no_connection}, openid_utils:validate_openid_login({"mock", "me"})),
                    ?assert(meck:validate(gui_utils)),
                    ?assert(meck:validate(lager))
                end}
        ]}.


% These tests check functions that parse user info from OpenID provider's response body
% and reactions to various error cases.
parameter_processing_test_() ->
    {foreach,
        fun() ->
            meck:new(wf),
            meck:new(gui_utils),
            meck:new(lager)
        end,
        fun(_) ->
            meck:unload(wf),
            meck:unload(gui_utils),
            meck:unload(lager)
        end,
        [{"POST request body correctness",
            fun() ->
                meck:expect(gui_utils, https_get, fun(_, _) -> {ok, <<?mock_xrds_file>>} end),
                meck:expect(gui_utils, to_list, fun(Arg) -> to_list(Arg) end),
                meck:expect(gui_utils, to_binary, fun(Arg) -> to_binary(Arg) end),
                KeyValueList = lists:zip(openid_keys(), openid_values()),
                FullKeyValueList = KeyValueList ++ [
                    {<<"openid.signed">>, <<"op_endpoint,claimed_id,identity,return_to,response_nonce,assoc_handle,",
                    "ns.ext1,ns.sreg,ext1.mode,ext1.type.dn1,ext1.value.dn1,ext1.type.teams,",
                    "ext1.value.teams,sreg.nickname,sreg.email,sreg.fullname">>}
                ],
                meck:expect(wf, q,
                    fun(Key) ->
                        proplists:get_value(Key, FullKeyValueList)
                    end),

                meck:expect(wf, url_encode, fun(Key) -> Key end),
                CorrectRequest = ?openid_check_authentication_mode ++
                    lists:foldl(
                        fun({Key, Value}, Acc) ->
                            Acc ++ "&" ++ binary_to_list(Key) ++ "=" ++ binary_to_list(Value)
                        end, "", FullKeyValueList),
                Server = binary_to_list(proplists:get_value(<<"openid.op_endpoint">>, FullKeyValueList)),
                ?assertEqual({Server, CorrectRequest}, openid_utils:prepare_validation_parameters()),
                ?assert(meck:validate(gui_utils)),
                ?assert(meck:validate(wf))
            end},

            {"Missing 'signed' parameter case",
                fun() ->
                    meck:expect(wf, q,
                        fun(<<"openid.signed">>) -> undefined;
                            (<<"openid.op_endpoint">>) -> undefined
                        end),
                    meck:expect(lager, log, fun(error, _, _) -> ok end),
                    meck:expect(gui_utils, to_list, fun(Arg) -> to_list(Arg) end),
                    meck:expect(gui_utils, to_binary, fun(Arg) -> to_binary(Arg) end),

                    ?assertEqual({error, invalid_request},
                        openid_utils:prepare_validation_parameters()),
                    ?assert(meck:validate(wf)),
                    ?assert(meck:validate(gui_utils)),
                    ?assert(meck:validate(lager))
                end},

            {"Missing parameters case",
                fun() ->
                    meck:expect(wf, q,
                        fun(<<"openid.signed">>) ->
                            <<"op_endpoint,claimed_id,identity,return_to,response_nonce,assoc_handle,",
                            "ns.ext1,ns.sreg,ext1.mode,ext1.type.dn1,ext1.value.dn1,ext1.type.teams,",
                            "ext1.value.teams,sreg.nickname,sreg.email,sreg.fullname">>;
                            (<<"openid.op_endpoint">>) -> <<"serverAddress">>
                        end),
                    meck:expect(lager, log, fun(error, _, _) -> ok end),
                    meck:expect(wf, q, fun(_) -> [] end),
                    meck:expect(wf, url_encode, fun(Key) -> Key end),
                    meck:expect(gui_utils, to_list, fun(Arg) -> to_list(Arg) end),
                    meck:expect(gui_utils, to_binary, fun(Arg) -> to_binary(Arg) end),

                    ?assertEqual({error, invalid_request},
                        openid_utils:prepare_validation_parameters()),
                    ?assert(meck:validate(wf)),
                    ?assert(meck:validate(gui_utils)),
                    ?assert(meck:validate(lager))
                end},

            {"User info correctness",
                fun() ->
                    KeyValueList = lists:zip(user_info_keys(), user_info_values()) ++ [
                        {<<"openid.signed">>, <<"op_endpoint,claimed_id,identity,return_to,response_nonce,assoc_handle,",
                        "ns.ext1,ns.sreg,ext1.mode,ext1.type.dn1,ext1.value.dn1,ext1.type.dn2,ext1.value.dn2,ext1.type.dn3,",
                        "ext1.value.dn3,ext1.type.teams,ext1.value.teams,sreg.nickname,sreg.email,sreg.fullname">>}
                    ],
                    meck:expect(wf, q,
                        fun(Key) ->
                            proplists:get_value(Key, KeyValueList)
                        end),
                    meck:expect(gui_utils, to_list, fun(Arg) -> to_list(Arg) end),
                    meck:expect(gui_utils, to_binary, fun(Arg) -> to_binary(Arg) end),

                    CorrectResult =
                        [
                            {login, lists:nth(1, user_info_processed_values())},
                            {name, lists:nth(2, user_info_processed_values())},
                            {teams, lists:nth(3, user_info_processed_values())},
                            {email, lists:nth(4, user_info_processed_values())},
                            {dn_list, lists:sublist(user_info_processed_values(), 5, 3)}
                        ],
                    {ok, Result} = openid_utils:retrieve_user_info(),
                    lists:foreach(
                        fun(Key) ->
                            ?assertEqual(proplists:get_value(Key, Result), proplists:get_value(Key, CorrectResult))
                        end, [login, name, teams, email, dn_list]),
                    ?assert(meck:validate(wf)),
                    ?assert(meck:validate(gui_utils))
                end},

            {"User info - undefined DN case",
                fun() ->
                    KeyValueList = lists:zip(user_info_keys(), user_info_values()) ++ [
                        {<<"openid.signed">>, <<"op_endpoint,claimed_id,identity,return_to,response_nonce,assoc_handle,",
                        "ns.ext1,ns.sreg,ext1.mode,ext1.type.dn1,ext1.value.dn1,ext1.type.dn2,ext1.value.dn2,ext1.type.dn3,",
                        "ext1.value.dn3,ext1.type.teams,ext1.value.teams,sreg.nickname,sreg.email,sreg.fullname">>}
                    ],
                    meck:expect(wf, q,
                        fun(Key) ->
                            case Key of
                            % Only dn2 is defined
                                <<?openid_dn1_key>> -> undefined;
                                <<?openid_dn3_key>> -> undefined;
                                OtherKey -> proplists:get_value(OtherKey, KeyValueList)
                            end
                        end),
                    meck:expect(gui_utils, to_list, fun(Arg) -> to_list(Arg) end),
                    meck:expect(gui_utils, to_binary, fun(Arg) -> to_binary(Arg) end),

                    CorrectResult =
                        [
                            {login, lists:nth(1, user_info_processed_values())},
                            {name, lists:nth(2, user_info_processed_values())},
                            {teams, lists:nth(3, user_info_processed_values())},
                            {email, lists:nth(4, user_info_processed_values())},
                            % Only dn2 should be in dn_list
                            {dn_list, lists:sublist(user_info_processed_values(), 6, 1)}
                        ],
                    {ok, Result} = openid_utils:retrieve_user_info(),
                    lists:foreach(
                        fun(Key) ->
                            ?assertEqual(proplists:get_value(Key, Result), proplists:get_value(Key, CorrectResult))
                        end, [login, name, teams, email, dn_list]),
                    ?assert(meck:validate(wf)),
                    ?assert(meck:validate(gui_utils))
                end}
        ]}.


%% ====================================================================
%% Auxiliary functions
%% ====================================================================

% Used in "assert POST request body correctness" test
openid_keys() ->
    [
        <<"openid.op_endpoint">>,
        <<"openid.claimed_id">>,
        <<"openid.identity">>,
        <<"openid.return_to">>,
        <<"openid.response_nonce">>,
        <<"openid.assoc_handle">>,
        <<"openid.ns.ext1">>,
        <<"openid.ns.sreg">>,
        <<"openid.ext1.mode">>,
        <<"openid.ext1.type.dn1">>,
        <<"openid.ext1.value.dn1">>,
        <<"openid.ext1.type.teams">>,
        <<"openid.ext1.value.teams">>,
        <<"openid.sreg.nickname">>,
        <<"openid.sreg.email">>,
        <<"openid.sreg.fullname">>,
        <<"openid.sig">>
    ].

% Used in "assert POST request body correctness" test
% Values are whatever, its important if they were all used.
% With exception of endpoint
openid_values() ->
    [<<"https://openid.plgrid.pl/server">> | lists:map(fun(X) -> integer_to_binary(X) end, lists:seq(1, 16))].


% Used in "Retrieve user info" test, names of keys
user_info_keys() ->
    [
        <<?openid_login_key>>,
        <<?openid_name_key>>,
        <<?openid_teams_key>>,
        <<?openid_email_key>>,
        <<?openid_dn1_key>>,
        <<?openid_dn2_key>>,
        <<?openid_dn3_key>>
    ].

% Used in "Retrieve user info" test, values used to mock request URL
user_info_values() ->
    [
        <<"login">>,
        <<"name">>,
        <<"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><userTeams><teams><team>plggveilfs(VeilFS)</team><team>plggsmthg(Something)</team></teams></userTeams>">>,
        <<"email@email.com">>,
        <<"dn1">>,
        <<"dn2">>,
        <<"dn3">>
    ].

% Used in "Retrieve user info" test, values that should be retrieved from request URL
user_info_processed_values() ->
    [
        "login",
        "name",
        ["plggveilfs(VeilFS)", "plggsmthg(Something)"],
        "email@email.com",
        "dn1",
        "dn2",
        "dn3"
    ].


to_list(undefined) -> [];
to_list(Term) when is_list(Term) -> Term;
to_list(Term) when is_binary(Term) -> binary_to_list(Term);
to_list(Term) ->
    try
        wf:to_list(Term)
    catch _:_ ->
        lists:flatten(io_lib:format("~p", [Term]))
    end.

to_binary(Term) when is_binary(Term) -> Term;
to_binary(Term) -> list_to_binary(to_list(Term)).

-endif.