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
-include_lib("include/veil_modules/control_panel/openid.hrl").

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

-define(correct_request_url, 
	"https://openid.plgrid.pl/server?openid.mode=checkid_setup&openid.ns=http://specs.openid.net/auth/2.0&" ++
	"openid.return_to=https://veilfsdev.com:8000/validate_login&openid.claimed_id=http://specs.openid.net/auth/2.0/identifier_select&" ++ 
	"openid.identity=http://specs.openid.net/auth/2.0/identifier_select&openid.realm=https://veilfsdev.com:8000&" ++
	"openid.sreg.required=nickname,email,fullname&openid.ns.ext1=http://openid.net/srv/ax/1.0&openid.ext1.mode=fetch_request&" ++ 
	"openid.ext1.type.dn1=http://openid.plgrid.pl/certificate/dn1&openid.ext1.type.dn2=http://openid.plgrid.pl/certificate/dn2&" ++
	"openid.ext1.type.dn3=http://openid.plgrid.pl/certificate/dn3&openid.ext1.type.teams=http://openid.plgrid.pl/userTeams&" ++
	"openid.ext1.if_available=dn1,dn2,dn3,teams").

	
get_url_test_() ->
	{foreach,
		fun() ->
			meck:new(ibrowse),
			meck:new(lager)
		end,
		fun(_) ->
			ok = meck:unload(ibrowse),
			ok = meck:unload(lager)
		end,
		[
			{"URL correctness",
				fun() ->
					meck:expect(ibrowse, send_req, fun(_, _, _) -> {ok, "200", [], ?mock_xrds_file} end),
					?assertEqual(?correct_request_url, openid_utils:get_login_url()),
					?assert(meck:validate(ibrowse))
				end},

			{"No 200 code case",
				fun() ->
					meck:expect(ibrowse, send_req, fun(_, _, _) -> {ok, "404", [], []} end),
					meck:expect(lager, log, fun(error, _, _, _) -> ok end),
					?assertEqual({error, endpoint_unavailable}, openid_utils:get_login_url()),
					?assert(meck:validate(ibrowse)),
					?assert(meck:validate(lager))
				end},			

			{"Connection refused case",
				fun() ->
					meck:expect(ibrowse, send_req, fun(_, _, _) -> {error, econnrefused} end),
					meck:expect(lager, log, fun(error, _, _, _) -> ok end),
					?assertEqual({error, endpoint_unavailable}, openid_utils:get_login_url()),
					?assert(meck:validate(ibrowse)),
					?assert(meck:validate(lager))
				end}		
	]}.

validate_login_test_() ->
	{foreach,
		fun() ->
			meck:new(ibrowse),
			meck:new(lager)
		end,
		fun(_) ->
			meck:unload(ibrowse),
			meck:unload(lager)
		end,
		[
			{"Login valid case",
				fun() ->
					meck:expect(ibrowse, send_req, fun("mock", _, post, "me") -> {ok, "200", [], ?valid_auth_info} end),
					?assertEqual(ok, openid_utils:validate_openid_login({"mock", "me"})),
					?assert(meck:validate(ibrowse))
				end},

			{"Login invalid case",
				fun() ->
					meck:expect(ibrowse, send_req, fun("mock", _, post, "me") -> {ok, "200", [], "is_valid: false\n"} end),
					meck:expect(lager, log, fun(alert, _, _, _) -> ok end),
					?assertEqual({error, auth_invalid}, openid_utils:validate_openid_login({"mock", "me"})),
					?assert(meck:validate(ibrowse)),
					?assert(meck:validate(lager))
				end},

			{"No 200 code case",
				fun() ->
					meck:expect(ibrowse, send_req, fun("mock", _, post, "me") -> {ok, "404", [], []} end),
					meck:expect(lager, log, fun(error, _, _, _) -> ok end),
					?assertEqual({error, no_connection}, openid_utils:validate_openid_login({"mock", "me"})),
					?assert(meck:validate(ibrowse)),
					?assert(meck:validate(lager))
				end},

			{"Connection refused case",
				fun() ->
					meck:expect(ibrowse, send_req, fun("mock", _, post, "me") -> {error, econnrefused} end),
					meck:expect(lager, log, fun(error, _, _, _) -> ok end),
					?assertEqual({error, no_connection}, openid_utils:validate_openid_login({"mock", "me"})),
					?assert(meck:validate(ibrowse)),
					?assert(meck:validate(lager))
				end}			
	]}.

nitrogen_parameter_processing_test_() ->
	{foreach,
		fun() ->
			meck:new(wf),
			meck:new(lager)
		end,
		fun(_) ->
			meck:unload(wf),
			meck:unload(lager)
		end,
		[{"POST request body correctness",
			fun() ->
				meck:expect(wf, q, 
					fun ('openid.signed') -> 
						"op_endpoint,claimed_id,identity,return_to,response_nonce,assoc_handle," ++
						"ns.ext1,ns.sreg,ext1.mode,ext1.type.dn1,ext1.value.dn1,ext1.type.teams," ++
						"ext1.value.teams,sreg.nickname,sreg.email,sreg.fullname";
						('openid.op_endpoint') -> "serverAddress"
					end),

				KeyValueList = lists:zip(openid_keys(), openid_values()), 
				meck:expect(wf, qs, 
					fun(Key) -> 
						% qs returns a list
						[proplists:get_value(atom_to_list(Key), KeyValueList)]
					end), 
				meck:expect(wf, url_encode, fun(Key) -> Key end), 
				CorrectRequest = ?openid_check_authentication_mode ++ 
					lists:foldl(
						fun({Key, Value}, Acc) ->
							Acc ++ "&" ++ Key ++ "=" ++ Value 
						end, "", KeyValueList), 
				?assertEqual({"serverAddress", CorrectRequest}, 
					openid_utils:nitrogen_prepare_validation_parameters()),
				?assert(meck:validate(wf))
			end},

		{"Missing 'signed' parameter case",
			fun() ->
				meck:expect(wf, q, 
					fun ('openid.signed') -> 
						undefined;
						('openid.op_endpoint') -> undefined
					end),
				meck:expect(lager, log, fun(error, _, _, _) -> ok end),

				?assertEqual({error, invalid_request}, 
					openid_utils:nitrogen_prepare_validation_parameters()),
				?assert(meck:validate(wf)),
				?assert(meck:validate(lager))
			end},

		{"Missing parameters case",
			fun() ->
				meck:expect(wf, q, 
					fun ('openid.signed') -> 
						"op_endpoint,claimed_id,identity,return_to,response_nonce,assoc_handle," ++
						"ns.ext1,ns.sreg,ext1.mode,ext1.type.dn1,ext1.value.dn1,ext1.type.teams," ++
						"ext1.value.teams,sreg.nickname,sreg.email,sreg.fullname";
						('openid.op_endpoint') -> "serverAddress"
					end),
				meck:expect(lager, log, fun(error, _, _, _) -> ok end),
				meck:expect(wf, qs, fun(_) -> [] end), 
				meck:expect(wf, url_encode, fun(Key) -> Key end), 

				?assertEqual({error, invalid_request}, 
					openid_utils:nitrogen_prepare_validation_parameters()),
				?assert(meck:validate(wf)),
				?assert(meck:validate(lager))
			end},

		{"User info correctness",
			fun() ->
				KeyValueList = lists:zip(user_info_keys(), user_info_values()), 
				meck:expect(wf, q, 
					fun(Key) -> 
						proplists:get_value(atom_to_list(Key), KeyValueList)
					end), 

				CorrectResult = 
				[
					{login, lists:nth(1, user_info_values())}, 
					{name, lists:nth(2, user_info_values())}, 
					{teams, lists:nth(3, user_info_values())}, 
					{email, lists:nth(4, user_info_values())}, 
					{dn_list, lists:sublist(user_info_values(), 5, 3)}
				],
				{ok, Result} = openid_utils:nitrogen_retrieve_user_info(),
				lists:foreach(
					fun(Key) ->
						?assertEqual(proplists:get_value(Key, Result), proplists:get_value(Key, CorrectResult))
					end, KeyValueList), 
				?assert(meck:validate(wf))
			end},

		{"User info - undefined DN case",
			fun() ->
				KeyValueList = lists:zip(user_info_keys(), user_info_values()), 
				meck:expect(wf, q, 
					fun(Key) -> 
						case Key of
							% Only dn2 is defined
							?openid_dn1_key -> undefined;
							?openid_dn3_key -> undefined;
							OtherKey -> proplists:get_value(atom_to_list(OtherKey), KeyValueList)
						end
					end), 

				CorrectResult = 
				[
					{login, lists:nth(1, user_info_values())}, 
					{name, lists:nth(2, user_info_values())}, 
					{teams, lists:nth(3, user_info_values())}, 
					{email, lists:nth(4, user_info_values())}, 
					% Only dn2 should be in dn_list
					{dn_list, lists:nth(6, user_info_values())}
				],
				{ok, Result} = openid_utils:nitrogen_retrieve_user_info(),
				lists:foreach(
					fun(Key) ->
						?assertEqual(proplists:get_value(Key, Result), proplists:get_value(Key, CorrectResult))
					end, KeyValueList), 
				?assert(meck:validate(wf))
			end}
	]}.



%% ====================================================================
%% Auxiliary functions
%% ====================================================================

% Used in "assert POST request body correctness" test
openid_keys() ->
[
	"openid.op_endpoint", 
	"openid.claimed_id",
	"openid.identity",
	"openid.return_to",
	"openid.response_nonce",
	"openid.assoc_handle",
	"openid.ns.ext1",
	"openid.ns.sreg",
	"openid.ext1.mode",
	"openid.ext1.type.dn1",
	"openid.ext1.value.dn1",
	"openid.ext1.type.teams",
	"openid.ext1.value.teams",
	"openid.sreg.nickname",
	"openid.sreg.email",
	"openid.sreg.fullname",
	"openid.sig",
	"openid.signed"
].

% Used in "assert POST request body correctness" test
% Values are whatever, its important if they were all used.
openid_values() ->
	lists:map(fun(X) -> integer_to_list(X) end, lists:seq(1, 18)).


% Used in "Retrieve user info" test
user_info_keys() ->
[
	?openid_login_key,
	?openid_name_key,
	?openid_teams_key,
	?openid_email_key,
	?openid_dn1_key,
	?openid_dn2_key,
	?openid_dn3_key
].

% Used in "Retrieve user info" test
% Values are whatever, its important if they were all used.
user_info_values() ->
	lists:map(fun(X) -> integer_to_list(X) end, lists:seq(1, 7)).

-endif.