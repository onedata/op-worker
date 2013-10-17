%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of user_logic, using eunit tests.
%% @end
%% ===================================================================
-module(user_logic_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("public_key/include/public_key.hrl").

	
basic_test_() ->
	{foreach,
		fun() ->
			meck:new(dao_lib)
		end,
		fun(_) ->
			ok = meck:unload(dao_lib)
		end,
		[
			{"get_user",
				fun() ->
					meck:expect(dao_lib, apply, 
						fun(dao_users, get_user, [Key], _) ->  
							case Key of
								{login, Login} when is_list(Login) -> {ok, #veil_document{ record = #user{} } };
								{email, Email} when is_list(Email) -> {ok, #veil_document{ record = #user{} } };
								{uuid, UUID} when is_list(UUID) -> {ok, #veil_document{ record = #user{} } };
								{dn, DN} when is_list(DN) -> {ok, #veil_document{ record = #user{} } };
								_ -> {error, reason}
							end
						end),
					?assertEqual({ok, #veil_document{ record = #user{} } }, user_logic:get_user({login, "a"})),
					?assertEqual({ok, #veil_document{ record = #user{} } }, user_logic:get_user({email, "a"})),
					?assertEqual({ok, #veil_document{ record = #user{} } }, user_logic:get_user({uuid, "a"})),
					?assertEqual({ok, #veil_document{ record = #user{} } }, user_logic:get_user({dn, "a"})),
					?assertEqual({error, reason}, user_logic:get_user(ubelibubelimuk)),
					?assert(meck:validate(dao_lib))
				end},

			{"remove_user",
				fun() ->
					meck:expect(dao_lib, apply, 
						fun (dao_users, remove_user, [Key], _) ->  
							case Key of
								{login, Login} when is_list(Login) -> ok;
								{email, Email} when is_list(Email) -> ok;
								{uuid, UUID} when is_list(UUID) -> ok;
								{dn, DN} when is_list(DN) -> ok;
								_ -> {error, reason}
							end;
							(dao_users, get_user, [Key], _) ->  
							case Key of
								{login, Login} when is_list(Login) -> {ok, #veil_document{ record = #user{} } };
								{email, Email} when is_list(Email) -> {ok, #veil_document{ record = #user{} } };
								{uuid, UUID} when is_list(UUID) -> {ok, #veil_document{ record = #user{} } };
								{dn, DN} when is_list(DN) -> {ok, #veil_document{ record = #user{} } };
								_ -> {error, reason}
							end;
							(dao_vfs, remove_file, _, _) -> ok 
						end),
					?assertEqual(ok, user_logic:remove_user({login, "login"})),
					?assertEqual(ok, user_logic:remove_user({email, "email"})),
					?assertEqual(ok, user_logic:remove_user({uuid, "uuid"})),
					?assertEqual(ok, user_logic:remove_user({dn, "dn"})),
					?assertEqual({error, reason}, user_logic:remove_user(ubelibubelimuk)),
					?assert(meck:validate(dao_lib))
				end},

			{"convinience_functions",
				fun() ->
					ExistingUser = #veil_document{ record = #user{ 
						login = "existing_user",
						name = "Existing User",
						teams = "Existing team",
						email_list = ["existing@email.com"],
						dn_list = ["O=existing-dn"] 
					}},

					?assertEqual("existing_user", user_logic:get_login(ExistingUser)),
					?assertEqual("Existing User", user_logic:get_name(ExistingUser)),
					?assertEqual("Existing team", user_logic:get_teams(ExistingUser)),
					?assertEqual(["existing@email.com"], user_logic:get_email_list(ExistingUser)),
					?assertEqual(["O=existing-dn"], user_logic:get_dn_list(ExistingUser))
				end}
	]}.

signing_in_test_() ->
	{foreach,
		fun() ->
			meck:new(dao_lib),
			meck:new(fslogic_utils)
		end,
		fun(_) ->
			ok = meck:unload(dao_lib),
			ok = meck:unload(fslogic_utils)
		end,
		[
			{"new user -> create_user",
				fun() ->
					% Possible info gathered from OpenID provider
					NewUserInfoProplist = 
					[
						{login, "new_user"}, 
						{name, "New User"}, 
						{teams, "New team"}, 
						{email, "new@email.com"}, 
						{dn_list, ["O=new-dn"]}
					],
					% New user record that should be generated from above
					NewUser = #user{ 
						login = "new_user",
						name = "New User",
						teams = "New team",
						email_list = ["new@email.com"],
						dn_list = ["O=new-dn"]
					},
					% #veil_document encapsulating user record 
					NewUserRecord = #veil_document { record = NewUser },

					meck:expect(dao_lib, apply, 
						fun (dao_users, get_user, [Key], _) -> 
							case Key of 
								{login, "new_user"} -> {error, user_not_found};
								{uuid, "uuid"} -> {ok, NewUserRecord} 
							end;			
							(dao_users, save_user, [UserDoc], _) -> 
							case UserDoc of 
								NewUser -> {ok, "uuid"};
								_ -> throw(error)
							end;
							(dao_vfs, get_file, _, _) -> {error, file_not_found};
							(dao_vfs, save_file, _, _) -> {ok, "file_uuid"}
						end),

					meck:expect(fslogic_utils, get_parent_and_name_from_path, 
						fun("new_user" ,_) -> {ok, {"some", "path"}} end),

                    Tim = 12345677,
                    meck:expect(fslogic_utils, time, fun() -> Tim end),
                    meck:expect(fslogic_utils, update_meta_attr, fun(File, times, {Tim, Tim, Tim}) -> File end),

					?assertEqual({"new_user", NewUserRecord}, user_logic:sign_in(NewUserInfoProplist)),
					?assert(meck:validate(dao_lib))
				end},

			{"existing user -> synchronize + update functions",
				fun() ->
					% Existing record in database
					ExistingUser = #veil_document{ record = #user{ 
						login = "existing_user",
						name = "Existing User",
						teams = "Existing team",
						email_list = ["existing@email.com"],
						dn_list = ["O=existing-dn"] 
					}},
					% Possible info gathered from OpenID provider
					ExistingUserInfoProplist = 
					[
						{login, "existing_user"}, 
						{name, "Existing User"}, 
						{teams, "Updated team"}, 
						{email, "some.other@email.com"}, 
						{dn_list, ["O=new-dn"]}
					],					
					% User record after updating teams
					UserWithUpdatedTeams = #veil_document{ record = #user{ 
						login = "existing_user",
						name = "Existing User",
						teams = "Updated team",
						email_list = ["existing@email.com"],
						dn_list = ["O=existing-dn"] 
					}},				
					% User record after updating emails
					UserWithUpdatedEmailList = #veil_document{ record = #user{ 
						login = "existing_user",
						name = "Existing User",
						teams = "Updated team",
						email_list = ["existing@email.com", "some.other@email.com"],
						dn_list = ["O=existing-dn"] 
					}},					
					% How should user end up after synchronization
					SynchronizedUser = #veil_document{ record = #user{ 
						login = "existing_user",
						name = "Existing User",
						teams = "Updated team",
						email_list = ["existing@email.com", "some.other@email.com"],
						dn_list = ["O=existing-dn", "O=new-dn"] 
					}},

					% These uuids should be the same, but this way we can simulate DB updates of the record
					meck:expect(dao_lib, apply, 
						fun (dao_users, get_user, [Key], _) ->
							case Key of
								{login,"existing_user"} -> {ok, ExistingUser};
								{uuid, "uuid_after_teams"} -> {ok, UserWithUpdatedTeams};
								{uuid, "uuid_after_emails"} -> {ok, UserWithUpdatedEmailList};
								{uuid, "uuid_after_synchronization"} -> {ok, SynchronizedUser}
							end;
							(dao_users, save_user, [UserDoc], _) -> 
								case UserDoc of
									UserWithUpdatedTeams -> {ok, "uuid_after_teams"}; 
									UserWithUpdatedEmailList -> {ok, "uuid_after_emails"}; 
									SynchronizedUser -> {ok, "uuid_after_synchronization"};
									_ -> throw(error)
								end
						end),

					?assertEqual({"existing_user", SynchronizedUser}, user_logic:sign_in(ExistingUserInfoProplist)),
					?assert(meck:validate(dao_lib))
				end}
	]}.


certificate_manipulation_test_() ->
	[
		{"rdnSequence_to_dn_string",
			{setup, 
				fun() ->
					meck:new(lager)
				end,
				fun(_) ->
					ok = meck:unload(lager)
				end,
				fun() ->
					% Generate an RDNSequence with all the keys
					RDNSequence = [[#'AttributeTypeAndValue'{ type = T, value = V }] || 
						{T, V} <- lists:zip(oid_codes(), attribute_values())],

					% Shuffle it
					ShuffledRDNSequence = [X||{_,X} <- lists:sort([{random:uniform(), N} || N <- RDNSequence])],

					% Generate an incorrect RDNSequence with all the keys
					IncorrectRDNSequence = [[#'AttributeTypeAndValue'{ type = T2, value = {ubelibubelimuk, V2} }] || 
						{T2, V2} <- lists:zip(oid_codes(), attribute_values())],
					
					% Generate correct DN
					CorrectDNWithComma = lists:foldr( % rdnSequence should be traversed from right to left
						fun([#'AttributeTypeAndValue'{type = Type, value = {_, Value}}], Acc) ->
							Acc ++ user_logic:oid_code_to_shortname(Type) ++ "=" ++ Value ++ ","
						end, "", ShuffledRDNSequence),
					% Remove tailing comma
					[_Comma|ReverseCorrectDN] = lists:reverse(CorrectDNWithComma),
					CorrectDN = lists:reverse(ReverseCorrectDN),

					?assertEqual({ok, CorrectDN}, user_logic:rdn_sequence_to_dn_string(ShuffledRDNSequence)),
					meck:expect(lager, log, fun(error, _, _, _) -> ok end),
					?assertEqual({error, conversion_failed}, user_logic:rdn_sequence_to_dn_string(IncorrectRDNSequence)),
					?assert(meck:validate(lager))
				end}},

		{"extract_dn_from_cert",
			{setup, 
				fun() ->
					meck:new(public_key),
					meck:new(gsi_handler),
					meck:new(lager)
				end,
				fun(_) ->
					ok = meck:unload(public_key),
					ok = meck:unload(gsi_handler),
					ok = meck:unload(lager)
				end,
				fun() ->					
					meck:expect(public_key, pem_decode, 
						fun(Bin) when is_binary(Bin) -> 
							case Bin of
								<<"Correct">> -> [{'Certificate', <<"DerCert">>, whatever}];
								<<"SelfSigned">> -> [{'Certificate', <<"DerCertSelfSigned">>, whatever}];
								<<"Proxy">> -> [{'Certificate', <<"DerCertProxy">>, whatever}]
							end
						end),
					% What decoded cert looks like doesn't matter, this test checks
					% if the function returns proper errors
					meck:expect(public_key, pkix_decode_cert, 
						fun(Bin, otp) when is_binary(Bin) -> Bin end),
					meck:expect(public_key, pkix_is_self_signed, 
						fun(Bin) -> 
							case Bin of
								<<"DerCert">> -> false;
								<<"DerCertSelfSigned">> -> true;
								<<"DerCertProxy">> -> false
							end
						end),
					meck:expect(gsi_handler, is_proxy_certificate, 
						fun(Bin) -> 
							case Bin of
								<<"DerCert">> -> false;
								<<"DerCertSelfSigned">> -> false;
								<<"DerCertProxy">> -> true
							end
						end),
					meck:expect(gsi_handler, proxy_subject, 
						fun(<<"DerCert">>) -> {rdnSequence, sequence} end),

					meck:expect(lager, log, fun(error, _, _, _) -> ok end),

					?assertEqual({rdnSequence, sequence}, user_logic:extract_dn_from_cert(<<"Correct">>)),
					?assertEqual({error, self_signed}, user_logic:extract_dn_from_cert(<<"SelfSigned">>)),
					?assertEqual({error, proxy_certificate}, user_logic:extract_dn_from_cert(<<"Proxy">>)),
					?assert(meck:validate(public_key)),
					?assertEqual({error, extraction_failed}, user_logic:extract_dn_from_cert(ubelibubelimuk)),
					?assert(meck:validate(gsi_handler)),
					?assert(meck:validate(lager))
				end}}
	].


%% ====================================================================
%% Auxiliary functions
%% ====================================================================

% Used in "rdnSequence_to_dn_string" test
oid_codes() ->
[	
	?'id-at-name',
	?'id-at-surname',
	?'id-at-givenName',
	?'id-at-initials',
	?'id-at-generationQualifier',
	?'id-at-commonName',
	?'id-at-localityName',
	?'id-at-stateOrProvinceName',
	?'id-at-organizationName',
	?'id-at-organizationalUnitName',
	?'id-at-title',
	?'id-at-dnQualifier',
	?'id-at-countryName',
	?'id-at-serialNumber',
	?'id-at-pseudonym'
].

% Used in "rdnSequence_to_dn_string" test
% Values are whatever, its important if they were all used.
attribute_values() ->
	lists:map(fun(X) -> {printable_string, integer_to_list(X)} end, lists:seq(1, 15)).

-endif.