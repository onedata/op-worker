%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides methods for managing users in the system.
%% @end
%% ===================================================================

-module(user_logic).

-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/dao_types.hrl").
-include_lib("public_key/include/public_key.hrl").


%% ====================================================================
%% API functions
%% ====================================================================
-export([sign_in/1, create_user/5, get_user/1, remove_user/1]).
-export([get_login/1, get_name/1, get_teams/1, update_teams/2]).
-export([get_email_list/1, update_email_list/2, get_dn_list/1, update_dn_list/2]).
-export([rdn_sequence_to_dn_string/1, extract_dn_from_cert/1, oid_code_to_shortname/1]).

-define(UserRootPerms, 8#600).

%% sign_in/1
%% ====================================================================
%% @doc
%% This function should be called after a user has logged in via OpenID. 
%% It looks the user up in database by login. If he is not there, it creates a proper document.
%% If the user already exists, synchronization is made between document
%% in the database and info received from OpenID provider.
%% @end
-spec sign_in(Proplist) -> Result when
	Proplist :: list(),
  	Result :: {string(), user_doc()}.
%% ====================================================================
sign_in(Proplist) ->
	Login = proplists:get_value(login, Proplist, ""), 
	Name = proplists:get_value(name, Proplist, ""), 
	Teams = proplists:get_value(teams, Proplist, ""), 
	Email = proplists:get_value(email, Proplist, ""), 
	DnList = proplists:get_value(dn_list, Proplist, []), 

	User = case get_user({login, Login}) of
		{ok, ExistingUser} ->
			synchronize_user_info(ExistingUser, Teams, Email, DnList);
		{error, user_not_found} ->
			{ok, NewUser} = create_user(Login, Name, Teams, Email, DnList),
			NewUser;
		%% Code below is only for development purposes (connection to DB is not required)
		{error, _} ->
			#veil_document{record=#user{
				login="Database", 
				name="Error", 
				teams="Probably connection problems",
				email_list=["Fix it"], 
				dn_list=["ASAP"]
			}}
	end,
	{Login, User}.


%% create_user/5
%% ====================================================================
%% @doc
%% Creates a user in the DB.
%% @end
-spec create_user(Login, Name, Teams, Email, DnList) -> Result when
	Login :: string(),
	Name :: string(),
	Teams :: string(),
	Email :: string(),
	DnList :: [string()],
  	Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
create_user(Login, Name, Teams, Email, DnList) ->
	User = #user
	{
		login = Login,
		name = Name,
		teams = Teams,
		email_list = case Email of
			Email when is_list(Email) -> [Email];
			_ -> []
		end,
		dn_list = case DnList of
			List when is_list(List) -> List;
			_ -> []
		end
	},
	{ok, UUID} = dao_lib:apply(dao_users, save_user, [User], 1),
	GetUserAns = get_user({uuid, UUID}),

	{GetUserFirstAns, UserRec} = GetUserAns,
	case GetUserFirstAns of
		ok ->
			RootAns = create_root(Login, UserRec#veil_document.uuid),
			case RootAns of
				ok -> GetUserAns;
				_ -> {RootAns, UserRec}
			end;
		_ -> GetUserAns
	end.


%% get_user/1
%% ====================================================================
%% @doc
%% Retrieves user from DB by login, email, uuid, DN or rdnSequence proplist. Returns veil_document wrapping a #user record.
%% @end
-spec get_user(Key::    {login, Login :: string()} | 
                        {email, Email :: string()} | 
                        {uuid, UUID :: user()} | 
                        {dn, DN :: string()} |
                        {rdnSequence, [#'AttributeTypeAndValue'{}]}) ->  
	{ok, user_doc()} | {error, any()}.
%% ====================================================================
get_user({rdnSequence, RDNSequence}) ->
	get_user({dn, rdn_sequence_to_dn_string(RDNSequence)});

get_user(Key) ->
	dao_lib:apply(dao_users, get_user, [Key], 1).


%% remove_user/1
%% ====================================================================
%% @doc
%% Removes user from DB by login.
%% @end
-spec remove_user(Key:: {login, Login :: string()} | 
                        {email, Email :: string()} | 
                        {uuid, UUID :: user()} | 
                        {dn, DN :: string()} |
                        {rdnSequence, [#'AttributeTypeAndValue'{}]}) -> 
  	Result :: ok | {error, any()}.
%% ====================================================================
remove_user({rdnSequence, RDNSequence}) ->
	remove_user({dn, rdn_sequence_to_dn_string(RDNSequence)});

remove_user(Key) ->
  GetUserAns = get_user(Key),
  {GetUserFirstAns, UserRec} = GetUserAns,
  case GetUserFirstAns of
    ok ->
      UserRec2 = UserRec#veil_document.record,
      dao_lib:apply(dao_vfs, remove_file, [UserRec2#user.login], 1);
    _ -> error
  end,
	dao_lib:apply(dao_users, remove_user, [Key], 1).


%% get_login/1
%% ====================================================================
%% @doc
%% Convinience function to get user login from #veil_document encapsulating #user record.
%% @end
-spec get_login(User) -> Result when
	User :: user_doc(),
  	Result :: string().
%% ====================================================================
get_login(User) ->
	User#veil_document.record#user.login.


%% get_name/1
%% ====================================================================
%% @doc
%% Convinience function to get user name from #veil_document encapsulating #user record.
%% @end
-spec get_name(User) -> Result when
	User :: user_doc(),
  	Result :: string().
%% ====================================================================
get_name(User) ->
	User#veil_document.record#user.name.


%% get_teams/1
%% ====================================================================
%% @doc
%% Convinience function to get user teams from #veil_document encapsulating #user record.
%% @end
-spec get_teams(User) -> Result when
	User :: user_doc(),
  	Result :: string().
%% ====================================================================
get_teams(User) ->
	User#veil_document.record#user.teams.


%% update_teams/2
%% ====================================================================
%% @doc
%% Update #veil_document encapsulating #user record with new teams and save it to DB.
%% @end
-spec update_teams(User, NewTeams) -> Result when
	User :: user_doc(),
	NewTeams :: string(),
  	Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
update_teams(#veil_document{record = UserInfo} = UserDoc, NewTeams) ->
	NewDoc = UserDoc#veil_document{record = UserInfo#user{teams = NewTeams}},
	case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
		{ok, UUID} -> dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
		{error, Reason} -> {error, Reason}
	end.

%% get_email_list/1
%% ====================================================================
%% @doc
%% Convinience function to get e-mail list from #veil_document encapsulating #user record.
%% @end
-spec get_email_list(User) -> Result when
	User :: user_doc(),
  	Result :: string().
%% ====================================================================
get_email_list(User) ->
	User#veil_document.record#user.email_list.


%% update_email_list/2
%% ====================================================================
%% @doc
%% Update #veil_document encapsulating #user record with new e-mail list and save it to DB.
%% @end
-spec update_email_list(User, NewEmailList) -> Result when
	User :: user_doc(),
	NewEmailList :: [string()],
  	Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
update_email_list(#veil_document{record = UserInfo} = UserDoc, NewEmailList) ->
	NewDoc = UserDoc#veil_document{record = UserInfo#user{email_list = NewEmailList}},
	case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
		{ok, UUID} -> dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
		{error, Reason} -> {error, Reason}
	end.


%% get_dn_list/1
%% ====================================================================
%% @doc
%% Convinience function to get DN list from #veil_document encapsulating #user record.
%% @end
-spec get_dn_list(User) -> Result when
	User :: user_doc(),
  	Result :: string().
%% ====================================================================
get_dn_list(User) ->
	User#veil_document.record#user.dn_list.


%% update_dn_list/2
%% ====================================================================
%% @doc
%% Update #veil_document encapsulating #user record with new DN list and save it to DB.
%% @end
-spec update_dn_list(User, NewDnList) -> Result when
	User :: user_doc(),
	NewDnList :: [string()],
  	Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
update_dn_list(#veil_document{record = UserInfo} = UserDoc, NewDnList) ->
	NewDoc = UserDoc#veil_document{record = UserInfo#user{dn_list = NewDnList}},
	case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
		{ok, UUID} -> dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
		{error, Reason} -> {error, Reason}
	end.


%% rdn_sequence_to_dn_string/1
%% ====================================================================
%% @doc Converts rdnSequence to DN string so that it can be compared to another DN.
%% @end
-spec rdn_sequence_to_dn_string([#'AttributeTypeAndValue'{}]) -> string() | no_return().
%% ====================================================================
rdn_sequence_to_dn_string(RDNSequence) ->
	try
	    DNString = lists:foldl(
	        fun([#'AttributeTypeAndValue'{type = Type, value = Value}], Acc) ->
	            ValueString = case Value of 
	                List when is_list(List) -> List;
	                {_, List2} when is_list(List2) -> List2;
	                {_, Binary} when is_binary(Binary) -> binary_to_list(Binary);
	                _ -> throw({cannot_retrieve_value, Type, Value})
	            end,
	            Acc ++ oid_code_to_shortname(Type) ++ "=" ++ ValueString ++ ","
	        end, "", lists:reverse(RDNSequence)),

	    %Remove tailing comma
	    [_Comma|ProperString] = lists:reverse(DNString),
	    {ok, lists:reverse(ProperString)}
	catch Type:Message ->
		lager:error("Failed to convert rdnSequence to dn string.~n~p: ~p~n~p", [Type, Message, erlang:get_stacktrace()]),
		{error, conversion_failed}
	end.


%% extract_dn_from_cert/1
%% ====================================================================
%% @doc Processes a .pem certificate and extracts subject (DN) part, returning it as an rdnSequence.
%% Returns an error if:
%%   - fails to extract DN
%%   - certificate is a proxy certificate -> {error, proxy_ceertificate}
%%   - certificate is self-signed -> {error, self_signed}
%% @end
-spec extract_dn_from_cert(PemBin :: binary()) -> {rdnSequence, [#'AttributeTypeAndValue'{}]} | {error, Reason} when
	Reason :: proxy_ceertificate | self_signed | extraction_failed.
%% ====================================================================
extract_dn_from_cert(PemBin) ->	
	try
		Cert = public_key:pem_decode(PemBin),
		{_, DerCert, _} = lists:keyfind('Certificate', 1, Cert),
		OtpCert = public_key:pkix_decode_cert(DerCert, otp),

		IsProxy = gsi_handler:is_proxy_certificate(OtpCert),
		IsSelfSigned = public_key:pkix_is_self_signed(OtpCert),
		if
			IsProxy -> 
				{error, proxy_certificate};
			IsSelfSigned -> 
				{error, self_signed};
			true ->				
				gsi_handler:proxy_subject(OtpCert)
		end
	catch Type:Message ->
		lager:error("Unable to extract subject from cert file.~n~p: ~p~n~p~n~nCertificate binary:~n~p", 
			[Type, Message, erlang:get_stacktrace(), PemBin]),
		{error, extraction_failed}
	end.



%% ====================================================================
%% Internal functions
%% ====================================================================

%% synchronize_user_info/4
%% ====================================================================
%% @doc
%% This function synchronizes data from database with the information
%% received from OpenID provider.
%% @end
-spec synchronize_user_info(User, Teams, Email, DnList) -> Result when
	User :: user_doc(),
	Teams :: string(),
	Email :: string(),
	DnList :: [string()],
  	Result :: user_doc().
%% ====================================================================
synchronize_user_info(User, Teams, Email, DnList) ->
	%% Actual updates will probably happen so rarely, that there is no need to scoop those 3 DB updates into one.
	User2 = case (get_teams(User) =:= Teams) or (Teams =:= "") of
		true -> User;
		false -> 
			{ok, NewUser} = update_teams(User, Teams), 
			NewUser
	end,

	User3 = case lists:member(Email, get_email_list(User2)) or (Email =:= "") of
		true -> User2;
		false -> 
			{ok, NewUser2} = update_email_list(User2, get_email_list(User2) ++ [Email]),
			 NewUser2
	end,

	NewDns = lists:filter(
		fun(X) ->
			not lists:member(X, get_dn_list(User3))
		end, DnList),
	User4 = case NewDns of
		[] -> User3;
		List -> 
			{ok, NewUser3} = update_dn_list(User3, get_dn_list(User3) ++ List),
			NewUser3
	end,
	User4.


%% oid_code_to_shortname/1
%% ====================================================================
%% @doc Converts erlang-like OID code to OpenSSL short name.
%% @end
-spec oid_code_to_shortname(term()) -> string() | no_return().
%% ====================================================================
oid_code_to_shortname(?'id-at-name') 					-> "name";
oid_code_to_shortname(?'id-at-surname') 				-> "SN";
oid_code_to_shortname(?'id-at-givenName') 				-> "GN";
oid_code_to_shortname(?'id-at-initials') 				-> "initials";
oid_code_to_shortname(?'id-at-generationQualifier') 	-> "generationQualifier";
oid_code_to_shortname(?'id-at-commonName') 				-> "CN";
oid_code_to_shortname(?'id-at-localityName') 			-> "L";
oid_code_to_shortname(?'id-at-stateOrProvinceName')		-> "ST";
oid_code_to_shortname(?'id-at-organizationName') 		-> "O";
oid_code_to_shortname(?'id-at-organizationalUnitName') 	-> "OU";
oid_code_to_shortname(?'id-at-title') 					-> "title";
oid_code_to_shortname(?'id-at-dnQualifier') 			-> "dnQualifier";
oid_code_to_shortname(?'id-at-countryName') 			-> "C";
oid_code_to_shortname(?'id-at-serialNumber') 			-> "serialNumber";
oid_code_to_shortname(?'id-at-pseudonym') 				-> "pseudonym";
oid_code_to_shortname(Code) -> throw({unknown_oid_code, Code}).

%% create_root/2
%% ====================================================================
%% @doc Creates root directory for user.
%% @end
-spec create_root(Dir :: string(), Uid :: term()) -> ok | Error when
  Error :: atom().
%% ====================================================================
create_root(Dir, Uid) ->
  {FindStatus, FindTmpAns} = dao_lib:apply(dao_vfs, get_file, [Dir], 1),
  case FindStatus of
    error -> case FindTmpAns of
               file_not_found ->
                 {ParentFound, ParentInfo} = fslogic_utils:get_parent_and_name_from_path(Dir, 1),

                 case ParentFound of
                   ok ->
                     {FileName, Parent} = ParentInfo,
                     File = #file{type = ?DIR_TYPE, name = FileName, uid = Uid, parent = Parent, perms = ?UserRootPerms},

                     dao_lib:apply(dao_vfs, save_file, [File], 1),
                     ok;
                   _ParentError -> parent_error
                 end;

               _Other -> dao_finding_error
             end;
    _Other2 -> root_exists
  end.