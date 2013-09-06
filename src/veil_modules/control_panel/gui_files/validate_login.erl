%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains nitrogen website code
%% @end
%% ===================================================================

-module (validate_login).
-compile(export_all).
-include_lib("nitrogen_core/include/wf.hrl").

%% Template points to the template file, which will be filled with content
main() ->
  #template { file="./gui_static/templates/login.html" }.

%% Page title
title() -> "VeilFS - login page".

%% This will be placed in the template instead of [[[page:header()]]] tag
header() ->
	#panel
	{
		class = header,
		body = 
		[
			#link { class = header_link, text="MAIN PAGE", url="/index" },
			#link { class = header_link, text="LOGIN / LOGOUT", url="/login" },
			#link { class = header_link, text="MANAGE ACCOUNT", url="/manage_account" }
		]
	}.


%% This will be placed in the template instead of [[[page:body()]]] tag
body() ->
	case (wf:user() =:= undefined) of
		false -> wf:redirect("/login");
		true -> 
			LoginMessage = case openid_utils:nitrogen_prepare_validation_parameters() of
				{error, invalid_request} -> {error, invalid_request};
				{EndpointURL, RequestBody} -> openid_utils:validate_openid_login({EndpointURL, RequestBody})
			end,				

			#panel { class = login_panel, body = 	
				case LoginMessage of
					{error, invalid_request} ->
						[
							#label { class = login_title, text = "Login finalization" },
							#label { class = login_error, text = "Unable to process this login request." },
							#button { text = "Back to login page", postback = {redirect, "/login"} }
						];

					{error, auth_invalid} ->
						[
							#label { class = login_title, text = "Login finalization" },
							#label { class = login_error, text = "OpenID Provider denied the authenticity of this login request." },
							#button { text = "Back to login page", postback = {redirect, "/login"} }
						];

					{error, no_connection} ->
						[
							#label { class = login_title, text = "Login finalization" },
							#label { class = login_error, text = "Unable to reach OpenID Provider." },
							#button { text = "Back to login page", postback = {redirect, "/login"} }
						];
						
					ok ->					
						{ok, Proplist} = openid_utils:nitrogen_retrieve_user_info(),
						{Login, UserDoc} = user_logic:sign_in(Proplist),						
						wf:user(Login),	
						wf:session(user_doc, UserDoc),
						wf:redirect("/manage_account"),
						[]		
				end
			}
	end.

% Button redirection events
event({redirect, Target}) ->
	wf:redirect(Target).

%% c("../../../src/veil_modules/control_panel/gui_files/validate_login.erl").