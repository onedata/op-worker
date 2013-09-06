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

-module (login).
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
	[
		case wf:user() of
			undefined -> 
				#panel { class = login_panel, body = 
				[
					#label { class = login_title, text = "Log in to VeilFS" },
					#span { id = login_prompt, body = "Click below to login via PlGrid OpenID" },
					#br {},
					#button { text="Sign in", postback = login }, 
					#br {} 
				]};
			Login -> 
				#panel { class = login_panel, body = 
				[
					#label { class = login_title, text = "Logout" },
					#panel { body = 
					[
						"Logged in as " ++ wf:to_list(Login),
						#br {},
						"Click below to logout"
					]},
					#button { text="Logout", postback = logout } 
				]}
		end		
	].


event(login) ->
	case openid_utils:get_login_url() of 
		{error, _} -> 
			wf:replace(login_prompt, #span { class = login_error, 
				text = "Unable to reach OpenID Provider. Click below to try again."});
		URL -> 
			wf:redirect(URL)
	end;

event(logout) ->
	wf:user(undefined),
	wf:redirect("/login").
	
	