%% ===================================================================
%% @author Lukasz Opiola
%%
%% This file contains nitrogen website code
%% (should it be mentioned in docs ??)
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
			#link { class = header_link, text="LOGIN / LOGOUT", url="/login" }
		]
	}.


%% This will be placed in the template instead of [[[page:body()]]] tag
body() ->
	case wf:user() of
		undefined -> continue;
		_ -> wf:redirect("/login")
	end,

	LoginMessage = openid_utils:validate_openid_login(),

	#panel { class = login_panel, body = 	
		case LoginMessage of
			{error, missing_credentials} -> 
			[
				#label { class = login_title, text = "Login finalization" },
				#label { class = login_error, text = "Login request invalid." },
				#button { text = "Back to login page", postback = {redirect, "/login"} }
			];

			{error, auth_invalid} ->
			[
				#label { class = login_title, text = "Login finalization" },
				#label { class = login_error, text = "OpenID Provider denied the authenticity of this login request." },
				#button { text = "Back to login page", postback = {redirect, "/login"} }
			];

			{error, auth_failed} ->
			[
				#label { class = login_title, text = "Login finalization" },
				#label { class = login_error, text = "Unable to reach OpenID Provider." },
				#button { text = "Back to login page", postback = {redirect, "/login"} }
			];
				
			{ok, [{nickname, Nickname}, {email, Email}, {fullname, FullName}]} ->
			wf:user(Nickname),
			[
				#label { class = login_title, text = "Login finalization" },
				#panel { body = 
				[
					"Welcome " ++ Nickname ++  "!",
					#br {},
					"Looks like your name is " ++ FullName,
					#br {},
					"And your email is " ++ Email
				]},
				#button { text = "Home page", postback = {redirect, "/index"} }
			]
		end
	}.

% Button redirection events
event({redirect, Target}) ->
	wf:redirect(Target).