%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains Nitrogen website code
%% @end
%% ===================================================================

-module(page_login).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").

%% Template points to the template file, which will be filled with content
main() -> #template { file="./gui_static/templates/bare.html" }.

%% Page title
title() -> "Login page".

%% This will be placed in the template instead of [[[page:body()]]] tag
body() -> 
	case gui_utils:user_logged_in() of
		true -> wf:redirect("/file_manager");
		false ->
			ErrorPanelStyle = case wf:q(x) of
				undefined -> "display: none;";
				_ -> ""
			end,
			[
				#panel { id=error_message, style=ErrorPanelStyle, class="dialog dialog-danger", body=#p { 
					text="Session error or session expired. Please log in again." } },
				#panel { class="alert alert-success login-page", body=[
					#h3 { text="Welcome to VeilFS" },
					#p { class="login-info", body="Logging in is handled by <b>PL-Grid OpenID</b>. 
						You need to have an account and possibly VeilFS service enabled." },
					#button { postback=login, class="btn btn-primary btn-block", text="Log in via PL-Grid OpenID" }
				]}
			]
	end.


% Login event handling
event(login) ->
	% Collect nitrogen redirect params if present
	RedirectParam = case wf:q(x) of
		undefined -> "";
		Val -> "?x=" ++ Val
	end,
	% Resolve hostname, which was requested by a client
	Hostname = gui_utils:get_requested_hostname(),
	case Hostname of
		undefined -> 
			wf:update(error_message, 
				"Cannot establish requested hostname. Please contact the site administrator."),
			wf:wire(error_message, #appear { speed=300 });
		Host ->
			% Get redirect URL and redirect to OpenID login
			case openid_utils:get_login_url(Host, RedirectParam) of 
				{error, _} -> 
					wf:update(error_message, 
						"Unable to reach OpenID Provider. Please try again later."),
					wf:wire(error_message, #appear { speed=300 });
				URL -> 
					wf:redirect(URL)
			end
	end.



%% c("../../../src/veil_modules/control_panel/gui_files/login.erl").