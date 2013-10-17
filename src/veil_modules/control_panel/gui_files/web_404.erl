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

-module(web_404).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").

%% Template points to the template file, which will be filled with content
main() -> #template { file="./gui_static/templates/bare.html" }.

%% Page title
title() -> "Error 404".

%% This will be placed in the template instead of [[[page:body()]]] tag
body() -> 
	#panel { class="alert alert-danger login-page", body=[
		#h3 { text="Error 404" },
		#p { class="login-info", text="Requested page could not be found on the server." },
		#button { postback=to_login, class="btn btn-warning btn-block", text="Login page" }
	]}.

event(to_login) ->
	wf:redirect("/login").