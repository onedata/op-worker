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

-module(page_logout).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").

%% Template points to the template file, which will be filled with content
main() -> #template { file="./gui_static/templates/bare.html" }.

%% Page title
title() -> "Logout page".

%% This will be placed in the template instead of [[[page:body()]]] tag
body() -> 
	wf:logout(),
	#panel { style="position: relative;", body = 
		[
			#panel { class="alert alert-success login-page", body=[
				#h3 { class="", text="Logout successful" },
				#p { class="login-info", text="Come back soon." },
				#button { postback=to_login, class="btn btn-primary btn-block", text="Login page" }
			]}
		] 
		++ gui_utils:logotype_footer(120)
		++ [#p { body="<iframe src=\"https://openid.plgrid.pl/logout\" style=\"display:none\"></iframe>" }]
	}.

event(to_login) ->	
	wf:redirect("/login").