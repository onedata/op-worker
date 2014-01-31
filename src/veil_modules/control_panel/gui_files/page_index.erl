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

-module(page_index).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").

main() -> #template { file="./gui_static/templates/bare.html" }.

title() -> "Index".

body() -> 
	case gui_utils:user_logged_in() of
		true -> wf:redirect("/file_manager");
		false -> wf:redirect("/login")
	end.
