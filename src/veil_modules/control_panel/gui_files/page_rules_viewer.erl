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

-module(page_rules_viewer).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").

main() -> #template { file="./gui_static/templates/bare.html" }.

title() -> "Rules viewer".

body() -> 
	gui_utils:apply_or_redirect(?MODULE, render_body, true).

render_body() ->
    #panel { class="page-container", body = [        
        gui_utils:top_menu(rules_tab),
        #panel { class="page-content", body=gui_utils:empty_page() }
    ]}.