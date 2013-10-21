%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains useful functions commonly used in control_panel modules.
%% @end
%% ===================================================================

-module (gui_utils).
-include("veil_modules/control_panel/common.hrl").
-export([get_requested_hostname/0, get_user_dn/0, user_logged_in/0, storage_defined/0, dn_and_storage_defined/0]).
-export([apply_or_redirect/3, apply_or_redirect/4, top_menu/1, top_menu/2, logotype_footer/1, empty_page/0]).


%% ====================================================================
%% API functions
%% ====================================================================

%% get_requested_hostname/0
%% ====================================================================
%% @doc Returns the hostname requested by the client.
%% @end
-spec get_requested_hostname() -> string().
%% ====================================================================
get_requested_hostname() ->
	RequestBridge = wf_context:request_bridge(),
	proplists:get_value(host, RequestBridge:headers()).


%% get_user_dn/0
%% ====================================================================
%% @doc Returns user's DN retrieved from his session state.
%% @end
-spec get_user_dn() -> string().
%% ====================================================================
get_user_dn() ->
	try user_logic:get_dn_list(wf:session(user_doc)) of
		[] -> undefined;
		L when is_list(L) -> lists:nth(1, L);
		_ -> undefined
	catch _:_ ->
		undefined
	end.


%% user_logged_in/0
%% ====================================================================
%% @doc Checks if the client has a valid login session.
%% @end
-spec user_logged_in() -> boolean().
%% ====================================================================
user_logged_in() ->
	(wf:user() /= undefined).


%% storage_defined/0
%% ====================================================================
%% @doc Checks if any storage is defined in the database.
%% @end
-spec storage_defined() -> boolean().
%% ====================================================================
storage_defined() ->
	case dao_lib:apply(dao_vfs, list_storage, [], 1) of
		{ok, []} -> false;
		{ok, L} when is_list(L) -> true;
		_ -> false
	end.


%% dn_and_storage_defined/0
%% ====================================================================
%% @doc Convienience function to check both conditions.
%% @end
-spec dn_and_storage_defined() -> boolean().
%% ====================================================================
dn_and_storage_defined() ->
	(get_user_dn() /= undefined) and storage_defined().


%% apply_or_redirect/3
%% ====================================================================
%% @doc Checks if the client has right to do the operation (is logged in and possibly 
%% has a certificate DN defined). If so, it executes the code.
%% @end
-spec apply_or_redirect(Module :: atom, Fun :: atom, boolean()) -> boolean().
%% ====================================================================
apply_or_redirect(Module, Fun, NeedDN) ->
	apply_or_redirect(Module, Fun, [], NeedDN).	

%% apply_or_redirect/4
%% ====================================================================
%% @doc Checks if the client has right to do the operation (is logged in and possibly 
%% has a certificate DN defined). If so, it executes the code.
%% @end
-spec apply_or_redirect(Module :: atom, Fun :: atom, Args :: list(), boolean()) -> boolean().
%% ====================================================================
apply_or_redirect(Module, Fun, Args, NeedDN) ->
	try 
		case user_logged_in() of
			false ->
				wf:redirect_to_login("/login");
			true -> 
				case NeedDN and (not dn_and_storage_defined()) of
					true -> wf:redirect("/manage_account");
					false -> erlang:apply(Module, Fun, Args)
				end
		end
	catch Type:Message ->
        lager:error("Error in ~p - ~p:~p~n~p", [Module, Type, Message, erlang:get_stacktrace()]),
        page_error:redirect_with_error("Internal server error", 
        	"Server encountered an unexpected error. Please contact the site administrator if the problem persists.")
	end.


%% top_menu/1
%% ====================================================================
%% @doc Convienience function to render top menu in GUI pages. 
%% Item with ActiveTabID will be highlighted as active.
%% @end
-spec top_menu(ActiveTabID :: any()) -> list().
%% ====================================================================
top_menu(ActiveTabID) ->
	top_menu(ActiveTabID, []).

%% top_menu/2
%% ====================================================================
%% @doc Convienience function to render top menu in GUI pages. 
%% Item with ActiveTabID will be highlighted as active. 
%% Submenu body (list of nitrogen elements) will be concatenated below the main menu.
%% @end
-spec top_menu(ActiveTabID :: any(), SubMenuBody :: any()) -> list().
%% ====================================================================
top_menu(ActiveTabID, SubMenuBody) ->
	% Define menu items with ids, so that proper tab can be made active via function parameter 
	% see old_menu_captions()
	MenuCaptions = 
	[
		{file_manager_tab, #listitem { body=[
			#link{ style="padding: 18px;", url="/file_manager", text="File manager" }
		]}},
		{shared_files_tab, #listitem { body=[
			#link{ style="padding: 18px;", url="/shared_files", text="Shared files" }
		]}}
	],

	MenuIcons = 
	[
		{manage_account_tab, #listitem { body=#link{ style="padding: 18px;", title="Manage account",
			url="/manage_account", body=[user_logic:get_name(wf:session(user_doc)) , #span{ class="fui-user", style="margin-left: 10px;" }] } } },
		%{contact_support_tab, #listitem { body=#link{ style="padding: 18px;", title="Contact & Support",
		%	url="/contact_support", body=#span{ class="fui-question" } } } },
		{logout_button,  #listitem { body=#link{ style="padding: 18px;", title="Log out",
			url="/logout", body=#span{ class="fui-power" } } } }
	],

	MenuCaptionsProcessed = lists:map(
		fun({TabID, ListItem}) ->
			case TabID of
				ActiveTabID -> ListItem#listitem { class="active" };
				_ -> ListItem
			end
		end, MenuCaptions),

	MenuIconsProcessed = lists:map(
		fun({TabID, ListItem}) ->
			case TabID of
				ActiveTabID -> ListItem#listitem { class="active" };
				_ -> ListItem
			end
		end, MenuIcons),

	#panel { class="navbar navbar-fixed-top", body=[
		#panel { class="navbar-inner", style="border-bottom: 2px solid gray;", body=[
			#panel { class="container", body=[	
				#list { class="nav pull-left", body=MenuCaptionsProcessed },
				#list { class="nav pull-right", body=MenuIconsProcessed }
			]}				
		]}
	] ++ SubMenuBody}.


%% logotype_footer/1
%% ====================================================================
%% @doc Convienience function to render logotype footer, coming after page content.
%% @ends
-spec logotype_footer(MarginTop :: integer()) -> list().
%% ====================================================================
logotype_footer(MarginTop) ->
	[
		#panel { style="text-align: center; position: absolute; bottom: 20px; left: 0; right: 0; z-index: -1;", 
			body=[
	        #image { style="position: absolute; left: 40px; top: 0;", image="/images/innow-gosp-logo.png" },
	        #image { style="margin: 2px;", image="/images/plgrid-plus-logo.png" },
	        #image { style="position: absolute; right: 40px; top: 14px;", image="/images/unia-logo.png" }
	    ]},
	    #panel { style=wf:f("height: ~Bpx;", [MarginTop + 82]) }
	].





% Development functions
empty_page() ->
	[
		#h6 { text="Not yet implemented" },
		#br{},  #br{},  #br{},  #br{},  #br{},  
		#br{},  #br{},  #br{},  #br{},  #br{}, 
		#br{},  #br{},  #br{},  #br{},  #br{}, 
		#br{},  #br{},  #br{},  #br{},  #br{},  
		#br{},  #br{},  #br{},  #br{},  #br{},  
		#br{},  #br{},  #br{},  #br{},  #br{},  
		#br{},  #br{},  #br{},  #br{},  #br{},  
		#br{},  #br{},  #br{},  #br{},  #br{},  
		#br{},  #br{},  #br{},  #br{},  #br{},  
		#br{},  #br{},  #br{},  #br{},  #br{},  
		#br{},  #br{},  #br{},  #br{},  #br{}
	].




% old_menu_captions() ->
% _MenuCaptions = 
% 	[
% 		{data_tab, #listitem { body=[
% 			#link{ style="padding: 18px;", url="/file_manager", text="Data" },
% 			#list { style="top: 37px;", body=[
% 				#listitem { body=#link{ url="/file_manager", text="File manager" } },
% 				#listitem { body=#link{ url="/shared_files", text="Shared files" } }
% 			]}
% 		]}},
% 		{rules_tab, #listitem { body=[
% 			#link{ style="padding: 18px;", url="/rules_composer", text="Rules" },
% 			#list {  style="top: 37px;", body=[
% 				#listitem { body=#link{ url="/rules_composer", text="Rules composer" } },
% 				#listitem { body=#link{ url="/rules_viewer", text="Rules viewer" } },
% 				#listitem { body=#link{ url="/rules_simulator", text="Rules simulator" } }
% 			]}
% 		]}},
% 		{administration_tab, #listitem { body=[
% 			#link{ style="padding: 18px;", url="/system_state", text="Administration" },
% 			#list {  style="top: 37px;", body=[
% 				#listitem { body=#link{ url="/system_state", text="System state" } },
% 				#listitem { body=#link{ url="/events", text="Events" } }
% 			]}
% 		]}}
% 	].





