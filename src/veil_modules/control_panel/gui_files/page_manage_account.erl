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

-module (page_manage_account).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").

%% Template points to the template file, which will be filled with content
main() -> #template { file="./gui_static/templates/bare.html" }.

%% Page title
title() -> "Manage account".

%% This will be placed in the template instead of [[[page:body()]]] tag
body() ->
	gui_utils:apply_or_redirect(?MODULE, render_body, false).


% Body content
render_body() ->
	[        
		gui_utils:top_menu(manage_account_tab),
		#panel { style="margin-top: 60px; padding: 20px;", body=[
			#panel { id=dn_error_panel, style="display: none;",
				class="dialog dialog-danger", body=[
				#p { text="To be able to use any functionalities, please do one of the following: " },
				#panel{ style="margin-left: auto; margin-right: auto; text-align: left; display: inline-block;", body=[
					#list { body=[
						#listitem { style="padding: 10px 0 0;", 
							text = "Enable certificate DN retrieval from your OpenID provider and log in again" },
						#listitem { style="padding: 7px 0 0;", 
							text = "Add your .pem certificate manually below" }
					]}
				]}
			]},
			#panel { id=helper_error_panel, style="display: none;",
				class="dialog dialog-danger", body=[
				#p { text="To be able to use any functionalities, there must be at least one storage helper defined." }
			]},
			#h6 { style=" text-align: center;", text="Manage account" },
			#panel { id=main_table, body = main_table() }
		]}
	].


% Info to register a DN
maybe_display_dn_message() ->
	case user_logic:get_dn_list(wf:session(user_doc)) of
		[] -> wf:wire(dn_error_panel, #show { });
		_ -> wf:wire(dn_error_panel, #hide { })
	end.


% Info to install a storage helper
maybe_display_helper_message() ->
	case gui_utils:storage_defined() of
		false -> wf:wire(helper_error_panel, #show { });
		true -> wf:wire(helper_error_panel, #hide { })
	end.


% Snippet generating account managment table
main_table() ->
	maybe_display_dn_message(),
	maybe_display_helper_message(),
	User = wf:session(user_doc),
	#table { style="border-width: 0px; width: auto;", rows=[

		#tablerow { cells=[
			#tablecell { style="border-width: 0px; padding: 10px 10px", body=
				#label { class="label label-large label-inverse", style="cursor: auto;", text="Login" }},
			#tablecell { style="border-width: 0px; padding: 10px 10px", body=#p { text=user_logic:get_login(User) } }
		]},

		#tablerow { cells=[
			#tablecell { style="border-width: 0px; padding: 10px 10px", body=
				#label { class="label label-large label-inverse", style="cursor: auto;", text="Name" }},
			#tablecell { style="border-width: 0px; padding: 10px 10px", body=#p {  text=user_logic:get_name(User) } }
		]},

		#tablerow { cells=[
			#tablecell { style="border-width: 0px; padding: 10px 10px", body=
				#label { class="label label-large label-inverse", style="cursor: auto;", text="Teams" }},
			#tablecell { style="border-width: 0px; padding: 10px 10px", body=team_list_body() }
		]},

		#tablerow { cells=[
			#tablecell { style="border-width: 0px; padding: 10px 10px", body=
				#label { class="label label-large label-inverse", style="cursor: auto;", text="E-mails" }},
			#tablecell { style="border-width: 0px; padding: 10px 10px", body = email_list_body() }
		]},

		#tablerow { cells=[
			#tablecell { style="border-width: 0px; padding: 10px 10px", body=
				#label { class="label label-large label-inverse", style="cursor: auto;", text="Certificates' DNs" }},
			#tablecell { style="border-width: 0px; padding: 10px 10px", body = dn_list_body() }
		]}  
	]}.


% HTML list with teams printed
team_list_body() ->
	User = wf:session(user_doc),
	TeamsString = user_logic:get_teams(User),
	Teams = string:tokens(TeamsString, ","),
	#list { numbered=true, body = 
		lists:map(
			fun(Team) ->
				#listitem { style="font-size: 18px; padding: 5px 0;", 
					body=re:replace(Team, "\\(", " (", [global, {return, list}]) }
			end, Teams)
	}.


% HTML list with emails printed
email_list_body() ->
	User = wf:session(user_doc),
	#list { numbered = true, body = 
		lists:map(
			fun(Email) ->
				#listitem { style="font-size: 18px; padding: 5px 0;", body = #span { body = 
					[ 
						Email,									
                        #link{ class="glyph-link", style="margin-left: 10px;", 
                            postback={action, update_email, [User, {remove, Email}] }, body=
                            #span{ class="fui-cross", style="font-size: 16px;" } }
					]}}
			end, user_logic:get_email_list(User)) 
		++ [
			#listitem { style="font-size: 18px; padding: 5px 0;", body = [
				#link{ id=add_email_button, class="glyph-link", style="margin-left: 10px;", 
					postback={action, show_email_adding, [true]}, body=
					#span{ class="fui-plus", style="font-size: 16px; position: relative;" } },
				#textbox { id=new_email_textbox, class="flat", text = "", style="display: none;",
					placeholder="New email address", postback={action, update_email, [User, {add, submitted}]} },
				#link{ id=new_email_submit, class="glyph-link", style="display: none; margin-left: 10px;", 
					postback={action, update_email, [User, {add, submitted}]}, body=
					#span{ class="fui-check-inverted", style="font-size: 20px;" } },
				#link{ id=new_email_cancel, class="glyph-link", style="display: none; margin-left: 10px;", 
					postback={action, show_email_adding, [false]}, body=
					#span{ class="fui-cross-inverted", style="font-size: 20px;" } }							
			]}
	]}.


% HTML list with DNs printed
dn_list_body() ->
	User = wf:session(user_doc),
	#list { numbered = true, body = 
		lists:map(
			fun(DN) ->
				#listitem { style="font-size: 18px; padding: 5px 0;", body = #span { body = 
					[ 
						DN,									
                        #link{ class="glyph-link", style="margin-left: 10px;", 
                            postback={action, update_dn, [User, {remove, DN}] }, body=
                            #span{ class="fui-cross", style="font-size: 16px;" } }
					]}}
			end, user_logic:get_dn_list(User)) 
		++ [
				#listitem { style="font-size: 18px; padding: 5px 0;", body = [
				#link{ id=add_dn_button, class="glyph-link", style="margin-left: 10px;", 
					postback={action, show_dn_adding, [true]}, body=
					#span{ class="fui-plus", style="font-size: 16px;" } },
				#textarea { id = new_dn_textbox, style="display: none; font-size: 12px; width: 600px; height: 200px; 
					vertical-align: top; overflow-y: scroll;", 
					text="", placeholder = "Paste your .pem certificate here..." },
				#link{ id=new_dn_submit, class="glyph-link", style="display: none; margin-left: 10px;", 
					postback={action, update_dn, [User, {add, submitted}]}, body=
					#span{ class="fui-check-inverted", style="font-size: 20px;" } },
				#link{ id=new_dn_cancel, class="glyph-link", style="display: none; margin-left: 10px;", 
					postback={action, show_dn_adding, [false]}, body=
					#span{ class="fui-cross-inverted", style="font-size: 20px;" } }	
				]}
	]}.



% Postback event handling
event({action, Fun}) -> 
    event({action, Fun, []});

event({action, Fun, Args}) ->   
	gui_utils:apply_or_redirect(?MODULE, Fun, Args, false).
	

% Update email list - add or remove one and save new user doc
update_email(User, AddOrRemove) ->
	OldEmailList = user_logic:get_email_list(User),
	{ok, NewUser} = case AddOrRemove of
		{add, submitted} -> 
			NewEmail = wf:q(new_email_textbox),
			case user_logic:get_user({email, NewEmail}) of
				{ok, _} -> 
					wf:wire(#alert { text="This e-mail address is in use." }),
					{ok, User}; 
				_ -> 
					user_logic:update_email_list(User, OldEmailList ++ [NewEmail])
			end;
		{remove, Email} -> user_logic:update_email_list(User, OldEmailList -- [Email])
	end, 	
	wf:session(user_doc, NewUser),
	wf:update(main_table, main_table()).


% Update DN list - add or remove one and save new user doc
update_dn(User, AddOrRemove) ->
	OldDnList = user_logic:get_dn_list(User),
	case AddOrRemove of
		{add, submitted} -> 
			case user_logic:extract_dn_from_cert(list_to_binary(wf:q(new_dn_textbox))) of
				{rdnSequence, RDNSequence} ->
					{ok, DnString} = user_logic:rdn_sequence_to_dn_string(RDNSequence),  
					case user_logic:get_user({dn, DnString}) of
						{ok, _} -> 
							wf:wire(#alert{ text = "This certificate is already registered." }); 
						_ ->
							{ok, NewUser} = user_logic:update_dn_list(User, OldDnList ++ [DnString]),
							wf:session(user_doc, NewUser)
					end;
				{error, proxy_ceertificate} ->
					wf:wire(#alert{ text = "Proxy certificates are not accepted." });
				{error, self_signed} ->
					wf:wire(#alert{ text = "Self signed certificates are not accepted." });
				{error, extraction_failed} ->
					wf:wire(#alert{ text = "Unable to process certificate." })
			end;
		{remove, DN} -> 
			{ok, NewUser} = user_logic:update_dn_list(User, OldDnList -- [DN]),
			wf:session(user_doc, NewUser)
	end,
	wf:update(main_table, main_table()).


% Show email adding form
show_email_adding(Flag) ->
	case Flag of
	    true -> 
	      wf:wire(add_email_button, #hide{ }),
	      wf:wire(new_email_textbox, #appear { speed=300 }),
	      wf:wire(new_email_submit, #appear { speed=300 }),	    
	      wf:wire(new_email_cancel, #appear { speed=300 }),
	      wf:wire(#script { script="obj('new_email_textbox').focus();" });  
	    false ->
	      wf:wire(add_email_button, #appear { speed=300 }),
	      wf:wire(new_email_textbox, #hide{ }),
	      wf:wire(new_email_cancel, #hide{ }),	   
	      wf:wire(new_email_submit, #hide{ })
	end.


% Show DN adding form
show_dn_adding(Flag) ->
	case Flag of
	    true -> 
	      wf:wire(add_dn_button, #hide{ }),
	      wf:wire(new_dn_textbox, #appear { speed=300 }),
	      wf:wire(new_dn_submit, #appear { speed=300 }),	    
	      wf:wire(new_dn_cancel, #appear { speed=300 }),
	      wf:wire(#script { script="obj('new_dn_submit').focus();" });     
	    false ->
	      wf:wire(add_dn_button, #appear { speed=300 }),
	      wf:wire(new_dn_textbox, #hide{ }),
	      wf:wire(new_dn_cancel, #hide{ }),	   
	      wf:wire(new_dn_submit, #hide{ })
	end.


%% c("../../../src/veil_modules/control_panel/gui_files/manage_account.erl").
  