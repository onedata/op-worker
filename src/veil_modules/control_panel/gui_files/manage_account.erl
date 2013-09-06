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

-module (manage_account).
-compile(export_all).
-include_lib("nitrogen_core/include/wf.hrl").

%% Template points to the template file, which will be filled with content
main() ->
  #template { file="./gui_static/templates/manage_account.html" }.

%% Page title
title() -> "VeilFS - manage account".

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
	case (wf:user() =:= undefined) or (wf:session(user_doc) =:= undefined) of
		true -> wf:redirect("/login");
		false -> 
			#panel { class = main_panel, body =  [
				#label { class = title, text = "Manage account" },
				#panel { class = inline_block, body = main_table()}
			]}
	end.

% Snippet generating account managment table
main_table() ->
	User = wf:session(user_doc),
	wf:wire(new_email_textbox, #hide{ }),   
	wf:wire(new_email_submit, #hide{ }),
	wf:wire(new_email_cancel, #hide{ }),	 
	wf:wire(new_dn_textbox, #hide{ }),   
	wf:wire(new_dn_submit, #hide{ }),
	wf:wire(new_dn_cancel, #hide{ }),	  
	
	#table { rows=[
		#tablerow { cells=[
			#tableheader { class = header_right, text="Property" },
			#tableheader { class = header_left,text="Value" }
		]},

		#tablerow { cells=[
			#tablecell { class = property, text="Login" },
			#tablecell { class = value,  text=user_logic:get_login(User) }
		]},

		#tablerow { cells=[
			#tablecell { class = property, text="Name" },
			#tablecell { class = value,  text=user_logic:get_name(User) }
		]},

		#tablerow { cells=[
			#tablecell { class = property, text="Teams" },
			#tablecell { class = value,  text=wf:to_list(user_logic:get_teams(User)) }
		]},

		#tablerow { cells=[
			#tablecell { class = property, text="E-mails" },
			#tablecell { class = value, body = [
				#list { numbered = true, body = 
					lists:map(
						fun(Email) ->
							#listitem { body = #span { body = 
								[ Email, #button { text = "X", postback = {update_email, User, {remove, Email} } } 
							]}}
						end, user_logic:get_email_list(User)) 
					++ [
							#listitem { body = [
								#button { id = add_email_button, text = "Add e-mail address", postback = {show_email_adding, true} },
								#textbox { id = new_email_textbox, text = "" },
								#button { id = new_email_submit, text = "Submit", postback = {update_email, User, {add, submitted} } },
								#button { id = new_email_cancel, text = "Cancel", postback = {show_email_adding, false} }
							]}
				]}
			]}
		]},

		#tablerow { cells=[
			#tablecell { class = property, text="Certificates' DNs" },
			#tablecell { class = value, body = [
				#list { numbered = true, body = 
					lists:map(
						fun(DN) ->
							#listitem { body = #span { body = 
								[ DN, #button { text = "X", postback = {update_dn, User, {remove, DN} } } 
							]}}
						end, user_logic:get_dn_list(User)) 
					++ [
							#listitem { body = [
								#button { id = add_dn_button, text = "Add new certificate", postback = {show_dn_adding, true} },
								#textarea { id = new_dn_textbox, style="width: 500px; height: 600px; vertical-align: top;", 
									text = "Paste your .pem certificate here..." },
								#button { id = new_dn_submit, text = "Submit", postback = {update_dn, User, {add, submitted} } },
								#button { id = new_dn_cancel, text = "Cancel", postback = {show_dn_adding, false} }
							]}
				]}
			]}
		]}  
	]}.


event({show_email_adding, Flag}) ->
	show_email_adding(Flag);

event({show_dn_adding, Flag}) ->
	show_dn_adding(Flag);
	
event({update_email, User, AddOrRemove}) ->
	OldEmailList = user_logic:get_email_list(User),
	{ok, NewUser} = case AddOrRemove of
		{add, submitted} -> user_logic:update_email_list(User, OldEmailList ++ [wf:q(new_email_textbox)]);
		{remove, Email} -> user_logic:update_email_list(User, OldEmailList -- [Email])
	end, 	
	wf:session(user_doc, NewUser),
	wf:redirect("/manage_account");

event({update_dn, User, AddOrRemove}) ->
	OldDnList = user_logic:get_dn_list(User),
	case AddOrRemove of
		{add, submitted} -> 
			case user_logic:extract_dn_from_cert(list_to_binary(wf:q(new_dn_textbox))) of
				{rdnSequence, RDNSequence} ->
					{ok, DnString} = user_logic:rdn_sequence_to_dn_string(RDNSequence),  
					{ok, NewUser} = user_logic:update_dn_list(User, OldDnList ++ [DnString]),
					wf:session(user_doc, NewUser),
					wf:redirect("/manage_account");
				{error, proxy_ceertificate} ->
					wf:wire(#alert{ text = "Proxy certificates are not accepted." }),
					show_dn_adding(false);
				{error, self_signed} ->
					wf:wire(#alert{ text = "Self signed certificates are not accepted." }),
					show_dn_adding(false);
				{error, extraction_failed} ->
					wf:wire(#alert{ text = "Unable to process certificate." }),
					show_dn_adding(false)
			end;
		{remove, DN} -> 
			{ok, NewUser} = user_logic:update_dn_list(User, OldDnList -- [DN]),
			wf:session(user_doc, NewUser),
			wf:redirect("/manage_account")
	end.


show_email_adding(Flag) ->
	case Flag of
	    true -> 
	      wf:wire(add_email_button, #hide{ }),
	      wf:wire(new_email_textbox, #appear { speed=500 }),
	      wf:wire(new_email_submit, #appear { speed=500 }),	    
	      wf:wire(new_email_cancel, #appear { speed=500 });     
	    false ->
	      wf:wire(add_email_button, #appear { speed=500 }),
	      wf:wire(new_email_textbox, #hide{ }),
	      wf:wire(new_email_cancel, #hide{ }),	   
	      wf:wire(new_email_submit, #hide{ })
	end.


show_dn_adding(Flag) ->
	case Flag of
	    true -> 
	      wf:wire(add_dn_button, #hide{ }),
	      wf:wire(new_dn_textbox, #appear { speed=500 }),
	      wf:wire(new_dn_submit, #appear { speed=500 }),	    
	      wf:wire(new_dn_cancel, #appear { speed=500 });     
	    false ->
	      wf:wire(add_dn_button, #appear { speed=500 }),
	      wf:wire(new_dn_textbox, #hide{ }),
	      wf:wire(new_dn_cancel, #hide{ }),	   
	      wf:wire(new_dn_submit, #hide{ })
	end.


%% c("../../../src/veil_modules/control_panel/gui_files/manage_account.erl").
  