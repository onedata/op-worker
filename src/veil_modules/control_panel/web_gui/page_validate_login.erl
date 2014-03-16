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

-module (page_validate_login).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

%% Template points to the template file, which will be filled with content
main() -> #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}]}.

%% Page title
title() -> [<<"Login page">>].

%% This will be placed in the template instead of [[[page:body()]]] tag
body() ->
	case gui_utils:user_logged_in() of
		true -> wf:redirect(<<"/">>);
		false -> 
			LoginMessage = case openid_utils:prepare_validation_parameters() of
				{error, invalid_request} -> {error, invalid_request};
				{EndpointURL, RequestBody} -> openid_utils:validate_openid_login({EndpointURL, RequestBody})
			end,
			
			case LoginMessage of
				{error, invalid_request} ->
					page_error:redirect_with_error("Invalid request", "Error occured while " ++
						"processing this authentication request.");

				{error, auth_invalid} ->
					page_error:redirect_with_error("Invalid request", "OpenID Provider denied " ++ 
						"the authenticity of this login request.");

				{error, no_connection} ->
					page_error:redirect_with_error("Connection problem", 
						"Unable to reach OpenID Provider.");

				ok ->					
					try
						{ok, Proplist} = openid_utils:retrieve_user_info(),
						{Login, UserDoc} = user_logic:sign_in(Proplist),						
						wf:user(Login),	
						wf:session(user_doc, UserDoc),
						gui_utils:redirect_from_login()
					catch Type:Message ->
				        ?error_stacktrace("Error in validate_login - ~p:~p", [Type, Message]),
				        page_error:redirect_with_error("Internal server error", 
				        	"Server encountered an unexpected error. Please contact the site administrator if the problem persists.")
					end	
			end
	end.

event(init) -> ok.
