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

-module(page_error).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").

%% Template points to the template file, which will be filled with content
main() -> #template { file="./gui_static/templates/bare.html" }.

%% Page title
title() -> "Error".

%% This will be placed in the template instead of [[[page:body()]]] tag
body() -> 
	#panel { class="alert alert-danger login-page", body=[
		#h3 { text="Error" },
		#p { class="login-info", style="font-weight: bold;", text=wf:q(reason) },
		#p { class="login-info", text=wf:q(details) },
		#button { postback=to_login, class="btn btn-warning btn-block", text="Login page" }
	]}.

event(to_login) ->
	wf:redirect("/login").




% This function causes a HTTP rediurect to error page, which displays an error message.

redirect_with_error(Reason, Details) ->
    wf:redirect(wf:f("/error?reason=~s&details=~s", 
    	[wf:url_encode(Reason), wf:url_encode(Details)])).



% These functions allow easy edition of error messages. They are called from nitrogen_handler
% to obtain redirect url with proper args enclosed in cowboy request

generate_redirect_request(Req, Reason, Details) ->
    Qs = list_to_binary(wf:f("reason=~s&details=~s", 
    	[wf:url_encode(Reason), wf:url_encode(Details)])),
	{false, cowboy_req:set([{path, <<"/error">>}, {qs, Qs}], Req)}.


user_content_request_error(Message, Req) ->
    {Reason, Details} = case Message of
        not_logged_in -> {"No active session", "You need to log in to download your content."};
        file_not_found -> {"Invalid URL", "This URL doesn't point to any file. "};
        sending_failed -> {"Internal server error", "Failed during serving the file. " ++ 
            "Please try again or contact the site administrator if the problem persists."}
    end,
    generate_redirect_request(Req, Reason, Details).


shared_file_request_error(Message, Req) ->
	{Reason, Details} = case Message of
	    file_not_found -> {"Invalid link", "This link doesn't point to any shared file. " ++
	        "This is because the file is no longer shared or the share has never existed."};
	    sending_failed -> {"Internal server error", "Failed during serving the file. " ++ 
	        "Please try again or contact the site administrator if the problem persists."}
	end,
	generate_redirect_request(Req, Reason, Details).
