%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code
%% @end
%% ===================================================================

-module(page_error).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").

%% Template points to the template file, which will be filled with content
main() -> #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}]}.

%% Page title
title() -> <<"Error">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    #panel{style = <<"position: relative;">>, body = [
        #panel{class = <<"alert alert-danger login-page">>, body = [
            #h3{body = <<"Error">>},
            #p{class = <<"login-info">>, style = <<"font-weight: bold;">>, body = wf:q(<<"reason">>)},
            #p{class = <<"login-info">>, body = wf:q(<<"details">>)},
            #button{postback = to_login, class = <<"btn btn-warning btn-block">>, body = <<"Login page">>}
        ]}
    ] ++ gui_utils:logotype_footer(120)}.

event(init) -> ok;
event(to_login) -> gui_utils:redirect_to_login(false).


% This function causes a HTTP rediurect to error page, which displays an error message.

redirect_with_error(Reason, Details) ->
    wf:redirect(wf:to_binary(wf:f("/error?reason=~s&details=~s",
        [wf:url_encode(Reason), wf:url_encode(Details)]))).


% These functions allow easy edition of error messages. They are called from n2o handlers
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
