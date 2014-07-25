%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page is displayed whenever an error occurs.
%% @end
%% ===================================================================

-module(page_error).
-include("veil_modules/control_panel/common.hrl").

% n2o API
-export([main/0, event/1]).
% Functions used externally
-export([redirect_with_error/1, generate_redirect_request/2]).

%% Template points to the template file, which will be filled with content
main() -> #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}.

%% Page title
title() -> <<"Error">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    {Reason, Description} = get_reason_and_description(),
    #panel{style = <<"position: relative;">>, body = [
        #panel{class = <<"alert alert-danger login-page">>, body = [
            #h3{body = <<"Error">>},
            #p{class = <<"login-info">>, style = <<"font-weight: bold;">>, body = Reason},
            #p{class = <<"login-info">>, body = Description},
            #button{postback = to_login, class = <<"btn btn-warning btn-block">>, body = <<"Main page">>}
        ]},
        gui_utils:cookie_policy_popup_body(?privacy_policy_url)
    ] ++ vcn_gui_utils:logotype_footer(120)}.

event(init) -> ok;
event(to_login) -> gui_jq:redirect_to_login(false);
event(terminate) -> ok.


% This function causes a HTTP redirect to error page, which displays an error message.
redirect_with_error(ErrorID) ->
    gui_jq:redirect(<<"/error?id=", (gui_str:to_binary(ErrorID))/binary>>).


% This functions is intended to be called from n2o handlers
% to make n2o render error page instead of requested page.
% wf_core:run is called so the rror page is displayed by n2o engine.
generate_redirect_request(Req, ErrorID) ->
    {ok, NewReq} = wf_core:run(cowboy_req:set([{path, <<"/error">>}, {qs, <<"id=", (gui_str:to_binary(ErrorID))/binary>>}], Req)),
    {ok, NewReq}.


get_reason_and_description() ->
    IDBinary = gui_str:to_binary(gui_ctx:url_param(<<"id">>)),
    id_to_reason_and_message(binary_to_atom(IDBinary, latin1)).


id_to_reason_and_message(?error_user_content_not_logged_in) ->
    {<<"No active session">>, <<"You need to log in to download your files.">>};

id_to_reason_and_message(?error_user_content_file_not_found) ->
    {<<"Invalid URL">>, <<"This URL doesn't point to any file.">>};

id_to_reason_and_message(?error_user_permission_denied) ->
    {<<"Permission denied">>, <<"You don't have permission to read this file.">>};

id_to_reason_and_message(?error_shared_file_not_found) ->
    {<<"Invalid link">>,
        <<"Invalid link", "This link doesn't point to any shared file. This is because the file is no longer shared or the share has never existed.">>};

id_to_reason_and_message(?error_internal_server_error) ->
    {<<"Internal server error">>,
        <<"Server encountered an unexpected error. Please contact the site administrator if the problem persists.">>};

id_to_reason_and_message(?error_openid_invalid_request) ->
    {<<"Invalid request">>,
        <<"Error occured while processing this authentication request.">>};

id_to_reason_and_message(?error_openid_auth_invalid) ->
    {<<"Invalid request">>,
        <<"OpenID Provider denied the authenticity of this login request.">>};

id_to_reason_and_message(?error_openid_no_connection) ->
    {<<"Connection problem">>,
        <<"Unable to reach OpenID Provider.">>};

id_to_reason_and_message(?error_openid_login_error) ->
    {<<"Login error">>,
        <<"Could not process OpenID response. Please contact the site administrator if the problem persists.">>};

id_to_reason_and_message(?error_login_dir_creation_error) ->
    {<<"User creation error">>,
        <<"Server could not create user directories. Please contact the site administrator if the problem persists.">>};

id_to_reason_and_message(?error_login_dir_chown_error) -> {<<"User creation error">>,
    <<"Server could not change owner of user directories. Please contact the site administrator if the problem persists.">>};

id_to_reason_and_message(?error_authentication) -> {<<"Authentication error">>,
    <<"Server could not authenticate you. Please try again to log in or contact the site administrator if the problem persists.">>};

id_to_reason_and_message(_) ->
    {<<"Unknown">>, <<"No description">>}.
