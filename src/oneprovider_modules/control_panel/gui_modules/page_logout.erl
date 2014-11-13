%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page handles users' logging out.
%% @end
%% ===================================================================

-module(page_logout).
-include("oneprovider_modules/control_panel/common.hrl").
-include_lib("ctool/include/logging.hrl").

% n2o API
-export([main/0, event/1]).

%% Template points to the template file, which will be filled with content
main() -> #dtl{file = "bare", app = ?APP_Name, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}.

%% Page title
title() -> <<"Logout page">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    case application:get_env(?APP_Name, developer_mode) of
        {ok, true} -> body_devel();
        _ -> body_production()
    end.


% In production, the user will be redirected to Global Registry and logged out there too
body_production() ->
    Params = gui_ctx:form_params(),
    LogoutToken = opn_gui_utils:get_logout_token(),
    {ok, GlobalRegistryHostname} = application:get_env(?APP_Name, global_registry_hostname),
    case proplists:get_value(?logout_token, Params) of
        LogoutToken ->
            ?debug("User ~p logged out", [gui_ctx:get_user_id()]),
            gui_ctx:clear_session(),
            gui_jq:redirect(list_to_binary(GlobalRegistryHostname));
        _ ->
            gui_jq:redirect(<<"/">>)
    end,
    [].


% For development, just go back to login page
body_devel() ->
    Params = gui_ctx:form_params(),
    LogoutToken = opn_gui_utils:get_logout_token(),
    case proplists:get_value(?logout_token, Params) of
        LogoutToken ->
            ?debug("User ~p logged out", [gui_ctx:get_user_id()]),
            gui_ctx:clear_session(),
            #panel{style = <<"position: relative;">>, body =
            [
                #panel{class = <<"alert alert-success login-page">>, body = [
                    #h3{class = <<"">>, body = <<"Logout successful">>},
                    #p{class = <<"login-info">>, body = <<"Come back soon.">>},
                    #link{url = <<"/">>, class = <<"btn btn-primary btn-block">>, body = <<"Login page">>}
                ]},
                gui_utils:cookie_policy_popup_body(?privacy_policy_url)
            ] ++ [#p{body = <<"<iframe src=\"https://openid.plgrid.pl/logout\" style=\"display:none\"></iframe>">>}]
            };
        _ ->
            gui_jq:redirect(<<"/">>),
            []
    end.

event(init) -> ok;
event(terminate) -> ok.
