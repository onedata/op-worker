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
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

% n2o API
-export([main/0, event/1]).

%% Template points to the template file, which will be filled with content
main() -> #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}.

%% Page title
title() -> <<"Logout page">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    Params = gui_ctx:form_params(),
    LogoutToken = vcn_gui_utils:get_logout_token(),
    case proplists:get_value(?logout_token, Params) of
        LogoutToken ->
            gui_ctx:clear_session(),
            #panel{style = <<"position: relative;">>, body =
            [
                #panel{class = <<"alert alert-success login-page">>, body = [
                    #h3{class = <<"">>, body = <<"Logout successful">>},
                    #p{class = <<"login-info">>, body = <<"Come back soon.">>},
                    #button{postback = to_login, class = <<"btn btn-primary btn-block">>, body = <<"Login page">>}
                ]},
                gui_utils:cookie_policy_popup_body(?privacy_policy_url)
            ]
            ++ vcn_gui_utils:logotype_footer(120)
                ++ [#p{body = <<"<iframe src=\"https://openid.plgrid.pl/logout\" style=\"display:none\"></iframe>">>}]
            };
        _ ->
            gui_jq:redirect("/")
    end.

event(init) -> ok;
event(to_login) -> gui_jq:redirect_to_login(false);
event(terminate) -> ok.
