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
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

%% Template points to the template file, which will be filled with content
main() -> #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}.

%% Page title
title() -> <<"Logout page">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    wf:user(undefined),
    wf:session(user_doc, undefined),
    %wf:logout(), % Not yet implemented in n2o stable realease
    #panel{style = <<"position: relative;">>, body =
    [
        #panel{class = <<"alert alert-success login-page">>, body = [
            #h3{class = <<"">>, body = <<"Logout successful">>},
            #p{class = <<"login-info">>, body = <<"Come back soon.">>},
            #button{postback = to_login, class = <<"btn btn-primary btn-block">>, body = <<"Login page">>}
        ]}
    ]
    ++ gui_utils:logotype_footer(120)
        ++ [#p{body = <<"<iframe src=\"https://openid.plgrid.pl/logout\" style=\"display:none\"></iframe>">>}]
    }.

event(init) -> ok;
event(to_login) -> gui_utils:redirect_to_login(false).