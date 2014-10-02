%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% This page performs authentication of users that are redirected
%% from the Global Registry.
%% @end
%% ===================================================================

-module(page_openid_login).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").
-include_lib("ctool/include/logging.hrl").

%% Template points to the template file, which will be filled with content
main() -> #dtl{file = "bare", app = ?APP_Name, bindings = [{title, title()}, {body, body()}]}.

%% Page title
title() -> <<"OpenID login">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    case openid_utils:validate_login() of
        ok ->
            gui_jq:redirect_from_login(),
            <<"">>;
        {error, ErrorID} ->
            page_error:redirect_with_error(ErrorID),
            <<"">>
    end.


event(init) -> ok;
event(terminate) -> ok.