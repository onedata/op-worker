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
-include_lib("ctool/include/logging.hrl").

% n2o API
-export([main/0, event/1]).

%% Template points to the template file, which will be filled with content
main() -> #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}.

%% Page title
title() -> <<"Logout page">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    gui_ctx:clear_session(),
    {ok, GlobalRegistryHostname} = application:get_env(veil_cluster_node, global_registry_hostname),
    gui_jq:redirect(list_to_binary(GlobalRegistryHostname ++ "/logout")).

event(init) -> ok;
event(to_login) -> gui_jq:redirect_to_login(false);
event(terminate) -> ok.
