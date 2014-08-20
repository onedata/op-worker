%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page handles users' logging in.
%% @end
%% ===================================================================

-module(page_login).
-include("veil_modules/control_panel/common.hrl").

% n2o API
-export([main/0, event/1]).

%% Template points to the template file, which will be filled with content
main() -> #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}.

%% Page title
title() -> <<"Login page">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    LoginProplist = [
        {global_id, "abcd"},
        {login, "plglopiola"},
        {name, "Siema Eniu"},
        {teams, ["plggveilfs"]},
        {emails, ["email@email.com"]},
        {dn_list, []}
    ],
    {Login, UserDoc} = user_logic:sign_in(LoginProplist),
    gui_ctx:create_session(),
    gui_ctx:set_user_id(Login),
    vcn_gui_utils:set_user_fullname(user_logic:get_name(UserDoc)),
    vcn_gui_utils:set_user_role(user_logic:get_role(UserDoc)),
    gui_jq:redirect(<<"/">>),
    ok.
%%     case gui_ctx:user_logged_in() of
%%         true ->
%%             gui_jq:redirect(<<"/">>),
%%             [];
%%         false ->
%%             {ok, GlobalRegistryHostname} = application:get_env(veil_cluster_node, global_registry_hostname),
%%             gui_jq:redirect(atom_to_binary(GlobalRegistryHostname, latin1)),
%%             []
%%     end.


event(init) -> ok;
event(terminate) -> ok.