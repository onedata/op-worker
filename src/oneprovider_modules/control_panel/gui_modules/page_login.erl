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
-include("oneprovider_modules/control_panel/common.hrl").
-include("oneprovider_modules/control_panel/global_registry_interfacing.hrl").
-include_lib("ctool/include/logging.hrl").

% n2o API
-export([main/0, event/1]).

%% Template points to the template file, which will be filled with content
main() -> #dtl{file = "bare", app = ?APP_Name, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}.

%% Page title
title() -> <<"Login page">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    case application:get_env(?APP_Name, developer_mode) of
        {ok, true} -> body_devel();
        _ -> body_production()
    end.


% In production, the user will be redirected straight to Global Registry
body_production() ->
    case gui_ctx:user_logged_in() of
        true -> gui_jq:redirect(<<"/">>);
        false -> event(globalregistry_login)
    end.


% For development, you can choose whether to log in via GR or directly via PLGrid OpenID
body_devel() ->
    case gui_ctx:user_logged_in() of
        true ->
            gui_jq:redirect(<<"/">>),
            [];
        false ->
            #panel{style = <<"position: relative;">>, body = [
                #panel{class = <<"alert alert-success login-page">>, body = [
                    #h3{body = <<"Welcome to onedata">>},
                    #p{class = <<"login-info">>, body = <<"THIS IS A NON-PRODUCTION, DEVELOPER-FRIENDLY LOGIN PAGE">>},
                    #button{postback = plgrid_login, class = <<"btn btn-primary">>, body = <<"Log in via PL-Grid OpenID">>},
                    #button{postback = globalregistry_login, class = <<"btn btn-warning">>, body = <<"Log in via Global Registry">>}
                ]},
                gui_utils:cookie_policy_popup_body(?privacy_policy_url)
            ]}
    end.


event(init) ->
    ok;

event(terminate) ->
    ok;

% Login event handling
event(globalregistry_login) ->
    {ok, GlobalRegistryHostname} = application:get_env(?APP_Name, global_registry_hostname),
    ProviderID = try cluster_manager_lib:get_provider_id() catch _:_ -> <<"">> end,
    RedirectURL =
        <<(list_to_binary(GlobalRegistryHostname))/binary, ?gr_login_endpoint,
        "?", ?referer_request_param, "=", ProviderID/binary>>,
    gui_jq:redirect(RedirectURL);

event(plgrid_login) ->
    % Resolve hostname, which was requested by a client
    Hostname = gui_ctx:get_requested_hostname(),
    case Hostname of
        undefined ->
            gui_jq:update(<<"error_message">>, <<"Cannot establish requested hostname. Please contact the site administrator.">>),
            gui_jq:fade_in(<<"error_message">>, 300);
        Host ->
            % Get redirect URL and redirect to OpenID login
            case plgrid_openid_utils:get_login_url(Host) of
                {error, _} ->
                    gui_jq:update(<<"error_message">>, <<"Unable to reach OpenID Provider. Please try again later.">>),
                    gui_jq:fade_in(<<"error_message">>, 300);
                URL ->
                    gui_jq:redirect(URL)
            end
    end.
