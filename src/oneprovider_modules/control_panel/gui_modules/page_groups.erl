%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page allows user to manage his groups.
%% @end
%% ===================================================================

-module(page_groups).
-include("oneprovider_modules/control_panel/common.hrl").
-include_lib("ctool/include/global_registry/gr_groups.hrl").
-include_lib("ctool/include/logging.hrl").

% n2o API and comet
-export([main/0, event/1, api_event/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% main/0
%% ====================================================================
%% @doc Template points to the template file, which will be filled with content.
-spec main() -> #dtl{}.
%% ====================================================================
main() ->
    case opn_gui_utils:maybe_redirect(true, false) of
        true ->
            #dtl{file = "bare", app = ?APP_Name, bindings = [
                {title, <<"">>},
                {body, <<"">>},
                {custom, <<"">>},
                {css, <<"">>}
            ]};
        false ->
            #dtl{file = "bare", app = ?APP_Name, bindings = [
                {title, title()},
                {body, body()},
                {custom, <<"">>},
                {css, css()}
            ]}
    end.


%% title/0
%% ====================================================================
%% @doc Page title.
-spec title() -> binary().
%% ====================================================================
title() -> <<"Groups">>.


%% css/0
%% ====================================================================
%% @doc Page specific CSS import clause.
-spec css() -> binary().
%% ====================================================================
css() ->
    <<"<link rel=\"stylesheet\" href=\"/css/groups.css\" type=\"text/css\" media=\"screen\" charset=\"utf-8\" />">>.


%% body/0
%% ====================================================================
%% @doc This will be placed instead of {{body}} tag in template.
-spec body() -> [#panel{}].
%% ====================================================================
body() ->
    gui_jq:wire(<<"$('#group-actions-dropdown-1').selectpicker({style: 'btn-small', menuStyle: 'dropdown'});">>),
    #panel{class = <<"page-container">>, body = [
        #panel{id = <<"spinner">>, body = #image{image = <<"/images/spinner.gif">>}},
        opn_gui_utils:top_menu(groups_tab),
        #panel{id = <<"page-content">>, body = [
            #panel{id = <<"message">>, class = <<"dialog">>},
            #h6{class = <<"manage-groups-header">>, body = <<"Manage groups">>},
            #panel{class = <<"top-buttons-panel">>, body = [
                #button{id = <<"create_group_button">>, postback = create_group,
                    class = <<"btn btn-inverse btn-small top-button">>, body = <<"Create new group">>},
                #button{id = <<"join_group_button">>, postback = join_group,
                    class = <<"btn btn-inverse btn-small top-button">>, body = <<"Join existing group">>}
            ]},
            #panel{class = <<"group-list-panel">>, body = [
                #list{class = <<"group-list">>, body = [
                    #li{body = [
                        #panel{class = <<"group-container">>, body = [
                            #panel{class = <<"group-header">>, body = [
                                #panel{class = <<"group-icon-ph">>, body = [
                                    #span{class = <<"icomoon-users">>}
                                ]},
                                #panel{class = <<"group-name-ph">>, body = [
                                    #p{class = <<"group-name">>, body = [<<"ACK Cyfronet AGH">>]},
                                    #p{class = <<"group-id">>, body = [<<"ID: 0s8dg7fss07gsdfg8sdfg7s9d7g98df7g">>]}
                                ]},
                                #panel{class = <<"group-actions-ph">>, body = [
                                    #panel{class = <<"btn-group group-actions-dropdown">>, body = [
                                        <<"<i class=\"dropdown-arrow\"></i>">>,
                                        #button{class = <<"btn btn-small btn-info">>, body = [
                                            <<"<i class=\"icomoon-cog group-actions-dropdown-cog\"></i>">>,
                                            <<"Actions">>
                                        ]},
                                        #button{title = <<"title">>,
                                            class = <<"btn btn-small btn-info dropdown-toggle">>,
                                            data_fields = [{<<"data-toggle">>, <<"dropdown">>}],
                                            body = #span{class = <<"caret">>}},
                                        #list{id = <<"argh">>, class = <<"dropdown-menu">>,
                                            body = [
                                                #li{body = [
                                                    #link{body = [
                                                        <<"<i class=\"icomoon-remove\"></i>">>,
                                                        <<"Delete">>
                                                    ]}
                                                    %TODO Z INDEX
                                                ]},
                                                #li{body = [
                                                    #link{body = [
                                                        <<"<i class=\"icomoon-arrow-up2\"></i>">>,
                                                        <<"Move up">>
                                                    ]}
                                                ]},
                                                #li{body = [
                                                    #link{body = [
                                                        <<"<i class=\"icomoon-arrow-down2\"></i>">>,
                                                        <<"Move down">>
                                                    ]}
                                                ]}
                                            ]}
                                    ]}
                                ]}
                            ]}
                        ]}
                    ]}
                ]}
            ]}
        ]}
    ]}.


%% event/1
%% ====================================================================
%% @doc Handles page events.
-spec event(Event :: term()) -> no_return().
%% ====================================================================
event(init) ->
    ok;

event(terminate) ->
    ok.


%% api_event/3
%% ====================================================================
%% @doc Handles page events.
-spec api_event(Name :: string(), Args :: string(), Req :: string()) -> no_return().
%% ====================================================================
api_event(_Function, _Args, _) ->
    ok.