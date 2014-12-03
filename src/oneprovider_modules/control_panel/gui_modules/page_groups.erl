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

% User priviledges - pairs of {ColumnName, PrivilegeName}
-define(PRIVILEGES, [
    {<<"View<br />group">>, <<"group_view_data">>},
    {<<"Modify<br />group">>, <<"group_change_data">>},
    {<<"Remove<br />group">>, <<"group_remove">>},
    {<<"Invite<br />user">>, <<"group_invite_user">>},
    {<<"Remove<br />user">>, <<"group_remove_user">>},
    {<<"Create<br />Space">>, <<"group_create_space">>},
    {<<"Join<br />Space">>, <<"group_join_space">>},
    {<<"Leave<br />Space">>, <<"group_leave_space">>},
    {<<"Invite<br />provider">>, <<"group_create_space_token">>},
    {<<"Set<br />privileges">>, <<"group_set_privileges">>}
]).

-define(COLUMNS_NAMES, [<<"View group">>, <<"Modify group">>, <<"Remove group">>, <<"Invite user">>, <<"Remove user">>,
    <<"Create Space">>, <<"Join Space">>, <<"Leave Space">>, <<"Invite provider">>, <<"Set privileges">>]).
-define(PRIVILEGES_NAMES, [<<"group_view_data">>, <<"group_change_data">>, <<"group_remove">>, <<"group_invite_user">>,
    <<"group_remove_user">>, <<"group_create_space">>, <<"group_join_space">>, <<"group_leave_space">>,
    <<"group_create_space_token">>, <<"group_set_privileges">>]).

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
    % Adjust user tables' headers width (scroll bar takes some space out of table and header would be too wide)
    gui_jq:wire(<<"var pad = $($('.users-table-wrapper')[0]).width() - $($('.users-table')[0]).width();",
    "$('.users-table-header-wrapper').css('padding-right', pad + 'px');">>),
    #panel{class = <<"page-container">>, body = [
        #panel{id = <<"spinner">>, body = #image{image = <<"/images/spinner.gif">>}},
        opn_gui_utils:top_menu(groups_tab),
        #panel{id = <<"page_content">>, body = [
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
                    group_list_element(<<"ACK Cyfronet AGH">>, <<"0s8dg7fss07gsdfg8sdfg7s9d7g98df7g">>),
                    group_list_element(<<"PCSS Poznan">>, <<"0906778dfg786dfg786as8d56ads5fa9sd8">>),
                    group_list_element(<<"WCSS Wroclaw">>, <<"564sdf564865d4fs56sd4f68g5d4fsg54">>)
                ]}
            ]}
        ]}
    ]}.


%% group_list_element/2
%% ====================================================================
%% @doc Renders HTML responsible for group list element.
-spec group_list_element(GroupName :: binary(), GroupID :: binary()) -> term().
%% ====================================================================
group_list_element(GroupName, GroupID) ->
    GroupNamePlaceholderID = <<"gr_name_ph_", GroupID/binary>>,
    ListElement = #li{body = [
        #panel{class = <<"group-container">>, body = [
            #panel{class = <<"group-header">>, body = [
                #panel{class = <<"group-header-icon-ph">>, body = [
                    #span{class = <<"icomoon-users">>}
                ]},
                #panel{id = GroupNamePlaceholderID, class = <<"group-name-ph">>, body = [
                    #p{class = <<"group-name">>, body = [GroupName]},
                    #p{class = <<"group-id">>, body = [<<"ID: ", GroupID/binary>>]}
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
                        #list{class = <<"dropdown-menu">>,
                            body = [
                                #li{body = [
                                    #link{body = [
                                        <<"<i class=\"icomoon-remove\"></i>">>,
                                        <<"Delete">>
                                    ]}
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
            ]},
            #panel{class = <<"group-users">>, body = [group_users_body()]},
            #panel{class = <<"group-spaces">>, body = []}
        ]}
    ]},
    gui_jq:wire(gui_jq:postback_action(GroupNamePlaceholderID, {expand, GroupNamePlaceholderID})),
    ListElement.


group_users_body() ->
    [
        #panel{class = <<"group-users-left-ph">>, body = [
            <<"USERS<br />&<br />RIGTHS">>
        ]},
        #panel{class = <<"group-users-ph">>, body = [
            #panel{class = <<"users-table-header-wrapper">>, body = [
                #table{class = <<"table table-striped users-table-header">>, header = #thead{body = [
                    #tr{cells =
                    lists:map(
                        fun({ColumnName, _PrivilegeName}) ->
                            #th{body = [ColumnName]}
                        end, [{<<"User">>, a}] ++ ?PRIVILEGES)
%%                     [
%%                         #th{body = [<<"User">>]},
%%                         #th{body = [<<"View Space">>]},
%%                         #th{body = [<<"Modify Space">>]},
%%                         #th{body = [<<"Remove Space">>]},
%%                         #th{body = [<<"Modify Space">>]},
%%                         #th{body = [<<"Remove Space">>]},
%%                         #th{body = [<<"Modify Space">>]},
%%                         #th{body = [<<"Remove Space">>]},
%%                         #th{body = [<<"Remove Provider">>]},
%%                         #th{body = [<<"Remove Space">>]},
%%                         #th{body = [<<"Remove Space">>]},
%%                         #th{body = [<<"Remove Space">>]}
%%                     ]
                    }
                ]}}
            ]},
            #panel{class = <<"users-table-wrapper">>, body = [
                #table{class = <<"table table-striped users-table">>, body = #tbody{body = [
                    #tr{cells = [
                        #td{body = [<<"Łukasz Opioła"/utf8>>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]}
                    ]},
                    #tr{cells = [
                        #td{body = [<<"Łukasz Opioła"/utf8>>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]}
                    ]},
                    #tr{cells = [
                        #td{body = [<<"Łukasz Opioła"/utf8>>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]}
                    ]},
                    #tr{cells = [
                        #td{body = [<<"Łukasz Opioła"/utf8>>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]}
                    ]},
                    #tr{cells = [
                        #td{body = [<<"Łukasz Opioła"/utf8>>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]}
                    ]},
                    #tr{cells = [
                        #td{body = [<<"Łukasz Opioła"/utf8>>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]}
                    ]},
                    #tr{cells = [
                        #td{body = [<<"Łukasz Opioładfsgdfsgdfsg"/utf8>>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"tak">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]},
                        #td{body = [<<"nie">>]}
                    ]}
                ]}}
            ]}
        ]},
        #panel{class = <<"group-users-right-ph">>, body = [
            <<"USERS<br />&<br />RIGTHS">>
        ]}
    ].


%% event/1
%% ====================================================================
%% @doc Handles page events.
-spec event(Event :: term()) -> no_return().
%% ====================================================================
event(init) ->
    ok;

event(expand) ->
    ?dump(expand);

event(terminate) ->
    ok.


%% api_event/3
%% ====================================================================
%% @doc Handles page events.
-spec api_event(Name :: string(), Args :: string(), Req :: string()) -> no_return().
%% ====================================================================
api_event(_Function, _Args, _) ->
    ok.