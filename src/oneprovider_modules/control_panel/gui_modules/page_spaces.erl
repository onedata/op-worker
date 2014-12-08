%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page allows user to manage his Spaces.
%% @end
%% ===================================================================

-module(page_spaces).
-include("oneprovider_modules/control_panel/common.hrl").
-include_lib("ctool/include/global_registry/gr_groups.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").
-include_lib("ctool/include/logging.hrl").

% n2o API and comet
-export([main/0, event/1, api_event/3, comet_loop/1]).


%% Columns names and associated privileges names
-define(COLUMNS_NAMES, [<<"View Space">>, <<"Modify Space">>, <<"Remove Space">>, <<"Invite user">>, <<"Remove user">>,
    <<"Invite group">>, <<"Remove group">>, <<"Invite provider">>, <<"Remove provider">>, <<"Set privileges">>]).
-define(PRIVILEGES_NAMES, [<<"space_view_data">>, <<"space_change_data">>, <<"space_remove">>, <<"space_invite_user">>,
    <<"space_remove_user">>, <<"space_invite_group">>, <<"space_remove_group">>, <<"space_add_provider">>,
    <<"space_remove_provider">>, <<"space_set_privileges">>]).

% User privileges
-define(PRVLG_VIEW, <<"space_view_data">>).
-define(PRVLG_CHANGE, <<"space_change_data">>).
-define(PRVLG_REMOVE, <<"space_remove">>).
-define(PRVLG_INVITE_USER, <<"space_invite_user">>).
-define(PRVLG_REMOVE_USER, <<"space_remove_user">>).
-define(PRVLG_INVITE_GROUP, <<"space_invite_group">>).
-define(PRVLG_REMOVE_GROUP, <<"space_remove_group">>).
-define(PRVLG_ADD_PROVIDER, <<"space_add_provider">>).
-define(PRVLG_REMOVE_PROVIDER, <<"space_remove_provider">>).
-define(PRVLG_SET_PRIVILEGES, <<"space_set_privileges">>).

% User privileges - tuples {PrivilegeID, PrivilegeName, ColumnName}
-define(PRIVILEGES, [
    {?PRVLG_VIEW, <<"View Space">>, <<"View<br />Space">>},
    {?PRVLG_CHANGE, <<"Modify Space">>, <<"Modify<br />Space">>},
    {?PRVLG_REMOVE, <<"Remove Space">>, <<"Remove<br />Space">>},
    {?PRVLG_INVITE_USER, <<"Invite user">>, <<"Invite<br />user">>},
    {?PRVLG_REMOVE_USER, <<"Remove user">>, <<"Remove<br />user">>},
    {?PRVLG_INVITE_GROUP, <<"Invite group">>, <<"Invite<br />group">>},
    {?PRVLG_REMOVE_GROUP, <<"Remove group">>, <<"Remove<br />group">>},
    {?PRVLG_ADD_PROVIDER, <<"Invite provider">>, <<"Invite<br />provider">>},
    {?PRVLG_REMOVE_PROVIDER, <<"Remove provider">>, <<"Remove<br />provider">>},
    {?PRVLG_SET_PRIVILEGES, <<"Set privileges">>, <<"Set<br />privileges">>}
]).

% Actions that can be performed by user concerning specific groups and posibly requiring privileges
% Theyare represented by tuples {space_action, ActionName, GroupID, Args}
-define(SPACE_ACTION_TOGGLE, toggle_space).

-define(SPACE_ACTION_SHOW_REMOVE_POPUP, show_remove_space_popup).
-define(SPACE_ACTION_REMOVE, remove_space).

-define(SPACE_ACTION_SHOW_RENAME_POPUP, show_rename_space_popup).
-define(SPACE_ACTION_RENAME, rename_space).

-define(SPACE_ACTION_SET_USER_PRIVILEGE, change_user_privilege).
-define(SPACE_ACTION_SAVE_USER_PRIVILEGES, save_user_privileges).
-define(SPACE_ACTION_DISCARD_USER_PRIVILEGES, discard_user_privileges).

-define(SPACE_ACTION_SET_GROUP_PRIVILEGE, change_group_privilege).
-define(SPACE_ACTION_SAVE_GROUP_PRIVILEGES, save_group_privileges).
-define(SPACE_ACTION_DISCARD_GROUP_PRIVILEGES, discard_group_privileges).

-define(SPACE_ACTION_INVITE_USER, invite_user).
-define(SPACE_ACTION_SHOW_REMOVE_USER_POPUP, show_remove_user_popup).
-define(SPACE_ACTION_REMOVE_USER, remove_user).

-define(SPACE_ACTION_INVITE_GROUP, invite_group).
-define(SPACE_ACTION_SHOW_REMOVE_GROUP_POPUP, show_remove_group_popup).
-define(SPACE_ACTION_REMOVE_GROUP, remove_group).

-define(SPACE_ACTION_REQUEST_SUPPORT, request_support).
-define(SPACE_ACTION_SHOW_REMOVE_PROVIDER_POPUP, show_remove_provider_popup).
-define(SPACE_ACTION_REMOVE_PROVIDER, remove_provider).

% What privilege is required for what action
-define(PRIVILEGES_FOR_ACTIONS, [
    {?SPACE_ACTION_TOGGLE, ?PRVLG_VIEW},

    {?SPACE_ACTION_SHOW_REMOVE_POPUP, ?PRVLG_REMOVE},
    {?SPACE_ACTION_REMOVE, ?PRVLG_REMOVE},

    {?SPACE_ACTION_SHOW_RENAME_POPUP, ?PRVLG_CHANGE},
    {?SPACE_ACTION_RENAME, ?PRVLG_CHANGE},

    {?SPACE_ACTION_SET_USER_PRIVILEGE, ?PRVLG_SET_PRIVILEGES},
    {?SPACE_ACTION_SAVE_USER_PRIVILEGES, ?PRVLG_SET_PRIVILEGES},
    {?SPACE_ACTION_DISCARD_USER_PRIVILEGES, ?PRVLG_SET_PRIVILEGES},

    {?SPACE_ACTION_SET_GROUP_PRIVILEGE, ?PRVLG_SET_PRIVILEGES},
    {?SPACE_ACTION_SAVE_GROUP_PRIVILEGES, ?PRVLG_SET_PRIVILEGES},
    {?SPACE_ACTION_DISCARD_GROUP_PRIVILEGES, ?PRVLG_SET_PRIVILEGES},

    {?SPACE_ACTION_INVITE_USER, ?PRVLG_INVITE_USER},
    {?SPACE_ACTION_SHOW_REMOVE_USER_POPUP, ?PRVLG_REMOVE_USER},
    {?SPACE_ACTION_REMOVE_USER, ?PRVLG_REMOVE_USER},

    {?SPACE_ACTION_INVITE_GROUP, ?PRVLG_INVITE_GROUP},
    {?SPACE_ACTION_SHOW_REMOVE_GROUP_POPUP, ?PRVLG_REMOVE_GROUP},
    {?SPACE_ACTION_REMOVE_GROUP, ?PRVLG_REMOVE_GROUP},

    {?SPACE_ACTION_REQUEST_SUPPORT, ?PRVLG_ADD_PROVIDER},
    {?SPACE_ACTION_SHOW_REMOVE_PROVIDER_POPUP, ?PRVLG_REMOVE_PROVIDER},
    {?SPACE_ACTION_REMOVE_PROVIDER, ?PRVLG_REMOVE_PROVIDER}
]).

% Actions that can be performed by user, they do not require privileges.
% Theyare represented by tuples {action, ActionName, Args}
-define(ACTION_SHOW_CREATE_SPACE_POPUP, show_create_space_popup).
-define(ACTION_CREATE_SPACE, create_space).
-define(ACTION_SHOW_JOIN_SPACE_POPUP, show_join_space_popup).
-define(ACTION_JOIN_SPACE, join_space).
-define(ACTION_MOVE_SPACE, move_space).
-define(ACTION_SET_DEFAULT, set_default).
-define(ACTION_SHOW_LEAVE_SPACE_POPUP, show_leave_space_popup).
-define(ACTION_LEAVE_SPACE, leave_space).
-define(ACTION_HIDE_POPUP, hide_popup).

%% Comet process pid
-define(COMET_PID, comet_pid).

%% Macros used to create HTML element IDs
-define(SPACE_LIST_ELEMENT_ID(GroupID), <<"sp_li_", GroupID/binary>>).
-define(SPACE_HEADER_ID(GroupID), <<"sp_name_ph_", GroupID/binary>>).
-define(COLLAPSE_WRAPPER_ID(GroupID), <<"sp_collapse_wrapper_", GroupID/binary>>).
-define(USERS_SECTION_ID(GroupID), <<"sp_users_ph_", GroupID/binary>>).
-define(GROUPS_SECTION_ID(GroupID), <<"sp_groups_ph_", GroupID/binary>>).
-define(PROVIDERS_SECTION_ID(GroupID), <<"sp_providers_ph_", GroupID/binary>>).
-define(PRVLGS_USER_HEADER_PH_ID(GroupID), <<"pr_user_header_ph_", GroupID/binary>>).
-define(PRVLGS_USER_SAVE_PH_ID(GroupID), <<"pr_save_ph_", GroupID/binary>>).
-define(USER_CHECKBOX_ID(SpaceID, UserID, PrivilegeID), <<"us_pr_chckbx_", SpaceID/binary, "_", UserID/binary, "_", PrivilegeID/binary>>).
-define(PRVLGS_GROUP_HEADER_PH_ID(GroupID), <<"pr_user_header_ph_", GroupID/binary>>).
-define(PRVLGS_GROUP_SAVE_PH_ID(GroupID), <<"pr_save_ph_", GroupID/binary>>).
-define(GROUP_CHECKBOX_ID(SpaceID, GroupID, PrivilegeID), <<"gr_pr_chckbx_", SpaceID/binary, "_", GroupID/binary, "_", PrivilegeID/binary>>).

% Macro used to format names and IDs that appear in messages
-define(FORMAT_ID_AND_NAME(ID, Name), <<"<b>", (gui_str:html_encode(Name))/binary, "</b> (ID: <b>", ID/binary, "</b>)">>).

% Macro used to generate redirect URL to show a group
-define(REDIRECT_TO_GROUP_URL(GroupID), <<"/groups?show=", GroupID/binary>>).

%% Page state
%% Edited privileges is a proplist with SpaceID keys,
%% which values are proplists with UserID/GroupID keys, which values
%% are proplists with {PrivilegeID, Flag} tuples.
-record(page_state, {
    spaces = [],
    expanded_spaces = [],
    edited_group_privileges = [],
    edited_user_privileges = [],
    gruid,
    access_token}).

%% Records used to store current info about spaces
%% current_privileges is a list of privileges of current user in specific space
-record(space_state, {id = <<"">>, name = <<"">>, users = [], groups = [], providers = [], current_privileges = []}).
-record(user_state, {id = <<"">>, name = <<"">>, privileges = []}).
-record(group_state, {id = <<"">>, name = <<"">>, privileges = []}).
-record(provider_state, {id = <<"">>, name = <<"">>}).


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
title() -> <<"Spaces">>.


%% css/0
%% ====================================================================
%% @doc Page specific CSS import clause.
-spec css() -> binary().
%% ====================================================================
css() ->
    <<"<link rel=\"stylesheet\" href=\"/css/groups_spaces_common.css\" type=\"text/css\" media=\"screen\" charset=\"utf-8\" />",
    "    <link rel=\"stylesheet\" href=\"/css/spaces.css\" type=\"text/css\" media=\"screen\" charset=\"utf-8\" />">>.


%% body/0
%% ====================================================================
%% @doc This will be placed instead of {{body}} tag in template.
-spec body() -> [#panel{}].
%% ====================================================================
body() ->
    % On resize, adjust tables' headers width (scroll bar takes some space out of table and header would be too wide)
    gui_jq:wire(<<"window.onresize = function(e) { ",
    "var pad = $($('.gen-table-wrapper')[0]).width() - $($('.gen-table')[0]).width();",
    "$('.gen-table-header-wrapper').css('padding-right', pad + 'px'); };">>),

    #panel{class = <<"page-container">>, body = [
        #panel{id = <<"spinner">>, body = #image{image = <<"/images/spinner.gif">>}},
        opn_gui_utils:top_menu(spaces_tab),
        #panel{class = <<"page-content">>, body = [
            #panel{id = <<"message">>, class = <<"dialog">>},
            #h6{class = <<"page-header">>, body = <<"Manage spaces">>},
            #panel{class = <<"top-buttons-panel">>, body = [
                #button{id = <<"create_space_button">>, postback = {action, ?ACTION_SHOW_CREATE_SPACE_POPUP},
                    class = <<"btn btn-inverse btn-small top-button">>, body = <<"Create new space">>},
                #button{id = <<"join_space_button">>, postback = {action, ?ACTION_SHOW_JOIN_SPACE_POPUP},
                    class = <<"btn btn-inverse btn-small top-button">>, body = <<"Join existing space">>}
            ]},
            #panel{class = <<"gen-list-panel">>, body = [
                #list{class = <<"gen-list">>, id = <<"space_list">>, body = []}
            ]}
        ]},
        footer_popup()
    ]}.


% Footer popup to display prompts and forms.
footer_popup() ->
    #panel{id = <<"footer_popup">>, class = <<"dialog success-dialog footer-popup">>}.


%% space_list_element/3
%% ====================================================================
%% @doc Renders HTML responsible for space list element.
-spec space_list_element(SpaceState :: #space_state{}, Expanded :: boolean, IsDefault :: boolean()) -> term().
%% ====================================================================
space_list_element(#space_state{id = SpaceID, name = SpaceNameOrUndef, users = UserStates,
    groups = GroupStates, providers = ProviderStates, current_privileges = UserPrivileges}, Expanded, IsDefault) ->
    {CanViewSpace, SpaceName, SpaceHeaderClass} =
        case SpaceNameOrUndef of
            undefined -> {
                false,
                <<"<i>You do not have privileges to view this space</i>">>,
                <<"gen-name-wrapper">>
            };
            _ -> {
                true,
                SpaceNameOrUndef,
                <<"gen-name-wrapper cursor-pointer">>
            }
        end,
    SpaceHeaderID = ?SPACE_HEADER_ID(SpaceID),
    {WrapperClass, UsersBody, GroupsBody, ProvidersBody} =
        case Expanded and (SpaceNameOrUndef /= undefined) of
            false ->
                {<<"collapse-wrapper collapsed">>, [], [], []};
            true ->
                {<<"collapse-wrapper">>, user_table_body(SpaceID, UserStates, UserPrivileges),
                    group_table_body(SpaceID, GroupStates, UserPrivileges), provider_table_body(SpaceID, ProviderStates)}
        end,
    ListElement = #li{id = ?SPACE_LIST_ELEMENT_ID(SpaceID), body = [
        #panel{class = <<"gen-container">>, body = [
            #panel{class = <<"gen-header">>, body = [
                #panel{class = <<"gen-header-icon-wrapper">>, body = [
                    #span{class = <<"icomoon-cloud">>}
                ]},
                #panel{id = SpaceHeaderID, class = SpaceHeaderClass, body = [
                    #p{class = <<"gen-name">>, body = [SpaceName]},
                    #p{class = <<"gen-id">>, body = [<<"ID: ", SpaceID/binary>>]}
                ]},
                case CanViewSpace of
                    false ->
                        #panel{class = <<"gen-actions-wrapper">>, body = [
                            #button{title = <<"Leave this space">>, class = <<"btn btn-small btn-info gen-leave-button">>,
                                postback = {action, ?ACTION_SHOW_LEAVE_SPACE_POPUP, [SpaceID, undefined]}, body = [
                                    <<"<i class=\"icomoon-exit action-button-icon\"></i>">>, <<"Leave space">>
                                ]}
                        ]};
                    true ->
                        #panel{class = <<"gen-actions-wrapper">>, body = [
                            #panel{class = <<"btn-group gen-actions-dropdown">>, body = [
                                <<"<i class=\"dropdown-arrow\"></i>">>,
                                #button{title = <<"Actions">>, class = <<"btn btn-small btn-info">>,
                                    data_fields = [{<<"data-toggle">>, <<"dropdown">>}], body = [
                                        <<"<i class=\"icomoon-cog action-button-icon\"></i>">>, <<"Actions">>
                                    ]},
                                #button{title = <<"Actions">>, class = <<"btn btn-small btn-info dropdown-toggle">>,
                                    data_fields = [{<<"data-toggle">>, <<"dropdown">>}], body = #span{class = <<"caret">>}},
                                #list{class = <<"dropdown-menu">>,
                                    body = [
                                        #li{body = [
                                            #link{title = <<"Move this space to the top">>,
                                                postback = {action, ?ACTION_MOVE_SPACE, [SpaceID, true]},
                                                body = [<<"<i class=\"icomoon-arrow-up2\"></i>">>, <<"Move up">>]}
                                        ]},
                                        #li{body = [
                                            #link{title = <<"Move this space to the bottom">>,
                                                postback = {action, ?ACTION_MOVE_SPACE, [SpaceID, false]},
                                                body = [<<"<i class=\"icomoon-arrow-down2\"></i>">>, <<"Move down">>]}
                                        ]},
                                        case IsDefault of
                                            true ->
                                                [];
                                            false ->
                                                #li{body = [
                                                    #link{title = <<"Set this space as default space">>,
                                                        postback = {action, ?ACTION_SET_DEFAULT, [SpaceID]},
                                                        body = [<<"<i class=\"icomoon-arrow-down2\"></i>">>, <<"Set default">>]}
                                                ]}
                                        end,
                                        #li{body = [
                                            #link{title = <<"Leave this space">>,
                                                postback = {action, ?ACTION_SHOW_LEAVE_SPACE_POPUP, [SpaceID, SpaceName]},
                                                body = [<<"<i class=\"icomoon-exit\"></i>">>, <<"Leave space">>]}
                                        ]},
                                        #li{body = [
                                            #panel{class = <<"divider">>},
                                            #link{title = <<"Rename this space">>,
                                                postback = {space_action, ?SPACE_ACTION_SHOW_RENAME_POPUP, SpaceID},
                                                body = [<<"<i class=\"icomoon-pencil2\"></i>">>, <<"Rename">>]}
                                        ]},
                                        #li{body = [
                                            #link{title = <<"Remove this space">>,
                                                postback = {space_action, ?SPACE_ACTION_SHOW_REMOVE_POPUP, SpaceID},
                                                body = [<<"<i class=\"icomoon-remove\"></i>">>, <<"Remove">>]}
                                        ]},
                                        #li{body = [
                                            #panel{class = <<"divider">>},
                                            #link{title = <<"Obtain a token to invite a user to this space">>,
                                                postback = {space_action, ?SPACE_ACTION_INVITE_USER, SpaceID},
                                                body = [<<"<i class=\"icomoon-user\"></i>">>, <<"Invite user">>]}
                                        ]},
                                        #li{body = [
                                            #panel{class = <<"divider">>},
                                            #link{title = <<"Obtain a token to invite a group to this space">>,
                                                postback = {space_action, ?SPACE_ACTION_INVITE_GROUP, SpaceID},
                                                body = [<<"<i class=\"icomoon-users\"></i>">>, <<"Invite group">>]}
                                        ]},
                                        #li{body = [
                                            #panel{class = <<"divider">>},
                                            #link{title = <<"Obtain a token to request a provider to support this space">>,
                                                postback = {space_action, ?SPACE_ACTION_REQUEST_SUPPORT, SpaceID},
                                                body = [<<"<i class=\"icomoon-question\"></i>">>, <<"Request support">>]}
                                        ]}
                                    ]}
                            ]}
                        ]}
                end
            ]},
            #panel{id = ?COLLAPSE_WRAPPER_ID(SpaceID), class = WrapperClass, body = [
                #panel{id = ?USERS_SECTION_ID(SpaceID), class = <<"users-section">>, body = UsersBody},
                #panel{id = ?GROUPS_SECTION_ID(SpaceID), class = <<"users-section">>, body = GroupsBody},
                #panel{id = ?GROUPS_SECTION_ID(SpaceID), class = <<"groups-section">>, body = ProvidersBody}
            ]}
        ]}
    ]},

    case CanViewSpace of
        true ->
            gui_jq:wire(gui_jq:postback_action(SpaceHeaderID, {space_action, ?SPACE_ACTION_TOGGLE, SpaceID}));
        false ->
            ok
    end,
    ListElement.


user_table_body(SpaceID, UserStates, CurrentUserPrivileges) ->
    CanSetPrivileges = lists:member(?PRVLG_SET_PRIVILEGES, CurrentUserPrivileges),
    JS = <<"function(e) {var box = bootbox.dialog({ title: 'Not authorized',",
    "message: 'To perform this operation, you need the <b>Set privileges</b> privileges.',",
    "buttons: {'OK': {className: 'btn-primary confirm', callback: function() {} } } }); }">>,
    gui_jq:wire(<<"$('.disabled-checkbox-wrapper').unbind('click.cantsetprivs').bind('click.cantsetprivs', ", JS/binary, ");">>),

    [
        #panel{class = <<"gen-left-wrapper">>, body = [
            <<"USERS<br />&<br />RIGHTS">>,
            #link{title = <<"Help">>, class = <<"glyph-link">>, postback = show_users_info,
                body = #span{class = <<"icomoon-question">>}}
        ]},
        #panel{class = <<"gen-middle-wrapper">>, body = [
            #panel{class = <<"gen-table-header-wrapper">>, body = [
                #table{class = <<"table table-striped gen-table-header users-table-header">>, header = #thead{body = [
                    #tr{cells = [
                        #th{body = [
                            #panel{id = ?PRVLGS_USER_HEADER_PH_ID(SpaceID), body = [<<"User">>]},
                            #panel{id = ?PRVLGS_USER_SAVE_PH_ID(SpaceID), class = <<"privileges-save-wrapper">>, body = [
                                #button{class = <<"btn btn-small btn-success privileges-save-button">>,
                                    postback = {space_action, ?SPACE_ACTION_SAVE_USER_PRIVILEGES, SpaceID},
                                    body = <<"Save">>},
                                #button{class = <<"btn btn-small btn-danger privileges-save-button">>,
                                    postback = {space_action, ?SPACE_ACTION_DISCARD_USER_PRIVILEGES, SpaceID},
                                    body = <<"Discard">>}
                            ]}
                        ]},
                        lists:map(
                            fun({_PrivilegeID, _PrivilegeName, ColumnName}) ->
                                #th{body = [ColumnName]}
                            end, ?PRIVILEGES)
                    ]}
                ]}}
            ]},
            #panel{class = <<"gen-table-wrapper">>, body = [
                #table{class = <<"table table-striped gen-table users-table">>, body = #tbody{body =
                lists:map(
                    fun(#user_state{id = UserID, name = UserName, privileges = UserPrivileges}) ->
                        #tr{cells = [
                            #td{body = [
                                #panel{class = <<"name-wrapper">>, body = [
                                    #link{title = <<"ID: ", UserID/binary>>, class = <<"glyph-link">>,
                                        body = #span{class = <<"icomoon-user action-button-icon top-1">>}},
                                    UserName
                                ]},
                                #panel{class = <<"remove-wrapper">>, body = [
                                    #link{title = <<"Remove this user from space">>, class = <<"glyph-link">>,
                                        postback = {space_action, ?SPACE_ACTION_SHOW_REMOVE_USER_POPUP, SpaceID, [UserID, UserName]},
                                        body = #span{class = <<"icomoon-remove">>}}
                                ]}
                            ]},
                            lists:map(
                                fun({PrivilegeID, _PrivilegeName, _ColumnName}) ->
                                    CheckboxID = ?USER_CHECKBOX_ID(SpaceID, UserID, PrivilegeID),
                                    flatui_checkbox:init_checkbox(CheckboxID),
                                    {TDClass, LabelClass} =
                                        case CanSetPrivileges of
                                            true ->
                                                {<<"">>, <<"privilege-checkbox checkbox no-label">>};
                                            false ->
                                                {<<"disabled-checkbox-wrapper">>, <<"privilege-checkbox checkbox primary no-label">>}
                                        end,
                                    #td{class = TDClass, body = [
                                        #flatui_checkbox{label_class = LabelClass, id = CheckboxID, delegate = ?MODULE,
                                            checked = lists:member(PrivilegeID, UserPrivileges),
                                            postback = {space_action, ?SPACE_ACTION_SET_USER_PRIVILEGE, SpaceID, [UserID, PrivilegeID, {query_value, CheckboxID}]},
                                            source = [gui_str:to_list(CheckboxID)],
                                            disabled = not CanSetPrivileges}
                                    ]}
                                end, ?PRIVILEGES)
                        ]}
                    end, UserStates)
                }}
            ]}
        ]}
    ].


group_table_body(SpaceID, GroupStates, CurrentUserPrivileges) ->
    CanSetPrivileges = lists:member(?PRVLG_SET_PRIVILEGES, CurrentUserPrivileges),
    JS = <<"function(e) {var box = bootbox.dialog({ title: 'Not authorized',",
    "message: 'To perform this operation, you need the <b>Set privileges</b> privileges.',",
    "buttons: {'OK': {className: 'btn-primary confirm', callback: function() {} } } }); }">>,
    gui_jq:wire(<<"$('.disabled-checkbox-wrapper').unbind('click.cantsetprivs').bind('click.cantsetprivs', ", JS/binary, ");">>),

    [
        #panel{class = <<"gen-left-wrapper">>, body = [
            <<"GROUPS<br />&<br />RIGHTS">>,
            #link{title = <<"Help">>, class = <<"glyph-link">>, postback = show_groups_info,
                body = #span{class = <<"icomoon-question">>}}
        ]},
        #panel{class = <<"gen-middle-wrapper">>, body = [
            #panel{class = <<"gen-table-header-wrapper">>, body = [
                #table{class = <<"table table-striped gen-table-header users-table-header">>, header = #thead{body = [
                    #tr{cells = [
                        #th{body = [
                            #panel{id = ?PRVLGS_GROUP_HEADER_PH_ID(SpaceID), body = [<<"Group">>]},
                            #panel{id = ?PRVLGS_GROUP_SAVE_PH_ID(SpaceID), class = <<"privileges-save-wrapper">>, body = [
                                #button{class = <<"btn btn-small btn-success privileges-save-button">>,
                                    postback = {space_action, ?SPACE_ACTION_SAVE_GROUP_PRIVILEGES, SpaceID},
                                    body = <<"Save">>},
                                #button{class = <<"btn btn-small btn-danger privileges-save-button">>,
                                    postback = {space_action, ?SPACE_ACTION_DISCARD_GROUP_PRIVILEGES, SpaceID},
                                    body = <<"Discard">>}
                            ]}
                        ]},
                        lists:map(
                            fun({_PrivilegeID, _PrivilegeName, ColumnName}) ->
                                #th{body = [ColumnName]}
                            end, ?PRIVILEGES)
                    ]}
                ]}}
            ]},
            #panel{class = <<"gen-table-wrapper">>, body = [
                #table{class = <<"table table-striped gen-table users-table">>, body = #tbody{body =
                lists:map(
                    fun(#group_state{id = GroupID, name = GroupName, privileges = UserPrivileges}) ->
                        #tr{cells = [
                            #td{body = [
                                #panel{class = <<"name-wrapper">>, body = [
                                    #link{title = <<"View this group">>, class = <<"glyph-link">>,
                                        url = ?REDIRECT_TO_GROUP_URL(GroupID), body = [
                                            #span{class = <<"icomoon-users action-button-icon top-1">>},
                                            GroupName
                                        ]}
                                ]},
                                #panel{class = <<"remove-wrapper">>, body = [
                                    #link{title = <<"Remove this group from space">>, class = <<"glyph-link">>,
                                        postback = {space_action, ?SPACE_ACTION_SHOW_REMOVE_GROUP_POPUP, SpaceID, [GroupID, GroupName]},
                                        body = #span{class = <<"icomoon-remove">>}}
                                ]}
                            ]},
                            lists:map(
                                fun({PrivilegeID, _PrivilegeName, _ColumnName}) ->
                                    CheckboxID = ?GROUP_CHECKBOX_ID(SpaceID, GroupID, PrivilegeID),
                                    flatui_checkbox:init_checkbox(CheckboxID),
                                    {TDClass, LabelClass} =
                                        case CanSetPrivileges of
                                            true ->
                                                {<<"">>, <<"privilege-checkbox checkbox no-label">>};
                                            false ->
                                                {<<"disabled-checkbox-wrapper">>, <<"privilege-checkbox checkbox primary no-label">>}
                                        end,
                                    #td{class = TDClass, body = [
                                        #flatui_checkbox{label_class = LabelClass, id = CheckboxID, delegate = ?MODULE,
                                            checked = lists:member(PrivilegeID, UserPrivileges),
                                            postback = {space_action, ?SPACE_ACTION_SET_GROUP_PRIVILEGE, SpaceID, [GroupID, PrivilegeID, {query_value, CheckboxID}]},
                                            source = [gui_str:to_list(CheckboxID)],
                                            disabled = not CanSetPrivileges}
                                    ]}
                                end, ?PRIVILEGES)
                        ]}
                    end, GroupStates)
                }}
            ]}
        ]}
    ].


provider_table_body(SpaceID, ProviderStates) ->
    [
        #panel{class = <<"gen-left-wrapper">>, body = [
            <<"PROVI-<br />DERS">>,
            #link{title = <<"Help">>, class = <<"glyph-link">>, postback = show_providers_info,
                body = #span{class = <<"icomoon-question">>}}
        ]},
        #panel{class = <<"gen-middle-wrapper">>, body = [
            #panel{class = <<"gen-table-header-wrapper">>, body = [
                #table{class = <<"table table-striped gen-table-header spaces-table-header">>, header = #thead{body = [
                    #tr{cells = [
                        #th{body = [<<"Provider name">>]},
                        #th{body = [<<"Provider ID">>]}
                    ]}
                ]}}
            ]},
            #panel{class = <<"gen-table-wrapper">>, body = [
                #table{class = <<"table table-striped gen-table spaces-table">>, body = #tbody{body =
                case ProviderStates of
                    [] ->
                        #tr{cells = [
                            #td{class = <<"empty-table-info">>, body = [
                                <<"No providers">>
                            ]}
                        ]};
                    _ ->
                        lists:map(
                            fun(#provider_state{id = ProviderID, name = ProviderName}) ->
                                #tr{cells = [
                                    #td{body = [
                                        #panel{class = <<"name-wrapper">>, body = [
                                            #span{class = <<"icomoon-home2 action-button-icon">>},
                                            ProviderName
                                        ]},
                                        #panel{class = <<"remove-wrapper">>, body = [
                                            #link{title = <<"Remove this provider">>, class = <<"glyph-link">>,
                                                postback = {space_action, ?SPACE_ACTION_SHOW_REMOVE_PROVIDER_POPUP, SpaceID, [ProviderID, ProviderName]},
                                                body = #span{class = <<"icomoon-remove">>}}
                                        ]}
                                    ]},
                                    #td{body = [
                                        ProviderID
                                    ]}
                                ]}
                            end, ProviderStates)
                end
                }}
            ]}
        ]}
    ].


synchronize_spaces_and_users(GRUID, AccessToken, ExpandedSpaces) ->
    {ok, #user_spaces{ids = SpaceIDsFromGR, default = DefaultSpace}} =
        gr_users:get_spaces({user, AccessToken}),
    % Move default space to the head of the list, if such space exists
    SpaceIDs = case DefaultSpace of
                   undefined -> SpaceIDsFromGR;
                   _ -> [DefaultSpace | lists:delete(DefaultSpace, SpaceIDsFromGR)]
               end,
    % Synchronize spaces data
    SpaceStates = lists:map(
        fun(SpaceID) ->
            case gr_spaces:get_details({user, AccessToken}, SpaceID) of
                {ok, #space_details{name = SpaceName}} ->
                    % Synchronize users data (belonging to certain space)
                    {ok, UsersIDs} = gr_spaces:get_users({user, AccessToken}, SpaceID),
                    UserStates = lists:map(
                        fun(UserID) ->
                            {ok, #user_details{name = UserName}} = gr_spaces:get_user_details({user, AccessToken}, SpaceID, UserID),
                            {ok, Privileges} = gr_spaces:get_user_privileges({user, AccessToken}, SpaceID, UserID),
                            #user_state{id = UserID, name = UserName, privileges = Privileges}
                        end, UsersIDs),
                    #user_state{id = CurrentUserID} = CurrentUser = lists:keyfind(GRUID, 2, UserStates),
                    % Get effective privileges of current user
                    {ok, CurrentPrivileges} = gr_spaces:get_effective_user_privileges({user, AccessToken}, SpaceID, CurrentUserID),
                    UserStatesWithoutCurrent = lists:keydelete(GRUID, 2, UserStates),
                    UserStatesSorted = [CurrentUser | sort_states(UserStatesWithoutCurrent)],

                    % Synchronize groups data (belonging to certain space)
                    {ok, GroupIDs} = gr_spaces:get_groups({user, AccessToken}, SpaceID),
                    GroupStates = lists:map(
                        fun(GroupID) ->
                            {ok, #group_details{name = GroupName}} = gr_spaces:get_group_details({user, AccessToken}, SpaceID, GroupID),
                            {ok, Privileges} = gr_spaces:get_group_privileges({user, AccessToken}, SpaceID, GroupID),
                            #group_state{id = GroupID, name = GroupName, privileges = Privileges}
                        end, GroupIDs),
                    GroupStatesSorted = sort_states(GroupStates),

                    % Synchronize providers data (supporting to certain space)
                    {ok, ProviderIDs} = gr_spaces:get_providers({user, AccessToken}, SpaceID),
                    ProviderStates = lists:map(
                        fun(ProviderID) ->
                            {ok, #provider_details{name = ProviderName}} = gr_spaces:get_provider_details({user, AccessToken}, SpaceID, ProviderID),
                            #provider_state{id = ProviderID, name = ProviderName}
                        end, ProviderIDs),
                    ProviderStatesSorted = sort_states(ProviderStates),
                    #space_state{id = SpaceID, name = SpaceName, users = UserStatesSorted, groups = GroupStatesSorted,
                        providers = ProviderStatesSorted, current_privileges = CurrentPrivileges};
                _ ->
                    % User does not have rights to view this group
                    #space_state{id = SpaceID, name = undefined, users = [], groups = [], current_privileges = []}
            end
        end, SpaceIDs),
    {CanView, CannotView} = lists:partition(
        fun(#space_state{name = Name}) ->
            Name /= undefined
        end, SpaceStates),
    SortedSpaceStates = CanView ++ CannotView,
    #page_state{spaces = SortedSpaceStates, gruid = GRUID, access_token = AccessToken, expanded_spaces = ExpandedSpaces}.


sort_states(TupleList) ->
    ListToSort = lists:map(
        fun(T) ->
            {string:to_lower(gui_str:binary_to_unicode_list(element(3, T))), element(2, T)}
        end, TupleList),
    lists:map(
        fun({_, StateID}) ->
            lists:keyfind(StateID, 2, TupleList)
        end, lists:sort(ListToSort)).


comet_loop_init(GRUID, AccessToken, ExpandedSpaces, ScrollToSpaceID) ->
    PageState = synchronize_spaces_and_users(GRUID, AccessToken, ExpandedSpaces),
    refresh_space_list(PageState),
    case ScrollToSpaceID of
        undefined -> ok;
        _ -> scroll_to_space(ScrollToSpaceID)
    end,
    gui_jq:hide(<<"spinner">>),
    gui_comet:flush(),
    comet_loop(PageState).


comet_loop(State) ->
    NewState =
        try
            receive
                {action, Action, Args} ->
                    % Actions that do not concern a specific space
                    _State = comet_handle_action(State, Action, Args);

                {space_action, Action, SpaceID, Args} ->
                    % Actions that concern a specific space
                    _State = comet_handle_space_action(State, Action, SpaceID, Args)
            end % receive
        catch _Type:_Message ->
            ?error_stacktrace("Error in page_spaces comet_loop - ~p:~p", [_Type, _Message]),
            gui_jq:hide(<<"spinner">>),
            opn_gui_utils:message(error, <<"Server encountered an unexpected error. Please refresh the page.">>),
            gui_comet:flush(),
            error
        end,
    case NewState of
        error ->
            timer:sleep(1000), % TODO comet process dies to fast and redirection does not work, needs to be fixed
            ok; % Comet process will terminate
        _ ->
            gui_jq:hide(<<"spinner">>),
            gui_comet:flush(),
            ?MODULE:comet_loop(NewState)
    end.


comet_handle_action(_State, Action, Args) ->
    ?dump({comet_handle_action, Action, Args}).

comet_handle_space_action(_State, Action, SpaceID, Args) ->
    ?dump({comet_handle_space_action, Action, SpaceID, Args}).


refresh_space_list(#page_state{spaces = Spaces, expanded_spaces = ExpandedSpaces}) ->
    % Default space is the first on the list
    [#space_state{id = DefaultSpace}] = Spaces,
    Body = case Spaces of
               [] ->
                   #li{class = <<"empty-list-info">>, body = [
                       #p{body = [<<"You don't belong to any spaces.">>]}
                   ]};
               _ ->
                   lists:map(
                       fun(#space_state{id = ID} = SpaceState) ->
                           space_list_element(SpaceState, lists:member(ID, ExpandedSpaces), ID =:= DefaultSpace)
                       end, Spaces)
           end,
    gui_jq:update(<<"space_list">>, Body),
    gui_jq:wire(<<"$(window).resize();">>).


scroll_to_space(SpaceID) ->
    gui_jq:wire(<<"var el = $('#", (?SPACE_LIST_ELEMENT_ID(SpaceID))/binary, "'); if ($(el).length > 0) {",
    "$('html, body').animate({scrollTop: parseInt($(el).offset().top - 150)}, 200); }">>).


%% event/1
%% ====================================================================
%% @doc Handles page events.
-spec event(Event :: term()) -> no_return().
%% ====================================================================
event(init) ->
    try
        GRUID = utils:ensure_binary(opn_gui_utils:get_global_user_id()),
        AccessToken = opn_gui_utils:get_access_token(),
        {ExpandedSpaces, ScrollToSpaceID} =
            case gui_ctx:url_param(<<"show">>) of
                undefined -> {[], undefined};
                Bin -> {[Bin], Bin}
            end,

        gui_jq:bind_key_to_click_on_class(<<"13">>, <<"confirm">>),
        gui_jq:register_escape_event("escape_pressed_event"),

        {ok, Pid} = gui_comet:spawn(fun() -> comet_loop_init(GRUID, AccessToken, ExpandedSpaces, ScrollToSpaceID) end),
        put(?COMET_PID, Pid)
    catch
        _:Reason ->
            ?error_stacktrace("Cannot initialize page ~p: ~p", [?MODULE, Reason]),
            gui_jq:hide(<<"spinner">>),
            opn_gui_utils:message(error, <<"Cannot fetch spaces.<br />Please try again later.">>)
    end;

event({action, Action}) ->
    event({action, Action, []});

event({action, Action, Args}) ->
    ProcessedArgs = lists:map(
        fun(Arg) ->
            case Arg of
                {query_value, FieldName} ->
                    % This tuple means that element with id=FieldName has to be queried
                    % and the result be put in args
                    gui_ctx:postback_param(FieldName);
                Other ->
                    Other
            end
        end, Args),
    gui_jq:show(<<"spinner">>),
    get(?COMET_PID) ! {action, Action, ProcessedArgs};

event({space_action, Action, GroupID}) ->
    event({space_action, Action, GroupID, []});

event({space_action, Action, GroupID, Args}) ->
    ProcessedArgs = lists:map(
        fun(Arg) ->
            case Arg of
                {query_value, FieldName} ->
                    % This tuple means that element with id=FieldName has to be queried
                    % and the result be put in args
                    gui_ctx:postback_param(FieldName);
                Other ->
                    Other
            end
        end, Args),
    gui_jq:show(<<"spinner">>),
    get(?COMET_PID) ! {space_action, Action, GroupID, ProcessedArgs};

event({close_message, MessageId}) ->
    gui_jq:hide(MessageId);

event({redirect_to_group, SpaceID}) ->
    gui_jq:redirect(<<"/groups?show=", SpaceID/binary>>);

event(show_users_info) ->
    gui_jq:info_popup(<<"Users section">>,
        <<"This table shows all users that belong to the space and their privileges.<br /><br />",
        "- to modify privileges, set corresponding checkboxes and click the \"Save\" button<br />",
        "- to remove a user, point at the user and use the trash button<br />",
        "- to display user ID, point at the user icon<br />",
        "- to invite a user to the space, select the action from \"Actions\" menu.<br />">>, <<"">>);

event(show_groups_info) ->
    gui_jq:info_popup(<<"Groups section">>,
        <<"This table shows all groups that belong to the space and their privileges.<br /><br />",
        "- to modify privileges, set corresponding checkboxes and click the \"Save\" button<br />",
        "- to remove a group, point at the group and use the trash button<br />",
        "- to see more details about a group, click on its name or icon<br />",
        "- to invite a group to the space, select the action from \"Actions\" menu.<br />">>, <<"">>);

event(show_providers_info) ->
    gui_jq:info_popup(<<"Providers section">>,
        <<"This table shows all providers that support this space.<br /><br />",
        "- to remove a provider, point at the provider and use the trash button<br />",
        "- to request support from a provider, select the action from \"Actions\" menu.<br />">>, <<"">>);

event(terminate) ->
    ok.


%% api_event/3
%% ====================================================================
%% @doc Handles page events.
-spec api_event(Name :: string(), Args :: string(), Req :: string()) -> no_return().
%% ====================================================================
%% Handling events
api_event("escape_pressed_event", _, _) ->
    event({action, ?ACTION_HIDE_POPUP}).

