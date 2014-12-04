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
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/logging.hrl").

% n2o API and comet
-export([main/0, event/1, api_event/3, comet_loop/1]).

% User privileges
-define(PRVLG_VIEW, <<"group_view_data">>).
-define(PRVLG_CHANGE, <<"group_change_data">>).
-define(PRVLG_REMOVE, <<"group_remove">>).
-define(PRVLG_INVITE_USER, <<"group_invite_user">>).
-define(PRVLG_REMOVE_USER, <<"group_remove_user">>).
-define(PRVLG_CREATE_SPACE, <<"group_create_space">>).
-define(PRVLG_JOIN_SPACE, <<"group_join_space">>).
-define(PRVLG_LEAVE_SPACE, <<"group_leave_space">>).
-define(PRVLG_REQUEST_SUPPORT, <<"group_create_space_token">>).
-define(PRVLG_SET_PRIVILEGES, <<"group_set_privileges">>).

% User privileges - tuples {PrivilegeID, PrivilegeName, ColumnName}
-define(PRIVILEGES, [
    {?PRVLG_VIEW, <<"View group">>, <<"View<br />group">>},
    {?PRVLG_CHANGE, <<"Modify group">>, <<"Modify<br />group">>},
    {?PRVLG_REMOVE, <<"Remove group">>, <<"Remove<br />group">>},
    {?PRVLG_INVITE_USER, <<"Invite user">>, <<"Invite<br />user">>},
    {?PRVLG_REMOVE_USER, <<"Remove user">>, <<"Remove<br />user">>},
    {?PRVLG_CREATE_SPACE, <<"Create Space">>, <<"Create<br />Space">>},
    {?PRVLG_JOIN_SPACE, <<"Join Space">>, <<"Join<br />Space">>},
    {?PRVLG_LEAVE_SPACE, <<"Leave Space">>, <<"Leave<br />Space">>},
    {?PRVLG_REQUEST_SUPPORT, <<"Invite provider">>, <<"Invite<br />provider">>},
    {?PRVLG_SET_PRIVILEGES, <<"Set privileges">>, <<"Set<br />privileges">>}
]).

% Actions that can be performed by user
% Theyare represented by tuples {action, ActionName, Args}
-define(ACTION_SHOW_CREATE_GROUP_POPUP, show_create_group_popup).
-define(ACTION_CREATE_GROUP, create_group).
-define(ACTION_SHOW_JOIN_GROUP_POPUP, show_join_group_popup).
-define(ACTION_JOIN_GROUP, join_group).
-define(ACTION_SHOW_LEAVE_GROUP_POPUP, show_leave_group_popup).
-define(ACTION_LEAVE_GROUP, leave_group).
-define(ACTION_HIDE_POPUP, hide_popup).


% Actions that can be performed by user concerning specific groups and posibly requiring privileges
% Theyare represented by tuples {group_action, ActionName, GroupID, Args}
-define(GROUP_ACTION_TOGGLE, toggle_group).
-define(GROUP_ACTION_SHOW_REMOVE_POPUP, show_remove_group_popup).
-define(GROUP_ACTION_REMOVE, remove_group).
-define(GROUP_ACTION_SHOW_RENAME_POPUP, show_rename_group_popup).
-define(GROUP_ACTION_RENAME, rename_group).
-define(GROUP_ACTION_INVITE_USER, invite_user).
-define(GROUP_ACTION_REQUEST_SUPPORT, request_space_creation).
-define(GROUP_ACTION_SHOW_JOIN_SPACE_POPUP, show_join_space_popup).
-define(GROUP_ACTION_JOIN_SPACE, join_space).
-define(GROUP_ACTION_SHOW_CREATE_SPACE_POPUP, show_create_space_popup).
-define(GROUP_ACTION_CREATE_SPACE, create_space).
-define(GROUP_ACTION_SHOW_LEAVE_SPACE_POPUP, show_leave_space_popup).
-define(GROUP_ACTION_LEAVE_SPACE, leave_space).

% What privilege is required for what action
-define(PRIVILEGES_FOR_ACTIONS, [
    {?GROUP_ACTION_SHOW_REMOVE_POPUP, ?PRVLG_REMOVE},
    {?GROUP_ACTION_REMOVE, ?PRVLG_REMOVE},
    {?GROUP_ACTION_TOGGLE, ?PRVLG_VIEW},
    {?GROUP_ACTION_SHOW_RENAME_POPUP, ?PRVLG_CHANGE},
    {?GROUP_ACTION_RENAME, ?PRVLG_CHANGE},
    {?GROUP_ACTION_INVITE_USER, ?PRVLG_INVITE_USER},
    {?GROUP_ACTION_REQUEST_SUPPORT, ?PRVLG_REQUEST_SUPPORT},
    {?GROUP_ACTION_SHOW_JOIN_SPACE_POPUP, ?PRVLG_JOIN_SPACE},
    {?GROUP_ACTION_JOIN_SPACE, ?PRVLG_JOIN_SPACE},
    {?GROUP_ACTION_SHOW_CREATE_SPACE_POPUP, ?PRVLG_CREATE_SPACE},
    {?GROUP_ACTION_CREATE_SPACE, ?PRVLG_CREATE_SPACE},
    {?GROUP_ACTION_SHOW_LEAVE_SPACE_POPUP, ?PRVLG_LEAVE_SPACE},
    {?GROUP_ACTION_LEAVE_SPACE, ?PRVLG_LEAVE_SPACE}
]).

%% Comet process pid
-define(COMET_PID, comet_pid).

%% Macros used to create HTML element IDs
-define(GROUP_LIST_ELEMENT_ID(GroupID), <<"gr_li_", GroupID/binary>>).
-define(GROUP_HEADER_ID(GroupID), <<"gr_name_ph_", GroupID/binary>>).
-define(COLLAPSE_WRAPPER_ID(GroupID), <<"gr_collapse_wrapper_", GroupID/binary>>).
-define(USERS_SECTION_ID(GroupID), <<"gr_users_ph_", GroupID/binary>>).
-define(SPACES_SECTION_ID(GroupID), <<"gr_spaces_ph_", GroupID/binary>>).
-define(CHECKBOX_ID(UserID, PrivilegeName), <<"pr_chckbx_", PrivilegeName/binary, "_", UserID/binary>>).

%% Page state
-record(page_state, {groups = [], expanded_groups = [], gruid, access_token}).

%% Records used to store current info about groups
-record(group_state, {id = <<"">>, name = <<"">>, users = [], spaces = [], current_privileges}).
-record(user_state, {id = <<"">>, name = <<"">>, privileges = <<"">>}).
-record(space_state, {id = <<"">>, name = <<"">>}).


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
    #panel{class = <<"page-container">>, body = [
        #panel{id = <<"spinner">>, body = #image{image = <<"/images/spinner.gif">>}},
        opn_gui_utils:top_menu(groups_tab),
        #panel{id = <<"page_content">>, body = [
            #panel{id = <<"message">>, class = <<"dialog">>},
            #h6{class = <<"manage-groups-header">>, body = <<"Manage groups">>},
            #panel{class = <<"top-buttons-panel">>, body = [
                #button{id = <<"create_group_button">>, postback = {action, ?ACTION_SHOW_CREATE_GROUP_POPUP},
                    class = <<"btn btn-inverse btn-small top-button">>, body = <<"Create new group">>},
                #button{id = <<"join_group_button">>, postback = {action, ?ACTION_SHOW_JOIN_GROUP_POPUP},
                    class = <<"btn btn-inverse btn-small top-button">>, body = <<"Join existing group">>}
            ]},
            #panel{class = <<"group-list-panel">>, body = [
                #list{id = <<"group_list">>, body = []}
            ]}
        ]},
        footer_popup()
    ]}.


% Footer popup to display prompts and forms.
footer_popup() ->
    #panel{id = <<"footer_popup">>, class = <<"dialog success-dialog">>}.


%% group_list_element/2
%% ====================================================================
%% @doc Renders HTML responsible for group list element.
-spec group_list_element(GroupState :: #group_state{}, Expanded :: boolean) -> term().
%% ====================================================================
group_list_element(#group_state{id = GroupID, name = GroupNameOrUndef, users = UserStates, spaces = SpaceStates}, Expanded) ->
    {HasPerms, GroupName, GroupHeaderClass} =
        case GroupNameOrUndef of
            undefined -> {
                false,
                <<"<i>You don not have privileges to view this group</i>">>,
                <<"group-name-ph-no-perms">>
            };
            _ -> {
                true,
                GroupNameOrUndef,
                <<"group-name-ph">>
            }
        end,
    GroupHeaderID = ?GROUP_HEADER_ID(GroupID),
    {WrapperClass, UsersBody, SpacesBody} =
        case Expanded of
            false -> {<<"collapse-wrapper collapsed">>, [], []};
            true -> {<<"collapse-wrapper">>, group_users_body(GroupID, UserStates), group_spaces_body(GroupID, SpaceStates)}
        end,
    ListElement = #li{id = ?GROUP_LIST_ELEMENT_ID(GroupID), body = [
        #panel{class = <<"group-container">>, body = [
            #panel{class = <<"group-header">>, body = [
                #panel{class = <<"group-header-icon-ph">>, body = [
                    #span{class = <<"icomoon-users">>}
                ]},
                #panel{id = GroupHeaderID, class = GroupHeaderClass, body = [
                    #p{class = <<"group-name">>, body = [GroupName]},
                    #p{class = <<"group-id">>, body = [<<"ID: ", GroupID/binary>>]}
                ]},
                case HasPerms of
                    false ->
                        [];
                    true ->
                        #panel{class = <<"group-actions-ph">>, body = [
                            #panel{class = <<"btn-group group-actions-dropdown">>, body = [
                                <<"<i class=\"dropdown-arrow\"></i>">>,
                                #button{title = <<"Actions">>, class = <<"btn btn-small btn-info">>, body = [
                                    <<"<i class=\"icomoon-cog action-button-icon\"></i>">>, <<"Actions">>
                                ]},
                                #button{title = <<"Actions">>, class = <<"btn btn-small btn-info dropdown-toggle">>,
                                    data_fields = [{<<"data-toggle">>, <<"dropdown">>}], body = #span{class = <<"caret">>}},
                                #list{class = <<"dropdown-menu">>,
                                    body = [
                                        #li{body = [
                                            #link{body = [<<"<i class=\"icomoon-arrow-up2\"></i>">>, <<"Move up">>]}
                                        ]},
                                        #li{body = [
                                            #link{body = [<<"<i class=\"icomoon-arrow-down2\"></i>">>, <<"Move down">>]}
                                        ]},
                                        #li{body = [
                                            #link{title = <<"Leave this group">>,
                                                postback = {action, ?ACTION_SHOW_LEAVE_GROUP_POPUP, [GroupID, GroupName]},
                                                body = [<<"<i class=\"icomoon-exit\"></i>">>, <<"Leave group">>]}
                                        ]},
                                        #li{body = [
                                            #panel{class = <<"divider">>},
                                            #link{title = <<"Rename this group">>, postback = {group_action, ?GROUP_ACTION_SHOW_RENAME_POPUP, GroupID},
                                                body = [<<"<i class=\"icomoon-pencil2\"></i>">>, <<"Rename">>]}
                                        ]},
                                        #li{body = [
                                            #link{title = <<"Remove this group">>, postback = {group_action, ?GROUP_ACTION_SHOW_REMOVE_POPUP, GroupID},
                                                body = [<<"<i class=\"icomoon-remove\"></i>">>, <<"Remove">>]}
                                        ]},
                                        #li{body = [
                                            #panel{class = <<"divider">>},
                                            #link{title = <<"Obtain a token to invite a user to this group">>,
                                                postback = {group_action, ?GROUP_ACTION_INVITE_USER, GroupID},
                                                body = [<<"<i class=\"icomoon-user\"></i>">>, <<"Invite user">>]}
                                        ]},
                                        #li{body = [
                                            #panel{class = <<"divider">>},
                                            #link{title = <<"Create new space for this group">>,
                                                postback = {group_action, ?GROUP_ACTION_SHOW_CREATE_SPACE_POPUP, GroupID},
                                                body = [<<"<i class=\"icomoon-plus3\"></i>">>, <<"Create space">>]}
                                        ]},
                                        #li{body = [
                                            #link{title = <<"Join a space that has been created for his group">>,
                                                postback = {group_action, ?GROUP_ACTION_SHOW_JOIN_SPACE_POPUP, GroupID},
                                                body = [<<"<i class=\"icomoon-users\"></i>">>, <<"Join space">>]}
                                        ]},
                                        #li{body = [
                                            #link{title = <<"Obtain a token to request space creation">>,
                                                postback = {group_action, ?GROUP_ACTION_REQUEST_SUPPORT, GroupID},
                                                body = [<<"<i class=\"icomoon-question\"></i>">>, <<"Request<br />space creation">>]}
                                        ]}
                                    ]}
                            ]}
                        ]}
                end
            ]},
            #panel{id = ?COLLAPSE_WRAPPER_ID(GroupID), class = WrapperClass, body = [
                #panel{id = ?USERS_SECTION_ID(GroupID), class = <<"group-users">>, body = UsersBody},
                #panel{id = ?SPACES_SECTION_ID(GroupID), class = <<"group-spaces">>, body = SpacesBody}
            ]}
        ]}
    ]},

    case HasPerms of
        true ->
            gui_jq:wire(gui_jq:postback_action(GroupHeaderID, {group_action, ?GROUP_ACTION_TOGGLE, GroupID}));
        false ->
            ok
    end,
    ListElement.


group_users_body(GroupID, UserStates) ->
    % TODO usunac
%%     ?dump(UserStates),
    random:seed(),
    [
        #panel{class = <<"group-left-ph">>, body = [
            <<"USERS<br />&<br />RIGTHS">>,
            #link{title = <<"Help">>, class = <<"glyph-link">>, postback = show_users_info,
                body = #span{class = <<"icomoon-question">>}}
        ]},
        #panel{class = <<"group-middle-ph">>, body = [
            #panel{class = <<"gen-table-header-wrapper">>, body = [
                #table{class = <<"table table-striped gen-table-header users-table-header">>, header = #thead{body = [
                    #tr{cells =
                    lists:map(
                        fun({_PrivilegeID, _PrivilegeName, ColumnName}) ->
                            #th{body = [ColumnName]}
                        end, [{a, a, <<"User">>}] ++ ?PRIVILEGES)
                    }
                ]}}
            ]},
            #panel{class = <<"gen-table-wrapper">>, body = [
                #table{class = <<"table table-striped gen-table users-table">>, body = #tbody{body =
                lists:map(
                    fun(#user_state{id = UserID, name = UserName, privileges = UserPrivileges}) ->
                        #tr{cells = [
                            #td{body = [
                                #panel{class = <<"name-wrapper">>, body = [
                                    #link{title = UserID, class = <<"glyph-link">>,
                                        body = #span{class = <<"icomoon-user action-button-icon top-1">>}},
                                    UserName
                                ]},
                                #panel{class = <<"remove-wrapper">>, body = [
                                    #link{title = <<"Remove user from group">>, class = <<"glyph-link">>,
                                        body = #span{class = <<"icomoon-remove">>}}
                                ]}
                            ]},
                            lists:map(
                                fun({PrivilegeID, _PrivilegeName, _ColumnName}) ->
                                    Bleh = integer_to_binary(random:uniform(5633456346346)),
                                    % TODO Bleh -> PrivilegeName
                                    CheckboxID = ?CHECKBOX_ID(UserID, Bleh),
                                    flatui_checkbox:init_checkbox(CheckboxID),
                                    #td{body = [
                                        #flatui_checkbox{label_class = <<"privilege-checkbox checkbox no-label">>,
                                            id = CheckboxID,
                                            checked = lists:member(PrivilegeID, UserPrivileges),
                                            delegate = ?MODULE,
                                            postback = {message, {checkbox_toggled}}}
                                    ]}
                                end, ?PRIVILEGES)
                        ]}
                    % TODO usunac dup
%%                     end, lists:flatten(lists:duplicate(5, UserStates)))
                    end, UserStates)
                }}
            ]}
        ]}

    ].


group_spaces_body(GroupID, SpaceStates) ->
    % TODO usunac
    random:seed(),
    [
        #panel{class = <<"group-left-ph">>, body = [
            <<"SPACES">>,
            #link{title = <<"Help">>, class = <<"glyph-link">>, postback = show_spaces_info,
                body = #span{class = <<"icomoon-question">>}}
        ]},
        #panel{class = <<"group-spaces-ph">>, body = [
            #panel{class = <<"gen-table-header-wrapper">>, body = [
                #table{class = <<"table table-striped gen-table-header spaces-table-header">>, header = #thead{body = [
                    #tr{cells = [
                        #th{body = [<<"Space name">>]},
                        #th{body = [<<"Space ID">>]}
                    ]}
                ]}}
            ]},
            #panel{class = <<"gen-table-wrapper">>, body = [
                #table{class = <<"table table-striped gen-table spaces-table">>, body = #tbody{body =
                lists:map(
                    fun(#space_state{id = SpaceID, name = SpaceName}) ->
                        #tr{cells = [
                            #td{body = [
                                #panel{class = <<"name-wrapper">>, body = [
                                    #link{title = <<"View this space">>, class = <<"glyph-link">>, body = [
                                        #span{class = <<"icomoon-cloud action-button-icon">>},
                                        SpaceName
                                    ]}
                                ]},
                                #panel{class = <<"remove-wrapper">>, body = [
                                    #link{title = <<"Leave this space">>, class = <<"glyph-link">>,
                                        postback = {group_action, ?GROUP_ACTION_SHOW_LEAVE_SPACE_POPUP, GroupID, [SpaceID, SpaceName]},
                                        body = #span{class = <<"icomoon-exit">>}}
                                ]}
                            ]},
                            #td{body = [
                                SpaceID
                            ]}
                        ]}
                    % TODO usunac dup
%%                     end, lists:flatten(lists:duplicate(5, SpaceStates)))
                    end, SpaceStates)
                }}
            ]}
        ]}
    ].



comet_loop_init(GRUID, AccessToken, ExpandedGroups, ScrollToGroupID) ->
    PageState = synchronize_groups_and_users(GRUID, AccessToken, ExpandedGroups),
    refresh_group_list(PageState),
    case ScrollToGroupID of
        undefined -> ok;
        _ -> scroll_to_group(ScrollToGroupID)
    end,
    gui_jq:hide(<<"spinner">>),
    gui_comet:flush(),
    comet_loop(PageState).


comet_loop(State) ->
    NewState =
        try
            receive
                {action, Action, Args} ->
                    % Actions that do not concern a specific group
                    _State = comet_handle_action(State, Action, Args);

                {group_action, Action, GroupID, Args} ->
                    % Actions that concern a specific group
                    _State = comet_handle_group_action(State, Action, GroupID, Args)
            end % receive
        catch _Type:_Message ->
            ?error_stacktrace("Error in page_groups comet_loop - ~p:~p", [_Type, _Message]),
            gui_jq:hide(<<"spinner">>),
            opn_gui_utils:message(error, <<"Server encountered an unexpected error. Please refresh the page.">>),
            gui_comet:flush(),
            error
        end,
    case NewState of
        error ->
            timer:sleep(1000),
            ok; % Comet process will terminate
        _ ->
            gui_jq:hide(<<"spinner">>),
            gui_comet:flush(),
            ?MODULE:comet_loop(NewState)
    end.


comet_handle_action(State, Action, Args) ->
    #page_state{expanded_groups = ExpandedGroups, gruid = GRUID, access_token = AccessToken} = State,
    case {Action, Args} of
        {?ACTION_SHOW_CREATE_GROUP_POPUP, _} ->
            TextboxID = <<"new_group_textbox">>,
            TextboxIDString = "new_group_textbox",
            ButtonID = <<"new_group_submit">>,
            gui_jq:bind_enter_to_submit_button(TextboxID, ButtonID),
            Body = [
                #p{body = <<"Create new group">>},
                #form{class = <<"control-group">>, body = [
                    #textbox{id = TextboxID, class = <<"flat name-textbox">>, placeholder = <<"New group name">>},
                    #button{class = <<"btn btn-success btn-wide">>, body = <<"Ok">>, id = ButtonID,
                        postback = {action, ?ACTION_CREATE_GROUP, [{query_value, TextboxID}]},
                        source = [TextboxIDString]}
                ]}
            ],
            show_popup(Body, <<"$('#", TextboxID/binary, "').focus();">>),
            State;

        {?ACTION_CREATE_GROUP, [<<"">>]} ->
            gui_jq:info_popup(<<"Error">>, <<"Please insert a group name">>, <<"">>),
            State;

        {?ACTION_CREATE_GROUP, [GroupName]} ->
            hide_popup(),
            try
                {ok, GroupId} = gr_users:create_group({user, AccessToken}, [{<<"name">>, GroupName}]),
                gr_adapter:synchronize_user_groups({GRUID, AccessToken}),
                opn_gui_utils:message(success, <<"Group <b>", GroupName/binary, "</b> created successfully, ID: <b>", GroupId/binary, "</b>">>),
                SyncedState = synchronize_groups_and_users(GRUID, AccessToken, ExpandedGroups),
                refresh_group_list(SyncedState),
                SyncedState
            catch
                _:Other ->
                    ?error_stacktrace("Cannot create group ~p: ~p", [GroupName, Other]),
                    opn_gui_utils:message(error, <<"Cannot create group: <b>", (gui_str:html_encode(GroupName))/binary,
                    "</b>.<br />Please try again later.">>),
                    State
            end;

        {?ACTION_SHOW_JOIN_GROUP_POPUP, _} ->
            TextboxID = <<"join_group_textbox">>,
            TextboxIDString = "join_group_textbox",
            ButtonID = <<"join_group_submit">>,
            gui_jq:bind_enter_to_submit_button(TextboxID, ButtonID),
            Body = [
                #p{body = <<"To join an existing group, please paste a user invitation token below:">>},
                #form{class = <<"control-group">>, body = [
                    #textbox{id = TextboxID, class = <<"flat token-textbox">>, placeholder = <<"Token">>},
                    #button{class = <<"btn btn-success btn-wide">>, body = <<"Ok">>, id = ButtonID,
                        postback = {action, ?ACTION_JOIN_GROUP, [{query_value, TextboxID}]},
                        source = [TextboxIDString]}
                ]}
            ],
            show_popup(Body, <<"$('#", TextboxID/binary, "').focus();">>),
            State;

        {?ACTION_JOIN_GROUP, [Token]} ->
            hide_popup(),
            try
                {ok, GroupID} = gr_users:join_group({user, AccessToken}, [{<<"token">>, Token}]),
                gr_adapter:synchronize_user_groups({GRUID, AccessToken}),
                SyncedState = synchronize_groups_and_users(GRUID, AccessToken, ExpandedGroups),
                #group_state{name = GroupName} = lists:keyfind(GroupID, 2, SyncedState#page_state.groups),
                opn_gui_utils:message(success, <<"Successfully joined group <b>", GroupName/binary, "</b> (ID: <b>", GroupID/binary, "</b>)">>),
                refresh_group_list(SyncedState),
                SyncedState
            catch
                _:Other ->
                    ?error("Cannot join group using token ~p: ~p", [Token, Other]),
                    opn_gui_utils:message(error, <<"Cannot join group using token: <b>", (gui_str:html_encode(Token))/binary, "</b>.<br />Please try again later.">>),
                    State
            end;

        {?ACTION_SHOW_LEAVE_GROUP_POPUP, [GroupID, GroupName]} ->
            ButtonID = <<"ok_button">>,
            gui_jq:bind_enter_to_submit_button(ButtonID, ButtonID),
            Body = [
                #p{body = <<"Are you sure you want to leave group:<br /><b>",
                (gui_str:html_encode(GroupName))/binary, " (", GroupID/binary, ")</b><br />">>},
                #form{class = <<"control-group">>, body = [
                    #button{id = ButtonID, postback = {action, ?ACTION_LEAVE_GROUP, [GroupID, GroupName]},
                        class = <<"btn btn-success btn-wide">>, body = <<"Ok">>},
                    #button{id = <<"cancel_button">>, postback = {action, ?ACTION_HIDE_POPUP},
                        class = <<"btn btn-danger btn-wide">>, body = <<"Cancel">>}
                ]}
            ],
            show_popup(Body, <<"$('#", ButtonID/binary, "').focus();">>),
            State;

        {?ACTION_LEAVE_GROUP, [GroupID, GroupName]} ->
            hide_popup(),
            case gr_users:leave_group({user, AccessToken}, GroupID) of
                ok ->
                    gr_adapter:synchronize_user_groups({GRUID, AccessToken}),
                    opn_gui_utils:message(success, <<"Successfully left group <b>", GroupName/binary, "</b> (ID: <b>", GroupID/binary, "</b>)">>),
                    SyncedState = synchronize_groups_and_users(GRUID, AccessToken, ExpandedGroups),
                    refresh_group_list(SyncedState),
                    SyncedState;
                Other ->
                    ?error("Cannot leave group with ID ~p: ~p", [GroupID, Other]),
                    opn_gui_utils:message(error, <<"Cannot leave group <b>", GroupName/binary, "</b> (ID: <b>", GroupID/binary, "</b>).<br />Please try again later.">>),
                    State
            end;

        {?ACTION_HIDE_POPUP, _} ->
            hide_popup(),
            State
    end.


comet_handle_group_action(State, Action, GroupID, Args) ->
    #page_state{groups = Groups, expanded_groups = ExpandedGroups, gruid = GRUID, access_token = AccessToken} = State,
    #group_state{
        users = UserStates,
        spaces = SpaceStates,
        name = GroupName,
        current_privileges = UserPrivileges} = lists:keyfind(GroupID, 2, Groups),
    % Check if the user is permitted to perform such action
    case check_privileges(Action, UserPrivileges) of
        {false, RequiredPrivilege} ->
            gui_jq:info_popup(<<"Not authorized">>, <<"To perform this operation, you need the <b>\"", RequiredPrivilege/binary, "\"</b> privileges.">>, <<"">>),
            State;
        true ->
            case {Action, Args} of
                {?GROUP_ACTION_TOGGLE, _} ->
                    NewExpandedGroups =
                        case lists:member(GroupID, ExpandedGroups) of
                            true ->
                                gui_jq:slide_up(?COLLAPSE_WRAPPER_ID(GroupID), 400),
                                ExpandedGroups -- [GroupID];
                            false ->
                                gui_jq:update(?USERS_SECTION_ID(GroupID), group_users_body(GroupID, UserStates)),
                                gui_jq:update(?SPACES_SECTION_ID(GroupID), group_spaces_body(GroupID, SpaceStates)),
                                gui_jq:slide_down(?COLLAPSE_WRAPPER_ID(GroupID), 600),
                                % Adjust user tables' headers width (scroll bar takes some space out of table and header would be too wide)
                                gui_jq:wire(<<"var pad = $($('.gen-table-wrapper')[0]).width() - $($('.gen-table')[0]).width();",
                                "$('.gen-table-header-wrapper').css('padding-right', pad + 'px');">>),
                                [GroupID | ExpandedGroups]
                        end,
                    State#page_state{expanded_groups = NewExpandedGroups};

                {?GROUP_ACTION_SHOW_REMOVE_POPUP, _} ->
                    ButtonID = <<"ok_button">>,
                    gui_jq:bind_enter_to_submit_button(ButtonID, ButtonID),
                    Body = [
                        #p{body = <<"Are you sure you want to remove group:<br /><b>",
                        (gui_str:html_encode(GroupName))/binary, " (", GroupID/binary, ")</b>?<br />">>},
                        #p{class = <<"warning-message">>, body = <<"This operation cannot be undone!">>},
                        #form{class = <<"control-group">>, body = [
                            #button{id = ButtonID, postback = {group_action, ?GROUP_ACTION_REMOVE, GroupID},
                                class = <<"btn btn-success btn-wide">>, body = <<"Ok">>},
                            #button{id = <<"cancel_button">>, postback = {action, ?ACTION_HIDE_POPUP},
                                class = <<"btn btn-danger btn-wide">>, body = <<"Cancel">>}
                        ]}
                    ],
                    show_popup(Body, <<"$('#", ButtonID/binary, "').focus();">>),
                    State;

                {?GROUP_ACTION_REMOVE, _} ->
                    hide_popup(),
                    case gr_groups:remove({user, AccessToken}, GroupID) of
                        ok ->
                            gr_adapter:synchronize_user_groups({GRUID, AccessToken}),
                            opn_gui_utils:message(success, <<"Group <b>", GroupName/binary,
                            "</b> (ID: <b>", GroupID/binary, "</b>) removed successfully.">>),
                            SyncedState = synchronize_groups_and_users(GRUID, AccessToken, ExpandedGroups),
                            refresh_group_list(SyncedState),
                            SyncedState;
                        Other ->
                            ?error("Cannot remove group with ID ~p: ~p", [GroupID, Other]),
                            opn_gui_utils:message(error, <<"Cannot remove group <b>", GroupName/binary,
                            "</b> (ID: <b>", GroupID/binary, "</b>).<br />Please try again later.">>),
                            State
                    end;

                {?GROUP_ACTION_SHOW_RENAME_POPUP, _} ->
                    TextboxID = <<"rename_group_textbox">>,
                    TextboxIDString = "rename_group_textbox",
                    ButtonID = <<"rename_group_submit">>,
                    gui_jq:bind_enter_to_submit_button(TextboxID, ButtonID),
                    Body = [
                        #p{body = <<"Rename <b>", GroupName/binary, "</b>">>},
                        #form{class = <<"control-group">>, body = [
                            #textbox{id = TextboxID, class = <<"flat name-textbox">>, value = GroupName, placeholder = <<"New group name">>},
                            #button{class = <<"btn btn-success btn-wide">>, body = <<"Ok">>, id = ButtonID,
                                postback = {group_action, ?GROUP_ACTION_RENAME, GroupID, [{query_value, TextboxID}]},
                                source = [TextboxIDString]}
                        ]}
                    ],
                    show_popup(Body, <<"$('#", TextboxID/binary, "').focus().select();">>),
                    State;

                {?GROUP_ACTION_RENAME, [NewGroupName]} ->
                    hide_popup(),
                    case gr_groups:modify_details({user, AccessToken}, GroupID, [{<<"name">>, NewGroupName}]) of
                        ok ->
                            gr_adapter:synchronize_user_groups({GRUID, AccessToken}),
                            opn_gui_utils:message(success, <<"Group renamed: <b>", GroupName/binary,
                            "</b> -> <b>", NewGroupName/binary, "</b> (ID: <b>", GroupID/binary, "</b>).">>),
                            SyncedState = synchronize_groups_and_users(GRUID, AccessToken, ExpandedGroups),
                            refresh_group_list(SyncedState),
                            SyncedState;
                        Other ->
                            ?error("Cannot change name of group ~p: ~p", [GroupID, Other]),
                            opn_gui_utils:message(error, <<"Cannot rename group <b>", GroupName/binary,
                            "</b> (ID: <b>", GroupID/binary, "</b>).<br />Please try again later.">>),
                            State

                    end;

                {?GROUP_ACTION_INVITE_USER, _} ->
                    case gr_groups:get_invite_user_token({user, AccessToken}, GroupID) of
                        {ok, Token} ->
                            TextboxID = <<"token_textbox">>,
                            ButtonID = <<"token_ok">>,
                            gui_jq:bind_enter_to_submit_button(TextboxID, ButtonID),
                            Body = [
                                #p{body = <<"Give the token below to a user willing to join the <b>", GroupName/binary, "</b> group:">>},
                                #form{class = <<"control-group">>, body = [
                                    #textbox{id = TextboxID, class = <<"flat token-textbox">>, value = Token, placeholder = <<"Token">>},
                                    #button{class = <<"btn btn-success btn-wide">>, body = <<"Ok">>, id = ButtonID,
                                        postback = {action, ?ACTION_HIDE_POPUP}}
                                ]}
                            ],
                            show_popup(Body, <<"$('#", TextboxID/binary, "').focus().select();">>);
                        Other ->
                            ?error("Cannot get user invitation token for group with ID ~p: ~p", [GroupID, Other]),
                            opn_gui_utils:message(error, <<"Cannot get invitation token for group <b>", GroupName/binary,
                            "</b> (ID: <b>", GroupID/binary, "</b>).<br />Please try again later.">>)
                    end,
                    State;

                {?GROUP_ACTION_REQUEST_SUPPORT, _} ->
                    case gr_groups:get_create_space_token({user, AccessToken}, GroupID) of
                        {ok, Token} ->
                            TextboxID = <<"token_textbox">>,
                            ButtonID = <<"token_ok">>,
                            gui_jq:bind_enter_to_submit_button(TextboxID, ButtonID),
                            Body = [
                                #p{body = <<"Give the token below to a provider willing to create a Space for the <b>", GroupName/binary, "</b> group:">>},
                                #form{class = <<"control-group">>, body = [
                                    #textbox{id = TextboxID, class = <<"flat token-textbox">>, value = Token, placeholder = <<"Token">>},
                                    #button{class = <<"btn btn-success btn-wide">>, body = <<"Ok">>, id = ButtonID,
                                        postback = {action, ?ACTION_HIDE_POPUP}}
                                ]}
                            ],
                            show_popup(Body, <<"$('#", TextboxID/binary, "').focus().select();">>);
                        Other ->
                            ?error("Cannot get support token for group with ID ~p: ~p", [GroupID, Other]),
                            opn_gui_utils:message(error, <<"Cannot get Space creation token for group <b>", GroupName/binary,
                            "</b> (ID: <b>", GroupID/binary, "</b>).<br />Please try again later.">>)
                    end,
                    State;

                {?GROUP_ACTION_SHOW_JOIN_SPACE_POPUP, _} ->
                    TextboxID = <<"join_space_textbox">>,
                    TextboxIDString = "join_space_textbox",
                    ButtonID = <<"join_space_button">>,
                    gui_jq:bind_enter_to_submit_button(TextboxID, ButtonID),
                    Body = [
                        #p{body = <<"To join an existing space as <b>", GroupName/binary, "</b>, please paste a group invitation token below:">>},
                        #form{class = <<"control-group">>, body = [
                            #textbox{id = TextboxID, class = <<"flat token-textbox">>, placeholder = <<"Token">>},
                            #button{class = <<"btn btn-success btn-wide">>, body = <<"Ok">>, id = ButtonID,
                                postback = {group_action, ?GROUP_ACTION_JOIN_SPACE, GroupID, [{query_value, TextboxID}]},
                                source = [TextboxIDString]}
                        ]}
                    ],
                    show_popup(Body, <<"$('#", TextboxID/binary, "').focus();">>),
                    State;

                {?GROUP_ACTION_JOIN_SPACE, [Token]} ->
                    hide_popup(),
                    try
                        {ok, SpaceId} = gr_groups:join_space({user, AccessToken}, GroupID, [{<<"token">>, Token}]),
                        opn_gui_utils:message(success, <<"Space joined as: <b>", GroupName/binary,
                        "</b> (ID: <b>", GroupID/binary, "</b>).">>),
                        SyncedState = synchronize_groups_and_users(GRUID, AccessToken, ExpandedGroups),
                        refresh_group_list(SyncedState),
                        SyncedState
                    catch
                        _:Other ->
                            ?error("Cannot join Space using token ~p: ~p", [Token, Other]),
                            opn_gui_utils:message(error, <<"Cannot join Space using token: <b>", (gui_str:html_encode(Token))/binary, "</b>.<br />Please try again later.">>),
                            State
                    end;

                {?GROUP_ACTION_SHOW_CREATE_SPACE_POPUP, _} ->
                    TextboxID = <<"new_space_textbox">>,
                    TextboxIDString = "new_space_textbox",
                    ButtonID = <<"new_space_submit">>,
                    gui_jq:bind_enter_to_submit_button(TextboxID, ButtonID),
                    Body = [
                        #p{body = <<"Create new space">>},
                        #form{class = <<"control-group">>, body = [
                            #textbox{id = TextboxID, class = <<"flat name-textbox">>, placeholder = <<"New space name">>},
                            #button{class = <<"btn btn-success btn-wide">>, body = <<"Ok">>, id = ButtonID,
                                postback = {group_action, ?GROUP_ACTION_CREATE_SPACE, GroupID, [{query_value, TextboxID}]},
                                source = [TextboxIDString]}
                        ]}
                    ],
                    show_popup(Body, <<"$('#", TextboxID/binary, "').focus();">>),
                    State;

                {?GROUP_ACTION_CREATE_SPACE, [<<"">>]} ->
                    gui_jq:info_popup(<<"Error">>, <<"Please insert a space name">>, <<"">>),
                    State;

                {?GROUP_ACTION_CREATE_SPACE, [SpaceName]} ->
                    hide_popup(),
                    try
                        {ok, SpaceID} = gr_groups:create_space({user, AccessToken}, GroupID, [{<<"name">>, SpaceName}]),
                        gr_adapter:synchronize_user_groups({GRUID, AccessToken}),
                        opn_gui_utils:message(success, <<"Space <b>", SpaceName/binary, "</b> created successfully, ID: <b>", SpaceID/binary, "</b>">>),
                        SyncedState = synchronize_groups_and_users(GRUID, AccessToken, ExpandedGroups),
                        refresh_group_list(SyncedState),
                        SyncedState
                    catch
                        _:Other ->
                            ?error("Cannot create Space ~p: ~p", [SpaceName, Other]),
                            opn_gui_utils:message(error, <<"Cannot create Space: <b>", (gui_str:html_encode(SpaceName))/binary,
                            "</b>.<br />Please try again later.">>),
                            State
                    end;

                {?GROUP_ACTION_SHOW_LEAVE_SPACE_POPUP, [SpaceID, SpaceName]} ->
                    ButtonID = <<"ok_button">>,
                    gui_jq:bind_enter_to_submit_button(ButtonID, ButtonID),
                    Body = [
                        #p{body = <<"Are you sure you want <b>", (gui_str:html_encode(GroupName))/binary, "</b> to leave space:<br /><b>",
                        (gui_str:html_encode(SpaceName))/binary, " (", SpaceID/binary, ")</b><br />">>},
                        #p{class = <<"warning-message">>, body = <<"This operation cannot be undone!">>},
                        #form{class = <<"control-group">>, body = [
                            #button{id = ButtonID, postback = {group_action, ?GROUP_ACTION_LEAVE_SPACE, GroupID, [SpaceID, SpaceName]},
                                class = <<"btn btn-success btn-wide">>, body = <<"Ok">>},
                            #button{id = <<"cancel_button">>, postback = {action, ?ACTION_HIDE_POPUP},
                                class = <<"btn btn-danger btn-wide">>, body = <<"Cancel">>}
                        ]}
                    ],
                    show_popup(Body, <<"$('#", ButtonID/binary, "').focus();">>),
                    State;

                {?GROUP_ACTION_LEAVE_SPACE, [SpaceID, SpaceName]} ->
                    hide_popup(),
                    case gr_groups:leave_space({user, AccessToken}, GroupID, SpaceID) of
                        ok ->
                            opn_gui_utils:message(success, <<"Group <b>", GroupName/binary, "</b> has successfully left the space <b>",
                            (gui_str:html_encode(SpaceName))/binary, " (", SpaceID/binary, ")</b>">>),
                            SyncedState = synchronize_groups_and_users(GRUID, AccessToken, ExpandedGroups),
                            refresh_group_list(SyncedState),
                            SyncedState;
                        Other ->
                            ?error("Cannot leave Space with ID ~p: ~p", [SpaceID, Other]),
                            opn_gui_utils:message(error, <<"Cannot leave Space <b>", SpaceName/binary,
                            "</b> (ID: <b>", SpaceID/binary, "</b>).<br />Please try again later.">>),
                            State
                    end
            end
    end.


refresh_group_list(#page_state{groups = Groups, expanded_groups = ExpandedGroups}) ->
    Body = lists:map(
        fun(#group_state{id = ID} = GroupState) ->
            group_list_element(GroupState, lists:member(ID, ExpandedGroups))
        end, Groups),
    gui_jq:update(<<"group_list">>, Body).


scroll_to_group(GroupID) ->
    gui_jq:wire(<<"$('html, body').animate({scrollTop: parseInt($('#",
    (?GROUP_LIST_ELEMENT_ID(GroupID))/binary, "').offset().top - 150)}, 200);">>).


check_privileges(ActionType, UserPrivileges) ->
    case proplists:get_value(ActionType, ?PRIVILEGES_FOR_ACTIONS) of
        undefined ->
            true;
        PrivilegeName ->
            case lists:member(PrivilegeName, UserPrivileges) of
                true ->
                    true;
                false ->
                    {_PrivID, PrivName, _ColumnName} = lists:keyfind(PrivilegeName, 1, ?PRIVILEGES),
                    {false, PrivName}
            end
    end.


show_popup(Body, ScriptAfterUpdate) ->
    case Body of
        [] ->
            skip;
        _ ->
            CloseButton = #link{id = <<"footer_close_button">>, postback = {action, ?ACTION_HIDE_POPUP}, title = <<"Hide">>,
                class = <<"glyph-link">>, body = #span{class = <<"fui-cross">>}},
            gui_jq:update(<<"footer_popup">>, [CloseButton | Body]),
            gui_jq:slide_down(<<"footer_popup">>, 300),
            case ScriptAfterUpdate of
                undefined ->
                    ok;
                _ ->
                    gui_jq:wire(ScriptAfterUpdate)
            end
    end.


hide_popup() ->
    gui_jq:slide_up(<<"footer_popup">>, 200).


synchronize_groups_and_users(GRUID, AccessToken, ExpandedGroups) ->
    {ok, GroupIDs} = gr_users:get_groups({user, AccessToken}),
    % Synchronize groups data
    GroupStates = lists:map(
        fun(GroupID) ->
            case gr_groups:get_details({user, AccessToken}, GroupID) of
                {ok, #group_details{name = GroupName}} ->
                    % Synchronize users data (belonging to certain group)
                    {ok, UsersIDs} = gr_groups:get_users({user, AccessToken}, GroupID),
                    UserStates = lists:map(
                        fun(UserID) ->
                            {ok, #user_details{name = UserName}} = gr_groups:get_user_details({user, AccessToken}, GroupID, UserID),
                            {ok, Privileges} = gr_groups:get_user_privileges({user, AccessToken}, GroupID, UserID),
                            #user_state{id = UserID, name = UserName, privileges = Privileges}
                        end, UsersIDs),
                    % Synchronize spaces data (belonging to certain group)
                    {ok, SpacesIDs} = gr_groups:get_spaces({user, AccessToken}, GroupID),
                    SpaceStates = lists:map(
                        fun(SpaceID) ->
                            {ok, #space_details{name = SpaceName}} = gr_groups:get_space_details({user, AccessToken}, GroupID, SpaceID),
                            #space_state{id = SpaceID, name = SpaceName}

                        end, SpacesIDs),
                    #user_state{privileges = CurrentPrivileges} = lists:keyfind(GRUID, 2, UserStates),
                    #group_state{id = GroupID, name = GroupName, users = UserStates, spaces = SpaceStates, current_privileges = CurrentPrivileges};
                _ ->
                    % User does not have rights to view this group
                    #group_state{id = GroupID, name = undefined, users = [], spaces = [], current_privileges = []}
            end
        end, GroupIDs),
    {CanView, CannotView} = lists:partition(
        fun(#group_state{name = Name}) ->
            Name /= undefined
        end, GroupStates),
    SortedGroupStates = CanView ++ CannotView,
    #page_state{groups = SortedGroupStates, gruid = GRUID, access_token = AccessToken, expanded_groups = ExpandedGroups}.


%% event/1
%% ====================================================================
%% @doc Handles page events.
-spec event(Event :: term()) -> no_return().
%% ====================================================================
event(init) ->
    try
        GRUID = utils:ensure_binary(opn_gui_utils:get_global_user_id()),
        AccessToken = opn_gui_utils:get_access_token(),
        {ExpandedGroups, ScrollToGroupID} =
            case gui_ctx:url_param(<<"show">>) of
                undefined -> {[], undefined};
                Bin -> {[Bin], Bin}
            end,

        gui_jq:wire(#api{name = "join_group", tag = "join_group"}, false),
        gui_jq:wire(#api{name = "leave_group", tag = "leave_group"}, false),
        gui_jq:bind_key_to_click_on_class(<<"13">>, <<"confirm">>),
        gui_jq:register_escape_event("escape_pressed_event"),

        {ok, Pid} = gui_comet:spawn(fun() -> comet_loop_init(GRUID, AccessToken, ExpandedGroups, ScrollToGroupID) end),
        put(?COMET_PID, Pid)
    catch
        _:Reason ->
            ?error_stacktrace("Cannot initialize page ~p: ~p", [?MODULE, Reason]),
            gui_jq:hide(<<"spinner">>),
            opn_gui_utils:message(error, <<"Cannot fetch groups.<br />Please try again later.">>)
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

event({group_action, Action, GroupID}) ->
    event({group_action, Action, GroupID, []});

event({group_action, Action, GroupID, Args}) ->
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
    get(?COMET_PID) ! {group_action, Action, GroupID, ProcessedArgs};

event({close_message, MessageId}) ->
    gui_jq:hide(MessageId);

event(show_users_info) ->
    gui_jq:info_popup(<<"Users section">>,
        <<"This table shows all users that belong to the group and their privileges.<br /><br />",
        "- to modify privileges, set corresponding checkboxes and click the \"Save\" button<br />",
        "- to remove a user, point at the user and use the trash button<br />",
        "- to display user ID, point at the user icon<br />",
        "- to invite a user to the group, select the action from \"Actions\" menu.<br />">>, <<"">>);

event(show_spaces_info) ->
    gui_jq:info_popup(<<"Spaces section">>,
        <<"This table shows all spaces to which the group belongs.<br /><br />",
        "- to leave a space, point at the space and use the exit button<br />",
        "- to see more details about a space, click on its name or icon<br />",
        "- to create a new space for the group, join an existing space or request space creation, select an action from \"Actions\" menu.<br />">>, <<"">>);

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

