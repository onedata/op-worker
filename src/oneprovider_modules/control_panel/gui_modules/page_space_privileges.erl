% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page allows user to manage Space privileges.
%% @end
%% ===================================================================

-module(page_space_privileges).
-include("oneprovider_modules/control_panel/common.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("ctool/include/global_registry/gr_groups.hrl").
-include_lib("ctool/include/logging.hrl").

%% n2o API and comet
-export([main/0, event/1, comet_loop/1]).

%% Common page CCS styles
-define(CONTENT_COLUMN_STYLE, <<"padding-right: 0">>).
-define(NAVIGATION_COLUMN_STYLE, <<"border-left-width: 0; width: 20px; padding-left: 0;">>).
-define(DETAIL_STYLE, <<"font-size: large; font-weight: normal; vertical-align: middle;">>).

%% Comet process pid
-define(COMET_PID, comet_pid).

%% Comet process state
-define(STATE, comet_state).
-record(?STATE, {space_id, users_privileges, new_users_privileges, groups_privileges, new_groups_privileges, access_token}).

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
            gui_jq:redirect_to_login(),
            #dtl{file = "bare", app = ?APP_Name, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
        false ->
            case gui_ctx:url_param(<<"id">>) of
                undefined ->
                    page_error:redirect_with_error(?error_space_not_found),
                    #dtl{file = "bare", app = ?APP_Name, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
                Id ->
                    SpaceId = gui_str:to_binary(Id),
                    case gr_users:get_space_details({user, opn_gui_utils:get_access_token()}, SpaceId) of
                        {ok, SpaceDetails} ->
                            #dtl{file = "bare", app = ?APP_Name, bindings = [{title, title()}, {body, body(SpaceDetails)}, {custom, <<"">>}]};
                        _ ->
                            page_error:redirect_with_error(?error_space_permission_denied),
                            #dtl{file = "bare", app = ?APP_Name, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]}
                    end
            end
    end.


%% title/0
%% ====================================================================
%% @doc This will be placed instead of {{title}} tag in template.
%% @end
-spec title() -> Result when
    Result :: binary().
%% ====================================================================
title() ->
    <<"Space privileges">>.


%% body/0
%% ====================================================================
%% @doc This will be placed instead of {{body}} tag in template.
%% @end
-spec body(SpaceDetails :: #space_details{}) -> Result when
    Result :: #panel{}.
%% ====================================================================
body(SpaceDetails) ->
    MessageStyle = <<"position: fixed; width: 100%; top: 55px; z-index: 1; display: none;">>,
    [
        #panel{
            id = <<"main_spinner">>,
            style = <<"position: absolute; top: 12px; left: 17px; z-index: 1234; width: 32px;">>,
            body = #image{
                image = <<"/images/spinner.gif">>
            }
        },
        opn_gui_utils:top_menu(spaces_tab),
        #panel{
            id = <<"ok_message">>,
            style = MessageStyle,
            class = <<"dialog dialog-success">>
        },
        #panel{
            id = <<"error_message">>,
            style = MessageStyle,
            class = <<"dialog dialog-danger">>
        },
        #panel{
            style = <<"margin-bottom: 100px;">>,
            body = [
                #h6{
                    style = <<"font-size: x-large; margin: 0 auto; margin-top: 160px; text-align: center;">>,
                    body = <<"Space privileges">>
                },
                space_details_table(SpaceDetails) |
                lists:map(fun({TableId, Panel}) ->
                    #panel{
                        body = [
                            #table{
                                id = TableId,
                                class = <<"table table-bordered">>,
                                style = <<"width: 50%; margin: 0 auto; margin-top: 3em; display: none;">>
                            },
                            Panel
                        ]
                    }
                end, [
                    {<<"users_table">>, save_button(<<"save_users_privileges_button">>, {message, save_users_privileges})},
                    {<<"groups_table">>, save_button(<<"save_groups_privileges_button">>, {message, save_groups_privileges})}
                ])
            ]
        }
    ].


%% space_details_table/1
%% ====================================================================
%% @doc Renders the body of Space details table
%% @end
-spec space_details_table(SpaceDetails :: #space_details{}) -> Result when
    Result :: #table{}.
%% ====================================================================
space_details_table(#space_details{id = SpaceId, name = SpaceName}) ->
    DescriptionStyle = <<"border-width: 0; text-align: right;">>,
    MainStyle = <<"border-width: 0;  text-align: left;">>,
    #table{
        style = <<"margin: 0 auto; width: 50%; margin-top: 30px; border-spacing: 1em; border-collapse: inherit;">>,
        body = lists:map(fun({Description, MainId, Main}) ->
            #tr{
                cells = [
                    #td{
                        style = DescriptionStyle,
                        body = #label{
                            style = <<"margin: 0 auto; cursor: auto;">>,
                            class = <<"label label-large label-inverse">>,
                            body = Description
                        }
                    },
                    #td{
                        id = MainId,
                        style = MainStyle,
                        body = Main
                    }
                ]
            }
        end, [
            {<<"Space Name">>, <<"space_name">>, #span{style = ?DETAIL_STYLE, body = SpaceName}},
            {<<"Space ID">>, <<"">>, #span{style = ?DETAIL_STYLE, body = SpaceId}}
        ])
    }.


%% save_button/2
%% ====================================================================
%% @doc Renders save button.
-spec save_button(ButtonId :: binary(), Postback :: term()) -> Result when
    Result :: #panel{}.
%% ====================================================================
save_button(ButtonId, Postback) ->
    #panel{
        style = <<"margin: 0 auto; width: 50%; margin-top: 30px; text-align: center;">>,
        body = #button{
            id = ButtonId,
            postback = Postback,
            style = <<"display: none">>,
            class = <<"btn btn-inverse btn-small">>,
            body = <<"Save">>
        }
    }.


%% privileges_table/4
%% ====================================================================
%% @doc Renders privileges table body.
%% @end
-spec privileges_table(TableName :: binary(), ColumnNames :: [binary()], PrivilegesNames :: [binary()],
    PrivilegesRows :: [{Name :: binary(), Id :: binary(), Privileges :: [binary()]}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
privileges_table(TableName, ColumnNames, PrivilegesNames, PrivilegesRows) ->
    ColumnStyle = <<"text-align: center; vertical-align: inherit;">>,

    Header = #tr{
        cells = lists:map(fun(ColumnName) ->
            #th{
                body = ColumnName,
                style = ColumnStyle
            }
        end, ColumnNames)
    },

    Rows = lists:map(fun({{Name, Id, Privileges}, N}) ->
        RowId = <<TableName/binary, "_", (integer_to_binary(N))/binary>>,
        #tr{
            cells = [
                #td{
                    body = <<"<b>", (gui_str:html_encode(Name))/binary, "</b> (", Id:7/binary, "...)">>,
                    style = ColumnStyle
                } | lists:map(fun({Privilege, M}) ->
                    CheckboxId = <<RowId/binary, "_", (integer_to_binary(M))/binary>>,
                    flatui_checkbox:init_checkbox(CheckboxId),
                    #td{
                        style = ColumnStyle,
                        body = #flatui_checkbox{
                            label_style = <<"width: 20px; margin: 0 auto;">>,
                            label_class = <<"checkbox no-label">>,
                            id = CheckboxId,
                            checked = lists:member(Privilege, Privileges),
                            delegate = ?MODULE,
                            postback = {message, {checkbox_toggled, TableName, Name, Id, Privilege}}
                        }
                    }
                end, lists:zip(PrivilegesNames, tl(lists:seq(0, length(PrivilegesNames)))))
            ]
        }
    end, lists:zip(lists:sort(PrivilegesRows), tl(lists:seq(0, length(PrivilegesRows))))),

    [Header | Rows].


%% ====================================================================
%% Events handling
%% ====================================================================

%% comet_loop/1
%% ====================================================================
%% @doc Handles space management actions.
-spec comet_loop(State :: #?STATE{}) -> Result when
    Result :: {error, Reason :: term()}.
%% ====================================================================
comet_loop({error, Reason}) ->
    {error, Reason};

comet_loop(#?STATE{space_id = SpaceId, new_users_privileges = NewUsersPrivileges, new_groups_privileges = NewGroupsPrivileges, access_token = AccessToken} = State) ->
    ?dump(NewUsersPrivileges),
    ?dump(NewGroupsPrivileges),
    NewState = try
                   receive
                       render_tables ->
                           UsersColumnsNames = [<<"User">>, <<"View Space">>, <<"Modify Space">>, <<"Remove Space">>, <<"Invite user">>,
                               <<"Remove user">>, <<"Invite group">>, <<"Remove group">>, <<"Invite provider">>,
                               <<"Remove provider">>, <<"Set privileges">>],
                           UsersPrivilegesNames = [<<"space_view_data">>, <<"space_change_data">>, <<"space_remove">>,
                               <<"space_invite_user">>, <<"space_remove_user">>, <<"space_invite_group">>, <<"space_remove_group">>,
                               <<"space_add_provider">>, <<"space_remove_provider">>, <<"space_set_privileges">>],
                           gui_jq:update(<<"users_table">>, privileges_table(<<"users_privileges">>, UsersColumnsNames, UsersPrivilegesNames, NewUsersPrivileges)),
                           gui_jq:fade_in(<<"users_table">>, 500),
                           gui_jq:show(<<"save_users_privileges_button">>),
                           case NewUsersPrivileges of
                               [] -> gui_jq:prop(<<"save_users_privileges_button">>, <<"disabled">>, <<"disabled">>);
                               _ -> ok
                           end,

                           GroupsColumnsNames = [<<"Group">>, <<"View group">>, <<"Modify group">>, <<"Remove group">>,
                               <<"Invite user">>, <<"Remove user">>, <<"Create Space">>, <<"Join Space">>, <<"Leave Space">>,
                               <<"Invite provider">>, <<"Set privileges">>],
                           GroupsPrivilegesNames = [<<"group_view_data">>, <<"group_change_data">>, <<"group_remove">>,
                               <<"group_invite_user">>, <<"group_remove_user">>, <<"group_create_space">>, <<"group_join_space">>,
                               <<"group_leave_space">>, <<"group_create_space_token">>, <<"group_set_privileges">>],
                           gui_jq:update(<<"groups_table">>, privileges_table(<<"groups_privileges">>, GroupsColumnsNames, GroupsPrivilegesNames, NewGroupsPrivileges)),
                           gui_jq:fade_in(<<"groups_table">>, 500),
                           gui_jq:show(<<"save_groups_privileges_button">>),
                           case NewGroupsPrivileges of
                               [] -> gui_jq:prop(<<"save_groups_privileges_button">>, <<"disabled">>, <<"disabled">>);
                               _ -> ok
                           end,
                           State;

                       {checkbox_toggled, <<"users_privileges">>, Name, Id, Privilege} ->
                           {_, _, Privileges} = lists:keyfind(Id, 2, NewUsersPrivileges),
                           case lists:member(Privilege, Privileges) of
                               true ->
                                   State#?STATE{new_users_privileges = [{Name, Id, lists:delete(Privilege, Privileges)} | lists:keydelete(Id, 2, NewUsersPrivileges)]};
                               _ ->
                                   State#?STATE{new_users_privileges = [{Name, Id, [Privilege | Privileges]} | lists:keydelete(Id, 2, NewUsersPrivileges)]}
                           end;

                       {checkbox_toggled, <<"groups_privileges">>, Name, Id, Privilege} ->
                           {_, _, Privileges} = lists:keyfind(Id, 2, NewGroupsPrivileges),
                           case lists:member(Privilege, Privileges) of
                               true ->
                                   State#?STATE{new_groups_privileges = [{Name, Id, lists:delete(Privilege, Privileges)} | lists:keydelete(Id, 2, NewGroupsPrivileges)]};
                               _ ->
                                   State#?STATE{new_groups_privileges = [{Name, Id, [Privilege | Privileges]} | lists:keydelete(Id, 2, NewGroupsPrivileges)]}
                           end;

                       save_users_privileges ->
                           try
                               lists:foreach(fun({_, UserId, NewUserPrivileges}) ->
                                   {_, _, OldUserPrivileges} = lists:keyfind(UserId, 2, State#?STATE.users_privileges),
                                   OldUserPrivilegesSorted = lists:sort(OldUserPrivileges),
                                   case lists:sort(NewUserPrivileges) of
                                       OldUserPrivilegesSorted ->
                                           ok;
                                       NewUserPrivilegesSorted ->
                                           ok = gr_spaces:set_user_privileges({user, AccessToken}, SpaceId, UserId, [{<<"privileges">>, NewUserPrivilegesSorted}])
                                   end
                               end, NewUsersPrivileges),
                               opn_gui_utils:message(<<"ok_message">>, <<"Users privileges saved successfully.">>),
                               State
                           catch
                               _:Reason ->
                                   ?error("Cannot save users privileges: ~p", [Reason]),
                                   opn_gui_utils:message(<<"error_message">>, <<"Cannot save users privileges.<br>Please try again later.">>),
                                   State
                           end;

                       save_groups_privileges ->
                           try
                               lists:foreach(fun({_, GroupId, NewGroupPrivileges}) ->
                                   {_, _, OldGroupPrivileges} = lists:keyfind(GroupId, 2, State#?STATE.groups_privileges),
                                   OldGroupPrivilegesSorted = lists:sort(OldGroupPrivileges),
                                   case lists:sort(NewGroupPrivileges) of
                                       OldGroupPrivilegesSorted ->
                                           ok;
                                       NewGroupPrivilegesSorted ->
                                           ok = gr_spaces:set_group_privileges({group, AccessToken}, SpaceId, GroupId, [{<<"privileges">>, NewGroupPrivilegesSorted}])
                                   end
                               end, NewGroupsPrivileges),
                               opn_gui_utils:message(<<"ok_message">>, <<"Groups privileges saved successfully.">>),
                               State
                           catch
                               _:Reason ->
                                   ?error("Cannot save groups privileges: ~p", [Reason]),
                                   opn_gui_utils:message(<<"error_message">>, <<"Cannot save groups privileges.<br>Please try again later.">>),
                                   State
                           end
                   end
               catch Type:Message ->
                   ?error_stacktrace("Comet process exception: ~p:~p", [Type, Message]),
                   opn_gui_utils:message(<<"error_message">>, <<"There has been an error in comet process. Please refresh the page.">>),
                   {error, Message}
               end,
    gui_jq:wire(<<"$('#main_spinner').delay(300).hide(0);">>, false),
    gui_comet:flush(),
    ?MODULE:comet_loop(NewState).


%% event/1
%% ====================================================================
%% @doc Handles page events.
-spec event(Event :: term()) -> no_return().
%% ====================================================================
event(init) ->
    try
        SpaceId = gui_str:to_binary(gui_ctx:url_param(<<"id">>)),
        GRUID = utils:ensure_binary(opn_gui_utils:get_global_user_id()),
        AccessToken = opn_gui_utils:get_access_token(),

        {ok, UsersIds} = gr_spaces:get_users({user, AccessToken}, SpaceId),
        UsersPrivileges = lists:map(fun(UserId) ->
            {ok, #user_details{id = Id, name = Name}} = gr_spaces:get_user_details({user, AccessToken}, SpaceId, UserId),
            {ok, Privileges} = gr_spaces:get_user_privileges({user, AccessToken}, SpaceId, UserId),
            {Name, Id, Privileges}
        end, UsersIds),

        {ok, GroupsIds} = gr_spaces:get_groups({user, AccessToken}, SpaceId),
        GroupsPrivileges = lists:map(fun(GroupId) ->
            {ok, #user_details{id = Id, name = Name}} = gr_spaces:get_group_details({user, AccessToken}, SpaceId, GroupId),
            {ok, Privileges} = gr_spaces:get_group_privileges({user, AccessToken}, SpaceId, GroupId),
            {Name, Id, Privileges}
        end, GroupsIds),

        {ok, Pid} = gui_comet:spawn(fun() ->
            comet_loop(#?STATE{space_id = SpaceId, users_privileges = UsersPrivileges, new_users_privileges = UsersPrivileges,
                groups_privileges = GroupsPrivileges, new_groups_privileges = GroupsPrivileges, access_token = AccessToken})
        end),
        put(?COMET_PID, Pid),
        Pid ! render_tables
    catch
        _:Reason ->
            ?error("Cannot initialize page ~p: ~p", [?MODULE, Reason]),
            gui_jq:hide(<<"main_spinner">>),
            opn_gui_utils:message(<<"error_message">>, <<"Cannot fetch Space details.<br>Please try again later.">>)
    end;

event({message, Message}) ->
    get(?COMET_PID) ! Message,
    gui_jq:show(<<"main_spinner">>);

event({close_message, MessageId}) ->
    gui_jq:hide(MessageId);

event(terminate) ->
    ok.
