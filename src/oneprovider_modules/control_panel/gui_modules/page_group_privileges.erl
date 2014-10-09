% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page allows user to manage Group privileges.
%% @end
%% ===================================================================

-module(page_group_privileges).
-include("oneprovider_modules/control_panel/common.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/global_registry/gr_groups.hrl").
-include_lib("ctool/include/global_registry/gr_groups.hrl").
-include_lib("ctool/include/logging.hrl").

%% n2o API and comet
-export([main/0, event/1, comet_loop/1]).

%% Common page CCS styles
-define(CONTENT_COLUMN_STYLE, <<"padding-right: 0">>).
-define(NAVIGATION_COLUMN_STYLE, <<"border-left-width: 0; width: 20px; padding-left: 0;">>).
-define(DETAIL_STYLE, <<"font-size: large; font-weight: normal; vertical-align: middle;">>).

%% Columns names and associated privileges names
-define(COLUMNS_NAMES, [<<"View group">>, <<"Modify group">>, <<"Remove group">>, <<"Invite user">>, <<"Remove user">>,
    <<"Create Space">>, <<"Join Space">>, <<"Leave Space">>, <<"Invite provider">>, <<"Set privileges">>]).
-define(PRIVILEGES_NAMES, [<<"group_view_data">>, <<"group_change_data">>, <<"group_remove">>, <<"group_invite_user">>,
    <<"group_remove_user">>, <<"group_create_space">>, <<"group_join_space">>, <<"group_leave_space">>,
    <<"group_create_space_token">>, <<"group_set_privileges">>]).

%% Comet process pid
-define(COMET_PID, comet_pid).

%% Comet process state
-define(STATE, comet_state).
-record(?STATE, {group_id, users_privileges, new_users_privileges, access_token}).

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
                    page_error:redirect_with_error(?error_group_not_found),
                    #dtl{file = "bare", app = ?APP_Name, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
                Id ->
                    GroupId = gui_str:to_binary(Id),
                    case gr_groups:get_details({user, opn_gui_utils:get_access_token()}, GroupId) of
                        {ok, GroupDetails} ->
                            #dtl{file = "bare", app = ?APP_Name, bindings = [{title, title()}, {body, body(GroupDetails)}, {custom, <<"">>}]};
                        _ ->
                            page_error:redirect_with_error(?error_group_permission_denied),
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
    <<"Group privileges">>.


%% body/0
%% ====================================================================
%% @doc This will be placed instead of {{body}} tag in template.
%% @end
-spec body(GroupDetails :: #group_details{}) -> Result when
    Result :: #panel{}.
%% ====================================================================
body(GroupDetails) ->
    MessageStyle = <<"position: fixed; width: 100%; top: 55px; z-index: 1; display: none;">>,
    [
        #panel{
            id = <<"main_spinner">>,
            style = <<"position: absolute; top: 12px; left: 17px; z-index: 1234; width: 32px;">>,
            body = #image{
                image = <<"/images/spinner.gif">>
            }
        },
        opn_gui_utils:top_menu(groups_tab),
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
                    body = <<"Group privileges">>
                },
                group_details_table(GroupDetails) |
                lists:map(fun({TableId, Body, Panel}) ->
                    #panel{
                        body = [
                            #table{
                                id = TableId,
                                class = <<"table table-bordered">>,
                                style = <<"width: 50%; margin: 0 auto; margin-top: 3em;">>,
                                body = Body
                            },
                            Panel
                        ]
                    }
                end, [
                    {<<"users_table">>, privileges_table(<<"users_table">>, ?COLUMNS_NAMES, [], []),
                        save_button(<<"save_users_privileges_button">>, {message, save_users_privileges})}
                ])
            ]
        }
    ].


%% group_details_table/1
%% ====================================================================
%% @doc Renders the body of Group details table
%% @end
-spec group_details_table(GroupDetails :: #group_details{}) -> Result when
    Result :: #table{}.
%% ====================================================================
group_details_table(#group_details{id = GroupId, name = GroupName}) ->
    DescriptionStyle = <<"border-width: 0; text-align: right;">>,
    MainStyle = <<"border-width: 0;  text-align: left;">>,
    #table{
        style = <<"margin: 0 auto; width: 50%; margin-top: 30px; border-spacing: 1em; border-collapse: inherit;">>,
        body = lists:map(fun({Description, Main}) ->
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
                        style = MainStyle,
                        body = Main
                    }
                ]
            }
        end, [
            {<<"Group Name">>, #span{style = ?DETAIL_STYLE, body = GroupName}},
            {<<"Group ID">>, #span{style = ?DETAIL_STYLE, body = GroupId}}
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
            disabled = true,
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
                            postback = {message, {checkbox_toggled, Name, Id, Privilege}}
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
%% @doc Handles group management actions.
-spec comet_loop(State :: #?STATE{}) -> Result when
    Result :: {error, Reason :: term()}.
%% ====================================================================
comet_loop({error, Reason}) ->
    {error, Reason};

comet_loop(#?STATE{group_id = GroupId, new_users_privileges = NewUsersPrivileges, access_token = AccessToken} = State) ->
    NewCometLoopState =
        try
            receive
                render_tables ->
                    UsersColumnsNames = [<<"User">> | ?COLUMNS_NAMES],
                    gui_jq:update(<<"users_table">>, privileges_table(<<"users_privileges">>, UsersColumnsNames, ?PRIVILEGES_NAMES, NewUsersPrivileges)),
                    case NewUsersPrivileges of
                        [] -> ok;
                        _ -> gui_jq:prop(<<"save_users_privileges_button">>, <<"disabled">>, <<"">>)
                    end,
                    State;

                {checkbox_toggled, Name, Id, Privilege} ->
                    {_, _, Privileges} = lists:keyfind(Id, 2, NewUsersPrivileges),
                    case lists:member(Privilege, Privileges) of
                        true ->
                            State#?STATE{new_users_privileges = [{Name, Id, lists:delete(Privilege, Privileges)} | lists:keydelete(Id, 2, NewUsersPrivileges)]};
                        _ ->
                            State#?STATE{new_users_privileges = [{Name, Id, [Privilege | Privileges]} | lists:keydelete(Id, 2, NewUsersPrivileges)]}
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
                                    ok = gr_groups:set_user_privileges({user, AccessToken}, GroupId, UserId, [{<<"privileges">>, NewUserPrivilegesSorted}])
                            end
                        end, NewUsersPrivileges),
                        opn_gui_utils:message(<<"ok_message">>, <<"Users privileges saved successfully.">>),
                        State
                    catch
                        _:Reason ->
                            ?error("Cannot save users privileges: ~p", [Reason]),
                            opn_gui_utils:message(<<"error_message">>, <<"Cannot save users privileges.<br>Please try again later.">>),
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
    ?MODULE:comet_loop(NewCometLoopState).


%% event/1
%% ====================================================================
%% @doc Handles page events.
-spec event(Event :: term()) -> no_return().
%% ====================================================================
event(init) ->
    try
        GroupId = gui_str:to_binary(gui_ctx:url_param(<<"id">>)),
        GRUID = utils:ensure_binary(opn_gui_utils:get_global_user_id()),
        AccessToken = opn_gui_utils:get_access_token(),

        {ok, UsersIds} = gr_groups:get_users({user, AccessToken}, GroupId),
        UsersPrivileges = lists:map(fun(UserId) ->
            {ok, #user_details{id = Id, name = Name}} = gr_groups:get_user_details({user, AccessToken}, GroupId, UserId),
            {ok, Privileges} = gr_groups:get_user_privileges({user, AccessToken}, GroupId, UserId),
            {Name, Id, Privileges}
        end, UsersIds),

        {ok, Pid} = gui_comet:spawn(fun() ->
            comet_loop(#?STATE{group_id = GroupId, users_privileges = UsersPrivileges,
                new_users_privileges = UsersPrivileges, access_token = AccessToken})
        end),
        put(?COMET_PID, Pid),
        Pid ! render_tables
    catch
        _:Reason ->
            ?error("Cannot initialize page ~p: ~p", [?MODULE, Reason]),
            gui_jq:hide(<<"main_spinner">>),
            opn_gui_utils:message(<<"error_message">>, <<"Cannot fetch group privileges.<br>Please try again later.">>)
    end;

event({message, Message}) ->
    get(?COMET_PID) ! Message,
    gui_jq:show(<<"main_spinner">>);

event({close_message, MessageId}) ->
    gui_jq:hide(MessageId);

event(terminate) ->
    ok.
