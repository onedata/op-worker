% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page allows user to manage his Group.
%% @end
%% ===================================================================

-module(page_group).
-include("oneprovider_modules/control_panel/common.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/global_registry/gr_groups.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("ctool/include/logging.hrl").

%% n2o API and comet
-export([main/0, event/1, api_event/3, comet_loop/1]).

%% Common page CCS styles
-define(CONTENT_COLUMN_STYLE, <<"padding-right: 0">>).
-define(NAVIGATION_COLUMN_STYLE, <<"border-left-width: 0; width: 20px; padding-left: 0;">>).
-define(DETAIL_STYLE, <<"font-size: large; font-weight: normal; vertical-align: middle;">>).

%% Comet process pid
-define(COMET_PID, comet_pid).

%% Comet process state
-define(STATE, comet_state).
-record(?STATE, {counter = 1, group_id, spaces_details, users_details, gruid, access_token}).

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
    <<"Group details">>.


%% body/0
%% ====================================================================
%% @doc This will be placed instead of {{body}} tag in template.
%% @end
-spec body(GroupDetails :: #group_details{}) -> Result when
    Result :: #panel{}.
%% ====================================================================
body(#group_details{id = GroupId, name = GroupName} = GroupDetails) ->
    #panel{class = <<"page-container">>, body = [
        #panel{
            id = <<"main_spinner">>,
            style = <<"position: absolute; top: 12px; left: 17px; z-index: 1234; width: 32px;">>,
            body = #image{
                image = <<"/images/spinner.gif">>
            }
        },
        opn_gui_utils:top_menu(groups_tab, opn_gui_utils:breadcrumbs([
            {<<"Groups">>, <<"/groups">>}, {GroupName, <<"/group?id=", GroupId/binary>>}
        ])),
        #panel{
            style = <<"margin-top: 103px; padding: 1px; margin-bottom: 30px;">>,
            body = [
                #panel{
                    id = <<"message">>,
                    style = <<"width: 100%; padding: 0.5em 0; margin: 0 auto; border: 0; display: none;">>,
                    class = <<"dialog">>
                },
                #h6{
                    style = <<"font-size: x-large; margin: 0 auto; margin-top: 30px; text-align: center;">>,
                    body = <<"Manage group">>
                },
                group_details_table(GroupDetails) |
                lists:map(fun({TableId, Body, Panel}) ->
                    #panel{
                        body = [
                            #table{
                                class = <<"table table-bordered table-striped">>,
                                style = <<"width: 50%; margin: 0 auto; margin-top: 3em; table-layout: fixed;">>,
                                body = #tbody{
                                    id = TableId,
                                    body = Body
                                }
                            },
                            Panel
                        ]
                    }
                end, [
                    {<<"spaces_table">>, spaces_table_collapsed([]), spaces_panel()},
                    {<<"users_table">>, users_table_collapsed([]), users_panel()}
                ])
            ]
        }
    ]}.


%% group_details_table/1
%% ====================================================================
%% @doc Renders the body of Group details table
%% @end
-spec group_details_table(GroupDetails :: #group_details{}) -> Result when
    Result :: #table{}.
%% ====================================================================
group_details_table(#group_details{id = GroupId} = GroupDetails) ->
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
            {<<"Group Name">>, <<"group_name">>, group_name(GroupDetails)},
            {<<"Group ID">>, <<"">>, #span{style = ?DETAIL_STYLE, body = GroupId}}
        ])
    }.


%% group_name/1
%% ====================================================================
%% @doc Renders editable Group name.
-spec group_name(GroupDetails :: #group_details{}) -> Result when
    Result :: #span{}.
%% ====================================================================
group_name(#group_details{name = GroupName} = GroupDetails) ->
    #span{
        style = <<"font-size: large;">>,
        body = [
            gui_str:html_encode(GroupName),
            #link{
                id = <<"change_group_name_span">>,
                title = <<"Edit">>,
                style = <<"margin-left: 1em; display: none;">>,
                class = <<"glyph-link">>,
                postback = {change_group_name, GroupDetails},
                body = #span{
                    class = <<"icomoon-pencil2">>
                }
            }
        ]
    }.


%% change_group_name/2
%% ====================================================================
%% @doc Renders textbox used to change Group name.
-spec change_group_name(GroupDetails :: #group_details{}) -> Result when
    Result :: list().
%% ====================================================================
change_group_name(GroupDetails) ->
    [
        #textbox{
            id = <<"new_group_name_textbox">>,
            style = <<"margin: 0 auto; padding: 1px;">>,
            class = <<"span">>,
            placeholder = <<"New Group name">>
        },
        #link{
            id = <<"new_group_name_submit">>,
            style = <<"margin-left: 10px;">>,
            class = <<"glyph-link">>,
            title = <<"Submit">>,
            actions = gui_jq:form_submit_action(<<"new_group_name_submit">>, {submit_new_group_name, GroupDetails}, <<"new_group_name_textbox">>),
            body = #span{
                class = <<"fui-check-inverted">>,
                style = <<"font-size: large; vertical-align: middle;">>
            }
        },
        #link{
            style = <<"margin-left: 10px;">>,
            class = <<"glyph-link">>,
            title = <<"Cancel">>,
            postback = {cancel_new_group_name_submit, GroupDetails},
            body = #span{
                class = <<"fui-cross-inverted">>,
                style = <<"font-size: large; vertical-align: middle;">>
            }
        }
    ].


%% spaces_panel/0
%% ====================================================================
%% @doc Renders spaces management panel.
-spec spaces_panel() -> Result when
    Result :: #panel{}.
%% ====================================================================
spaces_panel() ->
    #panel{
        style = <<"margin: 0 auto; width: 50%; margin-top: 30px; text-align: center;">>,
        body = lists:map(fun({ButtonId, ButtonPostback, ButtonBody}) ->
            #button{
                id = ButtonId,
                postback = ButtonPostback,
                disabled = true,
                class = <<"btn btn-inverse btn-small">>,
                style = <<"margin-right: 0.5em; margin-left: 0.5em;">>,
                body = ButtonBody
            }
        end, [
            {<<"create_space_button">>, create_space, <<"Create Space">>},
            {<<"join_space_button">>, join_space, <<"Join Space">>},
            {<<"request_space_creation_button">>, {message, request_space_creation}, <<"Request Space creation">>}
        ])
    }.


%% users_panel/0
%% ====================================================================
%% @doc Renders users management panel.
-spec users_panel() -> Result when
    Result :: #panel{}.
%% ====================================================================
users_panel() ->
    #panel{
        style = <<"margin: 0 auto; width: 50%; margin-top: 30px; text-align: center;">>,
        body = #button{
            id = <<"invite_user_button">>,
            disabled = true,
            postback = {message, invite_user},
            class = <<"btn btn-inverse btn-small">>,
            body = <<"Invite user">>
        }
    }.


%% spaces_table_collapsed/1
%% ====================================================================
%% @doc Renders collapsed spaces details table.
%% @end
-spec spaces_table_collapsed(SpacesDetails :: [{Id :: binary(), Privileges :: [binary()], SpaceDetails :: #space_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
spaces_table_collapsed(SpacesDetails) ->
    TableName = <<"Spaces">>,
    NavigationBody = opn_gui_utils:expand_button(<<"Expand All">>, {message, expand_spaces_table}),
    RenderRowFunction = fun space_row_collapsed/3,
    table(SpacesDetails, TableName, NavigationBody, RenderRowFunction).


%% spaces_table_expanded/1
%% ====================================================================
%% @doc Renders expanded spaces details table.
%% @end
-spec spaces_table_expanded(SpacesDetails :: [{Id :: binary(), Privileges :: [binary()], SpaceDetails :: #space_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
spaces_table_expanded(SpacesDetails) ->
    TableName = <<"Spaces">>,
    NavigationBody = opn_gui_utils:collapse_button(<<"Collapse All">>, {message, collapse_spaces_table}),
    RenderRowFunction = fun space_row_expanded/3,
    table(SpacesDetails, TableName, NavigationBody, RenderRowFunction).


%% users_table_collapsed/1
%% ====================================================================
%% @doc Renders collapsed users details table.
%% @end
-spec users_table_collapsed(UsersDetails :: [{Id :: binary(), Privileges :: [binary()], UserDetails :: #user_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
users_table_collapsed(UsersDetails) ->
    TableName = <<"Users">>,
    NavigationBody = opn_gui_utils:expand_button(<<"Expand All">>, {message, expand_users_table}),
    RenderRowFunction = fun user_row_collapsed/3,
    table(UsersDetails, TableName, NavigationBody, RenderRowFunction).


%% users_table_expanded/1
%% ====================================================================
%% @doc Renders expanded users details table.
%% @end
-spec users_table_expanded(UsersDetails :: [{Id :: binary(), Privileges :: [binary()], UserDetails :: #user_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
users_table_expanded(UsersDetails) ->
    TableName = <<"Users">>,
    NavigationBody = opn_gui_utils:collapse_button(<<"Collapse All">>, {message, collapse_users_table}),
    RenderRowFunction = fun user_row_expanded/3,
    table(UsersDetails, TableName, NavigationBody, RenderRowFunction).


%% table/4
%% ====================================================================
%% @doc Renders details table.
%% @end
-spec table(Details :: [{Id :: binary(), Details :: #user_details{} | #space_details{}}], TableName :: binary(),
    NavigationBody :: #link{}, RenderRowFunction :: function()) -> Result when
    Result :: [#tr{}].
%% ====================================================================
table(Details, TableName, NavigationBody, RenderRowFunction) ->
    Header = #tr{
        cells = [
            #th{
                style = <<"font-size: large;">>,
                body = TableName
            },
            #th{
                style = ?NAVIGATION_COLUMN_STYLE,
                body = NavigationBody
            }
        ]
    },

    Rows = lists:foldl(fun({RowId, Privileges, RowDetails}, RowsAcc) ->
        [#tr{
            id = RowId,
            cells = RenderRowFunction(RowId, Privileges, RowDetails)
        } | RowsAcc]
    end, [], Details),

    [Header | Rows].


%% space_row_collapsed/3
%% ====================================================================
%% @doc Renders collapsed space details row.
%% @end
-spec space_row_collapsed(RowId :: binary(), Privileges :: [binary()], SpaceDetails :: #space_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
space_row_collapsed(RowId, Privileges, #space_details{id = SpaceId, name = SpaceName} = SpaceDetails) ->
    NavigationBody = opn_gui_utils:expand_button({message, {expand_space_row, RowId, Privileges, SpaceDetails}}),
    row_collapsed(SpaceId, SpaceName, NavigationBody).


%% space_row_expanded/3
%% ====================================================================
%% @doc Renders expanded space details row.
%% @end
-spec space_row_expanded(RowId :: binary(), Privileges :: [binary()], SpaceDetails :: #space_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
space_row_expanded(RowId, Privileges, #space_details{id = SpaceId, name = SpaceName} = SpaceDetails) ->
    Details = [
        {<<"Name">>, detail(gui_str:html_encode(SpaceName), <<"Leave space">>, lists:member(<<"group_leave_space">>, Privileges), {leave_space, RowId, SpaceDetails}, <<"icomoon-exit">>)},
        {<<"Space ID">>, #span{style = ?DETAIL_STYLE, body = SpaceId}}
    ],
    NavigationBody = opn_gui_utils:collapse_button({message, {collapse_space_row, RowId, Privileges, SpaceDetails}}),
    row_expanded(Details, NavigationBody).


%% user_row_collapsed/3
%% ====================================================================
%% @doc Renders collapsed user details row.
%% @end
-spec user_row_collapsed(RowId :: binary(), Privileges :: [binary()], UserDetails :: #user_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
user_row_collapsed(RowId, Privileges, #user_details{id = UserId, name = UserName} = UserDetails) ->
    NavigationBody = opn_gui_utils:expand_button({message, {expand_user_row, RowId, Privileges, UserDetails}}),
    row_collapsed(UserId, UserName, NavigationBody).


%% user_row_expanded/3
%% ====================================================================
%% @doc Renders expanded user details row.
%% @end
-spec user_row_expanded(RowId :: binary(), Privileges :: [binary()], UserDetails :: #user_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
user_row_expanded(RowId, Privileges, #user_details{id = UserId, name = UserName} = UserDetails) ->
    Details = [
        {<<"Name">>, detail(gui_str:html_encode(UserName), <<"Remove user">>, lists:member(<<"group_remove_user">>, Privileges), {remove_user, RowId, UserDetails}, <<"icomoon-remove">>)},
        {<<"User ID">>, #span{style = ?DETAIL_STYLE, body = UserId}}
    ],
    NavigationBody = opn_gui_utils:collapse_button({message, {collapse_user_row, RowId, Privileges, UserDetails}}),
    row_expanded(Details, NavigationBody).


%% detail/5
%% ====================================================================
%% @doc Renders detail.
-spec detail(Content :: term(), Title :: binary(), Visible :: boolean(), Postback :: term(), Class :: binary()) -> Result when
    Result :: #span{}.
%% ====================================================================
detail(Content, Title, Visible, Postback, Class) ->
    Display = case Visible of
                  true -> <<"">>;
                  _ -> <<" display: none;">>
              end,
    #span{
        style = <<"font-size: large; font-weight: normal; vertical-align: middle;">>,
        body = [
            Content,
            #link{
                title = Title,
                style = <<"margin-left: 10px;", Display/binary>>,
                class = <<"glyph-link">>,
                postback = Postback,
                body = #span{
                    style = <<"vertical-align: middle;">>,
                    class = Class
                }
            }
        ]
    }.


%% row_collapsed/2
%% ====================================================================
%% @doc Renders collapsed details row.
%% @end
-spec row_collapsed(Id :: binary(), Name :: binary(), NavigationBody :: term()) -> Result when
    Result :: [#td{}].
%% ====================================================================
row_collapsed(Id, Name, NavigationBody) ->
    [
        #td{
            style = ?CONTENT_COLUMN_STYLE,
            body = #span{
                style = ?DETAIL_STYLE,
                body = <<"<b>", (gui_str:html_encode(Name))/binary, "</b> (", Id/binary, ")">>
            }
        },
        #td{
            style = ?NAVIGATION_COLUMN_STYLE,
            body = NavigationBody
        }
    ].


%% row_expanded/2
%% ====================================================================
%% @doc Renders expanded details row.
%% @end
-spec row_expanded([{DetailName :: binary(), DetailBody :: term()}], NavigationBody :: term()) -> Result when
    Result :: [#td{}].
%% ====================================================================
row_expanded(Details, NavigationBody) ->
    [
        #td{
            style = ?CONTENT_COLUMN_STYLE,
            body = #table{
                style = <<"border-width: 0; width: 100%; border-collapse: inherit;">>,
                body = lists:map(fun({DetailName, DetailBody}) ->
                    #tr{
                        cells = [
                            #td{
                                style = <<"border-width: 0; text-align: right; width: 10%; padding-left: 0; padding-right: 0;">>,
                                body = #label{
                                    style = <<"margin: 0 auto; cursor: auto;">>,
                                    class = <<"label label-large label-inverse">>,
                                    body = DetailName
                                }
                            },
                            #td{
                                style = <<"border-width: 0;  text-align: left; padding-left: 1em; width: 90%;">>,
                                body = DetailBody
                            }
                        ]
                    }
                end, Details)
            }
        },
        #td{
            style = ?NAVIGATION_COLUMN_STYLE,
            body = NavigationBody
        }
    ].


%% add_space_row/3
%% ====================================================================
%% @doc Adds collapsed Space settings row to Spaces settings table.
-spec add_space_row(RowId :: binary(), Privileges :: [binary()], SpaceDetails :: #space_details{}) -> Result when
    Result :: ok.
%% ====================================================================
add_space_row(RowId, Privileges, SpaceDetails) ->
    Row = #tr{
        id = RowId,
        cells = space_row_collapsed(RowId, Privileges, SpaceDetails)
    },
    gui_jq:insert_bottom(<<"spaces_table">>, Row).


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

comet_loop(#?STATE{counter = Counter, group_id = GroupId, spaces_details = SpacesDetails,
    users_details = UsersDetails, gruid = GRUID, access_token = AccessToken} = State) ->
    NewCometLoopState =
        try
            receive
                {render_tables, Privileges} ->
                    gui_jq:update(<<"spaces_table">>, spaces_table_collapsed(SpacesDetails)),
                    gui_jq:update(<<"users_table">>, users_table_collapsed(UsersDetails)),

                    lists:foreach(fun
                        ({false, _, _, _}) -> ok;
                        ({true, Module, Function, Args}) -> apply(Module, Function, Args)
                    end, [
                        {lists:member(<<"group_change_data">>, Privileges), gui_jq, show, [<<"change_group_name_span">>]},
                        {lists:member(<<"group_invite_user">>, Privileges), gui_jq, prop, [<<"invite_user_button">>, <<"disabled">>, <<"">>]},
                        {lists:member(<<"group_create_space">>, Privileges), gui_jq, prop, [<<"create_space_button">>, <<"disabled">>, <<"">>]},
                        {lists:member(<<"group_join_space">>, Privileges), gui_jq, prop, [<<"join_space_button">>, <<"disabled">>, <<"">>]},
                        {lists:member(<<"group_create_space_token">>, Privileges), gui_jq, prop, [<<"request_space_creation_button">>, <<"disabled">>, <<"">>]}
                    ]),

                    State;

                {create_space, Name} ->
                    NewState = try
                                   {ok, SpaceId} = gr_groups:create_space({user, AccessToken}, GroupId, [{<<"name">>, Name}]),
                                   {ok, SpaceDetails} = gr_groups:get_space_details({user, AccessToken}, GroupId, SpaceId),
                                   {ok, Privileges} = gr_groups:get_user_privileges({user, AccessToken}, GroupId, GRUID),
                                   opn_gui_utils:message(success, <<"Created Space ID: <b>", SpaceId/binary, "</b>">>),
                                   RowId = <<"space_", (integer_to_binary(Counter + 1))/binary>>,
                                   add_space_row(RowId, Privileges, SpaceDetails),
                                   State#?STATE{counter = Counter + 1, spaces_details = [{RowId, Privileges, SpaceDetails} | SpacesDetails]}
                               catch
                                   _:Other ->
                                       ?error("Cannot create Space ~p: ~p", [Name, Other]),
                                       opn_gui_utils:message(error, <<"Cannot create Space: <b>", (gui_str:html_encode(Name))/binary, "</b>.<br>Please try again later.">>),
                                       State
                               end,
                    gui_jq:prop(<<"create_space_button">>, <<"disabled">>, <<"">>),
                    NewState;

                {join_space, Token} ->
                    NewState = try
                                   {ok, SpaceId} = gr_groups:join_space({user, AccessToken}, GroupId, [{<<"token">>, Token}]),
                                   {ok, SpaceDetails} = gr_groups:get_space_details({user, AccessToken}, GroupId, SpaceId),
                                   {ok, Privileges} = gr_groups:get_user_privileges({user, AccessToken}, GroupId, GRUID),
                                   opn_gui_utils:message(success, <<"Joined Space ID: <b>", SpaceId/binary, "</b>">>),
                                   RowId = <<"space_", (integer_to_binary(Counter + 1))/binary>>,
                                   add_space_row(RowId, Privileges, SpaceDetails),
                                   State#?STATE{counter = Counter + 1, spaces_details = [{RowId, Privileges, SpaceDetails} | SpacesDetails]}
                               catch
                                   _:Other ->
                                       ?error("Cannot join Space using token ~p: ~p", [Token, Other]),
                                       opn_gui_utils:message(error, <<"Cannot join Space using token: <b>", (gui_str:html_encode(Token))/binary, "</b>.<br>Please try again later.">>),
                                       State
                               end,
                    gui_jq:prop(<<"join_space_button">>, <<"disabled">>, <<"">>),
                    NewState;

                request_space_creation ->
                    case gr_groups:get_create_space_token({user, AccessToken}, GroupId) of
                        {ok, Token} ->
                            Message = <<"Give the token below to a provider willing to create Space for your group.",
                            "<input id=\"create_space_token_textbox\" type=\"text\" style=\"margin-top: 1em;"
                            " width: 80%;\" value=\"", Token/binary, "\">">>,
                            gui_jq:info_popup(<<"Request Space creation">>, Message, <<"return true;">>, <<"btn-inverse">>),
                            gui_jq:wire(<<"box.on('shown',function(){ $(\"#create_space_token_textbox\").focus().select(); });">>);
                        Other ->
                            ?error("Cannot get support token for group with ID ~p: ~p", [GroupId, Other]),
                            opn_gui_utils:message(error, <<"Cannot get Space creation token for group with ID: <b>", GroupId, "</b>."
                            "<br>Please try again later.">>)
                    end,
                    State;

                invite_user ->
                    case gr_groups:get_invite_user_token({user, AccessToken}, GroupId) of
                        {ok, Token} ->
                            Message = <<"Give the token below to a user willing to join your group.",
                            "<input id=\"join_token_textbox\" type=\"text\" style=\"margin-top: 1em;"
                            " width: 80%;\" value=\"", Token/binary, "\">">>,
                            gui_jq:info_popup(<<"Invite user">>, Message, <<"return true;">>, <<"btn-inverse">>),
                            gui_jq:wire(<<"box.on('shown',function(){ $(\"#join_token_textbox\").focus().select(); });">>);
                        Other ->
                            ?error("Cannot get user invitation token for group with ID ~p: ~p", [GroupId, Other]),
                            opn_gui_utils:message(error, <<"Cannot get invitation token for Group with ID: <b>", GroupId, "</b>."
                            "<br>Please try again later.">>)
                    end,
                    State;

                {change_group_name, #group_details{id = GroupId} = GroupDetails, NewGroupName} ->
                    case gr_groups:modify_details({user, AccessToken}, GroupId, [{<<"name">>, NewGroupName}]) of
                        ok ->
                            gui_jq:update(<<"group_name">>, group_name(GroupDetails#group_details{name = NewGroupName}));
                        Other ->
                            ?error("Cannot change name of group ~p: ~p", [GroupDetails, Other]),
                            opn_gui_utils:message(error, <<"Cannot change name of group with ID:  <b>", GroupId, "</b>."
                            "<br>Please try again later.">>),
                            gui_jq:update(<<"group_name">>, group_name(GroupDetails))
                    end,
                    gui_jq:show(<<"change_group_name_span">>),
                    State;

                {leave_space, RowId, SpaceId} ->
                    case gr_groups:leave_space({user, AccessToken}, GroupId, SpaceId) of
                        ok ->
                            opn_gui_utils:message(success, <<"Space with ID: <b>", GroupId/binary, "</b> left successfully.">>),
                            gui_jq:remove(RowId),
                            State#?STATE{spaces_details = lists:keydelete(RowId, 1, SpacesDetails)};
                        Other ->
                            ?error("Cannot leave Space with ID ~p: ~p", [SpaceId, Other]),
                            opn_gui_utils:message(error, <<"Cannot leave Space with ID: <b>", SpaceId/binary, "</b>.<br>Please try again later.">>),
                            State
                    end;

                {remove_user, RowId, UserId} ->
                    case gr_groups:remove_user({user, AccessToken}, GroupId, UserId) of
                        ok ->
                            opn_gui_utils:message(success, <<"User with ID: <b>", GroupId/binary, "</b> removed successfully.">>),
                            gui_jq:remove(RowId),
                            State#?STATE{users_details = lists:keydelete(RowId, 1, UsersDetails)};
                        Other ->
                            ?error("Cannot remove user with ID ~p: ~p", [UserId, Other]),
                            opn_gui_utils:message(error, <<"Cannot remove user with ID: <b>", UserId/binary, "</b>.<br>Please try again later.">>),
                            State
                    end;

                Event ->
                    case Event of
                        collapse_spaces_table ->
                            gui_jq:update(<<"spaces_table">>, spaces_table_collapsed(SpacesDetails));
                        expand_spaces_table ->
                            gui_jq:update(<<"spaces_table">>, spaces_table_expanded(SpacesDetails));
                        {collapse_space_row, RowId, Privileges, SpaceDetails} ->
                            gui_jq:update(RowId, space_row_collapsed(RowId, Privileges, SpaceDetails));
                        {expand_space_row, RowId, Privileges, SpaceDetails} ->
                            gui_jq:update(RowId, space_row_expanded(RowId, Privileges, SpaceDetails));
                        collapse_users_table ->
                            gui_jq:update(<<"users_table">>, users_table_collapsed(UsersDetails));
                        expand_users_table ->
                            gui_jq:update(<<"users_table">>, users_table_expanded(UsersDetails));
                        {collapse_user_row, RowId, Privileges, UserDetails} ->
                            gui_jq:update(RowId, user_row_collapsed(RowId, Privileges, UserDetails));
                        {expand_user_row, RowId, Privileges, UserDetails} ->
                            gui_jq:update(RowId, user_row_expanded(RowId, Privileges, UserDetails));
                        _ ->
                            ok
                    end,
                    State
            end
        catch Type:Reason ->
            ?error_stacktrace("Comet process exception: ~p:~p", [Type, Reason]),
            opn_gui_utils:message(error, <<"There has been an error in comet process. Please refresh the page.">>),
            {error, Reason}
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
        {ok, Privileges} = gr_groups:get_user_privileges({user, AccessToken}, GroupId, GRUID),

        GetDetailsFun = fun(Ids, Function, RowPrefix) ->
            lists:foldl(fun(Id, {Rows, It}) ->
                {ok, Details} = gr_groups:Function({user, AccessToken}, GroupId, Id),
                {
                    [{<<RowPrefix/binary, (integer_to_binary(It + 1))/binary>>, Privileges, Details} | Rows],
                    It + 1
                }
            end, {[], 0}, Ids)
        end,

        {ok, SpacesIds} = gr_groups:get_spaces({user, AccessToken}, GroupId),
        {SpacesDetails, Counter} = GetDetailsFun(SpacesIds, get_space_details, <<"space_">>),
        {ok, UsersIds} = gr_groups:get_users({user, AccessToken}, GroupId),
        {UsersDetails, _} = GetDetailsFun(UsersIds, get_user_details, <<"user_">>),

        gui_jq:bind_key_to_click_on_class(<<"13">>, <<"confirm">>),

        gui_jq:wire(#api{name = "create_space", tag = "create_space"}, false),
        gui_jq:wire(#api{name = "join_space", tag = "join_space"}, false),
        gui_jq:wire(#api{name = "remove_user", tag = "remove_user"}, false),
        gui_jq:wire(#api{name = "leave_space", tag = "leave_space"}, false),

        {ok, Pid} = gui_comet:spawn(fun() ->
            comet_loop(#?STATE{counter = Counter, group_id = GroupId, spaces_details = SpacesDetails,
                users_details = UsersDetails, gruid = GRUID, access_token = AccessToken})
        end),
        put(?COMET_PID, Pid),
        Pid ! {render_tables, Privileges}
    catch
        _:Reason ->
            ?error("Cannot initialize page ~p: ~p", [?MODULE, Reason]),
            gui_jq:hide(<<"main_spinner">>),
            opn_gui_utils:message(error, <<"Cannot fetch Group details.<br>Please try again later.">>)
    end;

event({change_group_name, GroupDetails}) ->
    gui_jq:update(<<"group_name">>, change_group_name(GroupDetails)),
    gui_jq:bind_enter_to_submit_button(<<"new_group_name_textbox">>, <<"new_group_name_submit">>),
    gui_jq:focus(<<"new_group_name_textbox">>);

event({submit_new_group_name, GroupDetails}) ->
    NewGroupName = gui_ctx:postback_param(<<"new_group_name_textbox">>),
    get(?COMET_PID) ! {change_group_name, GroupDetails, NewGroupName},
    gui_jq:show(<<"main_spinner">>);

event({cancel_new_group_name_submit, GroupDetails}) ->
    gui_jq:update(<<"group_name">>, group_name(GroupDetails)),
    gui_jq:show(<<"change_group_name_span">>);

event(create_space) ->
    Message = <<"<div style=\"margin: 0 auto; width: 80%;\">",
    "<p id=\"create_space_alert\" style=\"width: 100%; color: red; font-size: medium; text-align: center; display: none;\"></p>",
    "<input id=\"create_space_name\" type=\"text\" style=\"width: 100%;\" placeholder=\"Name\">",
    "</div>">>,
    Script = <<"var alert = $(\"#create_space_alert\");",
    "var name = $.trim($(\"#create_space_name\").val());",
    "if(name.length == 0) { alert.html(\"Please provide Space name.\"); alert.fadeIn(300); return false; }",
    "else { create_space([name]); return true; }">>,
    gui_jq:dialog_popup(<<"Create Space">>, Message, Script, <<"btn-inverse">>),
    gui_jq:wire(<<"box.on('shown',function(){ $(\"#create_space_name\").focus(); });">>);

event(join_space) ->
    Message = <<"<div style=\"margin: 0 auto; width: 80%;\">",
    "<p id=\"join_space_alert\" style=\"width: 100%; color: red; font-size: medium; text-align: center; display: none;\"></p>",
    "<input id=\"join_space_token\" type=\"text\" style=\"width: 100%;\" placeholder=\"Token\">",
    "</div>">>,
    Script = <<"var alert = $(\"#join_space_alert\");",
    "var token = $.trim($(\"#join_space_token\").val());",
    "if(token.length == 0) { alert.html(\"Please provide Space token.\"); alert.fadeIn(300); return false; }",
    "else { join_space([token]); return true; }">>,
    gui_jq:dialog_popup(<<"Join Space">>, Message, Script, <<"btn-inverse">>),
    gui_jq:wire(<<"box.on('shown',function(){ $(\"#join_space_token\").focus(); });">>);

event({leave_space, RowId, #space_details{id = SpaceId, name = SpaceName}}) ->
    Message = <<"Are you sure you want to leave space:<br><b>", (gui_str:html_encode(SpaceName))/binary, " (", SpaceId/binary, ") </b>?">>,
    Script = <<"leave_space(['", RowId/binary, "','", SpaceId/binary, "']);">>,
    gui_jq:dialog_popup(<<"Leave space">>, Message, Script, <<"btn-inverse">>);

event({remove_user, RowId, #user_details{id = UserId, name = UserName}}) ->
    Message = <<"Are you sure you want to remove user:<br><b>", (gui_str:html_encode(UserName))/binary, " (", UserId/binary, ") </b>?">>,
    Script = <<"remove_user(['", RowId/binary, "','", UserId/binary, "']);">>,
    gui_jq:dialog_popup(<<"Remove user">>, Message, Script, <<"btn-inverse">>);

event({message, Message}) ->
    get(?COMET_PID) ! Message,
    gui_jq:show(<<"main_spinner">>);

event({close_message, MessageId}) ->
    gui_jq:hide(MessageId);

event(terminate) ->
    ok.

%% api_event/3
%% ====================================================================
%% @doc Handles page events.
-spec api_event(Name :: string(), Args :: string(), Req :: string()) -> no_return().
%% ====================================================================
api_event("create_space", Args, _) ->
    [Name] = mochijson2:decode(Args),
    get(?COMET_PID) ! {create_space, Name},
    gui_jq:prop(<<"create_space_button">>, <<"disabled">>, <<"disabled">>),
    gui_jq:show(<<"main_spinner">>);

api_event("join_space", Args, _) ->
    [Token] = mochijson2:decode(Args),
    get(?COMET_PID) ! {join_space, Token},
    gui_jq:prop(<<"join_space_button">>, <<"disabled">>, <<"disabled">>),
    gui_jq:show(<<"main_spinner">>);

api_event(Function, Args, _) ->
    [RowId, ObjectId] = mochijson2:decode(Args),
    get(?COMET_PID) ! {list_to_existing_atom(Function), RowId, ObjectId},
    gui_jq:show(<<"main_spinner">>).