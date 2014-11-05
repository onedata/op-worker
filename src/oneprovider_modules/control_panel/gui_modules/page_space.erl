% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page allows user to manage his Space.
%% @end
%% ===================================================================

-module(page_space).
-include("oneprovider_modules/control_panel/common.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("ctool/include/global_registry/gr_groups.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").
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
-record(?STATE, {space_id, providers_details, users_details, groups_details, gruid, access_token}).

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
    <<"Space details">>.


%% body/0
%% ====================================================================
%% @doc This will be placed instead of {{body}} tag in template.
%% @end
-spec body(SpaceDetails :: #space_details{}) -> Result when
    Result :: #panel{}.
%% ====================================================================
body(#space_details{id = SpaceId, name = SpaceName} = SpaceDetails) ->
    MessageStyle = <<"position: fixed; width: 100%; top: 55px; z-index: 1; display: none;">>,
    #panel{class = <<"page-container">>, body = [
        #panel{
            id = <<"main_spinner">>,
            style = <<"position: absolute; top: 12px; left: 17px; z-index: 1234; width: 32px;">>,
            body = #image{
                image = <<"/images/spinner.gif">>
            }
        },
        opn_gui_utils:top_menu(spaces_tab, opn_gui_utils:breadcrumbs([{<<"Spaces">>, <<"/spaces">>}, {SpaceName, <<"/space?id=", SpaceId/binary>>}])),
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
                    body = <<"Manage Space">>
                },
                space_details_table(SpaceDetails) |
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
                    {<<"providers_table">>, providers_table_collapsed([]), providers_panel()},
                    {<<"users_table">>, users_table_collapsed([]), users_panel()},
                    {<<"groups_table">>, groups_table_collapsed([]), groups_panel()}
                ])
            ]
        }
    ]}.


%% space_details_table/1
%% ====================================================================
%% @doc Renders the body of Space details table
%% @end
-spec space_details_table(SpaceDetails :: #space_details{}) -> Result when
    Result :: #table{}.
%% ====================================================================
space_details_table(#space_details{id = SpaceId} = SpaceDetails) ->
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
            {<<"Space Name">>, <<"space_name">>, space_name(SpaceDetails)},
            {<<"Space ID">>, <<"">>, #span{style = ?DETAIL_STYLE, body = SpaceId}}
        ])
    }.


%% space_name/1
%% ====================================================================
%% @doc Renders editable Space name.
-spec space_name(SpaceDetails :: #space_details{}) -> Result when
    Result :: #span{}.
%% ====================================================================
space_name(#space_details{name = SpaceName} = SpaceDetails) ->
    #span{
        style = <<"font-size: large;">>,
        body = [
            gui_str:html_encode(SpaceName),
            #link{
                id = <<"change_space_name_span">>,
                title = <<"Edit">>,
                style = <<"margin-left: 1em; display: none;">>,
                class = <<"glyph-link">>,
                postback = {change_space_name, SpaceDetails},
                body = #span{
                    class = <<"icomoon-pencil2">>
                }
            }
        ]
    }.


%% change_space_name/2
%% ====================================================================
%% @doc Renders textbox used to change Space name.
-spec change_space_name(SpaceDetails :: #space_details{}) -> Result when
    Result :: list().
%% ====================================================================
change_space_name(SpaceDetails) ->
    [
        #textbox{
            id = <<"new_space_name_textbox">>,
            style = <<"margin: 0 auto; padding: 1px;">>,
            class = <<"span">>,
            placeholder = <<"New Space name">>
        },
        #link{
            id = <<"new_space_name_submit">>,
            style = <<"margin-left: 10px;">>,
            class = <<"glyph-link">>,
            title = <<"Submit">>,
            actions = gui_jq:form_submit_action(<<"new_space_name_submit">>, {submit_new_space_name, SpaceDetails}, <<"new_space_name_textbox">>),
            body = #span{
                class = <<"fui-check-inverted">>,
                style = <<"font-size: large; vertical-align: middle;">>
            }
        },
        #link{
            style = <<"margin-left: 10px;">>,
            class = <<"glyph-link">>,
            title = <<"Cancel">>,
            postback = {cancel_new_space_name_submit, SpaceDetails},
            body = #span{
                class = <<"fui-cross-inverted">>,
                style = <<"font-size: large; vertical-align: middle;">>
            }
        }
    ].


%% providers_panel/0
%% ====================================================================
%% @doc Renders providers management panel.
-spec providers_panel() -> Result when
    Result :: #panel{}.
%% ====================================================================
providers_panel() ->
    #panel{
        style = <<"margin: 0 auto; width: 50%; margin-top: 30px; text-align: center;">>,
        body = #button{
            id = <<"request_support_button">>,
            disabled = true,
            postback = {message, request_support},
            class = <<"btn btn-inverse btn-small">>,
            body = <<"Request support">>
        }
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


%% groups_panel/0
%% ====================================================================
%% @doc Renders groups management panel.
-spec groups_panel() -> Result when
    Result :: #panel{}.
%% ====================================================================
groups_panel() ->
    #panel{
        style = <<"margin: 0 auto; width: 50%; margin-top: 30px; text-align: center;">>,
        body = #button{
            id = <<"invite_group_button">>,
            disabled = true,
            postback = {message, invite_group},
            class = <<"btn btn-inverse btn-small">>,
            body = <<"Invite group">>
        }
    }.

%% providers_table_collapsed/1
%% ====================================================================
%% @doc Renders collapsed providers details table.
%% @end
-spec providers_table_collapsed(ProvidersDetails :: [{Id :: binary(), Privileges :: [binary()], ProviderDetails :: #provider_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
providers_table_collapsed(ProvidersDetails) ->
    TableName = <<"Providers">>,
    NavigationBody = opn_gui_utils:expand_button(<<"Expand All">>, {message, expand_providers_table}),
    RenderRowFunction = fun provider_row_collapsed/3,
    table(ProvidersDetails, TableName, NavigationBody, RenderRowFunction).


%% providers_table_expanded/1
%% ====================================================================
%% @doc Renders expanded providers details table.
%% @end
-spec providers_table_expanded(ProvidersDetails :: [{Id :: binary(), Privileges :: [binary()], ProviderDetails :: #provider_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
providers_table_expanded(ProvidersDetails) ->
    TableName = <<"Providers">>,
    NavigationBody = opn_gui_utils:collapse_button(<<"Collapse All">>, {message, collapse_providers_table}),
    RenderRowFunction = fun provider_row_expanded/3,
    table(ProvidersDetails, TableName, NavigationBody, RenderRowFunction).


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


%% groups_table_collapsed/1
%% ====================================================================
%% @doc Renders collapsed groups details table.
%% @end
-spec groups_table_collapsed(GroupsDetails :: [{Id :: binary(), Privileges :: [binary()], GroupDetails :: #group_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
groups_table_collapsed(GroupsDetails) ->
    TableName = <<"Groups">>,
    NavigationBody = opn_gui_utils:expand_button(<<"Expand All">>, {message, expand_groups_table}),
    RenderRowFunction = fun group_row_collapsed/3,
    table(GroupsDetails, TableName, NavigationBody, RenderRowFunction).


%% groups_table_expanded/1
%% ====================================================================
%% @doc Renders expanded groups details table.
%% @end
-spec groups_table_expanded(GroupsDetails :: [{Id :: binary(), Privileges :: [binary()], GroupDetails :: #group_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
groups_table_expanded(GroupsDetails) ->
    TableName = <<"Groups">>,
    NavigationBody = opn_gui_utils:collapse_button(<<"Collapse All">>, {message, collapse_groups_table}),
    RenderRowFunction = fun group_row_expanded/3,
    table(GroupsDetails, TableName, NavigationBody, RenderRowFunction).


%% table/4
%% ====================================================================
%% @doc Renders details table.
%% @end
-spec table(Details :: [{Id :: binary(), Details :: #provider_details{} | #user_details{} | #group_details{}}], TableName :: binary(),
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


%% provider_row_collapsed/3
%% ====================================================================
%% @doc Renders collapsed provider details row.
%% @end
-spec provider_row_collapsed(RowId :: binary(), Privileges :: [binary()], ProviderDetails :: #provider_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
provider_row_collapsed(RowId, Privileges, #provider_details{id = ProviderId, name = ProviderName} = ProviderDetails) ->
    NavigationBody = opn_gui_utils:expand_button({message, {expand_provider_row, RowId, Privileges, ProviderDetails}}),
    row_collapsed(ProviderId, ProviderName, NavigationBody).


%% provider_row_expanded/3
%% ====================================================================
%% @doc Renders expanded provider details row.
%% @end
-spec provider_row_expanded(RowId :: binary(), Privileges :: [binary()], ProviderDetails :: #provider_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
provider_row_expanded(RowId, Privileges, #provider_details{id = ProviderId, name = ProviderName, redirection_point = RedirectionPoint, urls = URLs} = ProviderDetails) ->
    Details = [
        {<<"Name">>, detail(gui_str:html_encode(ProviderName), <<"Remove provider">>, lists:member(<<"space_remove_provider">>, Privileges), {remove_provider, RowId, ProviderDetails}, <<"icomoon-remove">>)},
        {<<"Provider ID">>, #span{style = ?DETAIL_STYLE, body = ProviderId}},
        {<<"URLs">>, #list{
            style = <<"list-style-type: none; margin: 0 auto;">>,
            body = lists:map(fun(URL) ->
                #li{body = #span{
                    style = ?DETAIL_STYLE,
                    body = URL}
                }
            end, lists:sort(URLs))
        }},
        {<<"Redirection point">>, #span{style = ?DETAIL_STYLE, body = gui_str:html_encode(RedirectionPoint)}}
    ],
    NavigationBody = opn_gui_utils:collapse_button({message, {collapse_provider_row, RowId, Privileges, ProviderDetails}}),
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
        {<<"Name">>, detail(gui_str:html_encode(UserName), <<"Remove user">>, lists:member(<<"space_remove_user">>, Privileges), {remove_user, RowId, UserDetails}, <<"icomoon-remove">>)},
        {<<"User ID">>, #span{style = ?DETAIL_STYLE, body = UserId}}
    ],
    NavigationBody = opn_gui_utils:collapse_button({message, {collapse_user_row, RowId, Privileges, UserDetails}}),
    row_expanded(Details, NavigationBody).


%% group_row_collapsed/3
%% ====================================================================
%% @doc Renders collapsed group details row.
%% @end
-spec group_row_collapsed(RowId :: binary(), Privileges :: [binary()], GroupDetails :: #group_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
group_row_collapsed(RowId, Privileges, #group_details{id = GroupId, name = GroupName} = GroupDetails) ->
    NavigationBody = opn_gui_utils:expand_button({message, {expand_group_row, RowId, Privileges, GroupDetails}}),
    row_collapsed(GroupId, GroupName, NavigationBody).


%% group_row_expanded/3
%% ====================================================================
%% @doc Renders expanded group details row.
%% @end
-spec group_row_expanded(RowId :: binary(), Privileges :: [binary()], GroupDetails :: #group_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
group_row_expanded(RowId, Privileges, #group_details{id = GroupId, name = GroupName} = GroupDetails) ->
    Details = [
        {<<"Name">>, detail(gui_str:html_encode(GroupName), <<"Remove group">>, lists:member(<<"space_remove_group">>, Privileges), {remove_group, RowId, GroupDetails}, <<"icomoon-remove">>)},
        {<<"Group ID">>, #span{style = ?DETAIL_STYLE, body = GroupId}}
    ],
    NavigationBody = opn_gui_utils:collapse_button({message, {collapse_group_row, RowId, Privileges, GroupDetails}}),
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

comet_loop(#?STATE{space_id = SpaceId, providers_details = ProvidersDetails, users_details = UsersDetails,
    groups_details = GroupsDetails, gruid = GRUID, access_token = AccessToken} = State) ->
    NewCometLoopState =
        try
            receive
                {render_tables, Privileges} ->
                    gui_jq:update(<<"providers_table">>, providers_table_collapsed(ProvidersDetails)),
                    gui_jq:update(<<"users_table">>, users_table_collapsed(UsersDetails)),
                    gui_jq:update(<<"groups_table">>, groups_table_collapsed(GroupsDetails)),

                    lists:foreach(fun
                        ({false, _, _, _}) -> ok;
                        ({true, Module, Function, Args}) -> apply(Module, Function, Args)
                    end, [
                        {lists:member(<<"space_change_data">>, Privileges), gui_jq, show, [<<"change_space_name_span">>]},
                        {lists:member(<<"space_invite_user">>, Privileges), gui_jq, prop, [<<"invite_user_button">>, <<"disabled">>, <<"">>]},
                        {lists:member(<<"space_invite_group">>, Privileges), gui_jq, prop, [<<"invite_group_button">>, <<"disabled">>, <<"">>]},
                        {lists:member(<<"space_add_provider">>, Privileges), gui_jq, prop, [<<"request_support_button">>, <<"disabled">>, <<"">>]}
                    ]),

                    State;

                request_support ->
                    case gr_spaces:get_invite_provider_token({user, AccessToken}, SpaceId) of
                        {ok, Token} ->
                            Message = <<"Give the token below to a provider willing to support your Space.",
                            "<input id=\"support_token_textbox\" type=\"text\" style=\"margin-top: 1em;"
                            " width: 80%;\" value=\"", Token/binary, "\">">>,
                            gui_jq:info_popup(<<"Request support">>, Message, <<"return true;">>, <<"btn-inverse">>),
                            gui_jq:wire(<<"box.on('shown',function(){ $(\"#support_token_textbox\").focus().select(); });">>);
                        Other ->
                            ?error("Cannot get support token for Space with ID ~p: ~p", [SpaceId, Other]),
                            opn_gui_utils:message(error, <<"Cannot get support token for Space with ID: <b>", SpaceId, "</b>."
                            "<br>Please try again later.">>)
                    end,
                    State;

                invite_user ->
                    case gr_spaces:get_invite_user_token({user, AccessToken}, SpaceId) of
                        {ok, Token} ->
                            Message = <<"Give the token below to a user willing to join your Space.",
                            "<input id=\"join_token_textbox\" type=\"text\" style=\"margin-top: 1em;"
                            " width: 80%;\" value=\"", Token/binary, "\">">>,
                            gui_jq:info_popup(<<"Invite user">>, Message, <<"return true;">>, <<"btn-inverse">>),
                            gui_jq:wire(<<"box.on('shown',function(){ $(\"#join_token_textbox\").focus().select(); });">>);
                        Other ->
                            ?error("Cannot get user invitation token for Space with ID ~p: ~p", [SpaceId, Other]),
                            opn_gui_utils:message(error, <<"Cannot get invitation token for Space with ID: <b>", SpaceId, "</b>."
                            "<br>Please try again later.">>)
                    end,
                    State;

                invite_group ->
                    case gr_spaces:get_invite_group_token({user, AccessToken}, SpaceId) of
                        {ok, Token} ->
                            Message = <<"Give the token below to a group willing to join your Space.",
                            "<input id=\"join_token_textbox\" type=\"text\" style=\"margin-top: 1em;"
                            " width: 80%;\" value=\"", Token/binary, "\">">>,
                            gui_jq:info_popup(<<"Invite group">>, Message, <<"return true;">>, <<"btn-inverse">>),
                            gui_jq:wire(<<"box.on('shown',function(){ $(\"#join_token_textbox\").focus().select(); });">>);
                        Other ->
                            ?error("Cannot get group invitation token for Space with ID ~p: ~p", [SpaceId, Other]),
                            opn_gui_utils:message(error, <<"Cannot get invitation token for Space with ID: <b>", SpaceId, "</b>."
                            "<br>Please try again later.">>)
                    end,
                    State;

                {change_space_name, #space_details{id = SpaceId} = SpaceDetails, NewSpaceName} ->
                    case gr_spaces:modify_details({user, AccessToken}, SpaceId, [{<<"name">>, NewSpaceName}]) of
                        ok ->
                            gr_adapter:synchronize_user_spaces({GRUID, AccessToken}),
                            gui_jq:update(<<"space_name">>, space_name(SpaceDetails#space_details{name = NewSpaceName}));
                        Other ->
                            ?error("Cannot change name of Space ~p: ~p", [SpaceDetails, Other]),
                            opn_gui_utils:message(error, <<"Cannot change name of Space with ID:  <b>", SpaceId, "</b>."
                            "<br>Please try again later.">>),
                            gui_jq:update(<<"space_name">>, space_name(SpaceDetails))
                    end,
                    gui_jq:show(<<"change_space_name_span">>),
                    State;

                {remove_provider, RowId, ProviderId} ->
                    case gr_spaces:remove_provider({user, AccessToken}, SpaceId, ProviderId) of
                        ok ->
                            opn_gui_utils:message(success, <<"Provider with ID: <b>", SpaceId/binary, "</b> removed successfully.">>),
                            gui_jq:remove(RowId),
                            State#?STATE{providers_details = lists:keydelete(RowId, 1, ProvidersDetails)};
                        Other ->
                            ?error("Cannot remove provider with ID ~p: ~p", [ProviderId, Other]),
                            opn_gui_utils:message(error, <<"Cannot remove provider with ID: <b>", ProviderId/binary, "</b>.<br>Please try again later.">>),
                            State
                    end;

                {remove_user, RowId, UserId} ->
                    case gr_spaces:remove_user({user, AccessToken}, SpaceId, UserId) of
                        ok ->
                            opn_gui_utils:message(success, <<"User with ID: <b>", SpaceId/binary, "</b> removed successfully.">>),
                            gui_jq:remove(RowId),
                            State#?STATE{users_details = lists:keydelete(RowId, 1, UsersDetails)};
                        Other ->
                            ?error("Cannot remove user with ID ~p: ~p", [UserId, Other]),
                            opn_gui_utils:message(error, <<"Cannot remove user with ID: <b>", UserId/binary, "</b>.<br>Please try again later.">>),
                            State
                    end;

                {remove_group, RowId, GroupId} ->
                    case gr_spaces:remove_group({user, AccessToken}, SpaceId, GroupId) of
                        ok ->
                            opn_gui_utils:message(success, <<"Group with ID: <b>", SpaceId/binary, "</b> removed successfully.">>),
                            gui_jq:remove(RowId),
                            State#?STATE{groups_details = lists:keydelete(RowId, 1, GroupsDetails)};
                        Other ->
                            ?error("Cannot remove group with ID ~p: ~p", [GroupId, Other]),
                            opn_gui_utils:message(error, <<"Cannot remove group with ID: <b>", GroupId/binary, "</b>.<br>Please try again later.">>),
                            State
                    end;

                Event ->
                    case Event of
                        collapse_providers_table ->
                            gui_jq:update(<<"providers_table">>, providers_table_collapsed(ProvidersDetails));
                        expand_providers_table ->
                            gui_jq:update(<<"providers_table">>, providers_table_expanded(ProvidersDetails));
                        {collapse_provider_row, RowId, Privileges, ProviderDetails} ->
                            gui_jq:update(RowId, provider_row_collapsed(RowId, Privileges, ProviderDetails));
                        {expand_provider_row, RowId, Privileges, ProviderDetails} ->
                            gui_jq:update(RowId, provider_row_expanded(RowId, Privileges, ProviderDetails));
                        collapse_users_table ->
                            gui_jq:update(<<"users_table">>, users_table_collapsed(UsersDetails));
                        expand_users_table ->
                            gui_jq:update(<<"users_table">>, users_table_expanded(UsersDetails));
                        {collapse_user_row, RowId, Privileges, UserDetails} ->
                            gui_jq:update(RowId, user_row_collapsed(RowId, Privileges, UserDetails));
                        {expand_user_row, RowId, Privileges, UserDetails} ->
                            gui_jq:update(RowId, user_row_expanded(RowId, Privileges, UserDetails));
                        collapse_groups_table ->
                            gui_jq:update(<<"groups_table">>, groups_table_collapsed(GroupsDetails));
                        expand_groups_table ->
                            gui_jq:update(<<"groups_table">>, groups_table_expanded(GroupsDetails));
                        {collapse_group_row, RowId, Privileges, GroupDetails} ->
                            gui_jq:update(RowId, group_row_collapsed(RowId, Privileges, GroupDetails));
                        {expand_group_row, RowId, Privileges, GroupDetails} ->
                            gui_jq:update(RowId, group_row_expanded(RowId, Privileges, GroupDetails));
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
        SpaceId = gui_str:to_binary(gui_ctx:url_param(<<"id">>)),
        GRUID = utils:ensure_binary(opn_gui_utils:get_global_user_id()),
        AccessToken = opn_gui_utils:get_access_token(),
        {ok, Privileges} = gr_spaces:get_effective_user_privileges({user, AccessToken}, SpaceId, GRUID),

        GetDetailsFun = fun(Ids, Function, RowPrefix) ->
            lists:foldl(fun(Id, {Rows, It}) ->
                {ok, Details} = gr_spaces:Function({user, AccessToken}, SpaceId, Id),
                {
                    [{<<RowPrefix/binary, (integer_to_binary(It + 1))/binary>>, Privileges, Details} | Rows],
                    It + 1
                }
            end, {[], 0}, Ids)
        end,

        {ok, ProvidersIds} = gr_spaces:get_providers({user, AccessToken}, SpaceId),
        {ProvidersDetails, _} = GetDetailsFun(ProvidersIds, get_provider_details, <<"provider_">>),
        {ok, UsersIds} = gr_spaces:get_users({user, AccessToken}, SpaceId),
        {UsersDetails, _} = GetDetailsFun(UsersIds, get_user_details, <<"user_">>),
        {ok, GroupsIds} = gr_spaces:get_groups({user, AccessToken}, SpaceId),
        {GroupsDetails, _} = GetDetailsFun(GroupsIds, get_group_details, <<"group_">>),

        gui_jq:bind_key_to_click_on_class(<<"13">>, <<"confirm">>),

        gui_jq:wire(#api{name = "remove_provider", tag = "remove_provider"}, false),
        gui_jq:wire(#api{name = "remove_user", tag = "remove_user"}, false),
        gui_jq:wire(#api{name = "remove_group", tag = "remove_group"}, false),

        {ok, Pid} = gui_comet:spawn(fun() ->
            comet_loop(#?STATE{space_id = SpaceId, providers_details = ProvidersDetails, users_details = UsersDetails,
                groups_details = GroupsDetails, gruid = GRUID, access_token = AccessToken})
        end),
        put(?COMET_PID, Pid),
        Pid ! {render_tables, Privileges}
    catch
        _:Reason ->
            ?error("Cannot initialize page ~p: ~p", [?MODULE, Reason]),
            gui_jq:hide(<<"main_spinner">>),
            opn_gui_utils:message(error, <<"Cannot fetch Space details.<br>Please try again later.">>)
    end;

event({change_space_name, SpaceDetails}) ->
    gui_jq:update(<<"space_name">>, change_space_name(SpaceDetails)),
    gui_jq:bind_enter_to_submit_button(<<"new_space_name_textbox">>, <<"new_space_name_submit">>),
    gui_jq:focus(<<"new_space_name_textbox">>);

event({submit_new_space_name, SpaceDetails}) ->
    NewSpaceName = gui_ctx:postback_param(<<"new_space_name_textbox">>),
    get(?COMET_PID) ! {change_space_name, SpaceDetails, NewSpaceName},
    gui_jq:show(<<"main_spinner">>);

event({cancel_new_space_name_submit, SpaceDetails}) ->
    gui_jq:update(<<"space_name">>, space_name(SpaceDetails)),
    gui_jq:show(<<"change_space_name_span">>);

event({remove_provider, RowId, #provider_details{id = ProviderId, name = ProviderName}}) ->
    Message = <<"Are you sure you want to remove provider:<br><b>", (gui_str:html_encode(ProviderName))/binary, " (", ProviderId/binary, ") </b>?">>,
    Script = <<"remove_provider(['", RowId/binary, "','", ProviderId/binary, "']);">>,
    gui_jq:dialog_popup(<<"Remove provider">>, Message, Script, <<"btn-inverse">>);

event({remove_user, RowId, #user_details{id = UserId, name = UserName}}) ->
    Message = <<"Are you sure you want to remove user:<br><b>", (gui_str:html_encode(UserName))/binary, " (", UserId/binary, ") </b>?">>,
    Script = <<"remove_user(['", RowId/binary, "','", UserId/binary, "']);">>,
    gui_jq:dialog_popup(<<"Remove user">>, Message, Script, <<"btn-inverse">>);

event({remove_group, RowId, #group_details{id = GroupId, name = GroupName}}) ->
    Message = <<"Are you sure you want to remove group:<br><b>", (gui_str:html_encode(GroupName))/binary, " (", GroupId/binary, ") </b>?">>,
    Script = <<"remove_group(['", RowId/binary, "','", GroupId/binary, "']);">>,
    gui_jq:dialog_popup(<<"Remove group">>, Message, Script, <<"btn-inverse">>);

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
api_event(Function, Args, _) ->
    [RowId, ObejctId] = mochijson2:decode(Args),
    get(?COMET_PID) ! {list_to_existing_atom(Function), RowId, ObejctId},
    gui_jq:show(<<"main_spinner">>).
