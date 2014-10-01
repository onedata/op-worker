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
-include("veil_modules/control_panel/common.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("ctool/include/global_registry/gr_groups.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").
-include_lib("ctool/include/logging.hrl").

%% n2o API and comet
-export([main/0, event/1, comet_loop/1]).

%% Common page CCS styles
-define(CONTENT_COLUMN_STYLE, <<"padding-right: 0">>).
-define(NAVIGATION_COLUMN_STYLE, <<"border-left-width: 0; width: 20px; padding-left: 0;">>).
-define(PARAGRAPH_STYLE, <<"margin: 0 auto;">>).

%% Comet process pid
-define(COMET_PID, comet_pid).

%% Comet process state
-define(STATE, comet_state).
-record(?STATE, {providers_details, users_details, groups_details}).

%% ====================================================================
%% API functions
%% ====================================================================


%% main/0
%% ====================================================================
%% @doc Template points to the template file, which will be filled with content.
-spec main() -> #dtl{}.
%% ====================================================================
main() ->
    case vcn_gui_utils:maybe_redirect(true, false, false) of
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
                    case gr_users:get_space_details({user, vcn_gui_utils:get_access_token()}, SpaceId) of
                        {ok, SpaceDetails} ->
                            #dtl{file = "bare", app = ?APP_Name, bindings = [{title, title()}, {body, body(SpaceDetails)}, {custom, custom()}]};
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


%% custom/0
%% ====================================================================
%% @doc This will be placed instead of {{custom}} tag in template.
-spec custom() -> binary().
%% ====================================================================
custom() ->
    <<"<script src='/flatui/bootbox.min.js' type='text/javascript' charset='utf-8'></script>">>.


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
        vcn_gui_utils:top_menu(spaces_tab),
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
                    style = <<"font-size: x-large; margin: 0 auto; margin-top: 160px; width: 152px;">>,
                    body = <<"Manage Space">>
                },
                space_details_table(SpaceDetails) |
                lists:map(fun({TableId, Panel}) ->
                    #panel{
                        body = [
                            #table{
                                class = <<"table table-bordered table-striped">>,
                                style = <<"width: 50%; margin: 0 auto; margin-top: 3em; table-layout: fixed;">>,
                                body = #tbody{
                                    id = TableId,
                                    style = <<"display: none;">>
                                }
                            },
                            Panel
                        ]
                    }
                end, [
                    {<<"providers_table">>, providers_panel(SpaceDetails)},
                    {<<"users_table">>, users_panel(SpaceDetails)},
                    {<<"groups_table">>, groups_panel(SpaceDetails)}
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
            {<<"Space ID">>, <<"">>, #p{style = ?PARAGRAPH_STYLE, body = SpaceId}}
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
            SpaceName,
            #link{
                title = <<"Edit">>,
                style = <<"margin-left: 1em;">>,
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


%% providers_panel/1
%% ====================================================================
%% @doc Renders providers management panel.
-spec providers_panel(SpaceDetails :: #space_details{}) -> Result when
    Result :: #panel{}.
%% ====================================================================
providers_panel(#space_details{id = SpaceId}) ->
    #panel{
        style = <<"margin: 0 auto; width: 50%; margin-top: 30px; text-align: center;">>,
        body = #button{
            id = <<"request_support">>,
            postback = {message, {request_support, SpaceId}},
            class = <<"btn btn-inverse btn-small">>,
            body = <<"Request support">>
        }
    }.


%% users_panel/1
%% ====================================================================
%% @doc Renders users management panel.
-spec users_panel(SpaceDetails :: #space_details{}) -> Result when
    Result :: #panel{}.
%% ====================================================================
users_panel(#space_details{id = SpaceId}) ->
    #panel{
        style = <<"margin: 0 auto; width: 50%; margin-top: 30px; text-align: center;">>,
        body = #button{
            id = <<"invite_user">>,
            postback = {message, {invite_user, SpaceId}},
            class = <<"btn btn-inverse btn-small">>,
            body = <<"Invite user">>
        }
    }.


%% groups_panel/1
%% ====================================================================
%% @doc Renders groups management panel.
-spec groups_panel(SpaceDetails :: #space_details{}) -> Result when
    Result :: #panel{}.
%% ====================================================================
groups_panel(#space_details{id = SpaceId}) ->
    #panel{
        style = <<"margin: 0 auto; width: 50%; margin-top: 30px; text-align: center;">>,
        body = #button{
            id = <<"invite_group">>,
            postback = {message, {invite_group, SpaceId}},
            class = <<"btn btn-inverse btn-small">>,
            body = <<"Invite group">>
        }
    }.

%% providers_table_collapsed/1
%% ====================================================================
%% @doc Renders collapsed providers details table.
%% @end
-spec providers_table_collapsed(ProvidersDetails :: [{Id :: binary(), ProviderDetails :: #provider_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
providers_table_collapsed(ProvidersDetails) ->
    TableName = <<"Providers">>,
    NavigationBody = vcn_gui_utils:expand_button(<<"Expand All">>, {message, expand_providers_table}),
    RenderRowFunction = fun provider_row_collapsed/2,
    table(ProvidersDetails, TableName, NavigationBody, RenderRowFunction).


%% providers_table_expanded/1
%% ====================================================================
%% @doc Renders expanded providers details table.
%% @end
-spec providers_table_expanded(ProvidersDetails :: [{Id :: binary(), ProviderDetails :: #provider_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
providers_table_expanded(ProvidersDetails) ->
    TableName = <<"Providers">>,
    NavigationBody = vcn_gui_utils:collapse_button(<<"Collapse All">>, {message, collapse_providers_table}),
    RenderRowFunction = fun provider_row_expanded/2,
    table(ProvidersDetails, TableName, NavigationBody, RenderRowFunction).


%% users_table_collapsed/1
%% ====================================================================
%% @doc Renders collapsed users details table.
%% @end
-spec users_table_collapsed(UsersDetails :: [{Id :: binary(), UserDetails :: #user_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
users_table_collapsed(UsersDetails) ->
    TableName = <<"Users">>,
    NavigationBody = vcn_gui_utils:expand_button(<<"Expand All">>, {message, expand_users_table}),
    RenderRowFunction = fun user_row_collapsed/2,
    table(UsersDetails, TableName, NavigationBody, RenderRowFunction).


%% users_table_expanded/1
%% ====================================================================
%% @doc Renders expanded users details table.
%% @end
-spec users_table_expanded(UsersDetails :: [{Id :: binary(), UserDetails :: #user_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
users_table_expanded(UsersDetails) ->
    TableName = <<"Users">>,
    NavigationBody = vcn_gui_utils:collapse_button(<<"Collapse All">>, {message, collapse_users_table}),
    RenderRowFunction = fun user_row_expanded/2,
    table(UsersDetails, TableName, NavigationBody, RenderRowFunction).


%% groups_table_collapsed/1
%% ====================================================================
%% @doc Renders collapsed groups details table.
%% @end
-spec groups_table_collapsed(GroupsDetails :: [{Id :: binary(), GroupDetails :: #group_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
groups_table_collapsed(GroupsDetails) ->
    TableName = <<"Groups">>,
    NavigationBody = vcn_gui_utils:expand_button(<<"Expand All">>, {message, expand_groups_table}),
    RenderRowFunction = fun group_row_collapsed/2,
    table(GroupsDetails, TableName, NavigationBody, RenderRowFunction).


%% groups_table_expanded/1
%% ====================================================================
%% @doc Renders expanded groups details table.
%% @end
-spec groups_table_expanded(GroupsDetails :: [{Id :: binary(), GroupDetails :: #group_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
groups_table_expanded(GroupsDetails) ->
    TableName = <<"Groups">>,
    NavigationBody = vcn_gui_utils:collapse_button(<<"Collapse All">>, {message, collapse_groups_table}),
    RenderRowFunction = fun group_row_expanded/2,
    table(GroupsDetails, TableName, NavigationBody, RenderRowFunction).


%% table/4
%% ====================================================================
%% @doc Renders details table.
%% @end
-spec table(Details :: [{Id :: binary(), Details :: #provider_details{} | #user_details{}}], TableName :: binary(), NavigationBody :: #link{}, RenderRowFunction :: function()) -> Result when
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

    Rows = lists:foldl(fun({RowId, RowDetails}, RowsAcc) ->
        [#tr{
            id = RowId,
            cells = RenderRowFunction(RowId, RowDetails)
        } | RowsAcc]
    end, [], Details),

    [Header | Rows].


%% provider_row_collapsed/2
%% ====================================================================
%% @doc Renders collapsed provider details row.
%% @end
-spec provider_row_collapsed(RowId :: binary(), ProviderDetails :: #provider_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
provider_row_collapsed(RowId, #provider_details{id = ProviderId, name = ProviderName} = ProviderDetails) ->
    NavigationBody = vcn_gui_utils:expand_button({message, {collapse_provider_row, RowId, ProviderDetails}}),
    row_collapsed(ProviderId, ProviderName, NavigationBody).


%% provider_row_expanded/2
%% ====================================================================
%% @doc Renders expanded provider details row.
%% @end
-spec provider_row_expanded(RowId :: binary(), ProviderDetails :: #provider_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
provider_row_expanded(RowId, #provider_details{id = ProviderId, name = ProviderName, redirection_point = RedirectionPoint, urls = URLs} = ProviderDetails) ->
    Details = [
        {<<"Name">>, #p{style = ?PARAGRAPH_STYLE, body = ProviderName}},
        {<<"Provider ID">>, #p{style = ?PARAGRAPH_STYLE, body = ProviderId}},
        {<<"URLs">>, #list{
            style = <<"list-style-type: none; margin: 0 auto;">>,
            body = lists:map(fun(URL) ->
                #li{body = #p{
                    style = ?PARAGRAPH_STYLE,
                    body = URL}
                }
            end, lists:sort(URLs))
        }},
        {<<"Redirection point">>, #p{style = ?PARAGRAPH_STYLE, body = RedirectionPoint}}
    ],
    NavigationBody = vcn_gui_utils:collapse_button({message, {collapse_provider_row, RowId, ProviderDetails}}),
    row_expanded(Details, NavigationBody).


%% user_row_collapsed/2
%% ====================================================================
%% @doc Renders collapsed user details row.
%% @end
-spec user_row_collapsed(RowId :: binary(), UserDetails :: #user_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
user_row_collapsed(RowId, #user_details{id = UserId, name = UserName} = UserDetails) ->
    NavigationBody = vcn_gui_utils:expand_button({message, {expand_user_row, RowId, UserDetails}}),
    row_collapsed(UserId, UserName, NavigationBody).


%% user_row_expanded/2
%% ====================================================================
%% @doc Renders expanded user details row.
%% @end
-spec user_row_expanded(RowId :: binary(), UserDetails :: #user_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
user_row_expanded(RowId, #user_details{id = UserId, name = UserName} = UserDetails) ->
    Details = [
        {<<"Name">>, #p{style = ?PARAGRAPH_STYLE, body = UserName}},
        {<<"User ID">>, #p{style = ?PARAGRAPH_STYLE, body = UserId}}
    ],
    NavigationBody = vcn_gui_utils:collapse_button({message, {collapse_user_row, RowId, UserDetails}}),
    row_expanded(Details, NavigationBody).


%% group_row_collapsed/2
%% ====================================================================
%% @doc Renders collapsed group details row.
%% @end
-spec group_row_collapsed(RowId :: binary(), GroupDetails :: #group_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
group_row_collapsed(RowId, #group_details{id = GroupId, name = GroupName} = GroupDetails) ->
    NavigationBody = vcn_gui_utils:expand_button({message, {expand_group_row, RowId, GroupDetails}}),
    row_collapsed(GroupId, GroupName, NavigationBody).


%% group_row_expanded/2
%% ====================================================================
%% @doc Renders expanded group details row.
%% @end
-spec group_row_expanded(RowId :: binary(), GroupDetails :: #group_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
group_row_expanded(RowId, #group_details{id = GroupId, name = GroupName} = GroupDetails) ->
    Details = [
        {<<"Name">>, #p{style = ?PARAGRAPH_STYLE, body = GroupName}},
        {<<"Group ID">>, #p{style = ?PARAGRAPH_STYLE, body = GroupId}}
    ],
    NavigationBody = vcn_gui_utils:collapse_button({message, {collapse_group_row, RowId, GroupDetails}}),
    row_expanded(Details, NavigationBody).


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
            body = #p{
                style = ?PARAGRAPH_STYLE,
                body = <<"<b>", Name/binary, "</b> (", Id/binary, ")">>
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

comet_loop(#?STATE{providers_details = ProvidersDetails, users_details = UsersDetails, groups_details = GroupsDetails} = State) ->
    NewState = try
                   receive
                       render_tables ->
                           gui_jq:update(<<"providers_table">>, providers_table_collapsed(ProvidersDetails)),
                           gui_jq:fade_in(<<"providers_table">>, 500),
                           gui_jq:update(<<"users_table">>, users_table_collapsed(UsersDetails)),
                           gui_jq:fade_in(<<"users_table">>, 500),
                           gui_jq:update(<<"groups_table">>, groups_table_collapsed(GroupsDetails)),
                           gui_jq:fade_in(<<"groups_table">>, 500),
                           State;

                       {request_support, SpaceId} ->
                           case gr_spaces:get_invite_provider_token({user, vcn_gui_utils:get_access_token()}, SpaceId) of
                               {ok, Token} ->
                                   Message = <<"Give the token below to a provider willing to support your Space.",
                                   "<input id=\"support_token_textbox\" type=\"text\" style=\"margin-top: 1em;"
                                   " width: 80%;\" value=\"", Token/binary, "\">">>,
                                   gui_jq:info_popup(<<"Request support">>, Message, <<"return true;">>, <<"btn-inverse">>),
                                   gui_jq:wire(<<"box.on('shown',function(){ $(\"#support_token_textbox\").focus().select(); });">>);
                               Other ->
                                   ?error("Cannot get support token for Space with ID ~p: ~p", [SpaceId, Other]),
                                   vcn_gui_utils:message(<<"error_message">>, <<"Cannot get support token for Space with ID: <b>", SpaceId, "</b>."
                                   "<br>Please try again later.">>)
                           end,
                           State;

                       {invite_user, SpaceId} ->
                           case gr_spaces:get_invite_user_token({user, vcn_gui_utils:get_access_token()}, SpaceId) of
                               {ok, Token} ->
                                   Message = <<"Give the token below to a user willing to join your Space.",
                                   "<input id=\"join_token_textbox\" type=\"text\" style=\"margin-top: 1em;"
                                   " width: 80%;\" value=\"", Token/binary, "\">">>,
                                   gui_jq:info_popup(<<"Invite user">>, Message, <<"return true;">>, <<"btn-inverse">>),
                                   gui_jq:wire(<<"box.on('shown',function(){ $(\"#join_token_textbox\").focus().select(); });">>);
                               Other ->
                                   ?error("Cannot get user invitation token for Space with ID ~p: ~p", [SpaceId, Other]),
                                   vcn_gui_utils:message(<<"error_message">>, <<"Cannot get invitation token for Space with ID: <b>", SpaceId, "</b>."
                                   "<br>Please try again later.">>)
                           end,
                           State;

                       {invite_group, SpaceId} ->
                           case gr_spaces:get_invite_group_token({user, vcn_gui_utils:get_access_token()}, SpaceId) of
                               {ok, Token} ->
                                   Message = <<"Give the token below to a group willing to join your Space.",
                                   "<input id=\"join_token_textbox\" type=\"text\" style=\"margin-top: 1em;"
                                   " width: 80%;\" value=\"", Token/binary, "\">">>,
                                   gui_jq:info_popup(<<"Invite group">>, Message, <<"return true;">>, <<"btn-inverse">>),
                                   gui_jq:wire(<<"box.on('shown',function(){ $(\"#join_token_textbox\").focus().select(); });">>);
                               Other ->
                                   ?error("Cannot get group invitation token for Space with ID ~p: ~p", [SpaceId, Other]),
                                   vcn_gui_utils:message(<<"error_message">>, <<"Cannot get invitation token for Space with ID: <b>", SpaceId, "</b>."
                                   "<br>Please try again later.">>)
                           end,
                           State;

                       {change_space_name, #space_details{id = SpaceId} = SpaceDetails, NewSpaceName} ->
                           case gr_spaces:modify_details({user, vcn_gui_utils:get_access_token()}, SpaceId, [{<<"name">>, NewSpaceName}]) of
                               ok ->
                                   gr_adapter:synchronize_user_spaces({vcn_gui_utils:get_global_user_id(), vcn_gui_utils:get_access_token()}),
                                   gui_jq:update(<<"space_name">>, space_name(SpaceDetails#space_details{name = NewSpaceName}));
                               Other ->
                                   ?error("Cannot change name of Space ~p: ~p", [SpaceDetails, Other]),
                                   vcn_gui_utils:message(<<"error_message">>, <<"Cannot change name of Space with ID:  <b>", SpaceId, "</b>."
                                   "<br>Please try again later.">>),
                                   gui_jq:update(<<"space_name">>, space_name(SpaceDetails))
                           end,
                           State;

                       Event ->
                           case Event of
                               collapse_providers_table ->
                                   gui_jq:update(<<"providers_table">>, providers_table_collapsed(ProvidersDetails));
                               expand_providers_table ->
                                   gui_jq:update(<<"providers_table">>, providers_table_expanded(ProvidersDetails));
                               {collapse_provider_row, RowId, ProviderDetails} ->
                                   gui_jq:update(RowId, provider_row_collapsed(RowId, ProviderDetails));
                               {expand_provider_row, RowId, ProviderDetails} ->
                                   gui_jq:update(RowId, provider_row_expanded(RowId, ProviderDetails));
                               collapse_users_table ->
                                   gui_jq:update(<<"users_table">>, users_table_collapsed(UsersDetails));
                               expand_users_table ->
                                   gui_jq:update(<<"users_table">>, users_table_expanded(UsersDetails));
                               {collapse_user_row, RowId, UserDetails} ->
                                   gui_jq:update(RowId, user_row_collapsed(RowId, UserDetails));
                               {expand_user_row, RowId, UserDetails} ->
                                   gui_jq:update(RowId, user_row_expanded(RowId, UserDetails));
                               collapse_groups_table ->
                                   gui_jq:update(<<"groups_table">>, groups_table_collapsed(GroupsDetails));
                               expand_groups_table ->
                                   gui_jq:update(<<"groups_table">>, groups_table_expanded(GroupsDetails));
                               {collapse_group_row, RowId, GroupDetails} ->
                                   gui_jq:update(RowId, group_row_collapsed(RowId, GroupDetails));
                               {expand_group_row, RowId, GroupDetails} ->
                                   gui_jq:update(RowId, group_row_expanded(RowId, GroupDetails));
                               _ ->
                                   ok
                           end,
                           State
                   end
               catch Type:Reason ->
                   ?error_stacktrace("Comet process exception: ~p:~p", [Type, Reason]),
                   vcn_gui_utils:message(<<"error_message">>, <<"There has been an error in comet process. Please refresh the page.">>),
                   {error, Reason}
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
        AccessToken = vcn_gui_utils:get_access_token(),

        GetDetailsFun = fun(Ids, Function, RowPrefix) ->
            lists:foldl(fun(Id, {Rows, It}) ->
                {ok, Details} = gr_spaces:Function({user, AccessToken}, SpaceId, Id),
                {
                    [{<<RowPrefix/binary, (integer_to_binary(It + 1))/binary>>, Details} | Rows],
                    It + 1
                }
            end, {[], 0}, Ids)
        end,

        {ok, ProvideIds} = gr_spaces:get_providers({user, AccessToken}, SpaceId),
        {ProvidersDetails, _} = GetDetailsFun(ProvideIds, get_provider_details, <<"provider_">>),
        {ok, UseIds} = gr_spaces:get_users({user, AccessToken}, SpaceId),
        {UsersDetails, _} = GetDetailsFun(UseIds, get_user_details, <<"user_">>),
        {ok, GrouIds} = gr_spaces:get_groups({user, AccessToken}, SpaceId),
        {GroupsDetails, _} = GetDetailsFun(GrouIds, get_group_details, <<"group_">>),

        gui_jq:bind_key_to_click_on_class(<<"13">>, <<"confirm">>),

        {ok, Pid} = gui_comet:spawn(fun() ->
            comet_loop(#?STATE{providers_details = ProvidersDetails, users_details = UsersDetails, groups_details = GroupsDetails})
        end),
        put(?COMET_PID, Pid),
        Pid ! render_tables
    catch
        _:Reason ->
            ?error("Cannot initialize page ~p: ~p", [?MODULE, Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch Space details.<br>Please try again later.">>)
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
    gui_jq:update(<<"space_name">>, space_name(SpaceDetails));

event({message, Message}) ->
    get(?COMET_PID) ! Message,
    gui_jq:show(<<"main_spinner">>);

event({close_message, MessageId}) ->
    gui_jq:hide(MessageId);

event(terminate) ->
    ok.