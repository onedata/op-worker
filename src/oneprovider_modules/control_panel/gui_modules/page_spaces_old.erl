%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page allows user to manage his Spaces.
%% @end
%% ===================================================================

-module(page_spaces_old).
-include("oneprovider_modules/control_panel/common.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("ctool/include/logging.hrl").

% n2o API and comet
-export([main/0, event/1, api_event/3, comet_loop/1]).

%% Common page CCS styles
-define(MESSAGE_STYLE, <<"position: fixed; width: 100%; top: 55px; z-index: 1; display: none;">>).
-define(CONTENT_COLUMN_STYLE, <<"padding-right: 0">>).
-define(NAVIGATION_COLUMN_STYLE, <<"border-left-width: 0; width: 20px; padding-left: 0;">>).
-define(DESCRIPTION_STYLE, <<"border-width: 0; text-align: right; width: 10%; padding-left: 0; padding-right: 0;">>).
-define(MAIN_STYLE, <<"border-width: 0;  text-align: left; padding-left: 1em; width: 90%;">>).
-define(LABEL_STYLE, <<"margin: 0 auto; cursor: auto;">>).
-define(DETAIL_STYLE, <<"font-size: large; font-weight: normal; vertical-align: middle;">>).
-define(TABLE_STYLE, <<"border-width: 0; width: 100%; border-collapse: inherit;">>).

%% Comet process pid
-define(COMET_PID, comet_pid).

%% Comet process state
-define(STATE, comet_state).
-record(?STATE, {counter = 1, default_row_id, spaces_details = [], gruid, access_token}).

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
            #dtl{file = "bare", app = ?APP_Name, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
        false ->
            #dtl{file = "bare", app = ?APP_Name, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}
    end.


%% title/0
%% ====================================================================
%% @doc Page title.
-spec title() -> binary().
%% ====================================================================
title() -> <<"Spaces">>.


%% body/0
%% ====================================================================
%% @doc This will be placed instead of {{body}} tag in template.
-spec body() -> [#panel{}].
%% ====================================================================
body() ->
    #panel{class = <<"page-container">>, body = [
        #panel{
            id = <<"main_spinner">>,
            style = <<"position: absolute; top: 12px; left: 17px; z-index: 1234; width: 32px;">>,
            body = #image{
                image = <<"/images/spinner.gif">>
            }
        },
        opn_gui_utils:top_menu(spaces_tab),
        #panel{
            style = <<"margin-top: 56px; padding-top: 1px; margin-bottom: 30px;">>,
            body = [
                #panel{
                    id = <<"message">>,
                    style = <<"width: 100%; padding: 0.5em 0; margin: 0 auto; border: 0; display: none;">>,
                    class = <<"dialog">>
                },
                #h6{
                    style = <<"font-size: x-large; margin: 0 auto; margin-top: 30px; text-align: center;">>,
                    body = <<"Manage Spaces">>
                },
                #panel{
                    style = <<"margin: 0 auto; width: 50%; margin-top: 30px; margin-bottom: 30px; text-align: center;">>,
                    body = lists:map(fun({ButtonId, ButtonPostback, ButtonBody}) ->
                        #button{
                            id = ButtonId,
                            postback = ButtonPostback,
                            class = <<"btn btn-inverse btn-small">>,
                            style = <<"margin-right: 0.5em; margin-left: 0.5em;">>,
                            body = ButtonBody
                        }
                    end, [
                        {<<"create_space_button">>, create_space, <<"Create Space">>},
                        {<<"join_space_button">>, join_space, <<"Join Space">>}
                    ])
                },
                #table{
                    class = <<"table table-bordered table-striped">>,
                    style = <<"width: 50%; margin: 0 auto; margin-top: 30px; table-layout: fixed;">>,
                    body = #tbody{
                        id = <<"spaces_table">>,
                        body = spaces_table_collapsed([])
                    }
                }
            ]
        }
    ]}.


%% spaces_table_collapsed/1
%% ====================================================================
%% @doc Renders collapsed spaces details table.
%% @end
-spec spaces_table_collapsed(SpacesDetails :: [{Id :: binary(), SpaceDetails :: #space_details{}}]) -> Result when
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
-spec spaces_table_expanded(SpacesDetails :: [{Id :: binary(), SpaceDetails :: #space_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
spaces_table_expanded(SpacesDetails) ->
    TableName = <<"Spaces">>,
    NavigationBody = opn_gui_utils:collapse_button(<<"Collapse All">>, {message, collapse_spaces_table}),
    RenderRowFunction = fun space_row_expanded/3,
    table(SpacesDetails, TableName, NavigationBody, RenderRowFunction).


%% table/4
%% ====================================================================
%% @doc Renders details table.
%% @end
-spec table(Details :: [{Id :: binary(), Details :: #space_details{}}], TableName :: binary(), NavigationBody :: #link{}, RenderRowFunction :: function()) -> Result when
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
%% @doc Renders collapsed space settings row.
%% @end
-spec space_row_collapsed(RowId :: binary(), Privileges :: [binary()], SpaceDetails :: #space_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
space_row_collapsed(RowId, Privileges, #space_details{id = SpaceId, name = SpaceName} = SpaceDetails) ->
    [
        #td{
            style = ?CONTENT_COLUMN_STYLE,
            body = #table{
                style = ?TABLE_STYLE,
                body = [
                    #tr{
                        cells = [
                            #td{
                                style = <<"border-width: 0; text-align: left; padding-left: 0; padding-right: 0;">>,
                                body = #span{
                                    style = ?DETAIL_STYLE,
                                    body = <<"<b>", (gui_str:html_encode(SpaceName))/binary, "</b> (", SpaceId/binary, ")">>
                                }
                            },
                            #td{
                                style = <<"border-width: 0; padding-left: 0;">>,
                                body = default_label(RowId)
                            }
                        ]
                    }
                ]
            }
        },
        #td{
            style = ?NAVIGATION_COLUMN_STYLE,
            body = opn_gui_utils:expand_button({message, {expand_space_row, RowId, Privileges, SpaceDetails}})
        }
    ].


%% space_row_expanded/3
%% ====================================================================
%% @doc Renders expanded space settings row.
%% @end
-spec space_row_expanded(RowId :: binary(), Privileges :: [binary()], SpaceDetails :: #space_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
space_row_expanded(RowId, Privileges, #space_details{id = SpaceId, name = SpaceName} = SpaceDetails) ->
    SettingsIcons = lists:filtermap(fun({Privilege, LinkId, LinkTitle, LinkPostback, SpanClass}) ->
        case lists:member(Privilege, [<<"standard">> | Privileges]) of
            true ->
                {true, #link{
                    id = LinkId,
                    title = LinkTitle,
                    style = <<"font-size: large; margin-right: 1em; vertical-align: middle;">>,
                    class = <<"glyph-link">>,
                    postback = LinkPostback,
                    body = #span{
                        style = <<"vertical-align: middle;">>,
                        class = SpanClass
                    }
                }};
            _ ->
                false
        end
    end, [
        {<<"standard">>, <<RowId/binary, "_default_option">>, <<"Set as a default Space">>, {message, {set_default_space, RowId, SpaceDetails}}, <<"icomoon-home">>},
        {<<"standard">>, <<RowId/binary, "_manage_option">>, <<"Manage Space">>, {manage_space, SpaceId}, <<"icomoon-cog">>},
        {<<"space_set_privileges">>, <<RowId/binary, "_set_privileges">>, <<"Set privileges">>, {set_privileges, SpaceId}, <<"icomoon-lock">>},
        {<<"standard">>, <<RowId/binary, "_leave_option">>, <<"Leave Space">>, {leave_space, RowId, SpaceDetails}, <<"icomoon-exit">>},
        {<<"space_remove">>, <<RowId/binary, "_remove_option">>, <<"Remove Space">>, {remove_space, RowId, SpaceDetails}, <<"icomoon-remove">>}
    ]),
    [
        #td{
            style = ?CONTENT_COLUMN_STYLE,
            body = #table{
                style = ?TABLE_STYLE,
                body = lists:map(fun({Description, Main}) ->
                    #tr{
                        cells = [
                            #td{
                                style = ?DESCRIPTION_STYLE,
                                body = #label{
                                    style = <<"margin: 0 auto; cursor: auto;">>,
                                    class = <<"label label-large label-inverse">>,
                                    body = Description
                                }
                            },
                            #td{
                                style = ?MAIN_STYLE,
                                body = #span{
                                    style = ?DETAIL_STYLE,
                                    body = Main
                                }
                            }
                        ]
                    }
                end, [
                    {<<"Name">>, [gui_str:html_encode(SpaceName), default_label(RowId)]},
                    {<<"Space ID">>, SpaceId},
                    {<<"Settings">>, SettingsIcons}
                ])
            }
        },
        #td{
            style = ?NAVIGATION_COLUMN_STYLE,
            body = opn_gui_utils:collapse_button({message, {collapse_space_row, RowId, Privileges, SpaceDetails}})
        }
    ].


%% default_label/1
%% ====================================================================
%% @doc Renders label that marks Space as a default user's Space.
-spec default_label(RowId :: binary()) -> Result when
    Result :: #label{}.
%% ====================================================================
default_label(RowId) ->
    #label{
        id = <<RowId/binary, "_label">>,
        style = <<"margin: 0 auto; cursor: auto; display: none;">>,
        class = <<"label label-large label-success pull-right">>,
        body = <<"Default">>
    }.


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
%% @doc Handles spaces management actions.
-spec comet_loop(State :: #?STATE{}) -> Result when
    Result :: {error, Reason :: term()}.
%% ====================================================================
comet_loop({error, Reason}) ->
    {error, Reason};

comet_loop(#?STATE{counter = Counter, default_row_id = DefaultRowId, spaces_details = SpacesDetails, gruid = GRUID, access_token = AccessToken} = State) ->
    NewCometLoopState =
        try
            receive
                {create_space, Name} ->
                    NewState = try
                                   {ok, SpaceId} = gr_users:create_space({user, AccessToken}, [{<<"name">>, Name}]),
                                   {ok, SpaceDetails} = gr_spaces:get_details({user, AccessToken}, SpaceId),
                                   {ok, Privileges} = gr_spaces:get_effective_user_privileges({user, AccessToken}, SpaceId, GRUID),
                                   opn_gui_utils:message(success, <<"Created Space ID: <b>", SpaceId/binary, "</b>">>),
                                   gr_adapter:synchronize_user_spaces({GRUID, AccessToken}),
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
                                   {ok, SpaceId} = gr_users:join_space({user, AccessToken}, [{<<"token">>, Token}]),
                                   {ok, SpaceDetails} = gr_spaces:get_details({user, AccessToken}, SpaceId),
                                   {ok, Privileges} = gr_spaces:get_effective_user_privileges({user, AccessToken}, SpaceId, GRUID),
                                   opn_gui_utils:message(success, <<"Joined Space ID: <b>", SpaceId/binary, "</b>">>),
                                   gr_adapter:synchronize_user_spaces({GRUID, AccessToken}),
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

                {set_default_space, RowId, #space_details{id = SpaceId}} ->
                    case gr_users:set_default_space({user, AccessToken}, [{<<"spaceId">>, SpaceId}]) of
                        ok ->
                            gr_adapter:synchronize_user_spaces({GRUID, AccessToken}),
                            gui_jq:hide(<<DefaultRowId/binary, "_label">>),
                            gui_jq:show(<<DefaultRowId/binary, "_default_option">>),
                            State#?STATE{default_row_id = RowId};
                        Other ->
                            ?error("Cannot set Space with ID ~p as a default Space: ~p", [SpaceId, Other]),
                            opn_gui_utils:message(error, <<"Cannot set Space with ID: <b>", SpaceId/binary, "</b> as a default Space.<br>Please try again later.">>),
                            State
                    end;

                {leave_space, RowId, SpaceId} ->
                    case gr_users:leave_space({user, AccessToken}, SpaceId) of
                        ok ->
                            opn_gui_utils:message(success, <<"Space with ID: <b>", SpaceId/binary, "</b> left successfully.">>),
                            gr_adapter:synchronize_user_spaces({GRUID, AccessToken}),
                            gui_jq:remove(RowId),
                            State#?STATE{spaces_details = lists:keydelete(RowId, 1, SpacesDetails)};
                        Other ->
                            ?error("Cannot leave Space with ID ~p: ~p", [SpaceId, Other]),
                            opn_gui_utils:message(error, <<"Cannot leave Space with ID: <b>", SpaceId/binary, "</b>.<br>Please try again later.">>),
                            State
                    end;

                {remove_space, RowId, SpaceId} ->
                    case gr_spaces:remove({user, AccessToken}, SpaceId) of
                        ok ->
                            opn_gui_utils:message(success, <<"Space with ID: <b>", SpaceId/binary, "</b> removed successfully.">>),
                            gr_adapter:synchronize_user_spaces({GRUID, AccessToken}),
                            gui_jq:remove(RowId),
                            State#?STATE{spaces_details = lists:keydelete(RowId, 1, SpacesDetails)};
                        Other ->
                            ?error("Cannot remove Space with ID ~p: ~p", [SpaceId, Other]),
                            opn_gui_utils:message(error, <<"Cannot remove Space with ID: <b>", SpaceId/binary, "</b>.<br>Please try again later.">>),
                            State
                    end;

                render_spaces_table ->
                    gui_jq:update(<<"spaces_table">>, spaces_table_collapsed(SpacesDetails)),
                    State;

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
    gui_jq:show(<<(NewCometLoopState#?STATE.default_row_id)/binary, "_label">>),
    gui_jq:hide(<<(NewCometLoopState#?STATE.default_row_id)/binary, "_default_option">>),
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
        GRUID = utils:ensure_binary(opn_gui_utils:get_global_user_id()),
        AccessToken = opn_gui_utils:get_access_token(),
        {ok, SpaceIds} = gr_adapter:synchronize_user_spaces({GRUID, AccessToken}),
        {ok, DefaultSpaceId} = gr_users:get_default_space({user, AccessToken}),
        {SpacesDetails, DefaultRowId, Counter} = lists:foldl(fun(SpaceId, {Rows, DefaultId, It}) ->
            {ok, SpaceDetails} = gr_spaces:get_details({user, AccessToken}, SpaceId),
            {ok, Privileges} = gr_spaces:get_effective_user_privileges({user, AccessToken}, SpaceId, GRUID),
            RowId = <<"space_", (integer_to_binary(It + 1))/binary>>,
            NewDefaultId = case SpaceId of
                               DefaultSpaceId -> RowId;
                               _ -> DefaultId
                           end,
            {
                [{RowId, Privileges, SpaceDetails} | Rows],
                NewDefaultId,
                It + 1
            }
        end, {[], <<"">>, 0}, SpaceIds),

        gui_jq:wire(#api{name = "create_space", tag = "create_space"}, false),
        gui_jq:wire(#api{name = "join_space", tag = "join_space"}, false),
        gui_jq:wire(#api{name = "leave_space", tag = "leave_space"}, false),
        gui_jq:wire(#api{name = "remove_space", tag = "remove_space"}, false),
        gui_jq:bind_key_to_click_on_class(<<"13">>, <<"confirm">>),

        {ok, Pid} = gui_comet:spawn(fun() ->
            comet_loop(#?STATE{counter = Counter, default_row_id = DefaultRowId, spaces_details = SpacesDetails, gruid = GRUID, access_token = AccessToken})
        end),
        Pid ! render_spaces_table,
        put(?COMET_PID, Pid)
    catch
        _:Reason ->
            ?error("Cannot initialize page ~p: ~p", [?MODULE, Reason]),
            gui_jq:hide(<<"main_spinner">>),
            opn_gui_utils:message(error, <<"Cannot fetch Spaces.<br>Please try again later.">>)
    end;

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

event({manage_space, SpaceId}) ->
    gui_jq:redirect(<<"space?id=", SpaceId/binary>>);

event({set_privileges, SpaceId}) ->
    gui_jq:redirect(<<"privileges/space?id=", SpaceId/binary>>);

event({leave_space, RowId, #space_details{id = SpaceId, name = SpaceName}}) ->
    Message = <<"Are you sure you want to leave Space:<br><b>", (gui_str:html_encode(SpaceName))/binary, " (", SpaceId/binary, ") </b>?">>,
    Script = <<"leave_space(['", RowId/binary, "','", SpaceId/binary, "']);">>,
    gui_jq:dialog_popup(<<"Leave Space">>, Message, Script, <<"btn-inverse">>);

event({remove_space, RowId, #space_details{id = SpaceId, name = SpaceName}}) ->
    Message = <<"Are you sure you want to remove Space:<br><b>", (gui_str:html_encode(SpaceName))/binary, " (", SpaceId/binary, ") </b>?">>,
    Script = <<"remove_space(['", RowId/binary, "','", SpaceId/binary, "']);">>,
    gui_jq:dialog_popup(<<"Remove Space">>, Message, Script, <<"btn-inverse">>);

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