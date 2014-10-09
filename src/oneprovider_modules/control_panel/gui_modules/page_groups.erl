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
-record(?STATE, {counter = 1, groups_details = [], gruid, access_token}).

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
            #dtl{file = "bare", app = ?APP_Name, bindings = [{title, title()}, {body, body()}, {custom, custom()}]}
    end.


%% title/0
%% ====================================================================
%% @doc Page title.
-spec title() -> binary().
%% ====================================================================
title() -> <<"Groups">>.


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
-spec body() -> [#panel{}].
%% ====================================================================
body() ->
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
            style = ?MESSAGE_STYLE,
            class = <<"dialog dialog-success">>
        },
        #panel{
            id = <<"error_message">>,
            style = ?MESSAGE_STYLE,
            class = <<"dialog dialog-danger">>
        },
        #panel{
            style = <<"position: relative; margin-top: 160px; margin-bottom: 100px;">>,
            body =
            [
                #h6{
                    style = <<"font-size: x-large; margin: 0 auto; margin-top: 160px; text-align: center;">>,
                    body = <<"Manage groups">>
                },
                #panel{
                    style = <<"margin: 0 auto; width: 50%; margin-top: 30px; text-align: center;">>,
                    body = lists:map(fun({ButtonId, ButtonPostback, ButtonBody}) ->
                        #button{
                            id = ButtonId,
                            postback = ButtonPostback,
                            class = <<"btn btn-inverse btn-small">>,
                            style = <<"margin-right: 0.5em; margin-left: 0.5em;">>,
                            body = ButtonBody
                        }
                    end, [
                        {<<"create_group_button">>, create_group, <<"Create group">>},
                        {<<"join_group_button">>, join_group, <<"Join group">>}
                    ])
                },
                #table{
                    class = <<"table table-bordered table-striped">>,
                    style = <<"width: 50%; margin: 0 auto; margin-top: 30px; table-layout: fixed;">>,
                    body = #tbody{
                        id = <<"groups_table">>,
                        body = groups_table_collapsed([])
                    }
                }
            ]
        }
    ].


%% groups_table_collapsed/1
%% ====================================================================
%% @doc Renders collapsed groups details table.
%% @end
-spec groups_table_collapsed(GroupsDetails :: [{Id :: binary(), GroupDetails :: #group_details{}}]) -> Result when
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
-spec groups_table_expanded(GroupsDetails :: [{Id :: binary(), GroupDetails :: #group_details{}}]) -> Result when
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
-spec table(Details :: [{Id :: binary(), Details :: #group_details{}}], TableName :: binary(), NavigationBody :: #link{}, RenderRowFunction :: function()) -> Result when
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


%% group_row_collapsed/3
%% ====================================================================
%% @doc Renders collapsed group settings row.
%% @end
-spec group_row_collapsed(RowId :: binary(), Privileges :: [binary()], GroupDetails :: #group_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
group_row_collapsed(RowId, Privileges, #group_details{id = GroupId, name = GroupName} = GroupDetails) ->
    [
        #td{
            style = ?CONTENT_COLUMN_STYLE,
            body = #span{
                style = ?DETAIL_STYLE,
                body = <<"<b>", GroupName/binary, "</b> (", GroupId/binary, ")">>
            }
        },
        #td{
            style = ?NAVIGATION_COLUMN_STYLE,
            body = opn_gui_utils:expand_button({message, {expand_group_row, RowId, Privileges, GroupDetails}})
        }
    ].


%% group_row_expanded/3
%% ====================================================================
%% @doc Renders expanded group settings row.
%% @end
-spec group_row_expanded(RowId :: binary(), Privileges :: [binary()], GroupDetails :: #group_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
group_row_expanded(RowId, Privileges, #group_details{id = GroupId, name = GroupName} = GroupDetails) ->
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
        {<<"standard">>, <<RowId/binary, "_manage_option">>, <<"Manage group">>, {manage_group, GroupId}, <<"icomoon-cog">>},
        {<<"group_set_privileges">>, <<RowId/binary, "_set_privileges">>, <<"Set privileges">>, {set_privileges, GroupId}, <<"icomoon-lock">>},
        {<<"standard">>, <<RowId/binary, "_leave_option">>, <<"Leave group">>, {leave_group, RowId, GroupDetails}, <<"icomoon-exit">>},
        {<<"group_remove">>, <<RowId/binary, "_remove_option">>, <<"Remove group">>, {remove_group, RowId, GroupDetails}, <<"icomoon-remove">>}
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
                    {<<"Name">>, GroupName},
                    {<<"Group ID">>, GroupId},
                    {<<"Settings">>, SettingsIcons}
                ])
            }
        },
        #td{
            style = ?NAVIGATION_COLUMN_STYLE,
            body = opn_gui_utils:collapse_button({message, {collapse_group_row, RowId, Privileges, GroupDetails}})
        }
    ].


%% add_group_row/3
%% ====================================================================
%% @doc Adds collapsed Group settings row to Groups settings table.
-spec add_group_row(RowId :: binary(), Privileges :: [binary()], GroupDetails :: #group_details{}) -> Result when
    Result :: ok.
%% ====================================================================
add_group_row(RowId, Privileges, GroupDetails) ->
    Row = #tr{
        id = RowId,
        cells = group_row_collapsed(RowId, Privileges, GroupDetails)
    },
    gui_jq:insert_bottom(<<"groups_table">>, Row).


%% ====================================================================
%% Events handling
%% ====================================================================

%% comet_loop/1
%% ====================================================================
%% @doc Handles groups management actions.
-spec comet_loop(State :: #?STATE{}) -> Result when
    Result :: {error, Reason :: term()}.
%% ====================================================================
comet_loop({error, Reason}) ->
    {error, Reason};

comet_loop(#?STATE{counter = Counter, groups_details = GroupsDetails, gruid = GRUID, access_token = AccessToken} = State) ->
    NewCometLoopState =
        try
            receive
                {create_group, Name} ->
                    NewState = try
                                   {ok, GroupId} = gr_users:create_group({user, AccessToken}, [{<<"name">>, Name}]),
                                   {ok, GroupDetails} = gr_groups:get_details({user, AccessToken}, GroupId),
                                   {ok, Privileges} = gr_groups:get_user_privileges({user, AccessToken}, GroupId, GRUID),
                                   opn_gui_utils:message(<<"ok_message">>, <<"Created Group ID: <b>", GroupId/binary, "</b>">>),
                                   RowId = <<"group_", (integer_to_binary(Counter + 1))/binary>>,
                                   add_group_row(RowId, Privileges, GroupDetails),
                                   State#?STATE{counter = Counter + 1, groups_details = GroupsDetails ++ [{RowId, Privileges, GroupDetails}]}
                               catch
                                   _:Other ->
                                       ?error("Cannot create group ~p: ~p", [Name, Other]),
                                       opn_gui_utils:message(<<"error_message">>, <<"Cannot create group: <b>", Name/binary, "</b>.<br>Please try again later.">>),
                                       State
                               end,
                    gui_jq:prop(<<"create_group_button">>, <<"disabled">>, <<"">>),
                    NewState;

                {join_group, Token} ->
                    NewState = try
                                   {ok, GroupId} = gr_users:join_group({user, AccessToken}, [{<<"token">>, Token}]),
                                   {ok, GroupDetails} = gr_groups:get_details({user, AccessToken}, GroupId),
                                   {ok, Privileges} = gr_groups:get_user_privileges({user, AccessToken}, GroupId, GRUID),
                                   opn_gui_utils:message(<<"ok_message">>, <<"Joined Group ID: <b>", GroupId/binary, "</b>">>),
                                   RowId = <<"group_", (integer_to_binary(Counter + 1))/binary>>,
                                   add_group_row(RowId, Privileges, GroupDetails),
                                   State#?STATE{counter = Counter + 1, groups_details = GroupsDetails ++ [{RowId, Privileges, GroupDetails}]}
                               catch
                                   _:Other ->
                                       ?error("Cannot join group using token ~p: ~p", [Token, Other]),
                                       opn_gui_utils:message(<<"error_message">>, <<"Cannot join group using token: <b>", Token/binary, "</b>.<br>Please try again later.">>),
                                       State
                               end,
                    gui_jq:prop(<<"join_group_button">>, <<"disabled">>, <<"">>),
                    NewState;

                {leave_group, RowId, GroupId} ->
                    case gr_users:leave_group({user, AccessToken}, GroupId) of
                        ok ->
                            opn_gui_utils:message(<<"ok_message">>, <<"Group with ID: <b>", GroupId/binary, "</b> left successfully.">>),
                            gui_jq:remove(RowId),
                            State#?STATE{groups_details = lists:keydelete(RowId, 1, GroupsDetails)};
                        Other ->
                            ?error("Cannot leave group with ID ~p: ~p", [GroupId, Other]),
                            opn_gui_utils:message(<<"error_message">>, <<"Cannot leave group with ID: <b>", GroupId/binary, "</b>.<br>Please try again later.">>),
                            State
                    end;

                {remove_group, RowId, GroupId} ->
                    case gr_groups:remove({user, AccessToken}, GroupId) of
                        ok ->
                            opn_gui_utils:message(<<"ok_message">>, <<"Group with ID: <b>", GroupId/binary, "</b> removed successfully.">>),
                            gui_jq:remove(RowId),
                            State#?STATE{groups_details = lists:keydelete(RowId, 1, GroupsDetails)};
                        Other ->
                            ?error("Cannot remove group with ID ~p: ~p", [GroupId, Other]),
                            opn_gui_utils:message(<<"error_message">>, <<"Cannot remove group with ID: <b>", GroupId/binary, "</b>.<br>Please try again later.">>),
                            State
                    end;

                render_groups_table ->
                    gui_jq:update(<<"groups_table">>, groups_table_collapsed(GroupsDetails)),
                    State;

                Event ->
                    case Event of
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
            opn_gui_utils:message(<<"error_message">>, <<"There has been an error in comet process. Please refresh the page.">>),
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
        GRUID = utils:ensure_binary(opn_gui_utils:get_global_user_id()),
        AccessToken = opn_gui_utils:get_access_token(),
        {ok, GroupIds} = gr_users:get_groups({user, AccessToken}),
        {GroupsDetails, Counter} = lists:foldl(fun(GroupId, {Rows, It}) ->
            {ok, GroupDetails} = gr_groups:get_details({user, AccessToken}, GroupId),
            {ok, Privileges} = gr_groups:get_user_privileges({user, AccessToken}, GroupId, GRUID),
            RowId = <<"group_", (integer_to_binary(It + 1))/binary>>,
            {
                [{RowId, Privileges, GroupDetails} | Rows],
                It + 1
            }
        end, {[], 0}, GroupIds),

        gui_jq:wire(#api{name = "create_group", tag = "create_group"}, false),
        gui_jq:wire(#api{name = "join_group", tag = "join_group"}, false),
        gui_jq:wire(#api{name = "leave_group", tag = "leave_group"}, false),
        gui_jq:wire(#api{name = "remove_group", tag = "remove_group"}, false),
        gui_jq:bind_key_to_click_on_class(<<"13">>, <<"confirm">>),

        {ok, Pid} = gui_comet:spawn(fun() ->
            comet_loop(#?STATE{counter = Counter, groups_details = GroupsDetails, gruid = GRUID, access_token = AccessToken})
        end),
        Pid ! render_groups_table,
        put(?COMET_PID, Pid)
    catch
        _:Reason ->
            ?error_stacktrace("Cannot initialize page ~p: ~p", [?MODULE, Reason]),
            gui_jq:hide(<<"main_spinner">>),
            opn_gui_utils:message(<<"error_message">>, <<"Cannot fetch groups.<br>Please try again later.">>)
    end;

event(create_group) ->
    Message = <<"<div style=\"margin: 0 auto; width: 80%;\">",
    "<p id=\"create_group_alert\" style=\"width: 100%; color: red; font-size: medium; text-align: center; display: none;\"></p>",
    "<input id=\"create_group_name\" type=\"text\" style=\"width: 100%;\" placeholder=\"Name\">",
    "</div>">>,
    Script = <<"var alert = $(\"#create_group_alert\");",
    "var name = $.trim($(\"#create_group_name\").val());",
    "if(name.length == 0) { alert.html(\"Please provide group name.\"); alert.fadeIn(300); return false; }",
    "else { create_group([name]); return true; }">>,
    gui_jq:dialog_popup(<<"Create group">>, Message, Script, <<"btn-inverse">>),
    gui_jq:wire(<<"box.on('shown',function(){ $(\"#create_group_name\").focus(); });">>);

event(join_group) ->
    Message = <<"<div style=\"margin: 0 auto; width: 80%;\">",
    "<p id=\"join_group_alert\" style=\"width: 100%; color: red; font-size: medium; text-align: center; display: none;\"></p>",
    "<input id=\"join_group_token\" type=\"text\" style=\"width: 100%;\" placeholder=\"Token\">",
    "</div>">>,
    Script = <<"var alert = $(\"#join_group_alert\");",
    "var token = $.trim($(\"#join_group_token\").val());",
    "if(token.length == 0) { alert.html(\"Please provide group token.\"); alert.fadeIn(300); return false; }",
    "else { join_group([token]); return true; }">>,
    gui_jq:dialog_popup(<<"Join group">>, Message, Script, <<"btn-inverse">>),
    gui_jq:wire(<<"box.on('shown',function(){ $(\"#join_group_token\").focus(); });">>);

event({manage_group, GroupId}) ->
    gui_jq:redirect(<<"group?id=", GroupId/binary>>);

event({set_privileges, GroupId}) ->
    gui_jq:redirect(<<"privileges/group?id=", GroupId/binary>>);

event({leave_group, RowId, #group_details{id = GroupId, name = GroupName}}) ->
    Message = <<"Are you sure you want to leave group:<br><b>", GroupName/binary, " (", GroupId/binary, ") </b>?">>,
    Script = <<"leave_group(['", RowId/binary, "','", GroupId/binary, "']);">>,
    gui_jq:dialog_popup(<<"Leave group">>, Message, Script, <<"btn-inverse">>);

event({remove_group, RowId, #group_details{id = GroupId, name = GroupName}}) ->
    Message = <<"Are you sure you want to remove group:<br><b>", GroupName/binary, " (", GroupId/binary, ") </b>?">>,
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
api_event("create_group", Args, _) ->
    [Name] = mochijson2:decode(Args),
    get(?COMET_PID) ! {create_group, Name},
    gui_jq:prop(<<"create_group_button">>, <<"disabled">>, <<"disabled">>),
    gui_jq:show(<<"main_spinner">>);

api_event("join_group", Args, _) ->
    [Token] = mochijson2:decode(Args),
    get(?COMET_PID) ! {join_group, Token},
    gui_jq:prop(<<"join_group_button">>, <<"disabled">>, <<"disabled">>),
    gui_jq:show(<<"main_spinner">>);

api_event(Function, Args, _) ->
    [RowId, ObjectId] = mochijson2:decode(Args),
    get(?COMET_PID) ! {list_to_existing_atom(Function), RowId, ObjectId},
    gui_jq:show(<<"main_spinner">>).