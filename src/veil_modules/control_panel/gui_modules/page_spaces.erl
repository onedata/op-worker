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

-module(page_spaces).
-include("veil_modules/control_panel/common.hrl").
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
-define(LABEL_STYLE, <<"margin: 0 auto;">>).
-define(PARAGRAPH_STYLE, <<"margin: 0 auto;">>).
-define(TABLE_STYLE, <<"border-width: 0; width: 100%; border-collapse: inherit;">>).

%% Comet process pid
-define(COMET_PID, comet_pid).

%% Comet process state
-define(STATE, state).
-record(?STATE, {counter = 1, expanded = false, space_rows = []}).

%% Space row state
-define(SPACE_ROW, space_row).
-record(?SPACE_ROW, {id, expanded, default}).

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
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
        false ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, custom()}]}
    end.


%% title/0
%% ====================================================================
%% @doc Page title.
-spec title() -> binary().
%% ====================================================================
title() -> <<"Spaces">>.


%% custom/0
%% ====================================================================
%% @doc This will be placed instead of {{custom}} tag in template.
-spec custom() -> binary().
%% ====================================================================
custom() ->
    <<"<script src='/js/bootbox.min.js' type='text/javascript' charset='utf-8'></script>">>.


%% body/0
%% ====================================================================
%% @doc This will be placed instead of {{body}} tag in template.
-spec body() -> [#panel{}].
%% ====================================================================
body() ->
    [
        #panel{
            id = <<"main_spinner">>,
            style = <<"position: absolute; top: 12px; left: 17px; z-index: 1234; width: 32px; display: none;">>,
            body = #image{
                image = <<"/images/spinner.gif">>
            }
        },
        vcn_gui_utils:top_menu(spaces_tab),
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
                    style = <<"font-size: x-large; margin: 0 auto; margin-top: 160px; width: 162px;">>,
                    body = <<"Manage Spaces">>
                },
                #panel{
                    style = <<"margin: 0 auto; width: 50%; margin-top: 30px; text-align: center;">>,
                    body = [
                        #button{
                            id = <<"create_space_button">>,
                            postback = create_space,
                            class = <<"btn btn-primary btn-small">>,
                            style = <<"margin-right: 1em;">>,
                            body = <<"Create Space">>
                        },
                        #button{
                            id = <<"join_space_button">>,
                            postback = join_space,
                            class = <<"btn btn-primary btn-small">>,
                            style = <<"margin-left: 1em;">>,
                            body = <<"Join Space">>
                        }
                    ]
                },
                #table{
                    class = <<"table table-bordered table-striped">>,
                    style = <<"width: 50%; margin: 0 auto; margin-top: 30px; table-layout: fixed;">>,
                    body = #tbody{
                        id = <<"spaces">>,
                        body = #tr{
                            cells = [
                                #th{
                                    style = <<"font-size: large;">>,
                                    body = <<"Spaces">>
                                },
                                #th{
                                    style = ?NAVIGATION_COLUMN_STYLE,
                                    body = vcn_gui_utils:spinner()
                                }
                            ]
                        }
                    }
                }
            ]
        }
    ].


%% spaces_table/2
%% ====================================================================
%% @doc Renders collapsed Spaces settings table.
-spec spaces_table(Expanded :: boolean(), Spaces :: [{SpaceId :: binary(), SpaceRow :: #?SPACE_ROW{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
spaces_table(Expanded, Spaces) ->
    Header = #tr{
        id = <<"spaces_header">>,
        cells = [
            #th{
                style = <<"font-size: large;">>,
                body = <<"Spaces">>
            },
            #th{
                id = <<"space_all_option">>,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = case Expanded of
                           true ->
                               vcn_gui_utils:collapse_button(<<"Collapse All">>, {spaces_table_collapse, <<"space_all_option">>});
                           false ->
                               vcn_gui_utils:expand_button(<<"Expand All">>, {spaces_table_expand, <<"space_all_option">>})
                       end
            }
        ]
    },

    Rows = lists:map(fun
        ({SpaceId, #?SPACE_ROW{id = RowId, expanded = true, default = Default}}) ->
            #tr{
                id = RowId,
                cells = space_row_expanded(SpaceId, RowId, Default)
            };
        ({SpaceId, #?SPACE_ROW{id = RowId, expanded = false, default = Default}}) ->
            #tr{
                id = RowId,
                cells = space_row_collapsed(SpaceId, RowId, Default)
            }
    end, Spaces),

    [Header | Rows].


%% space_row/4
%% ====================================================================
%% @doc Renders Space settings row.
-spec space_row(SpaceId :: binary(), RowId :: binary(), Default :: boolean(), Expanded :: boolean()) -> Result when
    Result :: [#td{}].
%% ====================================================================
space_row(SpaceId, RowId, Default, false) ->
    space_row_collapsed(SpaceId, RowId, Default);

space_row(SpaceId, RowId, Default, true) ->
    space_row_expanded(SpaceId, RowId, Default).


%% space_row_collapsed/3
%% ====================================================================
%% @doc Renders collapsed Space settings row.
-spec space_row_collapsed(SpaceId :: binary(), RowId :: binary(), Default :: boolean()) -> Result when
    Result :: [#td{}].
%% ====================================================================
space_row_collapsed(SpaceId, RowId, Default) ->
    try
        {ok, #space_details{name = Name}} = gr_spaces:get_details({user, vcn_gui_utils:get_access_token()}, SpaceId),
        OptionId = <<RowId/binary, "_option">>,
        LabelId = <<RowId/binary, "_label">>,
        LabelBody = case Default of
                        true -> <<"Default">>;
                        _ -> <<"">>
                    end,
        [
            #td{
                style = ?CONTENT_COLUMN_STYLE,
                body = #table{
                    style = ?TABLE_STYLE,
                    body = [
                        #tr{
                            cells = [
                                #td{
                                    style = <<"border-width: 0; text-align: left; padding-left: 0; padding-right: 1em;">>,
                                    body = #p{
                                        style = ?PARAGRAPH_STYLE,
                                        body = <<"<b>", Name/binary, "</b> ( ", SpaceId/binary, " )">>
                                    }
                                },
                                #td{
                                    style = <<"border-width: 0; padding-left: 0;">>,
                                    body = #label{
                                        id = LabelId,
                                        style = ?LABEL_STYLE,
                                        class = <<"label label-large label-success">>,
                                        body = LabelBody
                                    }
                                }
                            ]
                        }
                    ]
                }
            },
            #td{
                id = OptionId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = vcn_gui_utils:expand_button({space_row_expand, SpaceId, RowId, OptionId, Default})
            }
        ]
    catch
        _:Reason ->
            ?error("Cannot fetch details of Space with ID: ~p: ~p", [SpaceId, Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch details of Space with ID: <b>", SpaceId/binary, "</b>.<br>Please try again later.">>),
            []
    end.


%% space_row_expanded/3
%% ====================================================================
%% @doc Renders expanded Space settings row.
-spec space_row_expanded(SpaceId :: binary(), RowId :: binary(), Default :: boolean()) -> Result when
    Result :: [#td{}].
%% ====================================================================
space_row_expanded(SpaceId, RowId, Default) ->
    try
        {ok, #space_details{name = Name}} = gr_spaces:get_details({user, vcn_gui_utils:get_access_token()}, SpaceId),
        OptionId = <<RowId/binary, "_option">>,
        LabelId = <<RowId/binary, "_label">>,
        LabelBody = case Default of
                        true -> <<"Default">>;
                        _ -> <<"">>
                    end,
        DefaultSpaceOptionId = <<RowId/binary, "_default_option">>,
        ManageSpaceOptionId = <<RowId/binary, "_manage_option">>,
        LeaveSpaceOptionId = <<RowId/binary, "_leave_option">>,
        DeleteSpaceOptionId = <<RowId/binary, "_delete_option">>,
        SettingsIcons = lists:foldl(fun
            ({false, _, _, _, _}, Settings) ->
                Settings;
            ({true, LinkId, LinkTitle, LinkPostback, SpanClass}, Settings) ->
                [#link{
                    id = LinkId,
                    title = LinkTitle,
                    style = <<"font-size: large; margin-right: 1em;">>,
                    class = <<"glyph-link">>,
                    postback = LinkPostback,
                    body = #span{
                        class = SpanClass
                    }
                } | Settings]
        end, [], [
            {true, DefaultSpaceOptionId, <<"Delete Space">>, {delete_space, Name, SpaceId, RowId, DeleteSpaceOptionId}, <<"fui-trash">>},
            {true, LeaveSpaceOptionId, <<"Leave Space">>, {leave_space, Name, SpaceId, RowId, LeaveSpaceOptionId}, <<"fui-exit">>},
            {true, ManageSpaceOptionId, <<"Manage Space">>, {manage_space, SpaceId}, <<"fui-gear">>},
            {not Default, DeleteSpaceOptionId, <<"Set as a default Space">>, {set_default_space, SpaceId, DefaultSpaceOptionId}, <<"fui-home">>}
        ]),
        DefaultLabel = #label{
            id = LabelId,
            style = <<"margin: 0 auto;">>,
            class = <<"label label-large label-success pull-right">>,
            body = LabelBody
        },
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
                                        style = ?LABEL_STYLE,
                                        class = <<"label label-large label-inverse">>,
                                        body = Description
                                    }
                                },
                                #td{
                                    style = ?MAIN_STYLE,
                                    body = #p{
                                        style = ?PARAGRAPH_STYLE,
                                        body = Main
                                    }
                                }
                            ]
                        }
                    end, [
                        {<<"Name">>, [Name, DefaultLabel]},
                        {<<"Space ID">>, SpaceId},
                        {<<"Settings">>, SettingsIcons}
                    ])
                }
            },
            #td{
                id = OptionId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = vcn_gui_utils:collapse_button({space_row_collapse, SpaceId, RowId, OptionId, Default})
            }
        ]
    catch
        _:Reason ->
            ?error("Cannot fetch details of Space with ID: ~p: ~p", [SpaceId, Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch details of Space with ID: <b>", SpaceId/binary, "</b>.<br>Please try again later.">>),
            []
    end.


%% add_space_row/2
%% ====================================================================
%% @doc Adds collapsed Space settings row to Spaces settings table.
-spec add_space_row(SpaceId :: binary(), RowId :: binary()) -> Result when
    Result :: ok.
%% ====================================================================
add_space_row(SpaceId, RowId) ->
    Row = #tr{
        id = RowId,
        cells = space_row_collapsed(SpaceId, RowId, false)
    },
    gui_jq:insert_bottom(<<"spaces">>, Row).


%% get_space_row_id/1
%% ====================================================================
%% @doc Returns Space row ID.
%% @end
-spec get_space_row_id(Counter :: integer()) -> string().
%% ====================================================================
get_space_row_id(Counter) ->
    <<"space_", (integer_to_binary(Counter))/binary>>.


%% comet_loop/1
%% ====================================================================
%% @doc Handles spaces management actions.
-spec comet_loop(State :: #?STATE{}) -> Result when
    Result :: {error, Reason :: term()}.
%% ====================================================================
comet_loop({error, Reason}) ->
    {error, Reason};

comet_loop(#?STATE{counter = Counter, expanded = Expanded, space_rows = SpaceRows} = State) ->
    NewCometLoopState = try
        receive
            {create_space, Name} ->
                gui_jq:show(<<"main_spinner">>),
                NewState =
                    case gr_users:create_space({user, vcn_gui_utils:get_access_token()}, [{<<"name">>, Name}]) of
                        {ok, SpaceId} ->
                            vcn_gui_utils:message(<<"ok_message">>, <<"Created Space ID: <b>", SpaceId/binary, "</b>">>),
                            gr_adapter:synchronize_user_spaces({vcn_gui_utils:get_global_user_id(), vcn_gui_utils:get_access_token()}),
                            RowId = get_space_row_id(Counter + 1),
                            add_space_row(SpaceId, RowId),
                            State#?STATE{counter = Counter + 1, space_rows = SpaceRows ++ [{SpaceId, #?SPACE_ROW{id = RowId, expanded = false, default = false}}]};
                        Other ->
                            ?error("Cannot create Space ~p: ~p", [Name, Other]),
                            vcn_gui_utils:message(<<"error_message">>, <<"Cannot create Space: <b>", Name/binary, "</b>.<br>Please try again later.">>),
                            State
                    end,
                gui_jq:hide(<<"main_spinner">>),
                gui_jq:prop(<<"create_space_button">>, <<"disabled">>, <<"">>),
                gui_comet:flush(),
                NewState;

            {join_space, Token} ->
                gui_jq:show(<<"main_spinner">>),
                NewState =
                    case gr_users:join_space({user, vcn_gui_utils:get_access_token()}, [{<<"token">>, Token}]) of
                        {ok, SpaceId} ->
                            vcn_gui_utils:message(<<"ok_message">>, <<"Joined Space ID: <b>", SpaceId/binary, "</b>">>),
                            gr_adapter:synchronize_user_spaces({vcn_gui_utils:get_global_user_id(), vcn_gui_utils:get_access_token()}),
                            RowId = get_space_row_id(Counter + 1),
                            add_space_row(SpaceId, RowId),
                            State#?STATE{counter = Counter + 1, space_rows = SpaceRows ++ [{SpaceId, #?SPACE_ROW{id = RowId, expanded = false, default = false}}]};
                        Other ->
                            ?error("Cannot join Space using token ~p: ~p", [Token, Other]),
                            vcn_gui_utils:message(<<"error_message">>, <<"Cannot join Space using token: <b>", Token/binary, "</b>.<br>Please try again later.">>),
                            State
                    end,
                gui_jq:hide(<<"main_spinner">>),
                gui_jq:prop(<<"join_space_button">>, <<"disabled">>, <<"">>),
                gui_comet:flush(),
                NewState;

            {set_default_space, SpaceId, OptionId} ->
                NewState =
                    case gr_users:set_default_space({user, vcn_gui_utils:get_access_token()}, [{<<"spaceId">>, SpaceId}]) of
                        ok ->
                            gr_adapter:synchronize_user_spaces({vcn_gui_utils:get_global_user_id(), vcn_gui_utils:get_access_token()}),
                            [{DefaultSpaceId, DefaultSpaceRow} | _] = SpaceRows,
                            NewDefaultSpaceRow = proplists:get_value(SpaceId, SpaceRows),
                            NewSpaceRows = [
                                {SpaceId, NewDefaultSpaceRow#?SPACE_ROW{default = true}} |
                                lists:map(fun({Id, Row}) ->
                                    {Id, Row#?SPACE_ROW{default = false}}
                                end, proplists:delete(SpaceId, SpaceRows))
                            ],
                            gui_jq:update(DefaultSpaceRow#?SPACE_ROW.id, space_row(DefaultSpaceId, DefaultSpaceRow#?SPACE_ROW.id, false, DefaultSpaceRow#?SPACE_ROW.expanded)),
                            gui_jq:remove(NewDefaultSpaceRow#?SPACE_ROW.id),
                            gui_comet:flush(),
                            gui_jq:insert_after(<<"spaces_header">>, #tr{
                                id = NewDefaultSpaceRow#?SPACE_ROW.id,
                                cells = gui_jq:update(NewDefaultSpaceRow#?SPACE_ROW.id, space_row(SpaceId, NewDefaultSpaceRow#?SPACE_ROW.id, true, NewDefaultSpaceRow#?SPACE_ROW.expanded))
                            }),
                            State#?STATE{space_rows = NewSpaceRows};
                        Other ->
                            ?error("Cannot set Space with ID ~p as a default Space: ~p", [SpaceId, Other]),
                            vcn_gui_utils:message(<<"error_message">>, <<"Cannot set Space with ID: <b>", SpaceId/binary, "</b> as a default Space.<br>Please try again later.">>),
                            gui_jq:update(OptionId, #span{class = <<"fui-home">>}),
                            State
                    end,
                gui_comet:flush(),
                NewState;

            {leave_space, SpaceId, RowId, OptionId} ->
                NewState =
                    case gr_users:leave_space({user, vcn_gui_utils:get_access_token()}, SpaceId) of
                        ok ->
                            vcn_gui_utils:message(<<"ok_message">>, <<"Space with ID: <b>", SpaceId/binary, "</b> left successfully.">>),
                            gr_adapter:synchronize_user_spaces({vcn_gui_utils:get_global_user_id(), vcn_gui_utils:get_access_token()}),
                            gui_jq:remove(RowId),
                            State#?STATE{space_rows = proplists:delete(SpaceId, SpaceRows)};
                        Other ->
                            ?error("Cannot leave Space with ID ~p: ~p", [SpaceId, Other]),
                            vcn_gui_utils:message(<<"error_message">>, <<"Cannot leave Space with ID: <b>", SpaceId/binary, "</b>.<br>Please try again later.">>),
                            gui_jq:update(OptionId, #span{class = <<"fui-exit">>}),
                            State
                    end,
                gui_comet:flush(),
                NewState;

            {delete_space, SpaceId, RowId, OptionId} ->
                NewState =
                    case gr_spaces:remove({user, vcn_gui_utils:get_access_token()}, SpaceId) of
                        ok ->
                            vcn_gui_utils:message(<<"ok_message">>, <<"Space with ID: <b>", SpaceId/binary, "</b> deleted successfully.">>),
                            gr_adapter:synchronize_user_spaces({vcn_gui_utils:get_global_user_id(), vcn_gui_utils:get_access_token()}),
                            gui_jq:remove(RowId),
                            State#?STATE{space_rows = proplists:delete(SpaceId, SpaceRows)};
                        Other ->
                            ?error("Cannot delete Space with ID ~p: ~p", [SpaceId, Other]),
                            vcn_gui_utils:message(<<"error_message">>, <<"Cannot delete Space with ID: <b>", SpaceId/binary, "</b>.<br>Please try again later.">>),
                            gui_jq:update(OptionId, #span{class = <<"fui-trash">>}),
                            State
                    end,
                gui_comet:flush(),
                NewState;

            render_spaces_table ->
                gui_jq:update(<<"spaces">>, spaces_table(Expanded, SpaceRows)),
                gui_comet:flush(),
                State;

            spaces_table_collapse ->
                NewSpaceRows = lists:map(fun({SpaceId, SpaceRow}) ->
                    {SpaceId, SpaceRow#?SPACE_ROW{expanded = false}}
                end, SpaceRows),
                self() ! render_spaces_table,
                State#?STATE{expanded = false, space_rows = NewSpaceRows};

            spaces_table_expand ->
                NewSpaceRows = lists:map(fun({SpaceId, SpaceRow}) ->
                    {SpaceId, SpaceRow#?SPACE_ROW{expanded = true}}
                end, SpaceRows),
                self() ! render_spaces_table,
                State#?STATE{expanded = true, space_rows = NewSpaceRows};

            {space_row_collapse, SpaceId, RowId, Default} ->
                gui_jq:update(RowId, space_row_collapsed(SpaceId, RowId, Default)),
                gui_comet:flush(),
                State#?STATE{space_rows = lists:foldr(fun(SpaceRow, NewSpaceRows) ->
                    case SpaceRow of
                        {SpaceId, Row} -> [{SpaceId, Row#?SPACE_ROW{expanded = false}} | NewSpaceRows];
                        _ -> [SpaceRow | NewSpaceRows]
                    end
                end, [], SpaceRows)};

            {space_row_expand, SpaceId, RowId, Default} ->
                gui_jq:update(RowId, space_row_expanded(SpaceId, RowId, Default)),
                gui_comet:flush(),
                State#?STATE{space_rows = lists:foldr(fun(SpaceRow, NewSpaceRows) ->
                    case SpaceRow of
                        {SpaceId, Row} -> [{SpaceId, Row#?SPACE_ROW{expanded = true}} | NewSpaceRows];
                        _ -> [SpaceRow | NewSpaceRows]
                    end
                end, [], SpaceRows)}
        end
                        catch Type:Reason ->
                            ?error("Comet process exception: ~p:~p", [Type, Reason]),
                            vcn_gui_utils:message(<<"error_message">>, <<"There has been an error in comet process. Please refresh the page.">>),
                            gui_comet:flush(),
                            {error, Reason}
                        end,
    ?MODULE:comet_loop(NewCometLoopState).


%% event/1
%% ====================================================================
%% @doc Handles page events.
-spec event(Event :: term()) -> no_return().
%% ====================================================================
event(init) ->
    try
        {ok, SpaceIds} = gr_adapter:synchronize_user_spaces({vcn_gui_utils:get_global_user_id(), vcn_gui_utils:get_access_token()}),
        {ok, DefaultSpaceId} = gr_users:get_default_space({user, vcn_gui_utils:get_access_token()}),
        gui_jq:wire(#api{name = "createSpace", tag = "createSpace"}, false),
        gui_jq:wire(#api{name = "joinSpace", tag = "joinSpace"}, false),
        gui_jq:wire(#api{name = "leaveSpace", tag = "leaveSpace"}, false),
        gui_jq:wire(#api{name = "deleteSpace", tag = "deleteSpace"}, false),
        gui_jq:bind_key_to_click_on_class(<<"13">>, <<"button.confirm">>),
        SpaceRows = lists:map(fun({SpaceId, Counter}) ->
            {SpaceId, #?SPACE_ROW{id = get_space_row_id(Counter), expanded = false, default = (SpaceId =:= DefaultSpaceId)}}
        end, lists:zip(SpaceIds, tl(lists:seq(0, length(SpaceIds))))),
        {ok, Pid} = gui_comet:spawn(fun() ->
            comet_loop(#?STATE{counter = length(SpaceIds) + 1, space_rows = SpaceRows})
        end),
        Pid ! render_spaces_table,
        put(?COMET_PID, Pid)
    catch
        _:Reason ->
            ?error("Cannot fetch supported Spaces: ~p", [Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch supported Spaces.<br>Please try again later.">>)
    end;

event(create_space) ->
    Title = <<"Create Space">>,
    Message = <<"<div style=\"margin: 0 auto; width: 80%;\">",
    "<p id=\"create_space_alert\" style=\"width: 100%; color: red; font-size: medium; text-align: center; display: none;\"></p>",
    "<input id=\"create_space_name\" type=\"text\" style=\"width: 100%;\" placeholder=\"Name\">",
    "</div>">>,
    Script = <<"var alert = $(\"#create_space_alert\");",
    "var name = $.trim($(\"#create_space_name\").val());",
    "if(name.length == 0) { alert.html(\"Please provide Space name.\"); alert.fadeIn(300); return false; }",
    "else { createSpace([name]); return true; }">>,
    gui_jq:dialog_popup(Title, Message, Script),
    gui_jq:wire(<<"box.on('shown',function(){ $(\"#create_space_name\").focus(); });">>);

event(join_space) ->
    Title = <<"Join Space">>,
    Message = <<"<div style=\"margin: 0 auto; width: 80%;\">",
    "<p id=\"join_space_alert\" style=\"width: 100%; color: red; font-size: medium; text-align: center; display: none;\"></p>",
    "<input id=\"join_space_token\" type=\"text\" style=\"width: 100%;\" placeholder=\"Token\">",
    "</div>">>,
    Script = <<"var alert = $(\"#join_space_alert\");",
    "var token = $.trim($(\"#join_space_token\").val());",
    "if(token.length == 0) { alert.html(\"Please provide Space token.\"); alert.fadeIn(300); return false; }",
    "else { joinSpace([token]); return true; }">>,
    gui_jq:dialog_popup(Title, Message, Script),
    gui_jq:wire(<<"box.on('shown',function(){ $(\"#join_space_token\").focus(); });">>);

event({manage_space, SpaceId}) ->
    gui_jq:redirect(<<"space?id=", SpaceId/binary>>);

event({set_default_space, SpaceId, OptionId}) ->
    get(?COMET_PID) ! {set_default_space, SpaceId, OptionId},
    gui_jq:update(OptionId, vcn_gui_utils:spinner());

event({leave_space, SpaceName, SpaceId, RowId, OptionId}) ->
    Message = <<"Are you sure you want to leave Space:<br><b>", SpaceName/binary, " ( ", SpaceId/binary, " ) </b>?">>,
    Script = <<"leaveSpace(['", SpaceId/binary, "','", RowId/binary, "','", OptionId/binary, "']);">>,
    gui_jq:dialog_popup(<<"Leave Space">>, Message, Script);

event({delete_space, SpaceName, SpaceId, RowId, OptionId}) ->
    Message = <<"Are you sure you want to delete Space:<br><b>", SpaceName/binary, " ( ", SpaceId/binary, " ) </b>?">>,
    Script = <<"deleteSpace(['", SpaceId/binary, "','", RowId/binary, "','", OptionId/binary, "']);">>,
    gui_jq:dialog_popup(<<"Delete Space">>, Message, Script);

event({spaces_table_collapse, OptionId}) ->
    get(?COMET_PID) ! spaces_table_collapse,
    gui_jq:update(OptionId, vcn_gui_utils:spinner());

event({spaces_table_expand, OptionId}) ->
    get(?COMET_PID) ! spaces_table_expand,
    gui_jq:update(OptionId, vcn_gui_utils:spinner());

event({space_row_collapse, SpaceId, RowId, OptionId, Default}) ->
    get(?COMET_PID) ! {space_row_collapse, SpaceId, RowId, Default},
    gui_jq:update(OptionId, vcn_gui_utils:spinner());

event({space_row_expand, SpaceId, RowId, OptionId, Default}) ->
    get(?COMET_PID) ! {space_row_expand, SpaceId, RowId, Default},
    gui_jq:update(OptionId, vcn_gui_utils:spinner());

event({close_message, MessageId}) ->
    gui_jq:hide(MessageId);

event(terminate) ->
    ok.

%% api_event/3
%% ====================================================================
%% @doc Handles page events.
-spec api_event(Name :: string(), Args :: string(), Req :: string()) -> no_return().
%% ====================================================================
api_event("createSpace", Args, _) ->
    [Name] = mochijson2:decode(Args),
    get(?COMET_PID) ! {create_space, Name},
    gui_jq:prop(<<"create_space_button">>, <<"disabled">>, <<"disabled">>);

api_event("joinSpace", Args, _) ->
    [Token] = mochijson2:decode(Args),
    get(?COMET_PID) ! {join_space, Token},
    gui_jq:prop(<<"join_space_button">>, <<"disabled">>, <<"disabled">>);

api_event("leaveSpace", Args, _) ->
    [SpaceId, RowId, OptionId] = mochijson2:decode(Args),
    get(?COMET_PID) ! {leave_space, SpaceId, RowId, OptionId},
    gui_jq:update(OptionId, vcn_gui_utils:spinner());

api_event("deleteSpace", Args, _) ->
    [SpaceId, RowId, OptionId] = mochijson2:decode(Args),
    get(?COMET_PID) ! {delete_space, SpaceId, RowId, OptionId},
    gui_jq:update(OptionId, vcn_gui_utils:spinner()).