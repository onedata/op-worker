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
-include("veil_modules/dao/dao_spaces.hrl").
-include("veil_modules/control_panel/common.hrl").
-include_lib("ctool/include/logging.hrl").

% n2o API and comet
-export([main/0, event/1, api_event/3, comet_loop/1]).

%% Common page CCS styles
-define(MESSAGE_STYLE, <<"position: fixed; width: 100%; top: 120px; z-index: 1; display: none;">>).
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
-record(?STATE, {}).

%% ====================================================================
%% API functions
%% ====================================================================


%% main/0
%% ====================================================================
%% @doc Template points to the template file, which will be filled with content.
-spec main() -> #dtl{}.
%% ====================================================================
main() ->
    case vcn_gui_utils:maybe_redirect(true, false, false, true) of
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
            id = <<"spinner">>,
            style = <<"position: absolute; top: 12px; left: 17px; z-index: 1234; width: 32px; display: none;">>,
            body = #image{
                image = <<"/images/spinner.gif">>
            }
        },
        vcn_gui_utils:top_menu(spaces_tab, spaces_submenu()),
        #panel{
            id = <<"ok_message">>,
            style = ?MESSAGE_STYLE,
            class = <<"dialog dialog-success">>
        },
        #panel{
            id = <<"error_message">>,
            style = ?MESSAGE_STYLE,
            class = <<"dialog dialog-danger">>,
            body = <<"Error">>
        },
        #panel{
            style = <<"position: relative; margin-top: 200px; margin-bottom: 0px">>,
            body = #table{
                class = <<"table table-bordered table-striped">>,
                style = <<"width: 50%; margin: 0 auto; table-layout: fixed;">>,
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
                                body = spinner()
                            }
                        ]
                    }
                }
            }
        }
    ].


%% spaces_submenu/0
%% ====================================================================
%% @doc Submenu that will end up concatenated to top menu.
-spec spaces_submenu() -> [#panel{}].
%% ====================================================================
spaces_submenu() ->
    [
        #panel{
            class = <<"navbar-inner">>,
            style = <<"border-bottom: 1px solid gray; padding-bottom: 5px;">>,
            body = [
                #panel{
                    class = <<"container">>,
                    body = [
                        #panel{
                            class = <<"btn-group">>,
                            style = <<"margin: 12px 15px;">>,
                            body = #button{
                                postback = create_space,
                                class = <<"btn btn-inverse">>,
                                style = <<"height: 34px; padding: 6px 13px 8px;">>,
                                body = <<"Create Space">>
                            }
                        },
                        #panel{
                            class = <<"btn-group">>,
                            style = <<"margin: 12px 15px;">>,
                            body = #button{
                                postback = join_space,
                                class = <<"btn btn-inverse">>,
                                style = <<"height: 34px; padding: 6px 13px 8px;">>,
                                body = <<"Join Space">>
                            }
                        }
                    ]
                }
            ]
        }
    ].


%% spaces_table_collapsed/1
%% ====================================================================
%% @doc Renders collapsed Spaces settings table.
-spec spaces_table_collapsed(TableId :: binary()) -> Result when
    Result :: [#tr{}].
%% ====================================================================
spaces_table_collapsed(TableId) ->
    Header = #tr{
        cells = [
            #th{
                style = <<"font-size: large;">>,
                body = <<"Spaces">>
            },
            #th{
                id = <<"space_all_spinner">>,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = expand_button(<<"Expand All">>, {spaces_table_expand, TableId, <<"space_all_spinner">>})
            }
        ]
    },
    try
        {ok, SpaceIds} = registry_spaces:get_user_spaces(gui_ctx:get_access_token()),
        Rows = lists:map(fun({SpaceId, Counter}) ->
            RowId = <<"space_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = space_row_collapsed(SpaceId, RowId)
            }
        end, lists:zip(SpaceIds, tl(lists:seq(0, length(SpaceIds))))),
        [Header | Rows]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch supported Spaces.<br>Please try again later.">>),
            [Header]
    end.


%% spaces_table_expanded/1
%% ====================================================================
%% @doc Renders expanded Spaces settings table.
-spec spaces_table_expanded(TableId :: binary()) -> Result when
    Result :: [#tr{}].
%% ====================================================================
spaces_table_expanded(TableId) ->
    Header = #tr{
        cells = [
            #th{
                style = <<"font-size: large;">>,
                body = <<"Spaces">>
            },
            #th{
                id = <<"space_all_spinner">>,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = collapse_button(<<"Collapse All">>, {spaces_table_collapse, TableId, <<"space_all_spinner">>})
            }
        ]
    },
    try
        {ok, SpaceIds} = registry_spaces:get_user_spaces(gui_ctx:get_access_token()),
        Rows = lists:map(fun({SpaceId, Counter}) ->
            RowId = <<"space_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = space_row_expanded(SpaceId, RowId)
            }
        end, lists:zip(SpaceIds, tl(lists:seq(0, length(SpaceIds))))),
        [Header | Rows]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch supported Spaces.<br>Please try again later.">>),
            [Header]
    end.


%% space_row_collapsed/2
%% ====================================================================
%% @doc Renders collapsed Space settings row.
-spec space_row_collapsed(SpaceId :: binary(), RowId :: binary()) -> Result when
    Result :: [#td{}].
%% ====================================================================
space_row_collapsed(SpaceId, RowId) ->
    try
        {ok, #space_info{name = Name}} = fslogic_objects:get_space({uuid, SpaceId}),
        SpinnerId = <<RowId/binary, "_spinner">>,
        [
            #td{
                style = ?CONTENT_COLUMN_STYLE,
                body = #p{
                    body = <<"<b>", Name/binary, "</b> ( ", SpaceId/binary, " )">>
                }
            },
            #td{
                id = SpinnerId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = expand_button({space_row_expand, SpaceId, RowId, SpinnerId})
            }
        ]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch Space details.<br>Please try again later.">>),
            []
    end.


%% space_row_expanded/2
%% ====================================================================
%% @doc Renders expanded Space settings row.
-spec space_row_expanded(SpaceId :: binary(), RowId :: binary()) -> Result when
    Result :: [#td{}].
%% ====================================================================
space_row_expanded(SpaceId, RowId) ->
    try
        {ok, #space_info{name = Name}} = fslogic_objects:get_space({uuid, SpaceId}),
        SpinnerId = <<RowId/binary, "_spinner">>,
        DefaultSpaceSpinnerId = <<RowId/binary, "_default_spinner">>,
        ManageSpaceSpinnerId = <<RowId/binary, "_manage_spinner">>,
        DeleteSpaceSpinnerId = <<RowId/binary, "_delete_spinner">>,
        SettingsIcons = #panel{
            style = <<"width: 100%;">>,
            body = [
                #link{
                    id = DefaultSpaceSpinnerId,
                    title = <<"Set as default Space">>,
                    style = <<"font-size: large; margin-right: 1em;">>,
                    class = <<"glyph-link">>,
                    postback = {set_default_space, SpaceId, RowId, DefaultSpaceSpinnerId},
                    body = #span{
                        class = <<"fui-home">>
                    }
                },
                #link{
                    id = ManageSpaceSpinnerId,
                    title = <<"Manage Space">>,
                    style = <<"font-size: large; margin-right: 1em;">>,
                    class = <<"glyph-link">>,
                    postback = {manage_space, SpaceId, ManageSpaceSpinnerId},
                    body = #span{
                        class = <<"fui-gear">>
                    }
                },
                #link{
                    id = DeleteSpaceSpinnerId,
                    title = <<"Delete Space">>,
                    style = <<"font-size: large; margin-right: 1em;">>,
                    class = <<"glyph-link">>,
                    postback = {delete_space, SpaceId, RowId, DeleteSpaceSpinnerId},
                    body = #span{
                        class = <<"fui-trash">>
                    }
                }
            ]
        },
        [
            #td{
                style = ?CONTENT_COLUMN_STYLE,
                body = [
                    #table{
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
                        end, [{<<"Name">>, Name}, {<<"Space ID">>, SpaceId}, {<<"Settings">>, SettingsIcons}])
                    }
                ]
            },
            #td{
                id = SpinnerId,
                style = ?NAVIGATION_COLUMN_STYLE,
                body = collapse_button({space_row_collapse, SpaceId, RowId, SpinnerId})
            }
        ]
    catch
        _:_ ->
            message(<<"error_message">>, <<"Cannot fetch Space details.<br>Please try again later.">>),
            space_row_collapsed(SpaceId, RowId)
    end.


%% collapse_button/1
%% ====================================================================
%% @doc Renders collapse button.
-spec collapse_button(Postback :: term()) -> Result when
    Result :: #link{}.
%% ====================================================================
collapse_button(Postback) ->
    collapse_button(<<"Collapse">>, Postback).


%% collapse_button/2
%% ====================================================================
%% @doc Renders collapse button.
-spec collapse_button(Title :: binary(), Postback :: term()) -> Result when
    Result :: #link{}.
%% ====================================================================
collapse_button(Title, Postback) ->
    #link{
        title = Title,
        class = <<"glyph-link">>,
        postback = Postback,
        body = #span{
            style = <<"font-size: large; vertical-align: top;">>,
            class = <<"fui-triangle-up">>
        }
    }.


%% expand_button/1
%% ====================================================================
%% @doc Renders expand button.
-spec expand_button(Postback :: term()) -> Result when
    Result :: #link{}.
%% ====================================================================
expand_button(Postback) ->
    expand_button(<<"Expand">>, Postback).


%% expand_button/2
%% ====================================================================
%% @doc Renders expand button.
-spec expand_button(Title :: binary(), Postback :: term()) -> Result when
    Result :: #link{}.
%% ====================================================================
expand_button(Title, Postback) ->
    #link{
        title = Title,
        class = <<"glyph-link">>,
        postback = Postback,
        body = #span{
            style = <<"font-size: large;  vertical-align: top;">>,
            class = <<"fui-triangle-down">>
        }
    }.


%% message/3
%% ====================================================================
%% @doc Renders a message in given element and allows to hide this message.
-spec message(Id :: binary(), Message :: binary()) -> Result when
    Result :: ok.
%% ====================================================================
message(Id, Message) ->
    Body = [
        Message,
        #link{
            title = <<"Close">>,
            style = <<"position: absolute; right: 1em; top: 1em;">>,
            class = <<"glyph-link">>,
            postback = {close_message, Id},
            body = #span{
                class = <<"fui-cross">>
            }
        }
    ],
    gui_jq:update(Id, Body),
    gui_jq:fade_in(Id, 300).


%% dialog_popup/3
%% ====================================================================
%% @doc Displays custom dialog popup.
-spec dialog_popup(Title :: binary(), Message :: binary(), Script :: binary()) -> binary().
%% ====================================================================
dialog_popup(Title, Message, Script) ->
    gui_jq:wire(<<"var box = bootbox.dialog({
        title: '", Title/binary, "',
        message: '", Message/binary, "',
        buttons: {
            'Cancel': {
                className: 'cancel'
            },
            'OK': {
                className: 'btn-primary confirm',
                callback: function() {", Script/binary, "}
            }
        }
    });">>).


%% bind_key_to_click/2
%% ====================================================================
%% @doc Makes any keypresses of given key to click on selected class.
%% @end
-spec bind_key_to_click(KeyCode :: binary(), TargetID :: binary()) -> string().
%% ====================================================================
bind_key_to_click(KeyCode, TargetID) ->
    Script = <<"$(document).bind('keydown', function (e){",
    "if (e.which == ", KeyCode/binary, ") { e.preventDefault(); $('", TargetID/binary, "').click(); } });">>,
    gui_jq:wire(Script, false).


%% spinner/0
%% ====================================================================
%% @doc Renders spinner GIF.
-spec spinner() -> Result when
    Result :: #image{}.
%% ====================================================================
spinner() ->
    #image{
        image = <<"/images/spinner.gif">>,
        style = <<"width: 1.5em;">>
    }.


%% comet_loop/1
%% ====================================================================
%% @doc Handles spaces management actions.
-spec comet_loop(State :: #?STATE{}) -> Result when
    Result :: {error, Reason :: term()}.
%% ====================================================================
comet_loop({error, Reason}) ->
    {error, Reason};

comet_loop(#?STATE{} = State) ->
    NewState = try
        receive
            {spaces_table_collapse, TableId} ->
                gui_jq:update(TableId, spaces_table_collapsed(TableId)),
                gui_comet:flush(),
                State;

            {spaces_table_expand, TableId} ->
                gui_jq:update(TableId, spaces_table_expanded(TableId)),
                gui_comet:flush(),
                State;

            {space_row_collapse, SpaceId, RowId} ->
                gui_jq:update(RowId, space_row_collapsed(SpaceId, RowId)),
                gui_comet:flush(),
                State;

            {space_row_expand, SpaceId, RowId} ->
                ?info("Space row expand: ~p ~p", [SpaceId, RowId]),
                gui_jq:update(RowId, space_row_expanded(SpaceId, RowId)),
                gui_comet:flush(),
                State
        end
               catch Type:Reason ->
                   ?error("Comet process exception: ~p:~p", [Type, Reason]),
                   message(<<"error_message">>, <<"There has been an error in comet process. Please refresh the page.">>),
                   {error, Reason}
               end,
    ?MODULE:comet_loop(NewState).


%% event/1
%% ====================================================================
%% @doc Handles page events.
-spec event(Event :: term()) -> no_return().
%% ====================================================================
event(init) ->
    gui_jq:update(<<"spaces">>, spaces_table_collapsed(<<"spaces">>)),
    gui_jq:wire(#api{name = "createSpace", tag = "createSpace"}, false),
    gui_jq:wire(#api{name = "joinSpace", tag = "joinSpace"}, false),
    bind_key_to_click(<<"13">>, <<"button.confirm">>),
    {ok, Pid} = gui_comet:spawn(fun() -> comet_loop(#?STATE{}) end),
    put(?COMET_PID, Pid);

event(create_space) ->
    ?info("Create space."),
    Title = <<"Create Space">>,
    Message = <<"<div style=\"margin: 0 auto; width: 80%;\">",
    "<p id=\"create_space_alert\" style=\"width: 100%; color: red; font-size: medium; text-align: center; display: none;\"></p>",
    "<input id=\"create_space_name\" type=\"text\" style=\"width: 100%;\" placeholder=\"Name\">",
    "</div>">>,
    Script = <<"var alert = $(\"#create_space_alert\");",
    "var token = $.trim($(\"#create_space_token\").val());",
    "if(token.length == 0) { alert.html(\"Please provide Space token.\"); alert.fadeIn(300); return false; }",
    "else { createSpace([token]); return true; }">>,
    dialog_popup(Title, Message, Script),
    gui_jq:wire(<<"box.on('shown',function(){ $(\"#create_space_name\").focus(); });">>);

event(join_space) ->
    ?info("Join Space"),
    Title = <<"Join Space">>,
    Message = <<"<div style=\"margin: 0 auto; width: 80%;\">",
    "<p id=\"join_space_alert\" style=\"width: 100%; color: red; font-size: medium; text-align: center; display: none;\"></p>",
    "<input id=\"join_space_token\" type=\"text\" style=\"width: 100%;\" placeholder=\"Token\">",
    "</div>">>,
    Script = <<"var alert = $(\"#join_space_alert\");",
    "var token = $.trim($(\"#join_space_token\").val());",
    "if(token.length == 0) { alert.html(\"Please provide Space token.\"); alert.fadeIn(300); return false; }",
    "else { joinSpace([token]); return true; }">>,
    dialog_popup(Title, Message, Script),
    gui_jq:wire(<<"box.on('shown',function(){ $(\"#join_space_token\").focus(); });">>);

event({spaces_table_collapse, TableId, SpinnerId}) ->
    get(?COMET_PID) ! {spaces_table_collapse, TableId},
    gui_jq:update(SpinnerId, spinner());

event({spaces_table_expand, TableId, SpinnerId}) ->
    get(?COMET_PID) ! {spaces_table_expand, TableId},
    gui_jq:update(SpinnerId, spinner());

event({space_row_collapse, SpaceId, RowId, SpinnerId}) ->
    get(?COMET_PID) ! {space_row_collapse, SpaceId, RowId},
    gui_jq:update(SpinnerId, spinner());

event({space_row_expand, SpaceId, RowId, SpinnerId}) ->
    get(?COMET_PID) ! {space_row_expand, SpaceId, RowId},
    gui_jq:update(SpinnerId, spinner());

event({set_default_space, SpaceId, _RowId, _ActionSpinnerId}) ->
    message(<<"ok_message">>, <<"Set default Space: ", (list_to_binary(SpaceId))/binary>>);

event({manage_space, SpaceId, _ActionSpinnerId}) ->
    message(<<"ok_message">>, <<"Settings Space: ", (list_to_binary(SpaceId))/binary>>);

event({delete_space, SpaceId, _RowId, _ActionSpinnerId}) ->
    message(<<"ok_message">>, <<"Delete Space: ", (list_to_binary(SpaceId))/binary>>);

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
    [Token] = mochijson2:decode(Args),
    ?info("Create space: ~p", [Token]);
%%     get(?COMET_PID) ! {create_space, Name, Token, <<"space_", (integer_to_binary(Id + 1))/binary>>},
%%     gui_jq:prop(<<"create_space_button">>, <<"disabled">>, <<"disabled">>);

api_event("joinSpace", Args, _) ->
    [Token] = mochijson2:decode(Args),
    ?info("Join space: ~p", [Token]).
%%     get(?COMET_PID) ! {support_space, Token, <<"space_", (integer_to_binary(Id + 1))/binary>>},
%%     gui_jq:prop(<<"support_space_button">>, <<"disabled">>, <<"disabled">>);
