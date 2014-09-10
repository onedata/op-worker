% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page allows user to manage Space access tokens.
%% @end
%% ===================================================================

-module(page_tokens).
-include("veil_modules/control_panel/common.hrl").
-include_lib("ctool/include/global_registry/gr_openid.hrl").
-include_lib("ctool/include/logging.hrl").

% n2o API and comet
-export([main/0, event/1, comet_loop/1, api_event/3]).

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
title() -> <<"Manage tokens">>.


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
            style = <<"margin-bottom: 100px;">>,
            body = [
                #h6{
                    style = <<"font-size: x-large; margin: 0 auto; margin-top: 160px; width: 161px;">>,
                    body = <<"Manage tokens">>
                },
                #panel{
                    style = <<"margin: 0 auto; width: 50%; margin-top: 30px; text-align: center;">>,
                    body = #button{
                        id = <<"access_code">>,
                        postback = get_access_code,
                        class = <<"btn btn-primary">>,
                        body = <<"Get access code">>
                    }
                },
                #table{
                    class = <<"table table-bordered table-striped">>,
                    style = <<"width: 50%; margin: 0 auto; margin-top: 30px; table-layout: fixed;">>,
                    body = #tbody{
                        id = <<"tokens">>,
                        body = #tr{
                            cells = [
                                #th{
                                    style = <<"font-size: large;">>,
                                    body = <<"Tokens">>
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


%% tokens_table_/1
%% ====================================================================
%% @doc Renders  tokens table.
-spec tokens_table() -> Result when
    Result :: [#tr{}].
%% ====================================================================
tokens_table() ->
    Header = #tr{
        cells = [
            #th{
                style = <<"font-size: large;">>,
                body = <<"Tokens">>
            },
            #th{
                style = ?NAVIGATION_COLUMN_STYLE,
                body = <<"">>
            }
        ]
    },
    try
        {ok, Tokens} = gr_openid:get_client_tokens({user, vcn_gui_utils:get_access_token()}),
        Rows = lists:map(fun({#client_token{access_id = AccessId, client_name = ClientName}, Counter}) ->
            RowId = <<"token_", (integer_to_binary(Counter))/binary>>,
            #tr{
                id = RowId,
                cells = token_row(AccessId, ClientName, RowId)
            }
        end, lists:zip(Tokens, tl(lists:seq(0, length(Tokens))))),
        [Header | Rows]
    catch
        _:_ ->
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch tokens.<br>Please try again later.">>),
            [Header]
    end.


%% token_row_/3
%% ====================================================================
%% @doc Renders collapsed token row.
-spec token_row(AccessId :: binary(), Name :: binary(), RowId :: binary()) -> Result when
    Result :: [#td{}].
%% ====================================================================
token_row(AccessId, Name, RowId) ->
    SpinnerId = <<RowId/binary, "_spinner">>,
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
                end, [{<<"Access ID">>, AccessId}, {<<"Name">>, Name}])
            }
        },
        #td{
            id = SpinnerId,
            style = ?NAVIGATION_COLUMN_STYLE,
            body = remove_token(AccessId, Name, SpinnerId, RowId)
        }
    ].


%% remove_token/4
%% ====================================================================
%% @doc Renders remove token button.
-spec remove_token(AccessId :: binary(), Name :: binary(), SpinnerId :: binary(), RowId :: binary()) -> Result when
    Result :: #link{}.
%% ====================================================================
remove_token(AccessId, Name, SpinnerId, RowId) ->
    #link{
        title = <<"Remove token">>,
        class = <<"glyph-link">>,
        postback = {remove_token, AccessId, Name, SpinnerId, RowId},
        body = #span{
            style = <<"font-size: large;  vertical-align: top;">>,
            class = <<"fui-trash">>
        }
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
    NewCometLoopState = try
        receive
            get_access_code ->
                ?info("TOKEN: ~p",[vcn_gui_utils:get_access_token()]),
                case gr_openid:get_client_authorization_code({user, vcn_gui_utils:get_access_token()}) of
                    {ok, AccessCode} when is_binary(AccessCode) ->
                        Message = <<"Enter underlying access code into FUSE client.",
                        "<input type=\"text\" style=\"margin-top: 1em; width: 80%;\" value=\"", AccessCode/binary, "\">">>,
                        gui_jq:info_popup(<<"Access code">>, Message, <<"return true;">>);
                    O ->
                        ?info("ANS: ~p",[O]),
                        vcn_gui_utils:message(<<"error_message">>, <<"Cannot get access code.">>)
                end,
                gui_jq:hide(<<"main_spinner">>),
                gui_comet:flush(),
                State;

            {remove_token, AccessId, Name, SpinnerId, RowId} ->
                case gr_openid:remove_client_token({user, vcn_gui_utils:get_access_token()}, AccessId) of
                    ok ->
                        gui_jq:remove(RowId),
                        vcn_gui_utils:message(<<"ok_message">>, <<"Token: <b>", AccessId/binary, "</b> removed successfully.">>);
                    _ ->
                        gui_jq:update(SpinnerId, remove_token(AccessId, Name, SpinnerId, RowId)),
                        vcn_gui_utils:message(<<"error_message">>, <<"Cannot remove token: <b>", AccessId/binary, "</b>.">>)
                end,
                gui_comet:flush(),
                State
        end
                        catch Type:Reason ->
                            ?error_stacktrace("Comet process exception: ~p:~p", [Type, Reason]),
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
    gui_jq:update(<<"tokens">>, tokens_table()),
    gui_jq:wire(#api{name = "removeToken", tag = "removeToken"}, false),
    {ok, Pid} = gui_comet:spawn(fun() -> comet_loop(#?STATE{}) end),
    put(?COMET_PID, Pid);

event(get_access_code) ->
    get(?COMET_PID) ! get_access_code,
    gui_jq:show(<<"main_spinner">>);

event({remove_token, AccessId, Name, SpinnerId, RowId}) ->
    Message = <<"Are you sure you want to remove token:<br><b>", AccessId/binary, "</b>?">>,
    Script = <<"removeToken(['", AccessId/binary, "','", Name/binary, "','", SpinnerId/binary, "','", RowId/binary, "']);">>,
    gui_jq:dialog_popup(<<"Remove token">>, Message, Script);

event({close_message, MessageId}) ->
    gui_jq:hide(MessageId);

event(terminate) ->
    ok.

%% api_event/3
%% ====================================================================
%% @doc Handles page events.
-spec api_event(Name :: string(), Args :: string(), Req :: string()) -> no_return().
%% ====================================================================
api_event("removeToken", Args, _) ->
    [AccessId, Name, SpinnerId, RowId] = mochijson2:decode(Args),
    get(?COMET_PID) ! {remove_token, AccessId, Name, SpinnerId, RowId},
    gui_jq:update(SpinnerId, vcn_gui_utils:spinner()).