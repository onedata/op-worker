% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page allows user to manage Space authorization tokens.
%% @end
%% ===================================================================

-module(page_tokens).
-include("veil_modules/control_panel/common.hrl").
-include_lib("ctool/include/global_registry/gr_openid.hrl").
-include_lib("ctool/include/logging.hrl").

%% n2o API and comet
-export([main/0, event/1, comet_loop/1, api_event/3]).

%% Common page CCS styles
-define(MESSAGE_STYLE, <<"position: fixed; width: 100%; top: 55px; z-index: 1; display: none;">>).
-define(CONTENT_COLUMN_STYLE, <<"padding-right: 0">>).
-define(NAVIGATION_COLUMN_STYLE, <<"border-left-width: 0; width: 20px; padding-left: 0;">>).
-define(PARAGRAPH_STYLE, <<"margin: 0 auto; font-weight: normal;">>).

%$ Table names
-define(CLIENT_TOKENS_TABLE_NAME, <<"Client tokens">>).
-define(PROVIDER_TOKENS_TABLE_NAME, <<"Provider tokens">>).

%% Comet process pid
-define(COMET_PID, comet_pid).

%% Comet process state
-define(STATE, comet_state).
-record(?STATE, {client_tokens, provider_tokens}).

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
            #dtl{file = "bare", app = ?APP_Name, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
        false ->
            #dtl{file = "bare", app = ?APP_Name, bindings = [{title, title()}, {body, body()}, {custom, custom()}]}
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
        vcn_gui_utils:top_menu(tokens_tab),
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
                        id = <<"authorization_code">>,
                        postback = {message, get_authorization_code},
                        class = <<"btn btn-inverse btn-small">>,
                        body = <<"Get authorization code">>
                    }
                },
                #table{
                    style = <<"border-radius: 0; margin-top: 30px; margin-bottom: 0; table-layout: fixed; width: 100%;">>,
                    body = #tbody{
                        body = #tr{
                            style = <<"vertical-align: top;">>,
                            cells = lists:map(fun({TableId}) ->
                                #th{
                                    style = <<"width: 50%;">>,
                                    body = #table{
                                        class = <<"table table-bordered table-striped">>,
                                        style = <<"width: 90%; margin: 0 auto; table-layout: fixed;">>,
                                        body = #tbody{
                                            id = TableId,
                                            style = <<"display: none;">>
                                        }
                                    }
                                }
                            end, [
                                {<<"client_tokens">>},
                                {<<"provider_tokens">>}
                            ])
                        }
                    }
                }
            ]
        }
    ].


%% tokens_table_collapsed/3
%% ====================================================================
%% @doc Renders collapsed tokens settings table.
%% @end
-spec tokens_table_collapsed(TableId :: binary(), TableName :: binary(), TokensDetails :: [{Id :: binary(), TokenDetails :: #token_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
tokens_table_collapsed(TableId, TableName, TokensDetails) ->
    NavigationBody = vcn_gui_utils:expand_button(<<"Expand All">>, {message, {expand_tokens_table, TableId, TableName}}),
    RenderRowFun = fun token_row_collapsed/3,
    tokens_table(TableName, TokensDetails, NavigationBody, RenderRowFun).


%% tokens_table_expanded/3
%% ====================================================================
%% @doc Renders expanded tokens settings table.
%% @end
-spec tokens_table_expanded(TableId :: binary(), TableName :: binary(), TokensDetails :: [{Id :: binary(), TokenDetails :: #token_details{}}]) -> Result when
    Result :: [#tr{}].
%% ====================================================================
tokens_table_expanded(TableId, TableName, TokensDetails) ->
    NavigationBody = vcn_gui_utils:collapse_button(<<"Collapse All">>, {message, {collapse_tokens_table, TableId, TableName}}),
    RenderRowFun = fun token_row_expanded/3,
    tokens_table(TableName, TokensDetails, NavigationBody, RenderRowFun).


%% tokens_table/4
%% ====================================================================
%% @doc Renders tokens settings table.
%% @end
-spec tokens_table(TableName :: binary(), TokensDetails :: [{Id :: binary(), TokenDetails :: #token_details{}}], NavigationBody :: #link{}, RenderRowFun :: function()) -> Result when
    Result :: [#tr{}].
%% ====================================================================
tokens_table(TableName, TokensDetails, NavigationBody, RenderRowFun) ->
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

    Rows = lists:foldl(fun({RowId, TokenDetails}, RowsAcc) ->
        [#tr{
            id = RowId,
            cells = RenderRowFun(TableName, RowId, TokenDetails)
        } | RowsAcc]
    end, [], TokensDetails),

    [Header | Rows].


%% token_row_collapsed/3
%% ====================================================================
%% @doc Renders collapsed token settings row.
%% @end
-spec token_row_collapsed(TableName :: binary(), RowId :: binary(), TokenDetails :: #token_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
token_row_collapsed(TableName, RowId, #token_details{access_id = AccessId, client_name = ClientName} = TokenDetails) ->
    [
        #td{
            style = ?CONTENT_COLUMN_STYLE,
            body = #p{
                style = ?PARAGRAPH_STYLE,
                body = <<"<b>", ClientName/binary, "</b> (", AccessId/binary, ")">>
            }
        },
        #td{
            style = ?NAVIGATION_COLUMN_STYLE,
            body = vcn_gui_utils:expand_button({message, {expand_token_row, TableName, RowId, TokenDetails}})
        }
    ].


%% token_row_expanded/3
%% ====================================================================
%% @doc Renders expanded token settings row.
%% @end
-spec token_row_expanded(TableName :: binary(), RowId :: binary(), TokenDetails :: #token_details{}) -> Result when
    Result :: [#td{}].
%% ====================================================================
token_row_expanded(TableName, RowId, #token_details{access_id = AccessId} = TokenDetails) ->
    [
        #td{
            style = ?CONTENT_COLUMN_STYLE,
            body = #table{
                style = <<"border-width: 0; width: 100%; border-collapse: inherit;">>,
                body = lists:map(fun({Description, Id, Body}) ->
                    #tr{
                        cells = [
                            #td{
                                style = <<"border-width: 0; text-align: right; width: 10%; padding-left: 0; padding-right: 0;">>,
                                body = #label{
                                    style = <<"margin: 0 auto; cursor: auto;">>,
                                    class = <<"label label-large label-inverse">>,
                                    body = Description
                                }
                            },
                            #td{
                                id = Id,
                                style = <<"border-width: 0;  text-align: left; padding-left: 1em; width: 90%;">>,
                                body = Body
                            }
                        ]
                    }
                end, [
                    {<<"Name">>, <<RowId/binary, "_client_name">>, client_name(TableName, RowId, TokenDetails)},
                    {<<"Access ID">>, <<RowId/binary, "_revoke_token">>, token_detail(AccessId, <<"Revoke token">>,
                        {revoke_token, TableName, RowId, TokenDetails}, <<"icomoon-remove">>)}
                ])
            }
        },
        #td{
            style = ?NAVIGATION_COLUMN_STYLE,
            body = vcn_gui_utils:collapse_button({message, {collapse_token_row, TableName, RowId, TokenDetails}})
        }
    ].


%% token_detail/4
%% ====================================================================
%% @doc Renders token detail.
-spec token_detail(Content :: term(), Title :: binary(), Postback :: term(), Class :: binary()) -> Result when
    Result :: #span{}.
%% ====================================================================
token_detail(Content, Title, Postback, Class) ->
    #span{
        style = <<"font-size: large; font-weight: normal; fivertical-align: -webkit-baseline-middle;">>,
        body = [
            Content,
            #link{
                title = Title,
                style = <<"margin-left: 10px;">>,
                class = <<"glyph-link">>,
                postback = Postback,
                body = #span{
                    class = Class
                }
            }
        ]
    }.


%% client_name/3
%% ====================================================================
%% @doc Renders client name.
-spec client_name(TableName :: binary(), RowId :: binary(), TokenDetails :: #token_details{}) -> Result when
    Result :: #span{}.
%% ====================================================================
client_name(TableName, RowId, #token_details{client_name = ClientName} = TokenDetails) ->
    token_detail(ClientName, <<"Edit">>, {change_client_name, TableName, RowId, TokenDetails}, <<"icomoon-pencil2">>).


%% change_client_name/3
%% ====================================================================
%% @doc Renders textbox used to change client's name.
-spec change_client_name(TableName :: binary(), RowId :: binary(), TokenDetails :: #token_details{}) -> Result when
    Result :: list().
%% ====================================================================
change_client_name(TableName, RowId, TokenDetails) ->
    [
        #textbox{
            id = <<RowId/binary, "_textbox">>,
            style = <<"margin: 0 auto; padding: 1px;">>,
            class = <<"span">>,
            placeholder = <<"New name">>
        },
        #link{
            id = <<RowId/binary, "_submit">>,
            style = <<"margin-left: 10px;">>,
            class = <<"glyph-link">>,
            title = <<"Submit">>,
            actions = gui_jq:form_submit_action(<<RowId/binary, "_submit">>, {submit_new_client_name, TableName, RowId, TokenDetails}, <<RowId/binary, "_textbox">>),
            body = #span{
                class = <<"fui-check-inverted">>,
                style = <<"font-size: large; vertical-align: middle;">>
            }
        },
        #link{
            style = <<"margin-left: 10px;">>,
            class = <<"glyph-link">>,
            title = <<"Cancel">>,
            postback = {cancel_new_client_name_submit, TableName, RowId, TokenDetails},
            body = #span{
                class = <<"fui-cross-inverted">>,
                style = <<"font-size: large; vertical-align: middle;">>
            }
        }
    ].


%% ====================================================================
%% Events handling
%% ====================================================================

%% comet_loop/1
%% ====================================================================
%% @doc Handles tokens management actions.
-spec comet_loop(State :: #?STATE{}) -> Result when
    Result :: {error, Reason :: term()}.
%% ====================================================================
comet_loop({error, Reason}) ->
    {error, Reason};

comet_loop(#?STATE{client_tokens = ClientTokens, provider_tokens = ProviderTokens} = State) ->
    NewCometLoopState = try
                            receive
                                render_tokens_tables ->
                                    lists:foreach(fun({TableId, TableName, TokensDetails}) ->
                                        gui_jq:update(TableId, tokens_table_collapsed(TableId, TableName, TokensDetails)),
                                        gui_jq:fade_in(TableId, 500)
                                    end, [
                                        {<<"client_tokens">>, ?CLIENT_TOKENS_TABLE_NAME, ClientTokens},
                                        {<<"provider_tokens">>, ?PROVIDER_TOKENS_TABLE_NAME, ProviderTokens}
                                    ]),
                                    State;

                                get_authorization_code ->
                                    case gr_openid:get_client_authorization_code({user, vcn_gui_utils:get_access_token()}) of
                                        {ok, AuthorizationCode} when is_binary(AuthorizationCode) ->
                                            Message = <<"Use the authorization code below to log in with a FUSE client.",
                                            "<input id=\"authorization_code_textbox\" type=\"text\" style=\"margin-top: 1em;"
                                            " width: 80%;\" value=\"", AuthorizationCode/binary, "\">">>,
                                            gui_jq:info_popup(<<"Authorization code">>, Message, <<"return true;">>, <<"btn-inverse">>),
                                            gui_jq:wire(<<"box.on('shown',function(){ $(\"#authorization_code_textbox\").focus().select(); });">>);
                                        _ ->
                                            vcn_gui_utils:message(<<"error_message">>, <<"Cannot get authorization code.">>)
                                    end,
                                    State;

                                {change_client_name, TableName, RowId, #token_details{access_id = AccessId} = TokenDetails, NewClientName} ->
                                    NewTokenDetailsFun = fun(Details) ->
                                        lists:filtermap(fun({Id, Detail}) ->
                                            case Id of
                                                RowId ->
                                                    {true, {Id, Detail#token_details{client_name = NewClientName}}};
                                                _ -> {true, {Id, Detail}}
                                            end
                                        end, Details)
                                    end,

                                    {ChangeClientNameFun, SuccessfulState} =
                                        case TableName of
                                            ?CLIENT_TOKENS_TABLE_NAME ->
                                                {fun gr_openid:modify_client_token_details/3, State#?STATE{client_tokens = NewTokenDetailsFun(ClientTokens)}};
                                            ?PROVIDER_TOKENS_TABLE_NAME ->
                                                {fun gr_openid:modify_provider_token_details/3, State#?STATE{provider_tokens = NewTokenDetailsFun(ProviderTokens)}}
                                        end,

                                    NextState =
                                        case ChangeClientNameFun({user, vcn_gui_utils:get_access_token()}, AccessId, [{<<"clientName">>, NewClientName}]) of
                                            ok ->
                                                NewTokenDetails = client_name(TableName, RowId, TokenDetails#token_details{client_name = NewClientName}),
                                                gui_jq:update(<<RowId/binary, "_client_name">>, NewTokenDetails),
                                                SuccessfulState;
                                            Other ->
                                                ?error("Cannot change client name for token ~p: ~p", [AccessId, Other]),
                                                vcn_gui_utils:message(<<"error_message">>, <<"Cannot change name for token: <b>", AccessId/binary, "</b>."
                                                "<br>Please try again later.">>),
                                                gui_jq:update(<<RowId/binary, "_client_name">>, client_name(TableName, RowId, TokenDetails)),
                                                State
                                        end,
                                    NextState;

                                {revoke_token, TableName, AccessId, RowId} ->
                                    {RevokeTokenFun, SuccessfulState} =
                                        case TableName of
                                            ?CLIENT_TOKENS_TABLE_NAME ->
                                                {fun gr_openid:revoke_client_token/2, State#?STATE{client_tokens = proplists:delete(RowId, ClientTokens)}};
                                            ?PROVIDER_TOKENS_TABLE_NAME ->
                                                {fun gr_openid:revoke_provider_token/2, State#?STATE{provider_tokens = proplists:delete(RowId, ProviderTokens)}}
                                        end,
                                    NextState =
                                        case RevokeTokenFun({user, vcn_gui_utils:get_access_token()}, AccessId) of
                                            ok ->
                                                vcn_gui_utils:message(<<"ok_message">>, <<"Token: <b>", AccessId/binary, "</b> has been successfully revoked.">>),
                                                gui_jq:remove(RowId),
                                                SuccessfulState;
                                            Other ->
                                                ?error("Cannot revoke token ~p: ~p", [AccessId, Other]),
                                                vcn_gui_utils:message(<<"error_message">>, <<"Cannot revoke token: <b>", AccessId/binary, "</b>."
                                                "<br>Please try again later.">>),
                                                State
                                        end,
                                    NextState;

                                Event ->
                                    case Event of
                                        {collapse_tokens_table, <<"client_tokens">>, TableName} ->
                                            gui_jq:update(<<"client_tokens">>, tokens_table_collapsed(<<"client_tokens">>, TableName, ClientTokens));
                                        {collapse_tokens_table, <<"provider_tokens">>, TableName} ->
                                            gui_jq:update(<<"provider_tokens">>, tokens_table_collapsed(<<"provider_tokens">>, TableName, ProviderTokens));
                                        {expand_tokens_table, <<"client_tokens">>, TableName} ->
                                            gui_jq:update(<<"client_tokens">>, tokens_table_expanded(<<"client_tokens">>, TableName, ClientTokens));
                                        {expand_tokens_table, <<"provider_tokens">>, TableName} ->
                                            gui_jq:update(<<"provider_tokens">>, tokens_table_expanded(<<"provider_tokens">>, TableName, ProviderTokens));
                                        {collapse_token_row, TableName, RowId, TokenDetails} ->
                                            gui_jq:update(RowId, token_row_collapsed(TableName, RowId, TokenDetails));
                                        {expand_token_row, TableName, RowId, TokenDetails} ->
                                            gui_jq:update(RowId, token_row_expanded(TableName, RowId, TokenDetails));
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
    ?MODULE:comet_loop(NewCometLoopState).


%% event/1
%% ====================================================================
%% @doc Handles page events.
-spec event(Event :: term()) -> no_return().
%% ====================================================================
event(init) ->
    try
        TokensFun = fun(RowPrefix, Tokens) ->
            lists:foldl(fun(Token, {Rows, It}) ->
                {
                    [{<<RowPrefix/binary, (integer_to_binary(It + 1))/binary>>, Token} | Rows],
                    It + 1
                }
            end, {[], 0}, Tokens)
        end,

        {ok, ClientTokens} = gr_openid:get_client_tokens({user, vcn_gui_utils:get_access_token()}),
        {NewClientTokens, _} = TokensFun(<<"client_token_">>, ClientTokens),
        {ok, ProviderTokens} = gr_openid:get_provider_tokens({user, vcn_gui_utils:get_access_token()}),
        {NewProviderTokens, _} = TokensFun(<<"provider_token_">>, ProviderTokens),

        gui_jq:wire(#api{name = "revokeToken", tag = "revokeToken"}, false),
        gui_jq:bind_key_to_click_on_class(<<"13">>, <<"confirm">>),

        {ok, Pid} = gui_comet:spawn(fun() ->
            comet_loop(#?STATE{provider_tokens = NewProviderTokens, client_tokens = NewClientTokens})
        end),
        put(?COMET_PID, Pid),
        Pid ! render_tokens_tables
    catch
        _:Reason ->
            ?error("Cannot fetch supported Spaces: ~p", [Reason]),
            vcn_gui_utils:message(<<"error_message">>, <<"Cannot fetch supported Spaces.<br>Please try again later.">>)
    end;

event({change_client_name, TableName, RowId, TokenDetails}) ->
    gui_jq:update(<<RowId/binary, "_client_name">>, change_client_name(TableName, RowId, TokenDetails)),
    gui_jq:bind_enter_to_submit_button(<<RowId/binary, "_textbox">>, <<RowId/binary, "_submit">>),
    gui_jq:focus(<<RowId/binary, "_textbox">>);

event({submit_new_client_name, TableName, RowId, TokenDetails}) ->
    NewClientName = gui_ctx:postback_param(<<RowId/binary, "_textbox">>),
    get(?COMET_PID) ! {change_client_name, TableName, RowId, TokenDetails, NewClientName},
    gui_jq:show(<<"main_spinner">>);

event({cancel_new_client_name_submit, TableName, RowId, TokenDetails}) ->
    gui_jq:update(<<RowId/binary, "_client_name">>, client_name(TableName, RowId, TokenDetails));

event({revoke_token, TableName, RowId, #token_details{access_id = AccessId}}) ->
    Message = <<"Are you sure you want to revoke token: <b>", AccessId/binary, "</b>?<br>This operation cannot be undone.">>,
    Script = <<"revokeToken(['", TableName/binary, "','", AccessId/binary, "','", RowId/binary, "']);">>,
    gui_jq:dialog_popup(<<"Revoke token">>, Message, Script, <<"btn-inverse">>);

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
api_event("revokeToken", Args, _) ->
    [TableName, AccessId, RowId] = mochijson2:decode(Args),
    get(?COMET_PID) ! {revoke_token, TableName, AccessId, RowId},
    gui_jq:show(<<"main_spinner">>).