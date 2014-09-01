%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page (available for admins only) allows live viewing of logs sent from clients.
%% @end
%% ===================================================================

% n2o API
-module(page_client_logs).
-include_lib("ctool/include/logging.hrl").
-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/dao/dao_cluster.hrl").
-include("registered_names.hrl").
-include("logging_pb.hrl").

% n2o API and comet
-export([main/0, event/1, api_event/3, comet_loop/2]).

% Record used to store user preferences. One instance is kept in comet process, another one
% is remembered in page state for filter options to be persistent
-record(page_state, {
    loglevel = debug,
    auto_scroll = true,
    first_log = 1,
    max_logs = 200,
    message_filter = undefined,
    file_filter = undefined
}).

% Widths of columns
-define(SEVERITY_COLUMN_STYLE, "width: 90px; padding: 6px 12px;").
-define(TIME_COLUMN_STYLE, "width: 150px; padding: 6px 12px;").
-define(MESSAGE_COLUMN_STYLE, "padding: 6px 12px;").
-define(METADATA_COLUMN_STYLE, "width: 340px; padding: 6px 12px;").

% Prefixes used to generate IDs for logs
-define(COLLAPSED_LOG_ROW_ID_PREFIX, "clr").
-define(EXPANDED_LOG_ROW_ID_PREFIX, "elr").

% Available options of max log count
-define(MAX_LOGS_OPTIONS, [20, 50, 200, 500, 1000, 2000]).

-define(INFINITY, 10000000000000000000).

%% Template points to the template file, which will be filled with content
main() ->
    case vcn_gui_utils:maybe_redirect(true, false, false, true) of
        true ->
            #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
        false ->
            case vcn_gui_utils:can_view_logs() of
                false ->
                    gui_jq:redirect(<<"/">>),
                    #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
                true ->
                    #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}
            end
    end.


%% Page title
title() -> <<"Client logs">>.


% This will be placed instead of [[[body()]]] tag in template
body() ->
    gui_jq:register_escape_event("escape_pressed"),
    _Body = [
        vcn_gui_utils:top_menu(administration_tab, logs_submenu()),
        #panel{style = <<"margin-top: 122px; z-index: -1;">>, body = main_table()},
        footer_popup()
    ],
    _Body.


% Submenu that will end up concatenated to top menu
logs_submenu() ->
    [
        #panel{class = <<"navbar-inner">>, style = <<"border-bottom: 1px solid gray; padding-bottom: 5px;">>, body = [
            #panel{class = <<"container">>, body = [
                #panel{class = <<"btn-group">>, style = <<"margin: 12px 15px;">>, body = [
                    <<"<i class=\"dropdown-arrow dropdown-arrow-inverse\"></i>">>,
                    #button{id = <<"loglevel_label">>, title = <<"Minimum log severity to be displayed">>,
                        class = <<"btn btn-inverse btn-small">>, style = <<"width: 150px;">>, body = <<"Loglevel: <b>debug</b>">>},
                    #button{title = <<"Minimum log severity to be displayed">>, class = <<"btn btn-inverse btn-small dropdown-toggle">>,
                        data_fields = [{<<"data-toggle">>, <<"dropdown">>}], body = #span{class = <<"caret">>}},
                    #list{id = <<"loglevel_dropdown">>, class = <<"dropdown-menu dropdown-inverse">>, body = loglevel_dropdown_body(debug)}
                ]},
                #panel{class = <<"btn-group">>, style = <<"margin: 12px 15px;">>, body = [
                    <<"<i class=\"dropdown-arrow dropdown-arrow-inverse\"></i>">>,
                    #button{id = <<"max_logs_label">>, title = <<"Maximum number of logs to be displayed - oldest logs will be discarded">>,
                        class = <<"btn btn-inverse btn-small">>, style = <<"width: 120px;">>, body = <<"Max logs: <b>200</b>">>},
                    #button{title = <<"Maximum number of logs to be displayed - oldest logs will be discarded">>,
                        class = <<"btn btn-inverse btn-small dropdown-toggle">>, data_fields = [{<<"data-toggle">>, <<"dropdown">>}],
                        body = #span{class = <<"caret">>}},
                    #list{id = <<"max_logs_dropdown">>, class = <<"dropdown-menu dropdown-inverse">>, body = max_logs_dropdown_body(200)}
                ]},
                #panel{class = <<"btn-group">>, style = <<"margin: 12px 15px;">>, body = [
                    #button{postback = show_manage_clients_popup, class = <<"btn btn-inverse">>,
                        style = <<"width: 140px; height: 34px; padding: 6px 13px 8px;">>, id = <<"manage_clients_button">>,
                        body = <<"Manage clients">>}
                ]},
                #panel{class = <<"btn-group">>, style = <<"margin: 12px 15px;">>, body = [
                    #button{postback = show_filters_popup, class = <<"btn btn-inverse">>,
                        style = <<"width: 110px; height: 34px; padding: 6px 13px 8px;">>, id = <<"show_filters_button">>,
                        body = <<"Edit filters">>}
                ]},
                #list{class = <<"nav pull-right">>, body = [
                    #li{id = <<"generate_logs">>, body = #link{title = <<"Clear all logs">>, style = <<"padding: 18px 14px;">>,
                        body = #span{class = <<"fui-trash">>}, postback = clear_all_logs}}
                ]},
                #panel{class = <<"btn-group pull-right">>, style = <<"padding: 7px 14px 0px; margin-top: 7px;">>, body = [
                    #label{id = <<"auto_scroll_checkbox">>, class = <<"checkbox checked">>, for = <<"auto_scroll_checkbox">>,
                        style = <<"display: block; font-weight: bold;">>,
                        actions = gui_jq:postback_action(<<"auto_scroll_checkbox">>, toggle_auto_scroll), body = [
                            #span{class = <<"icons">>},
                            #checkbox{id = <<"auto_scroll_checkbox">>, data_fields = [{<<"data-toggle">>, <<"checkbox">>}],
                                value = <<"">>, checked = true},
                            <<"Auto scroll">>
                        ]}
                ]}
            ]}
        ]}
    ].


% Main table displaying logs
main_table() ->
    #table{id = <<"main_table">>, class = <<"table">>,
        style = <<"border-radius: 0; margin-bottom: 0; table-layout: fixed; width: 100%;">>,
        header = [
            #tr{cells = [
                #th{body = <<"Severity">>, style = <<?SEVERITY_COLUMN_STYLE>>},
                #th{body = <<"Time">>, style = <<?TIME_COLUMN_STYLE>>},
                #th{body = <<"Message">>, style = <<?MESSAGE_COLUMN_STYLE>>},
                #th{body = <<"Metadata">>, style = <<?METADATA_COLUMN_STYLE>>}
            ]}
        ]}.


% Footer popup panel containing filter preferences
footer_popup() ->
    #panel{class = <<"dialog success-dialog wide hidden">>,
        style = <<"z-index: 2; position:fixed; bottom: 0; margin-bottom: 0px; padding: 20px 0px; width: 100%;">>,
        id = <<"footer_popup">>, body = []}.


% This will be placed in footer_popup after user selects to edit logs
filters_panel() ->
    CloseButton = #link{postback = hide_filters_popup, title = <<"Hide">>, class = <<"glyph-link">>,
        style = <<"position: absolute; top: 8px; right: 8px; z-index: 3;">>,
        body = #span{class = <<"fui-cross">>, style = <<"font-size: 20px;">>}},
    [
        CloseButton,
        #panel{style = <<"margin: 0 40px; overflow:hidden; position: relative;">>, body = [
            #panel{style = <<"float: left; position: relative;">>, body = [
                filter_form(message_filter),
                filter_form(file_filter)
            ]}
        ]}
    ].


% This will be placed in footer_popup after user selects to edit logs
manage_clients_panel() ->
    CloseButton = #link{postback = hide_filters_popup, title = <<"Hide">>, class = <<"glyph-link">>,
        style = <<"position: absolute; top: 8px; right: 8px; z-index: 3;">>,
        body = #span{class = <<"fui-cross">>, style = <<"font-size: 20px;">>}},

    {ClientListBody, Identifiers} =
        case get_connected_clients() of
            empty ->
                Row = #tr{cells = [
                    #td{style = <<"border-color: rgb(82, 100, 118);">>, body = <<"--">>},
                    #td{style = <<"border-color: rgb(82, 100, 118);">>, body = <<"No clients are connected">>},
                    #td{style = <<"border-color: rgb(82, 100, 118);">>, body = <<"">>}
                ]},
                {Row, []};
            error ->
                Row = #tr{cells = [
                    #td{style = <<"border-color: rgb(82, 100, 118);">>, body = <<"--">>},
                    #td{style = <<"border-color: rgb(82, 100, 118);">>, body = <<"Error: cannot list fuse sessions">>},
                    #td{style = <<"border-color: rgb(82, 100, 118);">>, body = <<"">>}
                ]},
                {Row, []};
            Clients ->
                {ClientList, {_, Ids}} = lists:mapfoldl(
                    fun({UserName, FuseID}, {Counter, Idents}) ->
                        {Row, Identifier} = client_row(<<"client_row_", (integer_to_binary(Counter))/binary>>, false, UserName, FuseID),
                        {Row, {Counter + 1, Idents ++ Identifier}}
                    end, {1, []}, Clients),
                {ClientList, Ids}
        end,

    gui_jq:bind_enter_to_submit_button(<<"search_textbox">>, <<"search_button">>),
    [
        CloseButton,
        #panel{style = <<"margin: 0 40px; position: relative;">>, body = [
            #panel{style = <<"text-align: left; position: relative;">>, body = [
                #p{style = <<"margin-top: -5px; display: inline-block; margin-right: 170px;">>, body = <<"Change loglevels for chosen clients:">>},
                #panel{class = <<"input-append">>, style = <<"margin-bottom: 10px;">>, body = [
                    #textbox{id = <<"search_textbox">>, class = <<"span2">>,
                        style = <<"width: 150px;">>, placeholder = <<"Search users">>},
                    #panel{class = <<"btn-group">>, body = [
                        #button{id = <<"search_button">>, class = <<"btn">>, type = <<"button">>,
                            actions = gui_jq:form_submit_action(<<"search_button">>, {search_clients, Identifiers}, <<"search_textbox">>),
                            body = #span{class = <<"fui-search">>}}
                    ]}
                ]}
            ]},
            #panel{id = <<"client_table_viewport">>, style = <<"width: 650px; min-height: 200px; max-height: 200px; overflow-y: scroll;",
            "background-color: white; border-radius: 4px; border: 2px solid rgb(82, 100, 118); float: left; position: relative;">>, body = [
                #table{id = <<"client_table">>, class = <<"table table-stripped">>, style = <<"margin-bottom: 0;">>,
                    header = [
                        #tr{cells = [
                            #th{style = <<"border-color: rgb(82, 100, 118);">>, body = [
                                #link{postback = {toggle_clients, true, Identifiers}, title = <<"Hide">>, class = <<"glyph-link-gray">>,
                                    style = <<"position: absolute; top: 10px; left: 11px;">>,
                                    body = #span{class = <<"fui-checkbox-checked">>, style = <<"font-size: 20px;">>}
                                },
                                #link{postback = {toggle_clients, false, Identifiers}, title = <<"Hide">>, class = <<"glyph-link-gray">>,
                                    style = <<"position: absolute; top: 10px; left: 38px;">>,
                                    body = #span{class = <<"fui-checkbox-unchecked">>, style = <<"font-size: 20px;">>}
                                }
                            ]},
                            #th{style = <<"border-color: rgb(82, 100, 118);">>, body = <<"User">>},
                            #th{style = <<"border-color: rgb(82, 100, 118);">>, body = <<"Fuse ID">>}
                        ]}
                    ],
                    body = #tbody{body = ClientListBody}
                }
            ]},
            #panel{id = <<"loglevel_buttons_panel">>, style = <<"float: left; margin-left: 25px; ">>, body = loglevel_buttons_panel_body(Identifiers)}
        ]}
    ].


loglevel_buttons_panel_body(ClientIdentifiers) ->
    [
        #button{class = <<"btn btn-block btn-small btn-danger">>, style = <<"padding: 4px 13px 5px; width: 90px; font-weight: bold;">>,
            id = <<"lb_fatal">>, postback = {set_clients_loglevel, fatal, <<"lb_fatal">>, ClientIdentifiers}, body = <<"fatal">>},
        #button{class = <<"btn btn-block btn-small btn-danger">>, style = <<"padding: 4px 13px 5px; width: 90px; font-weight: bold;">>,
            id = <<"lb_error">>, postback = {set_clients_loglevel, error, <<"lb_error">>, ClientIdentifiers}, body = <<"error">>},
        #button{class = <<"btn btn-block btn-small btn-warning">>, style = <<"padding: 4px 13px 5px; width: 90px; font-weight: bold;">>,
            id = <<"lb_warning">>, postback = {set_clients_loglevel, warning, <<"lb_warning">>, ClientIdentifiers}, body = <<"warning">>},
        #button{class = <<"btn btn-block btn-small btn-success">>, style = <<"padding: 4px 13px 5px; width: 90px; font-weight: bold;">>,
            id = <<"lb_info">>, postback = {set_clients_loglevel, info, <<"lb_info">>, ClientIdentifiers}, body = <<"info">>},
        #button{class = <<"btn btn-block btn-small">>, style = <<"padding: 4px 13px 5px; width: 90px; font-weight: bold;">>,
            id = <<"lb_debug">>, postback = {set_clients_loglevel, debug, <<"lb_debug">>, ClientIdentifiers}, body = <<"debug">>},
        #button{class = <<"btn btn-block btn-small btn-inverse">>, style = <<"padding: 4px 13px 5px; width: 90px; font-weight: bold;">>,
            id = <<"lb_none">>, postback = {set_clients_loglevel, ?CLIENT_LOGLEVEL_NONE, <<"lb_none">>, ClientIdentifiers}, body = <<"none">>}
    ].


client_row(ID, Selected, UserName, FuseID) ->
    {RowClass, LinkClass, GlyphClass} =
        case Selected of
            true -> {<<"selected-item">>, <<"glyph-link-active">>, <<"fui-checkbox-checked">>};
            false -> {<<"">>, <<"glyph-link-gray">>, <<"fui-checkbox-unchecked">>}
        end,
    Identifier = [{ID, UserName, FuseID}],
    Row = #tr{class = RowClass, id = ID, cells = [
        #td{style = <<"border-color: rgb(82, 100, 118);">>, body = [
            #panel{style = <<"position: relative;">>, body = [
                #link{id = <<"link_", ID/binary>>, postback = {toggle_clients, not Selected, Identifier}, title = <<"Toggle this client">>,
                    class = LinkClass, style = <<"position: absolute; top: 0px;">>,
                    body = #span{class = GlyphClass, style = <<"font-size: 20px;">>}
                }]}
        ]},
        #td{style = <<"border-color: rgb(82, 100, 118);">>, body = gui_str:unicode_list_to_binary(UserName)},
        #td{style = <<"border-color: rgb(82, 100, 118);">>, body = gui_str:unicode_list_to_binary(FuseID)}
    ]},
    {Row, Identifier}.


% Creates a set of elements used to edit filter preferences of a single filter
filter_form(FilterType) ->
    #span{style = <<"display: inline-block; position: relative; height: 42px; margin-bottom: 15px; width: 410px; text-align: left;">>, body = [
        #label{id = get_filter_label(FilterType), style = <<"display: inline; margin: 9px 14px;">>,
            actions = gui_jq:postback_action(get_filter_label(FilterType), {toggle_filter, FilterType}),
            class = <<"label label-large label-inverse">>, body = get_filter_name(FilterType)},
        #p{id = get_filter_none(FilterType), style = <<"display: inline;">>, body = <<"off">>},
        #panel{id = get_filter_panel(FilterType), class = <<"input-append">>, style = <<"margin-bottom: 0px; display: inline;">>, body = [
            #textbox{id = get_filter_textbox(FilterType), class = <<"span2">>, body = <<"">>,
                placeholder = get_filter_placeholder(FilterType)},
            #panel{class = <<"btn-group">>, body = [
                #button{id = get_filter_submit_button(FilterType), class = <<"btn">>, type = <<"button">>, title = <<"Save">>,
                    body = #span{class = <<"fui-check">>},
                    actions = gui_jq:form_submit_action(get_filter_submit_button(FilterType), {update_filter, FilterType}, get_filter_textbox(FilterType))
                }
            ]}
        ]}
    ]}.


% Listing available clients - returns a list or one of two atoms: empty, error
get_connected_clients() ->
    try
        {ok, List} = dao_lib:apply(dao_cluster, list_fuse_sessions, [{by_valid_to, ?INFINITY}], 1),
        ClientList = lists:foldl(
            fun(#veil_document{uuid = UUID, record = #fuse_session{uid = UserID}} = SessionDoc, Acc) ->
                case dao_cluster:check_session(SessionDoc) of
                    ok ->
                        {ok, UserDoc} = user_logic:get_user({uuid, UserID}),
                        Acc ++ [{user_logic:get_login(UserDoc), UUID}];
                    _ ->
                        Acc
                end
            end, [], List),
        case ClientList of
            [] -> empty;
            _ -> ClientList
        end
    catch T:M ->
        ?error_stacktrace("Cannot list fuse sessions: ~p:~p", [T, M]),
        error
    end.


% Remebering which clients are selected on the list
set_selected_clients(List) ->
    put(selected_clients, List).

get_selected_clients() ->
    case get(selected_clients) of
        undefined -> [];
        List -> List
    end.


% Initialization of comet loop - trap_exit=true so we can control when a session terminates and
% the process should be removed from central_logger subscribers
comet_loop_init() ->
    process_flag(trap_exit, true),
    comet_loop(1, #page_state{}).

% Comet loop - waits for new logs, updates the page and repeats. Handles messages that change logging preferences.
comet_loop(Counter, PageState = #page_state{first_log = FirstLog, auto_scroll = AutoScroll}) ->
    Result =
        try
            receive
                {log, Log} ->
                    {NCounter, NPageState} = process_log(Counter, Log, PageState),
                    {NCounter, NPageState};
                toggle_auto_scroll ->
                    {Counter, PageState#page_state{auto_scroll = not AutoScroll}};
                clear_all_logs ->
                    remove_old_logs(Counter, FirstLog, 0),
                    {Counter, PageState#page_state{first_log = Counter}};
                {set_loglevel, Loglevel} ->
                    {Counter, PageState#page_state{loglevel = Loglevel}};
                {set_max_logs, MaxLogs} ->
                    NewFirstLog = remove_old_logs(Counter, FirstLog, MaxLogs),
                    {Counter, PageState#page_state{max_logs = MaxLogs, first_log = NewFirstLog}};
                {set_filter, FilterName, Filter} ->
                    {Counter, set_filter(PageState, FilterName, Filter)};
                {set_clients_loglevel, ClientList, Level, ButtonID} ->
                    set_clients_loglevel(ClientList, Level, ButtonID),
                    {Counter, PageState};
                display_error ->
                    catch gen_server:call(?Dispatcher_Name, {central_logger, 1, {unsubscribe, client, self()}}),
                    gui_jq:insert_bottom(<<"main_table">>, comet_error()),
                    gui_comet:flush(),
                    error;
                {'EXIT', _, _Reason} ->
                    catch gen_server:call(?Dispatcher_Name, {central_logger, 1, {unsubscribe, client, self()}}),
                    error;
                Other ->
                    ?debug("Unrecognized comet message in page_logs: ~p", [Other]),
                    {Counter, PageState}
            end
        catch _Type:_Msg ->
            ?error_stacktrace("Error in page_logs comet_loop - ~p: ~p", [_Type, _Msg]),
            catch gen_server:call(?Dispatcher_Name, {central_logger, 1, {unsubscribe, client, self()}}),
            gui_jq:insert_bottom(<<"main_table">>, comet_error()),
            gui_comet:flush(),
            error
        end,
    case Result of
        error -> ok; % Comet process terminates
        {NewCounter, NewState} -> ?MODULE:comet_loop(NewCounter, NewState)
    end.


% Check if log should be displayed, do if so and remove old logs if needed
process_log(Counter, {Message, Timestamp, Severity, Metadata},
    PageState = #page_state{
        loglevel = Loglevel,
        auto_scroll = AutoScroll,
        first_log = FirstLog,
        max_logs = MaxLogs,
        message_filter = MessageFilter,
        file_filter = FileFilter}) ->

    Filename = proplists:get_value(file, Metadata, ""),

    ShouldLog = filter_loglevel(Severity, Loglevel) and filter_contains(Message, MessageFilter) and filter_contains(Filename, FileFilter),

    {_NewCounter, _NewPageState} = case ShouldLog of
                                       false ->
                                           {Counter, PageState};
                                       true ->
                                           gui_jq:insert_bottom(<<"main_table">>, render_row(Counter, {Message, Timestamp, Severity, Metadata})),
                                           gui_jq:hide(<<?EXPANDED_LOG_ROW_ID_PREFIX, (integer_to_binary(Counter))/binary>>),
                                           NewFirstLog = remove_old_logs(Counter, FirstLog, MaxLogs),
                                           case AutoScroll of
                                               false ->
                                                   skip;
                                               true ->
                                                   gui_jq:wire(<<"$('html, body').animate({scrollTop: $(document).height()}, 0);">>)
                                           end,
                                           gui_comet:flush(),
                                           {Counter + 1, PageState#page_state{first_log = NewFirstLog}}
                                   end.


% Remove old logs until max_logs preference is satisfied
remove_old_logs(Counter, FirstLog, MaxLogs) ->
    case FirstLog + MaxLogs =< Counter of
        false ->
            gui_comet:flush(),
            FirstLog;
        true ->
            gui_jq:remove(<<?COLLAPSED_LOG_ROW_ID_PREFIX, (integer_to_binary(FirstLog))/binary>>),
            gui_jq:remove(<<?EXPANDED_LOG_ROW_ID_PREFIX, (integer_to_binary(FirstLog))/binary>>),
            remove_old_logs(Counter, FirstLog + 1, MaxLogs)
    end.


% Render a single row of logs - one collapsed and one expanded - they will be toggled with mouse clicks
render_row(Counter, {Message, Timestamp, Severity, Metadata}) ->
    CollapsedId = <<?COLLAPSED_LOG_ROW_ID_PREFIX, (integer_to_binary(Counter))/binary>>,
    ExpandedId = <<?EXPANDED_LOG_ROW_ID_PREFIX, (integer_to_binary(Counter))/binary>>,
    {CollapsedMetadata, ExpandedMetadata} = format_metadata(Metadata),

    CollapsedRow = #tr{class = <<"log_row">>, id = CollapsedId,
        actions = gui_jq:postback_action(CollapsedId, {toggle_log, Counter, true}), cells = [
            #td{body = format_severity(Severity), style = <<?SEVERITY_COLUMN_STYLE>>},
            #td{body = format_time(Timestamp), style = <<?TIME_COLUMN_STYLE>>},
            #td{body = gui_str:to_binary(Message), style = <<?MESSAGE_COLUMN_STYLE, " text-wrap:normal; word-wrap:break-word; white-space: nowrap; overflow: hidden;">>},
            #td{body = CollapsedMetadata, style = <<?METADATA_COLUMN_STYLE, " white-space: nowrap; overflow: hidden;">>}
        ]},

    ExpandedRow = #tr{class = <<"log_row">>, style = <<"background-color: rgba(26, 188, 156, 0.05);">>, id = ExpandedId,
        actions = gui_jq:postback_action(ExpandedId, {toggle_log, Counter, false}), cells = [
            #td{body = format_severity(Severity), style = <<?SEVERITY_COLUMN_STYLE>>},
            #td{body = format_time(Timestamp), style = <<?TIME_COLUMN_STYLE>>},
            #td{body = gui_str:to_binary(Message), style = <<?MESSAGE_COLUMN_STYLE, " text-wrap:normal; word-wrap:break-word;">>},
            #td{body = ExpandedMetadata, style = <<?METADATA_COLUMN_STYLE>>}
        ]},

    [CollapsedRow, ExpandedRow].


% Render the body of loglevel dropdown, so it highlights the current choice
loglevel_dropdown_body(Active) ->
    lists:map(
        fun(Loglevel) ->
            Class = case Loglevel of
                        Active -> <<"active">>;
                        _ -> <<"">>
                    end,
            ID = <<"loglevel_li_", (atom_to_binary(Loglevel, latin1))/binary>>,
            #li{id = ID, actions = gui_jq:postback_action(ID, {set_loglevel, Loglevel}),
                class = Class, body = #link{body = atom_to_binary(Loglevel, latin1)}}
        end, ?CLIENT_LOGLEVELS).


% Render the body of max logs dropdown, so it highlights the current choice
max_logs_dropdown_body(Active) ->
    lists:map(
        fun(Number) ->
            Class = case Number of
                        Active -> <<"active">>;
                        _ -> <<"">>
                    end,
            ID = <<"maxlogs_li_", (integer_to_binary(Number))/binary>>,
            #li{id = ID, actions = gui_jq:postback_action(ID, {set_max_logs, Number}),
                class = Class, body = #link{body = integer_to_binary(Number)}}
        end, ?MAX_LOGS_OPTIONS).


% Render a row in table informing about error in comet loop
comet_error() ->
    _TableRow = #tr{cells = [
        #td{body = <<"Error">>, style = <<?SEVERITY_COLUMN_STYLE, " color: red;">>},
        #td{body = format_time(now()), style = <<?TIME_COLUMN_STYLE, "color: red;">>},
        #td{body = <<"There has been an error in comet process. Please refresh the page.">>,
            style = <<?MESSAGE_COLUMN_STYLE, " body-wrap:normal; word-wrap:break-word; white-space: nowrap; overflow: hidden; color: red;">>},
        #td{body = <<"">>, style = <<?METADATA_COLUMN_STYLE, "color: red;">>}
    ]}.


% Format severity in logs
format_severity(debug) ->
    #label{class = <<"label">>, body = <<"debug">>, style = <<"display: block; font-weight: bold;">>};
format_severity(info) ->
    #label{class = <<"label label-success">>, body = <<"info">>, style = <<"display: block; font-weight: bold;">>};
format_severity(warning) ->
    #label{class = <<"label label-warning">>, body = <<"warning">>, style = <<"display: block; font-weight: bold;">>};
format_severity(error) ->
    #label{class = <<"label label-important">>, body = <<"error">>, style = <<"display: block; font-weight: bold;">>};
format_severity(fatal) ->
    #label{class = <<"label label-important">>, body = <<"fatal">>, style = <<"display: block; font-weight: bold;">>}.


% Format time in logs
format_time(Timestamp) ->
    {{YY, MM, DD}, {Hour, Min, Sec}} = calendar:now_to_local_time(Timestamp),
    TimeString = io_lib:format("~2..0w-~2..0w-~2..0w | ~2..0w:~2..0w:~2..0w",
        [YY rem 100, MM, DD, Hour, Min, Sec]),
    list_to_binary(TimeString).


% Format metadata in logs, for collapsed and expanded logs
format_metadata(Tags) ->
    Collapsed = case lists:keyfind(user, 1, Tags) of
                    {user, Value} ->
                        <<"<b>user:</b> ", (gui_str:unicode_list_to_binary(Value))/binary, " ...">>;
                    _ ->
                        <<"<b>unknown user</b> ...">>
                end,
    Expanded = lists:foldl(
        fun({Key, Value}, Acc) ->
            ValBinary = case is_integer(Value) of
                            true -> integer_to_binary(Value);
                            false -> gui_str:unicode_list_to_binary(Value)
                        end,
            <<Acc/binary, "<b>", (gui_str:to_binary(Key))/binary, ":</b> ", ValBinary/binary, "<br />">>
        end, <<"">>, Tags),
    {Collapsed, Expanded}.


% Return true if log should be displayed based on its severity and loglevel
filter_loglevel(LogSeverity, Loglevel) ->
    central_logger:client_loglevel_atom_to_int(LogSeverity) >= central_logger:client_loglevel_atom_to_int(Loglevel).


% Return true if given string satisfies given filter
filter_contains(String, Filter) ->
    case Filter of
        undefined -> true;
        ValidFilter ->
            binary:match(gui_str:to_binary(String), ValidFilter) /= nomatch
    end.


% =====================
% Event handling
api_event("escape_pressed", _, _) ->
    event(hide_filters_popup).


event(init) ->
    put(filters, #page_state{}),
    % Start a comet process
    {ok, Pid} = gui_comet:spawn(fun() -> comet_loop_init() end),
    put(comet_pid, Pid),
    % Subscribe for logs at central_logger
    case gen_server:call(?Dispatcher_Name, {central_logger, 1, {subscribe, client, Pid}}) of
        ok -> ok;
        Other ->
            ?error("central_logger is unreachable. RPC call returned: ~p", [Other]),
            Pid ! display_error
    end,
    ok;


event(terminate) ->
    ok;


event({toggle_clients, Selected, ClientList}) ->
    lists:foreach(
        fun({ID, UserName, FuseID}) ->
            SelectedClients = get_selected_clients() -- [FuseID],
            case Selected of
                true -> set_selected_clients([FuseID | SelectedClients]);
                false -> set_selected_clients(SelectedClients)
            end,
            {NewRow, _} = client_row(ID, Selected, UserName, FuseID),
            gui_jq:replace(ID, NewRow)
        end, ClientList);

event({set_clients_loglevel, Level, ButtonID, ClientIdentifiers}) ->
    case get_selected_clients() of
        [] ->
            ok;
        Clients ->
            set_selected_clients([]),
            event({toggle_clients, false, ClientIdentifiers}),
            gui_jq:update(ButtonID, #image{image = <<"/images/spinner.gif">>, style = <<"width: 16px;">>}),
            get(comet_pid) ! {set_clients_loglevel, Clients, Level, ButtonID}
    end;


event({search_clients, ClientList}) ->
    % Deselect all
    case gui_ctx:postback_param(<<"search_textbox">>) of
        <<"">> ->
            ok;
        Query ->
            {Select, Deselect} = lists:partition(
                fun({_, UserName, _}) ->
                    binary:match(gui_str:unicode_list_to_binary(UserName), Query) =/= nomatch
                end, ClientList),
            event({toggle_clients, true, Select}),
            event({toggle_clients, false, Deselect}),
            case Select of
                [] ->
                    ok;
                [{FirstRow, _, _} | _] ->
                    gui_jq:wire(<<"$('#client_table_viewport').animate({scrollTop: $('#", FirstRow/binary, "').offset().top - ",
                    "$('#client_table_viewport').offset().top + $('#client_table_viewport').scrollTop()}, 50);">>)
            end
    end;



event(toggle_auto_scroll) ->
    get(comet_pid) ! toggle_auto_scroll;


event(clear_all_logs) ->
    get(comet_pid) ! clear_all_logs;


% Collapse or expand a log
event({toggle_log, Id, ShowAll}) ->
    case ShowAll of
        true ->
            gui_jq:fade_in(<<?EXPANDED_LOG_ROW_ID_PREFIX, (integer_to_binary(Id))/binary>>, 300),
            gui_jq:hide(<<?COLLAPSED_LOG_ROW_ID_PREFIX, (integer_to_binary(Id))/binary>>);
        false ->
            gui_jq:hide(<<?EXPANDED_LOG_ROW_ID_PREFIX, (integer_to_binary(Id))/binary>>),
            gui_jq:fade_in(<<?COLLAPSED_LOG_ROW_ID_PREFIX, (integer_to_binary(Id))/binary>>, 300)
    end;


% Show filters edition panel
event(show_filters_popup) ->
    gui_jq:add_class(<<"footer_popup">>, <<"hidden">>),
    gui_jq:update(<<"footer_popup">>, filters_panel()),
    lists:foreach(
        fun(FilterType) ->
            gui_jq:bind_enter_to_submit_button(get_filter_textbox(FilterType), get_filter_submit_button(FilterType)),
            event({show_filter, FilterType})
        end, get_filter_types()),
    gui_jq:remove_class(<<"footer_popup">>, <<"hidden">>);


% Show client management panel
event(show_manage_clients_popup) ->
    gui_jq:add_class(<<"footer_popup">>, <<"hidden">>),
    gui_jq:update(<<"footer_popup">>, manage_clients_panel()),
    gui_jq:remove_class(<<"footer_popup">>, <<"hidden">>);


% Hide filters edition panel
event(hide_filters_popup) ->
    gui_jq:add_class(<<"footer_popup">>, <<"hidden">>);


% Change loglevel
event({set_loglevel, Loglevel}) ->
    gui_jq:update(<<"loglevel_label">>, <<"Loglevel: <b>", (atom_to_binary(Loglevel, latin1))/binary, "</b>">>),
    gui_jq:update(<<"loglevel_dropdown">>, loglevel_dropdown_body(Loglevel)),
    get(comet_pid) ! {set_loglevel, Loglevel};


% Change displayed log limit
event({set_max_logs, Number}) ->
    gui_jq:update(<<"max_logs_label">>, <<"Max logs: <b>", (integer_to_binary(Number))/binary, "</b>">>),
    gui_jq:update(<<"max_logs_dropdown">>, max_logs_dropdown_body(Number)),
    get(comet_pid) ! {set_max_logs, Number};


% Show patricular filter form
event({show_filter, FilterName}) ->
    Filter = get_filter(get(filters), FilterName),
    case (Filter =:= undefined) orelse (Filter =:= <<"">>) of
        true ->
            gui_jq:hide(get_filter_panel(FilterName)),
            gui_jq:show(get_filter_none(FilterName));
        _ ->
            gui_jq:show(get_filter_panel(FilterName)),
            gui_jq:hide(get_filter_none(FilterName)),
            gui_jq:set_value(get_filter_textbox(FilterName), Filter)
    end;


% Toggle patricular filter on/off
event({toggle_filter, FilterName}) ->
    Filter = get_filter(get(filters), FilterName),
    case Filter of
        undefined ->
            gui_jq:show(get_filter_panel(FilterName)),
            gui_jq:hide(get_filter_none(FilterName)),
            gui_jq:set_value(get_filter_textbox(FilterName), <<"''">>),
            gui_jq:focus(get_filter_textbox(FilterName)),
            put(filters, set_filter(get(filters), FilterName, <<"">>));
        _ ->
            gui_jq:hide(get_filter_panel(FilterName)),
            gui_jq:show(get_filter_none(FilterName)),
            put(filters, set_filter(get(filters), FilterName, undefined)),
            get(comet_pid) ! {set_filter, FilterName, undefined}
    end;


% Update patricular filter
event({update_filter, FilterName}) ->
    Filter = gui_ctx:postback_param(get_filter_textbox(FilterName)),
    case Filter of
        <<"">> ->
            put(filters, set_filter(get(filters), FilterName, undefined)),
            get(comet_pid) ! {set_filter, FilterName, undefined};
        Bin when is_binary(Bin) ->
            put(filters, set_filter(get(filters), FilterName, Filter)),
            get(comet_pid) ! {set_filter, FilterName, Filter};
        _ -> invalid
    end.


set_clients_loglevel(ClientList, Level, ButtonID) ->
    LoglevelInt = central_logger:client_loglevel_atom_to_int(Level),
    Result =
        try
            lists:foldl(
                fun(FuseID, Acc) ->
                    case request_dispatcher:send_to_fuse(FuseID, #changeremoteloglevel{level = logging_pb:int_to_enum(loglevel, LoglevelInt)}, "logging") of
                        ok -> Acc;
                        _ -> error
                    end
                end, ok, ClientList)
        catch _:_ ->
            error
        end,
    case Result of
        ok -> gui_jq:update(ButtonID, <<"success!">>);
        error -> gui_jq:update(ButtonID, <<"failed!">>)
    end,
    gui_jq:wire(<<"setTimeout(function f() {$('#", ButtonID/binary, "').html('", (gui_str:to_binary(Level))/binary, "')}, 1000);">>),
    gui_comet:flush().


% =====================
% Define types of filters and elements connected to them
get_filter_types() -> [message_filter, file_filter].

set_filter(PageState, message_filter, Filter) -> PageState#page_state{message_filter = Filter};
set_filter(PageState, file_filter, Filter) -> PageState#page_state{file_filter = Filter}.

get_filter(#page_state{message_filter = Filter}, message_filter) -> Filter;
get_filter(#page_state{file_filter = Filter}, file_filter) -> Filter.

get_filter_name(message_filter) -> <<"Toggle message filter">>;
get_filter_name(file_filter) -> <<"Toggle file filter">>.

get_filter_placeholder(message_filter) -> <<"Message contains">>;
get_filter_placeholder(file_filter) -> <<"File name contains">>.

get_filter_label(message_filter) -> <<"message_filter_label">>;
get_filter_label(file_filter) -> <<"file_filter_label">>.

get_filter_none(message_filter) -> <<"message_filter_none">>;
get_filter_none(file_filter) -> <<"file_filter_none">>.

get_filter_panel(message_filter) -> <<"message_filter_panel">>;
get_filter_panel(file_filter) -> <<"file_filter_panel">>.

get_filter_textbox(message_filter) -> <<"message_filter_textbox">>;
get_filter_textbox(file_filter) -> <<"file_filter_textbox">>.

get_filter_submit_button(message_filter) -> <<"message_filter_button">>;
get_filter_submit_button(file_filter) -> <<"file_filter_button">>.

