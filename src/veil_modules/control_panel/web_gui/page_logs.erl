%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains Nitrogen website code
%% @end
%% ===================================================================

-module(page_logs).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").
-include("registered_names.hrl").
-include("logging.hrl").

% Record used to store user preferences. One instance is kept in comet process, another one
% is remembered in page state for filter options to be persistent
-record(page_state, {
        loglevel=debug,
        auto_scroll=true,
        first_log=1,
        max_logs=200,
        message_filter=off,
        node_filter=off,
        module_filter=off,
        function_filter=off
    }).

% Widths of columns
-define(SEVERITY_COLUMN_WIDTH, "width: 90px;").
-define(TIME_COLUMN_WIDTH, "width: 180px;").
-define(METADATA_COLUMN_WIDTH, "width: 300px;").

% Prefixes used to generate IDs for logs
-define(COLLAPSED_LOG_ROW_ID_PREFIX, "clr").
-define(EXPANDED_LOG_ROW_ID_PREFIX, "elr").

% Available options of max log count
-define(MAX_LOGS_OPTIONS, [20, 50, 200, 500, 1000, 2000]).


main() -> #template { file="./gui_static/templates/bare.html" }.

title() -> "Logs".


% This will be placed instead of [[[body()]]] tag in template
body() -> 
    case gui_utils:user_logged_in() of
        false ->
            wf:redirect_to_login("/login");
        true -> 
            case gui_utils:can_view_logs() of
                false ->
                    wf:redirect_to_login("/manage_account");
                true ->
                    render_body()
            end
    end.


% User can view logs, render the body
render_body() -> 
    % Start a comet process
    {ok, Pid} = wf:comet(fun() -> comet_loop_init() end),
    %% TODO - what if central loger is down?
    % Subscribe for logs at central_logger
    gen_server:call(?Dispatcher_Name, {central_logger, 1, {subscribe, Pid}}),
    wf:state(comet_pid, Pid),
    wf:state(filters, #page_state{ }),
    Body = [        
        gui_utils:top_menu(logs_tab, logs_submenu()),        
        #panel { style="margin-top: 122px; z-index: -1;", body = main_table() },
        footer_popup()
    ],
    Body.


% Submenu that will end up concatenaetd to top menu
logs_submenu() ->
    [
        #panel { class="navbar-inner", style="border-bottom: 1px solid gray; padding-bottom: 5px;", body=[
            #panel { class="container", body=[
                #panel { class="btn-group", style="margin: 12px 15px;", body=[
                    "<i class=\"dropdown-arrow dropdown-arrow-inverse\"></i>",
                    #bootstrap_button { id=loglevel_label, title="Minimum log severity to be displayed", 
                        class="btn btn-inverse btn-small", style="width: 150px;", body="Loglevel: <b>debug</b>" },
                    #bootstrap_button { title="Minimum log severity to be displayed", class="btn btn-inverse btn-small dropdown-toggle", 
                        data_toggle="dropdown", body=#span { class="caret" } },
                    #list { id=loglevel_dropdown, class="dropdown-menu dropdown-inverse", body=loglevel_dropdown_body(debug)}  
                ]},
                #panel { class="btn-group", style="margin: 12px 15px;", body=[
                    "<i class=\"dropdown-arrow dropdown-arrow-inverse\"></i>",
                    #bootstrap_button { id=max_logs_label, title="Maximum number of logs to be displayed - oldest logs will be discarded", 
                        class="btn btn-inverse btn-small", style="width: 120px;", body="Max logs: <b>200</b>" },
                    #bootstrap_button { title="Maximum number of logs to be displayed - oldest logs will be discarded", 
                        class="btn btn-inverse btn-small dropdown-toggle", data_toggle="dropdown", body=#span { class="caret" } },
                    #list { id=max_logs_dropdown, class="dropdown-menu dropdown-inverse", body=max_logs_dropdown_body(200)}  
                ]},
                #panel { class="btn-group", style="margin: 12px 15px;", body=[
                    #button { postback=show_filters_popup, class="btn btn-inverse", style="width: 110px; height: 34px; padding: 6px 13px 8px;", 
                        id="show_filters_button", text="Edit filters" }
                ]},

                % Uncomment for development
                % #link{ title="Generate logs", style="padding: 18px 14px;",
                %        body="GENERUJ LOGI", postback=generate_logs },
                
                  
                #list {class="nav pull-right", body=[  
                    #listitem { id="generate_logs", body=#link{ title="Clear all logs", style="padding: 18px 14px;",
                        body=#span{ class="fui-trash" }, postback=clear_all_logs } }
                ]},
                #panel { class="btn-group pull-right", style="padding: 12px 14px 0px; margin-top: 7px;", body=[
                    #label { class="checkbox checked", for="auto_scroll_checkbox", 
                        actions=#event{ type=click, postback=toggle_auto_scroll}, body=[
                        #span { class="icons" },
                        #bootstrap_checkbox { data_toggle="checkbox", value="", checked=true },
                        "Auto scroll"
                    ]}        
                ]}      
            ]}
        ]}
    ].


% Main table displaying logs
main_table() ->
    #table { id="main_table", class="table table-stripped", 
        style="border-radius: 0; margin-bottom: 0; table-layout: fixed; width: 100%;", 
        rows=#tablerow { cells=[
        #tableheader { text="Severity", style=?SEVERITY_COLUMN_WIDTH },
        #tableheader { text="Time", style=?TIME_COLUMN_WIDTH },
        #tableheader { text="Message" },
        #tableheader { text="Metadata", style=?METADATA_COLUMN_WIDTH }
    ]}}.


% Footer popup panel containing filter preferences
footer_popup() ->
    #panel { class="dialog success-dialog wide hidden", 
        style="z-index: 2; position:fixed; bottom: 0; margin-bottom: 0px; padding: 20px 0px; width: 100%;", 
        id="footer_popup",  body=[] }.


% This will be placed in footer_popup after user selects to edit logs
filters_panel() ->
    CloseButton = #link{ postback=hide_filters_popup, title="Hide", class="glyph-link", 
        style="position: absolute; top: 8px; right: 8px; z-index: 3;", 
        body=#span{ class="fui-cross", style="font-size: 20px;" } },

    [
        CloseButton,
        #panel { style="margin: 0 40px; overflow:hidden; position: relative;", body=[
            #panel { style="float: left; position: relative;", body=[
                filter_form(message_filter),
                filter_form(node_filter)
            ]},
            #panel { style="float: left; position: relative; clear: both;", body=[
                filter_form(module_filter),
                filter_form(function_filter)
            ]}
        ]}
    ].


% Creates a set of elements used to edit filter preferences of a single filter
filter_form(FilterType) ->
    #span { style="display: inline-block; position: relative; height: 42px; margin-bottom: 15px; width: 410px; text-align: left;", body=[
        #label{ actions=#event{ type=click, postback={toggle_filter, FilterType} },
            class="label label-large label-inverse", text=get_filter_name(FilterType), style="display: inline; margin: 9px 14px;"}, 
        #p { id=get_filter_none(FilterType), style="display: inline;", text="off" },
        #panel { id=get_filter_panel(FilterType), class="input-append", style="; margin-bottom: 0px; display: inline;", body=[
            #textbox { id=get_filter_textbox(FilterType), class="span2", text="", 
                placeholder=get_filter_placeholder(FilterType), postback={update_filter, FilterType} },
            #panel { class="btn-group", body=[
                #bootstrap_button { class="btn", type="button", 
                    body=#span { class="fui-check" }, postback={update_filter, FilterType} }
            ]}
        ]}
    ]}.



% Initialization of comet loop - trap_exit=true so we can control when a session terminates and
% the process should be removed from central_logger subscribers
comet_loop_init() ->
    process_flag(trap_exit, true),
    comet_loop(1, #page_state { }).

% Comet loop - waits for new logs, updates the page and repeats. Handles messages that changelogging preferences.
comet_loop(Counter, PageState=#page_state { first_log=FirstLog, auto_scroll=AutoScroll }) ->
    try
        receive
            {log, Log} ->
                {NewCounter, NewPageState} = process_log(Counter, Log, PageState),
                comet_loop(NewCounter, NewPageState);
            toggle_auto_scroll ->
                comet_loop(Counter, PageState#page_state { auto_scroll=not AutoScroll }); 
            clear_all_logs ->
                remove_old_logs(Counter, FirstLog, 0),
                comet_loop(Counter, PageState#page_state { first_log=Counter });           
            {set_loglevel, Loglevel} ->
                comet_loop(Counter, PageState#page_state { loglevel=Loglevel });
            {set_max_logs, MaxLogs} ->
                NewFirstLog = remove_old_logs(Counter, FirstLog, MaxLogs),
                comet_loop(Counter, PageState#page_state { max_logs=MaxLogs, first_log=NewFirstLog });
            {set_filter, FilterName, Filter} ->       
                comet_loop(Counter, set_filter(PageState, FilterName, Filter));
            {'EXIT', _, _Reason} ->
                gen_server:call(?Dispatcher_Name, {central_logger, 1, {unsubscribe, self()}});
            _ ->
                comet_loop(Counter, PageState)
        end
    catch _Type:_Msg ->
        ?debug_stacktrace("~p ~p", [_Type, _Msg]),
        try
          gen_server:call(?Dispatcher_Name, {central_logger, 1, {unsubscribe, self()}})
        catch
          _:_ ->
            error
        end,
        wf:insert_bottom(main_table, comet_error()),
        wf:flush()
    end.


% Check if log should be displayed, do if so and remove old logs if needed
process_log(Counter, {Message, Timestamp, Severity, Metadata}, PageState=#page_state{ loglevel=Loglevel, auto_scroll=AutoScroll, first_log=FirstLog, 
        max_logs=MaxLogs, message_filter=MessageFilter, node_filter=NodeFilter, module_filter=ModuleFilter, function_filter=FunctionFilter }) ->

    Node = proplists:get_value(node, Metadata, ""),
    Module = proplists:get_value(module, Metadata, ""),
    Function = proplists:get_value(function, Metadata, ""),

    ShouldLog = filter_loglevel(Severity, Loglevel) and filter_contains(Message, MessageFilter) and filter_contains(Node, NodeFilter) 
            and filter_contains(Module, ModuleFilter) and filter_contains(Function, FunctionFilter),

    {_NewCounter, _NewPageState} = case ShouldLog of
        false ->
            {Counter, PageState};
        true ->
            wf:insert_bottom(main_table, render_row(Counter, {Message, Timestamp, Severity, Metadata})),
            wf:wire(?EXPANDED_LOG_ROW_ID_PREFIX ++ integer_to_list(Counter), #hide {}),
            NewFirstLog = remove_old_logs(Counter, FirstLog, MaxLogs),
            case AutoScroll of
                false -> skip;
                true -> wf:wire("$('html, body').animate({scrollTop: $(document).height()}, 50);")
            end,
            wf:flush(),
            {Counter + 1, PageState#page_state { first_log=NewFirstLog }}
    end.


% Remove old logs until max_logs preference is satisfied
remove_old_logs(Counter, FirstLog, MaxLogs) ->
    case FirstLog + MaxLogs =< Counter of 
        false ->
            wf:flush(),
            FirstLog;
        true ->
            wf:remove(?COLLAPSED_LOG_ROW_ID_PREFIX ++ integer_to_list(FirstLog)),
            wf:remove(?EXPANDED_LOG_ROW_ID_PREFIX ++ integer_to_list(FirstLog)),
            remove_old_logs(Counter, FirstLog + 1, MaxLogs)
    end.


% Render a single row of logs - one collapsed and one expanded - they will be toggled with mouse clicks
render_row(Counter, {Message, Timestamp, Severity, Metadata}) ->
    CollapsedId = ?COLLAPSED_LOG_ROW_ID_PREFIX ++ integer_to_list(Counter),
    ExpandedId = ?EXPANDED_LOG_ROW_ID_PREFIX ++ integer_to_list(Counter),
    {CollapsedMetadata, ExpandedMetadata} = format_metadata(Metadata),

    CollapsedRow = #tablerow { class="log_row", id=CollapsedId, 
            actions=#event{type = click, postback = {toggle_log, Counter, true}}, cells=[
        #tablecell { body=format_severity(Severity), style=?SEVERITY_COLUMN_WIDTH },
        #tablecell { text=format_time(Timestamp), style=?TIME_COLUMN_WIDTH },
        #tablecell { text=Message, style="text-wrap:normal; word-wrap:break-word; white-space: nowrap; overflow: hidden;" },
        #tablecell { body=CollapsedMetadata, style=?METADATA_COLUMN_WIDTH ++ "white-space: nowrap; overflow: hidden;" }
    ]},

    ExpandedRow = #tablerow { class="log_row", style="background-color: rgba(26, 188, 156, 0.05);", id=ExpandedId, 
            actions=#event{type = click, postback = {toggle_log, Counter, false}}, cells=[
        #tablecell { body=format_severity(Severity), style=?SEVERITY_COLUMN_WIDTH },
        #tablecell { text=format_time(Timestamp), style=?TIME_COLUMN_WIDTH },
        #tablecell { text=Message, style="text-wrap:normal; word-wrap:break-word;" },
        #tablecell { body=ExpandedMetadata, style=?METADATA_COLUMN_WIDTH }
    ]},

    [CollapsedRow, ExpandedRow].


% Render the body of loglevel dropdown, so it highlights the current choice
loglevel_dropdown_body(Active) ->
    lists:map(
        fun(Loglevel) ->
            Class = case Loglevel of
                Active -> "active";
                _ -> ""
            end,                    
            #listitem { actions = #event{type = click, postback = {set_loglevel, Loglevel}}, 
                class=Class, body=#link { text=Loglevel } }
        end, ?LOGLEVEL_LIST).


% Render the body of max logs dropdown, so it highlights the current choice
max_logs_dropdown_body(Active) ->
    lists:map(
        fun(Number) ->
            Class = case Number of
                Active -> "active";
                _ -> ""
            end,                    
            #listitem { actions = #event{type = click, postback = {set_max_logs, Number}}, 
                class=Class, body=#link { text=integer_to_list(Number) } }
        end, ?MAX_LOGS_OPTIONS).


% Render a row in table informing about error in comet loop
comet_error() ->
    _TableRow = #tablerow { cells=[
        #tablecell { text="Error", style=?SEVERITY_COLUMN_WIDTH ++ "color: red;" },
        #tablecell { text=format_time(now()), style=?TIME_COLUMN_WIDTH ++ "color: red;" },
        #tablecell { text="There has been an error in comet process. Please refresh the page.", 
            style="text-wrap:normal; word-wrap:break-word; white-space: nowrap; overflow: hidden; color: red;" },
        #tablecell { text="", style=?METADATA_COLUMN_WIDTH ++ "color: red;" }                      
    ]}.


% Format severity in logs
format_severity(debug)      -> #label{class="label",                    text="debug"};
format_severity(info)       -> #label{class="label label-success",      text="info"};
format_severity(notice)     -> #label{class="label label-warning",      text="notice"};
format_severity(warning)    -> #label{class="label label-warning",      text="warning"};
format_severity(error)      -> #label{class="label label-important",    text="error"};
format_severity(critical)   -> #label{class="label label-important",    text="critical"};
format_severity(alert)      -> #label{class="label label-important",    text="alert"};
format_severity(emergency)  -> #label{class="label label-important",    text="emergency"}.


% Format time in logs
format_time(Timestamp) ->
    {_, _, Micros} = Timestamp,
    {{YY, MM, DD}, {Hour, Min, Sec}} = calendar:now_to_local_time(Timestamp),
    io_lib:format("~2..0w-~2..0w-~2..0w | ~2..0w:~2..0w:~2..0w.~3..0w",
                  [YY rem 100, MM, DD, Hour, Min, Sec, Micros div 1000]).


% Format metadata in logs, for collapsed and expanded logs
format_metadata(Tags) ->
    Collapsed = case lists:keyfind(node, 1, Tags) of
        {Key, Value} ->
            "<b>" ++ wf:to_list(Key) ++ ":</b> " ++ wf:to_list(Value) ++ " ...";
        _ ->
            "<b>unknown node</b> ..."
    end,
    Expanded = lists:foldl(
        fun({Key, Value}, Acc) ->
            Acc ++ "<b>" ++ to_list(Key) ++ ":</b> " ++ to_list(Value) ++ "<br />"
        end, [], Tags),
    {Collapsed, Expanded}.


% Return true if log should be displayed based on its severity and loglevel
filter_loglevel(LogSeverity, Loglevel) ->
    logger:loglevel_atom_to_int(LogSeverity) >= logger:loglevel_atom_to_int(Loglevel).


% Return true if given string satisfies given filter
filter_contains(String, Filter) ->
    case Filter of 
        off -> true;
        "" -> true;
        ValidFilter -> 
            string:str(to_list(String), ValidFilter) > 0
    end.


% =====================
% Event handling 

event(toggle_auto_scroll) -> 
    wf:state(comet_pid) ! toggle_auto_scroll;


event(clear_all_logs) -> 
    wf:state(comet_pid) ! clear_all_logs;


% Collapse or expand a log
event({toggle_log, Id, ShowAll}) ->
    case ShowAll of
        true -> 
            wf:wire(?EXPANDED_LOG_ROW_ID_PREFIX ++ integer_to_list(Id), #appear { speed=300 }),
            wf:wire(?COLLAPSED_LOG_ROW_ID_PREFIX ++ integer_to_list(Id), #hide{ });
        false ->
            wf:wire(?EXPANDED_LOG_ROW_ID_PREFIX ++ integer_to_list(Id), #hide{ }),
            wf:wire(?COLLAPSED_LOG_ROW_ID_PREFIX ++ integer_to_list(Id), #appear { speed=300 })
    end,
    wf:flush();


% Show filters edition panel
event(show_filters_popup) ->
    wf:wire("footer_popup", #add_class { class="hidden", speed=0 }),
    wf:update("footer_popup", filters_panel()),
    lists:foreach(
        fun(FilterType) ->
            event({show_filter, FilterType})
        end, get_filter_types()),    
    wf:wire("footer_popup", #remove_class { class="hidden", speed=50 }),
    wf:flush();


% Hide filters edition panel
event(hide_filters_popup) ->
    wf:wire("footer_popup", #add_class { class="hidden", speed=0 }),
    wf:flush();


% Change loglevel
event({set_loglevel, Loglevel}) ->
    wf:update(loglevel_label, wf:f("Loglevel: <b>~p</b>", [Loglevel])),
    wf:update(loglevel_dropdown, loglevel_dropdown_body(Loglevel)),
    wf:flush(),
    wf:state(comet_pid) ! {set_loglevel, Loglevel};


% Change displayed log limit
event({set_max_logs, Number}) ->
    wf:update(max_logs_label, wf:f("Max logs: <b>~B</b>", [Number])),
    wf:update(max_logs_dropdown, max_logs_dropdown_body(Number)),
    wf:flush(),
    wf:state(comet_pid) ! {set_max_logs, Number};


% Show patricular filter form
event({show_filter, FilterName}) ->
    MessageFilter = get_filter(wf:state(filters), FilterName),
    case MessageFilter of
        off ->
            wf:wire(get_filter_panel(FilterName), #hide{ }),
            wf:wire(get_filter_none(FilterName), #appear { speed=0 });
        _ ->
            wf:wire(get_filter_panel(FilterName), #appear { speed=0 }),
            wf:wire(get_filter_none(FilterName), #hide{ }),        
            wf:set(get_filter_textbox(FilterName), MessageFilter)
    end,
    wf:flush();


% Toggle patricular filter on/off
event({toggle_filter, FilterName}) ->   
    MessageFilter = get_filter(wf:state(filters), FilterName),
    case MessageFilter of
        off ->
            wf:wire(get_filter_panel(FilterName), #appear { speed=0 }),
            wf:wire(get_filter_none(FilterName), #hide{ }),        
            wf:set(get_filter_textbox(FilterName), ""),
            wf:state(filters, set_filter(wf:state(filters), FilterName, ""));
        _ ->
            wf:wire(get_filter_panel(FilterName), #hide{ }),
            wf:wire(get_filter_none(FilterName), #appear { speed=0 }),
            wf:state(filters, set_filter(wf:state(filters), FilterName, off)),
            wf:state(comet_pid) ! {set_filter, FilterName, off}
    end,
    wf:flush();


% Update patricular filter
event({update_filter, FilterName}) ->
    Filter = wf:q(get_filter_textbox(FilterName)),
    wf:state(filters, set_filter(wf:state(filters), FilterName, Filter)),
    wf:state(comet_pid) ! {set_filter, FilterName, Filter};


% Development helper function - generates logs
event(generate_logs) ->
    random:seed(now()),
    Metadata = [
        {node, "ccm1@127.0.0.1"},
        {module, "dummy_mod"},
        {function, "dummy_fun"},
        {line, "456"},
        {pid, "<0.0.0>"}
    ],
    lists:foreach(
        fun(Severity) -> 
            Message = lists:flatten(lists:duplicate(10, io_lib:format("~.36B", [random:uniform(98*567*456*235*232*3465*23552*3495*43534*345436*45)]))),
            lager:log(Severity, Metadata, Message)
        end, ?LOGLEVEL_LIST).


% =====================
% Define types of filters and elements connected to them
get_filter_types() -> [message_filter, node_filter, module_filter, function_filter].

set_filter(PageState, message_filter,   Filter) -> PageState#page_state { message_filter=Filter };   
set_filter(PageState, node_filter,      Filter) -> PageState#page_state { node_filter=Filter };
set_filter(PageState, module_filter,    Filter) -> PageState#page_state { module_filter=Filter };
set_filter(PageState, function_filter,  Filter) -> PageState#page_state { function_filter=Filter }.
    
get_filter(#page_state { message_filter=Filter },   message_filter)     -> Filter;
get_filter(#page_state { node_filter=Filter },      node_filter)        -> Filter;
get_filter(#page_state { module_filter=Filter },    module_filter)      -> Filter;
get_filter(#page_state { function_filter=Filter },  function_filter)    -> Filter.

get_filter_name(message_filter)  -> "Toggle message filter";
get_filter_name(node_filter)     -> "Toggle node filter";
get_filter_name(module_filter)   -> "Toggle module filter";
get_filter_name(function_filter) -> "Toggle function filter".

get_filter_placeholder(message_filter)  -> "Message contains";
get_filter_placeholder(node_filter)     -> "Node contains";
get_filter_placeholder(module_filter)   -> "Module contains";
get_filter_placeholder(function_filter) -> "Function contains".

get_filter_none(message_filter)  -> "message_filter_none";
get_filter_none(node_filter)     -> "node_filter_none";
get_filter_none(module_filter)   -> "module_filter_none";
get_filter_none(function_filter) -> "function_filter_none".

get_filter_panel(message_filter)  -> "message_filter_panel";
get_filter_panel(node_filter)     -> "node_filter_panel";
get_filter_panel(module_filter)   -> "module_filter_panel";
get_filter_panel(function_filter) -> "function_filter_panel".

get_filter_textbox(message_filter)  -> "message_filter_textbox";
get_filter_textbox(node_filter)     -> "node_filter_textbox";
get_filter_textbox(module_filter)   -> "module_filter_textbox";
get_filter_textbox(function_filter) -> "function_filter_textbox".


% convenience function, converts any term to string
to_list(Arg) when is_list(Arg) -> Arg;
to_list(Arg) ->
    try wf:to_list(Arg) of
        Res -> Res
    catch
        _:_ -> lists:flatten(io_lib:format("~p", [Arg]))
    end.