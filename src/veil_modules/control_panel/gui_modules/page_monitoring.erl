%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code
%% @end
%% ===================================================================

-module(page_monitoring).
-compile(export_all).
-include("veil_modules/control_panel/common.hrl").
-include("registered_names.hrl").
-include("logging.hrl").

-define(ENCODING, latin1).
-define(TIME_RANGES, [<<"last 5 minutes">>, <<"last 15 minutes">>, <<"last hour">>, <<"last 24 hours">>, <<"last 7 days">>, <<"last 30 days">>, <<"last 365 days">>]).
-define(SUMMARY_CHARTS, [<<"CPU utilization">>, <<"memory usage">>, <<"network throughput">>, <<"network transfer">>, <<"ports transfer">>, <<"storage IO transfer">>]).
-define(NODE_CHARTS, [<<"CPU utilization">>, <<"memory usage">>, <<"network throughput">>, <<"network transfer">>, <<"ports transfer">>]).
-define(REMOVE_CHART_COLUMN_WIDTH, "width: 10px;").

-record(page_state, {node = undefined, time_range = undefined, chart = undefined, charts = dict:new()}).
-record(chart, {id, node, time_range, chart}).

%% Template points to the template file, which will be filled with content
main() ->
  case gui_utils:maybe_redirect(true, false, false, true) of
    true ->
      #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, <<"">>}, {body, <<"">>}]};
    false ->
      case gui_utils:can_view_logs() of
        false ->
          wf:redirect(<<"/">>),
          #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, <<"">>}, {body, <<"">>}]};
        true ->
          #dtl{file = "monitoring", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}]}
      end
  end.

%% Page title
title() -> <<"Monitoring">>.

% This will be placed instead of [[[body()]]] tag in template
body() ->
  [
    gui_utils:top_menu(monitoring_tab, monitoring_submenu()),
    #panel{style = <<"margin-top: 122px; z-index: -1;">>, body = main_table()}
  ].

% Submenu that will end up concatenated to top menu
monitoring_submenu() ->
  [
    #panel{class = <<"navbar-inner">>, style = <<"border-bottom: 1px solid gray; padding-bottom: 5px;">>, body = [
      #panel{class = <<"container">>, style = <<"text-align: center">>, body = [
        #panel{class = <<"btn-group">>, style = <<"margin: 12px 15px;">>, body = [
          <<"<i class=\"dropdown-arrow dropdown-arrow-inverse\"></i>">>,
          #button{id = <<"nodes_button">>, class = <<"btn btn-danger btn-inverse btn-small dropdown-toggle">>, style = <<"width: 180px;">>,
            data_fields = [{<<"data-toggle">>, <<"dropdown">>}], body = [
              #span{id = <<"nodes_label">>, class = <<"filter-option pull-left">>, body = <<"<b>Node</b>">>},
              #span{class = <<"caret pull-right">>}
            ]},
          #list{id = <<"nodes_dropdown">>, class = <<"dropdown-menu dropdown-inverse">>,
            style = <<"overflow-y: auto; max-height: 200px;">>, body = nodes_dropdown_body(undefined)}
        ]},
        #panel{class = <<"btn-group">>, style = <<"margin: 12px 15px;">>, body = [
          <<"<i class=\"dropdown-arrow dropdown-arrow-inverse\"></i>">>,
          #button{id = <<"time_ranges_button">>, class = <<"btn btn-danger btn-inverse btn-small dropdown-toggle">>, style = <<"width: 200px;">>,
            data_fields = [{<<"data-toggle">>, <<"dropdown">>}], body = [
              #span{id = <<"time_ranges_label">>, class = <<"filter-option pull-left">>, body = <<"<b>Time range</b>">>},
              #span{class = <<"caret pull-right">>}
            ]},
          #list{id = <<"time_ranges_dropdown">>, class = <<"dropdown-menu dropdown-inverse">>,
            style = <<"overflow-y: auto; max-height: 200px;">>, body = time_ranges_dropdown_body(undefined)}
        ]},
        #panel{class = <<"btn-group">>, style = <<"margin: 12px 15px;">>, body = [
          <<"<i class=\"dropdown-arrow dropdown-arrow-inverse\"></i>">>,
          #button{id = <<"charts_button">>, class = <<"btn btn-danger btn-inverse btn-small dropdown-toggle">>, style = <<"width: 200px;">>,
            data_fields = [{<<"data-toggle">>, <<"dropdown">>}], body = [
              #span{id = <<"charts_label">>, class = <<"filter-option pull-left">>, body = <<"<b>Chart</b>">>},
              #span{class = <<"caret pull-right">>}
            ]},
          #list{id = <<"charts_dropdown">>, class = <<"dropdown-menu dropdown-inverse">>,
            style = <<"overflow-y: auto; max-height: 200px;">>, body = charts_dropdown_body(undefined, ?NODE_CHARTS)}
        ]},
        #panel{class = <<"btn-group">>, style = <<"margin: 12px 15px;">>, body = [
          #button{postback = add_chart, class = <<"btn btn-primary btn-small">>,
            style = <<"font-weight: bold;">>, body = <<"Add chart">>}
        ]}
      ]}
    ]}
  ].

% Main table displaying charts
main_table() ->
  #table{id = <<"main_table">>, class = <<"table table-stripped">>,
    style = <<"border-radius: 0; margin-bottom: 0; table-layout: fixed; width: 100%;">>}.

% Initialization of comet loop
comet_loop_init() ->
  comet_loop(1, #page_state{}).

% Comet loop - Adds, updates and removes monitoring charts. Handles messages that change chart preferences.
comet_loop(Counter, PageState = #page_state{charts = Charts}) ->
  try
    receive
      {set_node, Node} -> comet_loop(Counter, PageState#page_state{node = Node});
      {set_time_range, TimeRange} -> comet_loop(Counter, PageState#page_state{time_range = TimeRange});
      {set_chart, Chart} -> comet_loop(Counter, PageState#page_state{chart = Chart});
      add_chart ->
        case validate(PageState) of
          ok ->
            lager:info("Page state: ~p ~p ~p", [PageState#page_state.node, PageState#page_state.time_range, PageState#page_state.chart]),
            NewChart = add_chart(Counter, PageState#page_state.node, PageState#page_state.time_range, PageState#page_state.chart),
            NewCharts = dict:store(Counter, NewChart, Charts),
            {ok, Period} = case PageState#page_state.node of
                             summary -> application:get_env(?APP_Name, cluster_monitoring_period);
                             _ -> application:get_env(?APP_Name, node_monitoring_period)
                           end,
            erlang:send_after(1000 * Period, self(), {update_chart, Counter}),
            comet_loop(Counter + 1, PageState#page_state{node = undefined, time_range = undefined, chart = undefined, charts = NewCharts});
          _ -> comet_loop(Counter, PageState)
        end;
      {update_chart, ID} ->
        try
          Chart = dict:fetch(ID, Charts),
          update_chart(Chart),
          {ok, Period} = case Chart#chart.node of
                           summary -> application:get_env(?APP_Name, cluster_monitoring_period);
                           _ -> application:get_env(?APP_Name, node_monitoring_period)
                         end,
          erlang:send_after(1000 * Period, self(), {update_chart, ID})
        catch
          _:Error -> lager:error("Can not update chart ~p: ~p", [ID, Error])
        end,
        comet_loop(Counter, PageState);
      {delete_chart, ID} ->
        NewCharts = dict:erase(ID, Charts),
        comet_loop(Counter, PageState#page_state{charts = NewCharts})
    end
  catch _Type:_Msg ->
    ?debug_stacktrace("~p ~p", [_Type, _Msg]),
    lager:info("Error: ~p:~p", [_Type, _Msg]),
    gui_utils:insert_bottom("main_table", comet_error()),
    gui_utils:flush()
  end.

% Render a row in table informing about error in comet loop
comet_error() ->
  #tr{cells = [
    #td{body = <<"There has been an error in comet process. Please refresh the page.">>,
      style = <<"body-wrap:normal; word-wrap:break-word; white-space: nowrap; overflow: hidden; color: red;">>},
    #td{body = <<"">>, style = <<?REMOVE_CHART_COLUMN_WIDTH, "color: red;">>}
  ]}.

% Checks whether user has selected all requiered options to plot chart
validate(#page_state{node = undefined}) ->
  wf:wire(#jquery{target = "nodes_button", method = ["removeClass"], args = ["\"btn-inverse\""]}),
  gui_utils:flush(),
  error;
validate(#page_state{time_range = undefined}) ->
  wf:wire(#jquery{target = "time_ranges_button", method = ["removeClass"], args = ["\"btn-inverse\""]}),
  gui_utils:flush(),
  error;
validate(#page_state{chart = undefined}) ->
  wf:wire(#jquery{target = "charts_button", method = ["removeClass"], args = ["\"btn-inverse\""]}),
  gui_utils:flush(),
  error;
validate(_) ->
  gui_utils:update("nodes_label", <<"<b>Node</b>">>),
  gui_utils:update("nodes_dropdown", nodes_dropdown_body(undefined)),
  gui_utils:update("time_ranges_label", <<"<b>Time range</b>">>),
  gui_utils:update("time_ranges_dropdown", time_ranges_dropdown_body(undefined)),
  gui_utils:update("charts_label", <<"<b>Chart</b>">>),
  gui_utils:update("charts_dropdown", charts_dropdown_body(undefined, ?NODE_CHARTS)),
  ok.

% Render the body of nodes dropdown, so that it highlights the current choice
nodes_dropdown_body(ActiveNode) ->
  Nodes = try gen_server:call({global, ?CCM}, get_nodes, 1000) of
            [] -> [];
            NonEmptyList -> lists:zip([summary | NonEmptyList], lists:seq(1, 1 + length(NonEmptyList)))
          catch
            Type:Error ->
              lager:error("Ccm connection error: ~p:~p", [Type, Error]),
              []
          end,
  lists:map(fun({Node, Index}) ->
    Class = case Node of
              ActiveNode -> <<"active">>;
              _ -> <<"">>
            end,
    ID = <<"node_li_", (integer_to_binary(Index))/binary>>,
    #li{id = ID, actions = #event{type = "click", postback = {set_node, Node}, target = ID},
      class = Class, body = #link{style = <<"text-align: left;">>, body = get_node_name(Node)}}
  end, Nodes).

% Render the body of time ranges dropdown, so that it highlights the current choice
time_ranges_dropdown_body(ActiveTimeRange) ->
  lists:map(fun({TimeRange, Index}) ->
    Class = case TimeRange of
              ActiveTimeRange -> <<"active">>;
              _ -> <<"">>
            end,
    ID = <<"time_range_li_", (integer_to_binary(Index))/binary>>,
    #li{id = ID, actions = #event{type = "click", postback = {set_time_range, TimeRange}, target = ID},
      class = Class, body = #link{style = <<"text-align: left;">>, body = TimeRange}}
  end, lists:zip(?TIME_RANGES, lists:seq(1, length(?TIME_RANGES)))).

% Render the body of charts dropdown, so that it highlights the current choice
charts_dropdown_body(ActiveChart, Charts) ->
  lists:map(
    fun({Chart, Index}) ->
      Class = case Chart of
                ActiveChart -> <<"active">>;
                _ -> <<"">>
              end,
      ID = <<"chart_li_", (integer_to_binary(Index))/binary>>,
      #li{id = ID, actions = #event{type = "click", postback = {set_chart, Chart, Charts}, target = ID},
        class = Class, body = #link{style = <<"text-align: left;">>, body = Chart}}
    end, lists:zip(Charts, lists:seq(1, length(Charts)))).

% returns ip address of node or 'summary'
get_node_name(summary) ->
  <<"summary">>;
get_node_name(Node) ->
  [_, NodeIP] = binary:split(atom_to_binary(Node, ?ENCODING), <<"@">>),
  NodeIP.

% adds new chart on page
add_chart(Counter, Node, TimeRange, Chart) ->
  {StatsJSON, IdJSON, ChartJSON, TitleJSON} = get_json_data(Counter, Node, TimeRange, Chart),
  RowID = "row_" ++ integer_to_list(Counter),
  ChartID = <<"chart_", (integer_to_binary(Counter))/binary>>,
  gui_utils:insert_top("main_table", #tr{id = list_to_binary(RowID),
    cells = [
      #th{body = #panel{id = ChartID}},
      #th{body = #link{postback = {delete_chart, RowID, Counter}, title = <<"Remove">>, class = <<"glyph-link">>,
        body = #span{class = <<"fui-cross">>, style = <<"font-size: 20px;">>}},
        style = <<?REMOVE_CHART_COLUMN_WIDTH>>}
    ]}),
  wf:wire("drawChart(" ++ StatsJSON ++ "," ++ IdJSON ++ "," ++ ChartJSON ++ "," ++ TitleJSON ++ ");"),
  gui_utils:flush(),
  #chart{id = Counter, node = Node, time_range = TimeRange, chart = Chart}.

% updates specified chart on page
update_chart(#chart{id = ID, node = Node, time_range = TimeRange, chart = Chart}) ->
  {StatsJSON, IdJSON, ChartJSON, TitleJSON} = get_json_data(ID, Node, TimeRange, Chart),
  wf:wire("drawChart(" ++ StatsJSON ++ "," ++ IdJSON ++ "," ++ ChartJSON ++ "," ++ TitleJSON ++ ");"),
  gui_utils:flush(),
  ok.

% returns data in json format applicable for Google Charts API
get_json_data(ID, Node, TimeRange, Chart) ->
  {Target, Command} = case Node of
                        summary -> {{global, ?CCM}, get_cluster_stats_json};
                        _ -> {{?Node_Manager_Name, Node}, get_node_stats_json}
                      end,
  {MegaSecs, Secs, _} = erlang:now(),
  EndTime = 1000000 * MegaSecs + Secs,
  StartTime = EndTime - translate_time_range(TimeRange),
  StatsJSON = try gen_server:call(Target, {Command, StartTime, EndTime}, 1000) of
                {ok, JSON} -> JSON;
                Error ->
                  lager:error("Can not fetch node stats json data: ~p", [Error]),
                  "{}"
              catch
                Type:Error ->
                  lager:error("Node: ~p connection error: ~p:~p", [Node, Type, Error]),
                  "{}"
              end,
  IdJSON = "\"" ++ integer_to_list(ID) ++ "\"",
  ChartJSON = "\"" ++ binary_to_list(Chart) ++ "\"",
  TitleJSON = "\"" ++ binary_to_list(<<" / ", (get_node_name(Node))/binary, " / ", TimeRange/binary>>) ++ "\"",
  {StatsJSON, IdJSON, ChartJSON, TitleJSON}.

% =====================
% Event handling

event(init) ->
  {ok, Pid} = gui_utils:comet(fun() -> comet_loop_init() end),
  put(comet_pid, Pid);

event({set_node, Node}) ->
  wf:wire(#jquery{target = "nodes_button", method = ["addClass"], args = ["\"btn-inverse\""]}),
  gui_utils:update("nodes_label", <<"Node: <b>", (get_node_name(Node))/binary, "</b>">>),
  gui_utils:update("nodes_dropdown", nodes_dropdown_body(Node)),
  gui_utils:update("charts_label", <<"<b>Chart</b>">>),
  case Node of
    summary -> gui_utils:update("charts_dropdown", charts_dropdown_body(undefined, ?SUMMARY_CHARTS));
    _ -> gui_utils:update("charts_dropdown", charts_dropdown_body(undefined, ?NODE_CHARTS))
  end,
  get(comet_pid) ! {set_chart, undefined},
  get(comet_pid) ! {set_node, Node};

event({set_time_range, TimeRange}) ->
  wf:wire(#jquery{target = "time_ranges_button", method = ["addClass"], args = ["\"btn-inverse\""]}),
  gui_utils:update("time_ranges_label", <<"Time range: <b>", TimeRange/binary, "</b>">>),
  gui_utils:update("time_ranges_dropdown", time_ranges_dropdown_body(TimeRange)),
  get(comet_pid) ! {set_time_range, TimeRange};

event({set_chart, Chart, Charts}) ->
  wf:wire(#jquery{target = "charts_button", method = ["addClass"], args = ["\"btn-inverse\""]}),
  gui_utils:update("charts_label", <<"Chart: <b>", Chart/binary, "</b>">>),
  gui_utils:update("charts_dropdown", charts_dropdown_body(Chart, Charts)),
  get(comet_pid) ! {set_chart, Chart};

event(add_chart) ->
  get(comet_pid) ! add_chart;

event({delete_chart, RowID, ID}) ->
  wf:wire(#jquery{target = RowID, method = ["remove"], args = []}),
  get(comet_pid) ! {delete_chart, ID}.

%% translates time ranges form binary string to integer
translate_time_range(<<"last 5 minutes">>) -> 5 * 60;
translate_time_range(<<"last 15 minutes">>) -> 15 * 60;
translate_time_range(<<"last 24 hours">>) -> 24 * 60 * 60;
translate_time_range(<<"last 7 days">>) -> 7 * 24 * 60 * 60;
translate_time_range(<<"last 30 days">>) -> 30 * 24 * 60 * 60;
translate_time_range(<<"last 365 days">>) -> 365 * 24 * 60 * 60;
translate_time_range(_) -> 60 * 60.

