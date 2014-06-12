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
-include("veil_modules/control_panel/common.hrl").
-include("registered_names.hrl").
-include("logging.hrl").

-define(TIME_RANGES, [<<"last 5 minutes">>, <<"last 15 minutes">>, <<"last hour">>, <<"last 24 hours">>, <<"last 7 days">>, <<"last 30 days">>, <<"last 365 days">>]).
-define(SUMMARY_CHART_TYPES, [<<"CPU utilization">>, <<"memory usage">>, <<"network throughput">>, <<"network transfer">>, <<"Erlang ports transfer">>, <<"storage IO transfer">>]).
-define(HOST_CHART_TYPES, [<<"CPU utilization">>, <<"memory usage">>, <<"network throughput">>, <<"network transfer">>, <<"Erlang ports transfer">>]).
-define(GEN_SERVER_TIMEOUT, 5000).

-record(page_state, {nodes, node, time_range, chart_type, charts = []}).
-record(chart, {id, node, time_range, type, update_period}).

-export([main/0, event/1, comet_loop/2]).

%% main/0
%% ====================================================================
%% @doc Template points to the template file, which will be filled with content.
-spec main() -> #dtl{}.
%% ====================================================================
main() ->
  case gui_utils:maybe_redirect(true, false, false, true) of
    true ->
      #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
    false ->
      case vcn_gui_utils:can_view_monitoring() of
        false ->
          wf:redirect(<<"/">>),
          #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, <<"">>}, {body, <<"">>}, {custom, <<"">>}]};
        true ->
          #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, custom()}]}
      end
  end.

%% title/0
%% ====================================================================
%% @doc Page title.
-spec title() -> binary().
%% ====================================================================
title() -> <<"Monitoring">>.

%% body/0
%% ====================================================================
%% @doc This will be placed instead of {{body}} tag in template.
-spec body() -> [#panel{}].
%% ====================================================================
body() ->
  [
    gui_utils:top_menu(monitoring_tab, monitoring_submenu()),
    #panel{style = <<"position: relative; margin-top: 122px; margin-bottom: 0px">>, body = [
      #table{id = <<"main_table">>, class = <<"table table-stripped">>, style = <<"width: 100%;">>}
    ]}
  ].

%% custom/0
%% ====================================================================
%% @doc This will be placed instead of {{custom}} tag in template.
-spec custom() -> binary().
%% ====================================================================
custom() ->
  <<"<script src='/js/jsapi.js' type='text/javascript' charset='utf-8'></script>
    <script type='text/javascript'>
        google.load('visualization', '1.1', {'packages': ['corechart']});
    </script>
    <script src='/js/charts.js' type='text/javascript' charset='utf-8'></script>">>.


%% monitoring_submenu/0
%% ====================================================================
%% @doc Submenu that will end up concatenated to top menu.
-spec monitoring_submenu() -> [#panel{}].
%% ====================================================================
monitoring_submenu() ->
  [
    #panel{class = <<"navbar-inner">>, style = <<"border-bottom: 1px solid gray; padding-bottom: 5px;">>, body = [
      #panel{class = <<"container">>, style = <<"text-align: center">>, body = [
        #panel{class = <<"btn-group">>, style = <<"margin: 12px 15px;">>, body = [
          <<"<i class=\"dropdown-arrow dropdown-arrow-inverse\"></i>">>,
          #button{class = <<"btn btn-danger btn-inverse btn-small dropdown-toggle">>, style = <<"width: 180px;">>, data_fields = [{<<"data-toggle">>, <<"dropdown">>}], body = [
            #span{id = <<"host_label">>, class = <<"filter-option pull-left">>, body = <<"<b>Host</b>">>},
            #span{class = <<"caret pull-right">>}
          ]},
          #list{id = <<"host_dropdown">>, class = <<"dropdown-menu dropdown-inverse">>, style = <<"overflow-y: auto; max-height: 250px;">>, body = host_dropdown_body()}
        ]},
        #panel{class = <<"btn-group">>, style = <<"margin: 12px 15px;">>, body = [
          <<"<i class=\"dropdown-arrow dropdown-arrow-inverse\"></i>">>,
          #button{class = <<"btn btn-danger btn-inverse btn-small dropdown-toggle">>, style = <<"width: 220px;">>, data_fields = [{<<"data-toggle">>, <<"dropdown">>}], body = [
            #span{id = <<"time_range_label">>, class = <<"filter-option pull-left">>, body = <<"<b>Time range</b>">>},
            #span{class = <<"caret pull-right">>}
          ]},
          #list{id = <<"time_range_dropdown">>, class = <<"dropdown-menu dropdown-inverse">>, style = <<"overflow-y: auto; max-height: 250px;">>, body = time_range_dropdown_body(undefined)}
        ]},
        #panel{class = <<"btn-group">>, style = <<"margin: 12px 15px;">>, body = [
          <<"<i class=\"dropdown-arrow dropdown-arrow-inverse\"></i>">>,
          #button{class = <<"btn btn-danger btn-inverse btn-small dropdown-toggle">>, style = <<"width: 250px;">>, data_fields = [{<<"data-toggle">>, <<"dropdown">>}], body = [
            #span{id = <<"chart_type_label">>, class = <<"filter-option pull-left">>, body = <<"<b>Chart type</b>">>},
            #span{class = <<"caret pull-right">>}
          ]},
          #list{id = <<"chart_type_dropdown">>, class = <<"dropdown-menu dropdown-inverse">>, style = <<"overflow-y: auto; max-height: 250px;">>, body = chart_type_dropdown_body(undefined, ?HOST_CHART_TYPES)}
        ]},
        #panel{class = <<"btn-group">>, style = <<"margin: 12px 15px;">>, body = [
          #button{postback = add_chart, class = <<"btn btn-primary btn-small">>, style = <<"font-weight: bold;">>, body = <<"Add chart">>}
        ]}
      ]}
    ]}
  ].

%% comet_loop/1
%% ====================================================================
%% @doc Adds, updates and removes monitoring charts. Handles messages that change chart preferences.
-spec comet_loop(Counter :: integer(), PageState :: #page_state{}) -> no_return().
%% ====================================================================
comet_loop(Counter, #page_state{nodes = Nodes, node = Node, time_range = TimeRange, chart_type = ChartType, charts = Charts} = PageState) ->
  try
    receive
      {set_node, NewNode, ChartTypes} ->
        case lists:member(ChartType, ChartTypes) of
          true -> gui_utils:update("chart_type_dropdown", chart_type_dropdown_body(ChartType, ChartTypes));
          _ -> self() ! {set_chart_type, undefined, ChartTypes}
        end,
        gui_utils:update("host_label", <<"Host: <b>", (get_hostname(NewNode))/binary, "</b>">>),
        gui_utils:update("host_dropdown", host_dropdown_body(NewNode, Nodes)),
        gui_utils:flush(),
        ?MODULE:comet_loop(Counter, PageState#page_state{node = NewNode});

      {set_time_range, NewTimeRange} ->
        gui_utils:update("time_range_label", <<"Time range: <b>", NewTimeRange/binary, "</b>">>),
        gui_utils:update("time_range_dropdown", time_range_dropdown_body(NewTimeRange)),
        gui_utils:flush(),
        ?MODULE:comet_loop(Counter, PageState#page_state{time_range = NewTimeRange});

      {set_chart_type, NewChartType, ChartTypes} ->
        case NewChartType of
          undefined -> gui_utils:update("chart_type_label", <<"<b>Chart type</b>">>);
          _ -> gui_utils:update("chart_type_label", <<"Chart type: <b>", NewChartType/binary, "</b>">>)
        end,
        gui_utils:update("chart_type_dropdown", chart_type_dropdown_body(NewChartType, ChartTypes)),
        gui_utils:flush(),
        ?MODULE:comet_loop(Counter, PageState#page_state{chart_type = NewChartType});

      add_chart ->
        gui_utils:remove("error_message"),
        gui_utils:flush(),
        case validate_page_state(PageState) of
          ok ->
            case validate_chart(Node, TimeRange, ChartType, Charts) of
              ok ->
                case create_chart(Counter, Node, TimeRange, ChartType) of
                  {ok, Chart} ->
                    reset_dropdowns(Nodes),
                    erlang:send_after(1000 * Chart#chart.update_period, self(), {update_chart, Counter}),
                    ?MODULE:comet_loop(Counter + 1, PageState#page_state{node = undefined, time_range = undefined, chart_type = undefined, charts = [{Counter, Chart} | Charts]});
                  _ ->
                    error_message(<<"There has been an error during chart creation. Please try again.">>)
                end;
              _ -> error_message(<<"Chart already added.">>)
            end;
          _ -> ok
        end,
        ?MODULE:comet_loop(Counter, PageState);

      {update_chart, Id} ->
        try
          case proplists:get_value(Id, Charts) of
            undefined ->
              ?error("Can not update chart ~p: chart does not exist.", [Id]);
            Chart ->
              ok = update_chart(Chart),
              erlang:send_after(1000 * Chart#chart.update_period, self(), {update_chart, Id})
          end
        catch
          _:Error -> ?error("Can not update chart ~p: ~p", [Id, Error])
        end,
        ?MODULE:comet_loop(Counter, PageState);

      {delete_chart, Id} ->
        wf:wire("deleteChart(\"" ++ integer_to_list(Id) ++ "\");"),
        gui_utils:flush(),
        ?MODULE:comet_loop(Counter, PageState#page_state{charts = proplists:delete(Id, Charts)})

    end
  catch Type:Msg ->
    ?debug_stacktrace("~p ~p", [Type, Msg]),
    error_message(<<"There has been an error in comet process. Please refresh the page.">>)
  end.


%% error_message/1
%% ====================================================================
%% @doc Displays error message before main table.
-spec error_message(Message :: binary()) -> ok.
%% ====================================================================
error_message(Message) ->
  Error = #panel{id = <<"error_message">>, style = <<"width: 100%; margin-bottom: 0px;">>, class = <<"dialog dialog-danger">>, body = [Message]},
  gui_utils:insert_before("main_table", Error),
  gui_utils:flush().


%% validate_page_state/1
%% ====================================================================
%% @doc Checks whether user has selected all requiered options to draw chart.
-spec validate_page_state(PageState :: #page_state{}) -> ok | error.
%% ====================================================================
validate_page_state(#page_state{node = undefined}) ->
  error_message(<<"Please select host.">>),
  error;
validate_page_state(#page_state{time_range = undefined}) ->
  error_message(<<"Please select time range.">>),
  error;
validate_page_state(#page_state{chart_type = undefined}) ->
  error_message(<<"Please select chart type.">>),
  error;
validate_page_state(_) ->
  ok.


%% validate_chart/4
%% ====================================================================
%% @doc Checks whether chart can be drawn on page. Chart can be drawn
%% when it is not already available on page.
-spec validate_chart(Node :: summary | node(), TimeRange :: binary(), ChartType :: binary(), Charts :: [{Key :: integer(), #chart{}}]) -> ok | error.
%% ====================================================================
validate_chart(_, _, _, []) ->
  ok;
validate_chart(Node, TimeRange, ChartType, [{_, #chart{node = Node, time_range = TimeRange, type = ChartType}} | _]) ->
  error;
validate_chart(Node, TimeRange, ChartType, [_ | Charts]) ->
  validate_chart(Node, TimeRange, ChartType, Charts).


%% host_dropdown_body/0
%% ====================================================================
%% @doc Renders the body of host dropdown and highlights the current choice
-spec host_dropdown_body() -> [#li{}].
%% ====================================================================
host_dropdown_body() ->
  host_dropdown_body(undefined, get_nodes()).


%% host_dropdown_body/2
%% ====================================================================
%% @doc Renders the body of host dropdown and highlights the current choice
-spec host_dropdown_body(ActiveNode :: node(), Nodes :: [node()]) -> [] | [#li{}].
%% ====================================================================
host_dropdown_body(_, []) ->
  [];
host_dropdown_body(ActiveNode, Nodes) ->
  lists:map(fun({Node, Index}) ->
    Class = case Node of
              ActiveNode -> <<"active">>;
              _ -> <<"">>
            end,
    ID = <<"host_li_", (integer_to_binary(Index))/binary>>,
    #li{id = ID, actions = #event{type = "click", postback = {set_node, Node}, target = ID}, class = Class, body = #link{style = <<"text-align: left;">>, body = get_hostname(Node)}}
  end, lists:zip(Nodes, lists:seq(1, length(Nodes)))).


%% time_range_dropdown_body/2
%% ====================================================================
%% @doc Renders the body of time range dropdown and highlights the current choice
-spec time_range_dropdown_body(ActiveTimeRange :: binary()) -> [#li{}].
%% ====================================================================
time_range_dropdown_body(ActiveTimeRange) ->
  lists:map(fun({TimeRange, Index}) ->
    Class = case TimeRange of
              ActiveTimeRange -> <<"active">>;
              _ -> <<"">>
            end,
    ID = <<"time_range_li_", (integer_to_binary(Index))/binary>>,
    #li{id = ID, actions = #event{type = "click", postback = {set_time_range, TimeRange}, target = ID},
      class = Class, body = #link{style = <<"text-align: left;">>, body = TimeRange}}
  end, lists:zip(?TIME_RANGES, lists:seq(1, length(?TIME_RANGES)))).


%% chart_type_dropdown_body/2
%% ====================================================================
%% @doc Renders the body of chart type dropdown and highlights the current choice
-spec chart_type_dropdown_body(ActiveChartType :: binary(), ChartTypes :: [binary()]) -> [#li{}].
%% ====================================================================
chart_type_dropdown_body(ActiveChartType, ChartTypes) ->
  lists:map(fun({ChartType, Index}) ->
    Class = case ChartType of
              ActiveChartType -> <<"active">>;
              _ -> <<"">>
            end,
    ID = <<"chart_type_li_", (integer_to_binary(Index))/binary>>,
    #li{id = ID, actions = #event{type = "click", postback = {set_chart_type, ChartType, ChartTypes}, target = ID},
      class = Class, body = #link{style = <<"text-align: left;">>, body = ChartType}}
  end, lists:zip(ChartTypes, lists:seq(1, length(ChartTypes)))).


%% reset_dropdowns/1
%% ====================================================================
%% @doc Resets dropdowns to show default labels without selection.
-spec reset_dropdowns(Nodes :: [node()]) -> ok.
%% ====================================================================
reset_dropdowns(Nodes) ->
  gui_utils:update("host_label", <<"<b>Host</b>">>),
  gui_utils:update("host_dropdown", host_dropdown_body(undefined, Nodes)),
  gui_utils:update("time_range_label", <<"<b>Time range</b>">>),
  gui_utils:update("time_range_dropdown", time_range_dropdown_body(undefined)),
  gui_utils:update("chart_type_label", <<"<b>Chart type</b>">>),
  gui_utils:update("chart_type_dropdown", chart_type_dropdown_body(undefined, ?HOST_CHART_TYPES)),
  gui_utils:flush().


%% create_chart/4
%% ====================================================================
%% @doc Creates new chart and displays it on page.
-spec create_chart(Counter :: integer(), Node :: summary | node(), TimeRange :: binary(), ChartType :: binary()) -> {ok, #chart{}} | error.
%% ====================================================================
create_chart(Counter, Node, TimeRange, ChartType) ->
  try
    UpdatePeriod = get_update_period(Node, TimeRange),
    {IdJSON, TypeJSON, TitleJSON, VAxisTitleJSON, HeaderJSON, BodyJSON} = get_json_data(Counter, Node, TimeRange, ChartType, UpdatePeriod),
    RowID = "row_" ++ integer_to_list(Counter),
    gui_utils:insert_top("main_table", #tr{id = list_to_binary(RowID),
      cells = [
        #th{body = #panel{id = <<"chart_", (integer_to_binary(Counter))/binary>>}},
        #th{body = #link{postback = {delete_chart, Counter}, title = <<"Remove">>, class = <<"glyph-link">>,
          body = #span{class = <<"fui-cross">>, style = <<"font-size: 20px;">>}},
          style = <<"width: 10px;">>}
      ]}),
    wf:wire("createChart(" ++ IdJSON ++ "," ++ TypeJSON ++ "," ++ TitleJSON ++ "," ++ VAxisTitleJSON ++ "," ++ HeaderJSON ++ "," ++ BodyJSON ++ ");"),
    gui_utils:flush(),
    {ok, #chart{id = Counter, node = Node, time_range = TimeRange, type = ChartType, update_period = UpdatePeriod}}
  catch
    _:_ -> error
  end.


%% update_chart/1
%% ====================================================================
%% @doc Updates specified chart on page.
-spec update_chart(Chart :: #chart{}) -> ok | error.
%% ====================================================================
update_chart(#chart{id = ID, node = Node, time_range = TimeRange, type = ChartType, update_period = UpdatePeriod}) ->
  try
    {IdJSON, _, _, _, _, BodyJSON} = get_json_data(ID, Node, TimeRange, ChartType, UpdatePeriod),
    wf:wire("updateChart(" ++ IdJSON ++ "," ++ BodyJSON ++ ");"),
    gui_utils:flush(),
    ok
  catch
    _:_ -> error
  end.


%% get_json_data/4
%% ====================================================================
%% @doc Returns data in json format applicable for Google Charts API.
-spec get_json_data(Id :: integer(), Node :: summary | node(), TimeRange :: binary(), ChartType :: binary(), UpdatePeriod :: integer()) -> Result when
  Result :: {IdJSON, TypeJSON, TitleJSON, VAxisTitleJSON, HeaderJSON, BodyJSON},
  IdJSON :: string(),
  TypeJSON :: string(),
  TitleJSON :: string(),
  VAxisTitleJSON :: string(),
  HeaderJSON :: string(),
  BodyJSON :: string().
%% ====================================================================
get_json_data(Id, summary, TimeRange, ChartType, UpdatePeriod) ->
  get_json_data(Id, summary, {global, ?CCM}, get_cluster_stats, TimeRange, ChartType, UpdatePeriod);

get_json_data(Id, Node, TimeRange, ChartType, UpdatePeriod) ->
  get_json_data(Id, Node, {?Node_Manager_Name, Node}, get_node_stats, TimeRange, ChartType, UpdatePeriod).


%% get_json_data/7
%% ====================================================================
%% @doc Returns data in json format applicable for Google Charts API.
%% Should not be used directly, use get_json_data/4 instead.
-spec get_json_data(Id :: integer(), Node :: summary | node(), Targer :: term(), Command :: atom(), TimeRange :: binary(), ChartType :: binary(), UpdatePeriod :: integer()) -> Result when
  Result :: {IdJSON, TypeJSON, TitleJSON, VAxisTitleJSON, HeaderJSON, BodyJSON},
  IdJSON :: string(),
  TypeJSON :: string(),
  TitleJSON :: string(),
  VAxisTitleJSON :: string(),
  HeaderJSON :: string(),
  BodyJSON :: string().
%% ====================================================================
get_json_data(Id, Node, Target, Command, TimeRange, ChartType, UpdatePeriod) ->
  {MegaSecs, Secs, _} = erlang:now(),
  EndTime = trunc(((1000000 * MegaSecs + Secs - 2 * UpdatePeriod) / UpdatePeriod) * UpdatePeriod),
  StartTime = EndTime - time_range_to_integer(TimeRange),
  {ok, {Header, Body}} = gen_server:call(Target, {Command, StartTime, EndTime, chart_type_to_columns(ChartType)}, ?GEN_SERVER_TIMEOUT),
  HeaderJSON = "{ cols: [{label: 'Time', type: 'string'}, " ++
    string:join(lists:map(fun(Column) ->
      header_to_json(Column)
    end, Header), ", ") ++ "], rows: []}",
  BodyJSON = "[" ++ string:join(lists:map(fun({Timestamp, Row}) ->
    "['" ++ integer_to_list(1000 * Timestamp) ++ "', " ++
      string:join(lists:map(fun(Value) ->
        value_to_list(Value)
      end, Row), ", ") ++ "]"
  end, Body), ", ") ++ "]",
  IdJSON = "'" ++ integer_to_list(Id) ++ "'",
  TypeJSON = chart_type_to_google_chart_type(ChartType),
  TitleJSON = "'" ++ binary_to_list(get_hostname(Node)) ++ " / " ++ binary_to_list(TimeRange) ++ " / " ++ binary_to_list(ChartType) ++ "'",
  VAxisTitleJSON = chart_type_to_v_axis_title(ChartType),
  {IdJSON, TypeJSON, TitleJSON, VAxisTitleJSON, HeaderJSON, BodyJSON}.


%% get_update_period/2
%% ====================================================================
%% @doc Returns period in seconds that say how often graph should be updated.
-spec get_update_period(Node :: summary | node(), TimeRange :: binary()) -> integer().
%% ====================================================================
get_update_period(summary, TimeRange) ->
  {ok, Period} = application:get_env(?APP_Name, cluster_monitoring_period),
  Period * get_period_multiplication(TimeRange);
get_update_period(_, TimeRange) ->
  {ok, Period} = application:get_env(?APP_Name, node_monitoring_period),
  Period * get_period_multiplication(TimeRange).


%% get_period_multiplication/1
%% ====================================================================
%% @doc Returns update period multiplication in terms of Round Robin Archive size.
-spec get_period_multiplication(TimeRange :: binary()) -> integer().
%% ====================================================================
get_period_multiplication(<<"last 5 minutes">>) -> 1;
get_period_multiplication(<<"last 15 minutes">>) -> 1;
get_period_multiplication(<<"last hour">>) -> 1;
get_period_multiplication(<<"last 24 hours">>) -> 24;
get_period_multiplication(<<"last 7 days">>) -> 7 * 24;
get_period_multiplication(<<"last 30 days">>) -> 30 * 24;
get_period_multiplication(<<"last 365 days">>) -> 365 * 24.


%% get_nodes/0
%% ====================================================================
%% @doc Returns list on cluster nodes plus 'summary' node or empty list.
-spec get_nodes() -> Nodes :: [] | [summary | [node()]].
%% ====================================================================
get_nodes() ->
  try gen_server:call({global, ?CCM}, get_nodes, 1000) of
    [] -> [];
    Nodes -> [summary | Nodes]
  catch
    Type:Error -> ?error("Can not get nodes: ~p:~p", [Type, Error]), []
  end.


%% get_hostname/1
%% ====================================================================
%% @doc Returns node's hostname.
%% In case of 'summary' atom, <<"summary">> binary is returned.
-spec get_hostname(Node :: node() | summary) -> Hostname :: binary().
%% ====================================================================
get_hostname(summary) ->
  <<"summary">>;
get_hostname(Node) ->
  [_, Host] = binary:split(atom_to_binary(Node, latin1), <<"@">>),
  Host.


%% time_range_to_integer/1
%% ====================================================================
%% @doc Maps time ranges to integers.
-spec time_range_to_integer(TimeRange :: binary()) -> integer().
%% ====================================================================
time_range_to_integer(<<"last 5 minutes">>) -> 5 * 60;
time_range_to_integer(<<"last 15 minutes">>) -> 15 * 60;
time_range_to_integer(<<"last hour">>) -> 60 * 60;
time_range_to_integer(<<"last 24 hours">>) -> 24 * 60 * 60;
time_range_to_integer(<<"last 7 days">>) -> 7 * 24 * 60 * 60;
time_range_to_integer(<<"last 30 days">>) -> 30 * 24 * 60 * 60;
time_range_to_integer(<<"last 365 days">>) -> 365 * 24 * 60 * 60.


%% chart_type_to_columns/1
%% ====================================================================
%% @doc Maps chart types to columns that will be fetched from Round Robin Database.
-spec chart_type_to_columns(ChartType :: binary()) -> [binary() | {starts_with, binary()}].
%% ====================================================================
chart_type_to_columns(<<"CPU utilization">>) -> {name, [<<"cpu">>]};
chart_type_to_columns(<<"memory usage">>) -> {name, [<<"mem">>]};
chart_type_to_columns(<<"network throughput">>) -> {starts_with, [<<"net_rx_pps">>, <<"net_tx_pps">>]};
chart_type_to_columns(<<"network transfer">>) -> {starts_with, [<<"net_rx_b">>, <<"net_tx_b">>]};
chart_type_to_columns(<<"Erlang ports transfer">>) -> {name, [<<"ports_rx_b">>, <<"ports_tx_b">>]};
chart_type_to_columns(<<"storage IO transfer">>) -> {name, [<<"storage_read_b">>, <<"storage_write_b">>]}.


%% chart_type_to_google_chart_type/1
%% ====================================================================
%% @doc Maps chart types to Google API chart types.
-spec chart_type_to_google_chart_type(ChartType :: binary()) -> string().
%% ====================================================================
chart_type_to_google_chart_type(<<"CPU utilization">>) -> "'LineChart'";
chart_type_to_google_chart_type(<<"memory usage">>) -> "'LineChart'";
chart_type_to_google_chart_type(_) -> "'AreaChart'".


%% chart_type_to_v_axis_title/1
%% ====================================================================
%% @doc Maps chart type to chart's vertical axis label.
-spec chart_type_to_v_axis_title(ChartType :: binary()) -> string().
%% ====================================================================
chart_type_to_v_axis_title(<<"CPU utilization">>) -> "'Utilization [%]'";
chart_type_to_v_axis_title(<<"memory usage">>) -> "'Usage [%]'";
chart_type_to_v_axis_title(<<"network throughput">>) -> "'packets / sec'";
chart_type_to_v_axis_title(<<"network transfer">>) -> "'bytes'";
chart_type_to_v_axis_title(<<"Erlang ports transfer">>) -> "'bytes'";
chart_type_to_v_axis_title(<<"storage IO transfer">>) -> "'bytes'".


%% header_to_json/1
%% ====================================================================
%% @doc Maps column header name to chart's legend label.
-spec header_to_json(Header :: binary()) -> string().
%% ====================================================================
header_to_json(<<"cpu">>) -> "{label: 'CPU', type: 'number'}";
header_to_json(<<"core", Core/binary>>) -> "{label: 'Core " ++ binary_to_list(Core) ++ "', type: 'number'}";
header_to_json(<<"mem">>) -> "{label: 'Memory', type: 'number'}";
header_to_json(<<"ports_rx_b">>) -> "{label: 'RX bytes', type: 'number'}";
header_to_json(<<"ports_tx_b">>) -> "{label: 'TX bytes', type: 'number'}";
header_to_json(<<"storage_read_b">>) -> "{label: 'read bytes', type: 'number'}";
header_to_json(<<"storage_write_b">>) -> "{label: 'written bytes', type: 'number'}";
header_to_json(<<"net_rx_b">>) -> "{label: 'RX bytes', type: 'number'}";
header_to_json(<<"net_tx_b">>) -> "{label: 'TX bytes', type: 'number'}";
header_to_json(<<"net_rx_b_", Interface/binary>>) ->
  "{label: '" ++ binary_to_list(Interface) ++ " RX bytes', type: 'number'}";
header_to_json(<<"net_tx_b_", Interface/binary>>) ->
  "{label: '" ++ binary_to_list(Interface) ++ " TX bytes', type: 'number'}";
header_to_json(<<"net_rx_pps">>) -> "{label: 'RX pps', type: 'number'}";
header_to_json(<<"net_tx_pps">>) -> "{label: 'TX pps', type: 'number'}";
header_to_json(<<"net_rx_pps_", Interface/binary>>) ->
  "{label: '" ++ binary_to_list(Interface) ++ " RX pps', type: 'number'}";
header_to_json(<<"net_tx_pps_", Interface/binary>>) ->
  "{label: '" ++ binary_to_list(Interface) ++ " TX pps', type: 'number'}";
header_to_json(_) -> throw(<<"Unknown column.">>).


%% value_to_list/1
%% ====================================================================
%% @doc For integer and float returns string representation. For rest returns "null".
-spec value_to_list(Value :: term()) -> string().
%% ====================================================================
value_to_list(Value) when is_integer(Value) ->
  integer_to_list(Value);
value_to_list(Value) when is_float(Value) ->
  float_to_list(Value);
value_to_list(_) ->
  "null".


%% event/1
%% ====================================================================
%% @doc Handles page events.
-spec event(Event :: term()) -> no_return().
%% ====================================================================
event(init) ->
  Nodes = get_nodes(),
  {ok, Pid} = gui_utils:comet(fun() -> comet_loop(1, #page_state{nodes = Nodes}) end),
  put(comet_pid, Pid);

event({set_node, summary}) ->
  get(comet_pid) ! {set_node, summary, ?SUMMARY_CHART_TYPES};

event({set_node, Node}) ->
  get(comet_pid) ! {set_node, Node, ?HOST_CHART_TYPES};

event({set_time_range, TimeRange}) ->
  get(comet_pid) ! {set_time_range, TimeRange};

event({set_chart_type, ChartType, ChartTypes}) ->
  get(comet_pid) ! {set_chart_type, ChartType, ChartTypes};

event(add_chart) ->
  get(comet_pid) ! add_chart;

event({delete_chart, Id}) ->
  get(comet_pid) ! {delete_chart, Id}.

