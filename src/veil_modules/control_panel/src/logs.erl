-module (logs).
-author("lopiola").
-compile(export_all).
-include_lib("nitrogen_core/include/wf.hrl").

main() ->
  #template { file="src/veil_modules/control_panel/static/templates/bare.html" }.

title() -> "VeilFS homepage v. 0.00000000000001 - logs".

header() ->
  #panel { style="margin: 10px; background-color: #66CC33;", body=
  [
    #link { text="STRONA GLOWNA", url="/index", style="margin: 15px; color: white; font-weight: bold;
      text-decoration: none; border: 1px dotted white" },
    #link { text="LOGI", url="/logs", style="margin: 15px; color: white; font-weight: bold;
      text-decoration: none; border: 1px dotted white" },
    #link { text="STAN KLASTRA", url="/cluster_state", style="margin: 15px; color: white; font-weight: bold;
      text-decoration: none; border: 1px dotted white" }
  ]}.

body() ->
  {ok, DAO_address} = application:get_env(veil_cluster_node, dao_address),
  wf:comet(fun() -> refresh_handler(DAO_address, infinity) end, logs_pool),
  wf:wire(refresh_period, #event{ type=change, postback=period_changed }),
  [
    #panel { id=main_panel, style="margin: 30px;", body=[
      #h2 { text="Demo logow" },
      #span { id=main_span, body=[
        #button { id=refresh_logs, text="Odswiez teraz", postback=refresh_logs },
        #checkbox { id=auto_refresh, text="automatyczne odswiezanie co: ", checked=false, style="margin-left: 40px;",
          postback=period_changed },
        #textbox { id=refresh_period, text="5", style="width: 45px" },
        " s.",
        #span { id=last_refresh, style="float: right;", body=["Ostatnie odswiezenie: nigdy"]}
      ]},
      #hr {},
      #panel { id=logs }
    ]}
  ].

event(period_changed) ->
  Period =
    case string:to_float(string:concat(wf:q(refresh_period), ".0")) of
      {error, _} ->
        5000;
      {Value, _} ->
        round(Value * 1000)
    end,

  case wf:q(auto_refresh) of
    "on" ->
      wf:send(logs_pool, {auto_refresh, Period});
    _ ->
      wf:send(logs_pool, {auto_refresh, infinity})
  end;

event(refresh_logs) ->
  wf:send(logs_pool, refresh_now).


refresh_handler(DAO_address, Period) ->

  receive
    {logs, Logs} ->
      {ok, FieldList} = httpd_util:split(httpd_util:rfc1123_date(), " ", 6),
      wf:replace(last_refresh, #span { id=last_refresh, style="float: right;", body=["Ostatnie odswiezenie:   <b>",
        lists:nth(5, FieldList), "</b>"], actions=#effect { effect=highlight } }),
      wf:flush(),
      wf:insert_top(logs, [Logs, "<br />"]),
      wf:flush();
    refresh_now ->
      DAO_address ! {self(), logs};
    {auto_refresh, NewPeriod} ->
      refresh_handler(DAO_address, NewPeriod)
  after Period ->
    DAO_address ! {self(), logs}
  end,

  refresh_handler(DAO_address, Period).
















