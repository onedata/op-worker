-module (cluster_state).
-author("lopiola").
-compile(export_all).
-include_lib("nitrogen_core/include/wf.hrl").

main() ->
  #template { file="src/veil_modules/control_panel/static/templates/bare.html" }.

title() -> "VeilFS homepage v. 0.00000000000001 - cluster state".

header() ->
  #panel { style="margin: 10px; background-color: #66CC33;", body=
  [
    #link { text="STRONA GLOWNA", url="/index", style="margin: 15px; color: white; font-weight: bold;
       text-decoration: none; border: 1px dotted white" },
    #link { text="LOGI", url="/logs", style="margin: 15px; color: white; font-weight: bold;
       text-decoration: none; border: 1px dotted white" },
    #link { text="STAN KLASTRA", url="/cluster_state", style="margin: 15px; color: white;
       font-weight: bold; text-decoration: none; border: 1px dotted white" }
  ]}.

body() ->
  {ok, CCM_address} = application:get_env(veil_cluster_node, ccm_address),
  wf:comet(fun() -> refresh_handler(CCM_address, infinity) end, state_pool),
  wf:wire(refresh_period, #event{ type=change, postback=period_changed }),
  [
    #panel { id=main_panel, style="margin: 30px;", body=[
      #h2 { text="Demo stanu klastra" },
      #span { id=main_span, body=[
        #button { id=refresh_state, text="Odswiez teraz", postback=refresh_state },
        #checkbox { id=auto_refresh, text="automatyczne odswiezanie co: ", checked=false, style="margin-left: 40px;",
          postback=period_changed },
        #textbox { id=refresh_period, text="5", style="width: 45px" },
        " s.",
        #span { id=last_refresh, style="float: right;", body=["Ostatnie odswiezenie: nigdy"]}
      ]},
      #hr {},
      #panel { id=cluster_state }
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
      wf:send(state_pool, {auto_refresh, Period});
    _ ->
      wf:send(state_pool, {auto_refresh, infinity})
  end;

event(refresh_state) ->
  wf:send(state_pool, refresh_now).


refresh_handler(CCM_address, Period) ->

  receive
    {state, State} ->
      {ok, FieldList} = httpd_util:split(httpd_util:rfc1123_date(), " ", 6),
      wf:replace(last_refresh, #span { id=last_refresh, style="float: right;", body=["Ostatnie odswiezenie:   <b>",
        lists:nth(5, FieldList), "</b>"], actions=#effect { effect=highlight } }),
      wf:flush(),
      wf:update(cluster_state, [State, "<br />"]),
      wf:flush();
    refresh_now ->
      CCM_address ! {self(), state};
    {auto_refresh, NewPeriod} ->
      refresh_handler(CCM_address, NewPeriod)
  after Period ->
    CCM_address ! {self(), state}
  end,

  refresh_handler(CCM_address, Period).


