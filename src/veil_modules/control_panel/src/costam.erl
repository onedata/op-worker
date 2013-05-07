-module(costam).
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
  [
    #panel { id=main_panel, style="margin: 30px;", body=[
      #h2 { text="Costam" },
      #span { id=main_span, body=[
        #button { id=refresh_logs, text="Odswiez teraz", postback={refresh_logs, DAO_address} }
      ]},
      #hr {},
      #panel { id=logs }
    ]}
  ].

event({refresh_logs, DAO_address}) ->
  wf:insert_top(logs, ["Guzik przycisniety", "<br />"]),
  DAO_address ! {self(), logs}.













