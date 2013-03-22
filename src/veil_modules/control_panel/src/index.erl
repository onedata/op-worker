-module (index).
-author("lopiola").
-compile(export_all).
-include_lib("nitrogen_core/include/wf.hrl").

main() ->
  #template { file="src/veil_modules/control_panel/static/templates/bare.html" }.

title() -> "VeilFS homepage v. 0.00000000000001".

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
  {ok, DAO_address} = application:get_env(veil_cluster_node, dao_address),
  [
    #panel { style="margin: 30px;", body=[
      #h1 { text="Nic ciekawego tutaj nie ma :(" },
      #p { body=["Na podstronach <b>/logs</b> oraz <b>/cluster_state</b> mozna ogladac przykladowa komunikacje z innymi wezlami."]}
    ]}
  ].

event({click, DAO_address}) ->
  DAO_address ! {self(), logs},
  receive
    Logs ->
      wf:insert_bottom(logs, [Logs, "<br />"])
  after 200 ->
    wf:insert_bottom(logs, ["No response from DAO"]),
    wf:flush()
  end.

refresh_logs(DAO_address) ->
  DAO_address ! {self(), logs},
  receive
    Logs ->
      wf:insert_bottom(logs, [Logs, "<br />"]),
      timer:sleep(1000),
      wf:flush()
      %refresh_logs(DAO_address)
  after 2000 ->
      wf:insert_bottom(logs, ["No response from DAO"]),
      wf:flush(),
      refresh_logs(DAO_address)
  end.

