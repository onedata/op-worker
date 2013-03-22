-module(website_SUITE).
-author("lopiola").
-compile(export_all).

-include_lib("common_test/include/ct.hrl").


all() ->
  [
    test_website
  ].

init_per_suite(_Config) ->
  os:cmd("epmd -daemon"),
  net_kernel:start([nitrogen_test, shortnames]),
  c:cd("../.."),
  spawn(fun() -> dao_dummy() end),
  spawn(fun() -> ccm_dummy() end),
  control_panel:init([{ccm, node()}, {dao, node()}]),
  [
    {root, filename:absname("")}].

end_per_suite(_Config) ->
  {dao, node()} ! {self(), finish},
  ok.


test_website(Config) ->
  c:cd(?config(root, Config)),
  timer:sleep(70000).




dao_dummy() ->
  register(dao, self()),
  dao_dummy_loop(0).

dao_dummy_loop(N) ->
  receive
    {Pid, logs} ->
      error_logger:info_msg("Got logs request number ~p.~n", [N]),
      Logs = string:join(
      [
        "<br />",
        integer_to_list(random:uniform(42353452523523543657866543245676543245678)),
        "<hr />"
      ], ""),

      Pid ! {logs, Logs},
      dao_dummy_loop(N + 1);

    {Pid, finish} ->
      Pid ! N,
      ok
  end.



ccm_dummy() ->
  register(ccm, self()),
  ccm_dummy_loop(0).

ccm_dummy_loop(N) ->
  receive
    {Pid, state} ->
      error_logger:info_msg("Got state request.~n"),
      Pid ! {state, "Tutaj sa rozne informacje na temat stanu klastra.<br />
        A tutaj jest losowa liczba, co by bylo widac, ze cos sie zmienia " ++ integer_to_list(random:uniform(42))},
      ccm_dummy_loop(N + 1);

    {Pid, finish} ->
      Pid ! N,
      ok
  end.