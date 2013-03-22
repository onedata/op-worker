-module(control_panel_SUITE).
-author("lopiola").
-compile(export_all).

-include_lib("common_test/include/ct.hrl").


all() ->
  [
    test_button_refresh_logs
    %test_button_refresh_state
  ].

init_per_suite(_Config) ->
  os:cmd("epmd -daemon"),
  net_kernel:start([nitrogen_test, shortnames]),
  application:start(inets),
  c:cd("../.."),
  control_panel:init([{ccm, node()}, {dao, node()}]),
  [
    {root, filename:absname("")}].

end_per_suite(_Config) ->
  ok.

test_button_refresh_state(Config) ->
  c:cd(?config(root, Config)),
  spawn(fun() -> ccm_dummy() end),
  NumberOfClicks = 100,
  button_test_setup(
    "http://localhost:8000/cluster_state",
    {["main_panel", "main_span", "refresh_state"], "Odswiez+teraz"},
    NumberOfClicks),

  whereis(ccm) ! {self(), finish},
  NumberOfClicks = receive
    N ->
      N
  end.


test_button_refresh_logs(Config) ->
  c:cd(?config(root, Config)),
  spawn(fun() -> dao_dummy() end),

  NumberOfClicks = 3,
  button_test_with_comet_setup(
    "http://localhost:8000/logs",
    [
      {["main_panel", "main_span", "refresh_logs"], "Odswiez+teraz"},
      {["main_panel", "main_span", "refresh_period"], "5"}
    ],
    NumberOfClicks),

  whereis(dao) ! {self(), finish},
  NumberOfClicks = receive
                     N ->
                       N
                   end.



button_test_with_comet_setup(Address, FormElementPaths,  NumberOfClicks) ->
  {ok, {{_Version, 200, _ReasonPhrase}, _Headers, ResponseBody}} =
    httpc:request(get, {Address, []}, [], []),

  {match, [{From, Through}]} = re:run(ResponseBody, "\'pageContext\', \'.*\'"),
  PageContext = string:substr(ResponseBody, From + 17, Through - 17),

  ButtonID = lists:last(element(1, lists:nth(1, FormElementPaths))),
  {match, [{From2, Through2}]} = re:run(ResponseBody, string:concat(ButtonID, "\', \'.*\'")),
  EventContext = string:substr(ResponseBody, From2 + 5 + length(ButtonID), Through2 - 5 - length(ButtonID)),

  {match, [{From3, Through3}]} = re:run(ResponseBody, "queue_event.*\'"),
  SystemEventContext = string:substr(ResponseBody, From3 + 22, Through3 - 22),

  WFID_list = lists:map(
    fun({IDPath, ButtonText}) ->
        TempList = lists:map(
          fun(ID) ->
            {match, [{From4, Through4}]} = re:run(ResponseBody, string:concat("wfid_temp[0-9]+ wfid_", ID)),
            Temp = string:substr(ResponseBody, From4 + 1, Through4),
            string:join(string:tokens(Temp, " "), ".")
          end, IDPath),
        string:join(
        [
          "&",
          string:join(TempList, "."),
          "=",
          ButtonText
        ], "")
    end, FormElementPaths),

  button_test_with_comet_do(Address, {PageContext, {SystemEventContext, EventContext}, SystemEventContext, string:join(WFID_list, "")}, NumberOfClicks).


button_test_with_comet_do(_Address, {_PageContext, _EventContext, _SystemEventContext, _WFID_list}, N) when N =:= 0 ->
  done;

button_test_with_comet_do(Address, {PageContext, EventContext, SystemEventContext, WFID_list}, N) ->

  ResolvedEventContext =
  if
    is_tuple(EventContext) -> element(1, EventContext);
    true -> EventContext
  end,

  Method = post,
  URL = Address,
  Header = [],
  Type = "application/x-www-form-urlencoded; charset=UTF-8",
  Body = string:join(
    ["pageContext=",
      PageContext,
      WFID_list,
      "&eventContext=",
      ResolvedEventContext,
      "&"
    ],
    ""),
  HTTPOptions = [],
  Options = [],
  {ok, {{_, 200, _}, _, ResponseBody}} =
    httpc:request(Method, {URL, Header, Type, Body}, HTTPOptions, Options),

  {match, [{From, Through}]} = re:run(ResponseBody, "\'pageContext\', \'.*\'"),
  NewPageContext = string:substr(ResponseBody, From + 17, Through - 17),

  ResolvedSystemEventContext =
    if
      is_tuple(EventContext) ->
        {match, [{From2, Through2}]} = re:run(ResponseBody, "queue_system.*\'"),
        string:substr(ResponseBody, From2 + 21, Through2 - 21);
      true ->
        SystemEventContext
    end,


  Body2 = string:join(
    ["pageContext=",
      NewPageContext,
      "&eventContext=",
      ResolvedSystemEventContext,
      "&is_system_event=1"
    ],
    ""),
  {ok, {{_, 200, _}, _, ResponseBody2}} =
    httpc:request(Method, {URL, Header, Type, Body2}, HTTPOptions, Options),


  {match, [{From3, Through3}]} = re:run(ResponseBody2, "\'pageContext\', \'.*\'"),
  NewPageContext2 = string:substr(ResponseBody2, From3 + 17, Through3 - 17),

  {match, [{From4, Through4}]} = re:run(ResponseBody2, "queue_system.*\'"),
  NewSystemEventContext = string:substr(ResponseBody2, From4 + 21, Through4 - 21),

  case EventContext of
    {_, Event} ->
      button_test_with_comet_do(Address, {NewPageContext2, Event, NewSystemEventContext, WFID_list}, N);
    _ ->
      button_test_with_comet_do(Address, {NewPageContext2, EventContext, NewSystemEventContext, WFID_list}, N - 1)
  end.


button_test_setup(Address, {IDPath, ButtonText},  NumberOfClicks) ->
  {ok, {{_Version, 200, _ReasonPhrase}, _Headers, ResponseBody}} =
    httpc:request(get, {Address, []}, [], []),

  {match, [{From, Through}]} = re:run(ResponseBody, "\'pageContext\', \'.*\'"),
  PageContext = string:substr(ResponseBody, From + 17, Through - 17),

  ButtonID = lists:last(IDPath),
  {match, [{From2, Through2}]} = re:run(ResponseBody, string:concat(ButtonID, "\', \'.*\'")),
  EventContext = string:substr(ResponseBody, From2 + 5 + length(ButtonID), Through2 - 5 - length(ButtonID)),

  WFID_list = lists:map(
        fun(ID) ->
          {match, [{From3, Through3}]} = re:run(ResponseBody, string:concat("wfid_temp[0-9]+ wfid_", ID)),
          Temp = string:substr(ResponseBody, From3 + 1, Through3),
          string:join(string:tokens(Temp, " "), ".")
        end, IDPath),

  BodyWithoutPageContext = string:join(
    [
      "&",
      string:join(WFID_list, "."),
      "=",
      ButtonText,
      "&eventContext=",
      EventContext,
      "&"
    ], ""),
  button_test_do(Address, {PageContext, BodyWithoutPageContext}, NumberOfClicks).


button_test_do(_Address, {_PageContext, _BodyWithoutPageContext}, N) when N =:= 0 ->
  done;

button_test_do(Address, {PageContext, BodyWithoutPageContext}, N) ->
  Method = post,
  URL = Address,
  Header = [],
  Type = "application/x-www-form-urlencoded; charset=UTF-8",
  Body = string:join(
    ["pageContext=",
      PageContext,
      BodyWithoutPageContext
    ],
    ""),
  HTTPOptions = [],
  Options = [],
  {ok, {{_Version, 200, _ReasonPhrase}, _Headers, ResponseBody}} =
    httpc:request(Method, {URL, Header, Type, Body}, HTTPOptions, Options),

  {match, [{From, Through}]} = re:run(ResponseBody, "\'pageContext\', \'.*\'"),
  NewPageContext = string:substr(ResponseBody, From + 17, Through - 17),

  button_test_do(Address, {NewPageContext, BodyWithoutPageContext}, N - 1).




dao_dummy() ->
  register(dao, self()),
  dao_dummy_loop(0).

dao_dummy_loop(N) ->
  receive
    {Pid, logs} ->
      error_logger:info_msg("Got logs request number ~p.~n", [N]),
      Pid ! integer_to_list(random:uniform(42353452523523543657866543245676543245678)),
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
      Pid ! "Tutaj sa rozne informacje na temat stanu klastra.~n",
      ccm_dummy_loop(N + 1);

    {Pid, finish} ->
      Pid ! N,
      ok
  end.