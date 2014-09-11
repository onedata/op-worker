%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module tests the functionality of rrderlang module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(rrderlang_tests).
-include("registered_names.hrl").

%% gen_server call timeout
-define(TIMEOUT, 5000).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(RRD_NAME, "test_database.rrd").
-endif.

-ifdef(TEST).

%% ===================================================================
%% Tests description
%% ===================================================================

rrderlang_test_() ->
  {foreach,
    fun setup/0,
    fun teardown/1,
    [
      {"should select columns", fun should_select_header/0},
      {"should select row", fun should_select_row/0},
      {"should start rrderlang application", fun should_start_rrderlang/0},
      {"should create rrd", fun should_create_rrd/0},
      {"should not create rrd when missing parameters", fun should_not_create_rrd_when_missing_parameters/0},
      {"should not overwrite rrd", fun should_not_overwrite_rrd/0},
      {"should update rrd", fun should_update_rrd/0},
      {"should not update rrd when not created", fun should_not_update_rrd_when_not_created/0},
      {"should not update rrd when missing parameters", fun should_not_update_rrd_when_missing_parameters/0},
      {"should fetch data", {timeout, 15, fun should_fetch_data/0}},
      {"should not fetch data", fun should_not_fetch_data/0},
      {"should fetch selected data", {timeout, 15, fun should_fetch_selected_data/0}},
      {"should fetch start with data", {timeout, 15, fun should_fetch_start_with_data/0}}
    ]
  }.

%% ===================================================================
%% Setup/teardown functions
%% ===================================================================

setup() ->
  application:set_env(?APP_Name, rrd_timeout, ?TIMEOUT).

teardown(_) ->
  ?assertCmd("rm -f " ++ ?RRD_NAME).

%% ===================================================================
%% Tests functions
%% ===================================================================

should_start_rrderlang() ->
  {Status, _} = rrderlang:start_link(),
  ?assertEqual(ok, Status).

should_select_header() ->
  Header = [<<"Aa">>, <<"Bb">>, <<"Ac">>, <<"Dd">>, <<"Bc">>],
  {Status1, Result1} = rrderlang:select_header(Header, all),
  ?assertEqual(ok, Status1),
  {Header1, Columns1} = Result1,
  ?assertEqual(Header, Header1),
  ?assertEqual([1, 2, 3, 4, 5], Columns1),
  {Status2, Result2} = rrderlang:select_header(Header, {index, [1, 3]}),
  ?assertEqual(ok, Status2),
  {Header2, Columns2} = Result2,
  ?assertEqual([<<"Aa">>, <<"Ac">>], Header2),
  ?assertEqual([1, 3], Columns2),
  {Status3, Result3} = rrderlang:select_header(Header, {name, [<<"Aa">>, <<"Dd">>]}),
  ?assertEqual(ok, Status3),
  {Header3, Columns3} = Result3,
  ?assertEqual([<<"Aa">>, <<"Dd">>], Header3),
  ?assertEqual([1, 4], Columns3),
  {Status4, Result4} = rrderlang:select_header(Header, {starts_with, [<<"A">>, <<"B">>]}),
  ?assertEqual(ok, Status4),
  {Header4, Columns4} = Result4,
  ?assertEqual([<<"Aa">>, <<"Bb">>, <<"Ac">>, <<"Bc">>], Header4),
  ?assertEqual([1, 2, 3, 5], Columns4).

should_select_row() ->
  Row = <<"1000000: 1.1 1.2 1.3 1.4 1.5 1.6">>,
  {Status1, Result1} = rrderlang:select_row(Row, [1, 2, 3, 4, 5, 6]),
  ?assertEqual(ok, Status1),
  ?assertEqual({1000000, [1.1, 1.2, 1.3, 1.4, 1.5, 1.6]}, Result1),
  {Status2, Result2} = rrderlang:select_row(Row, [1, 3, 5]),
  ?assertEqual(ok, Status2),
  ?assertEqual({1000000, [1.1, 1.3, 1.5]}, Result2),
  {Status3, Result3} = rrderlang:select_row(Row, [1, 6]),
  ?assertEqual(ok, Status3),
  ?assertEqual({1000000, [1.1, 1.6]}, Result3).

should_create_rrd() ->
  rrderlang:start_link(),
  Filename = list_to_binary(?RRD_NAME),
  Options = <<"--step 1">>,
  DSs = [
    <<"DS:first:GAUGE:20:-100:100">>,
    <<"DS:second:COUNTER:20:-100:100">>,
    <<"DS:third:DERIVE:20:-100:100">>,
    <<"DS:fourth:ABSOLUTE:20:-100:100">>
  ],
  RRAs = [
    <<"RRA:AVERAGE:0.5:1:100">>,
    <<"RRA:MIN:0.5:1:100">>,
    <<"RRA:MAX:0.5:1:100">>,
    <<"RRA:LAST:0.5:1:100">>
  ],
  {CreateAnswer, _} = gen_server:call(?RrdErlang_Name, {create, Filename, Options, DSs, RRAs}, ?TIMEOUT),
  ?assertEqual(ok, CreateAnswer).

should_not_create_rrd_when_missing_parameters() ->
  rrderlang:start_link(),
  Filename = list_to_binary(?RRD_NAME),
  Options = <<>>,
  DSs = [],
  RRAs = [],
  {CreateAnswer, _} = gen_server:call(?RrdErlang_Name, {create, Filename, Options, DSs, RRAs}, ?TIMEOUT),
  ?assertEqual(error, CreateAnswer).

should_not_overwrite_rrd() ->
  should_create_rrd(),
  Filename = list_to_binary(?RRD_NAME),
  Options = <<"--no-overwrite">>,
  DSs = [
    <<"DS:first:GAUGE:20:-100:100">>
  ],
  RRAs = [
    <<"RRA:AVERAGE:0.5:1:100">>
  ],
  {CreateAnswer, _} = gen_server:call(?RrdErlang_Name, {create, Filename, Options, DSs, RRAs}, ?TIMEOUT),
  ?assertEqual(error, CreateAnswer).

should_update_rrd() ->
  should_create_rrd(),
  Filename = list_to_binary(?RRD_NAME),
  Options = <<>>,
  {FirstUpdateAnswer, _} = gen_server:call(?RrdErlang_Name, {update, Filename, Options, [1.0, 2, 3, 4], <<"N">>}, ?TIMEOUT),
  ?assertEqual(ok, FirstUpdateAnswer),
  {SecondUpdateAnswer, _} = gen_server:call(?RrdErlang_Name, {update, Filename, Options, [1, 2, 3, 4], <<"N">>}, ?TIMEOUT),
  ?assertEqual(ok, SecondUpdateAnswer),
  {ThirdUpdateAnswer, _} = gen_server:call(?RrdErlang_Name, {update, Filename, Options, [1.0, 2, 3, 4], <<"N">>}, ?TIMEOUT),
  ?assertEqual(ok, ThirdUpdateAnswer).

should_not_update_rrd_when_not_created() ->
  should_create_rrd(),
  Filename = list_to_binary(?RRD_NAME),
  Options = <<>>,
  {UpdateAnswer, _} = gen_server:call(?RrdErlang_Name, {update, Filename, Options, [1.0, 2.0, 3.0, 4.0], <<"N">>}, ?TIMEOUT),
  ?assertEqual(error, UpdateAnswer).

should_not_update_rrd_when_missing_parameters() ->
  should_create_rrd(),
  Filename = list_to_binary(?RRD_NAME),
  Options = <<>>,
  {UpdateAnswer, _} = gen_server:call(?RrdErlang_Name, {update, Filename, Options, [], <<"N">>}, ?TIMEOUT),
  ?assertEqual(error, UpdateAnswer).

should_fetch_data() ->
  should_create_rrd(),
  Data = update_rrd_ntimes(10, 1),
  [{StartTime, _} | _] = Data,
  BinaryStartTime = integer_to_binary(StartTime - 1),
  {EndTime, _} = lists:last(Data),
  BinaryEndTime = integer_to_binary(EndTime - 1),

  Filename = list_to_binary(?RRD_NAME),
  Options = <<"--start ", BinaryStartTime/binary, " --end ", BinaryEndTime/binary>>,
  CF = <<"AVERAGE">>,
  {FetchAnswer, {FetchHeader, FetchData}} = gen_server:call(?RrdErlang_Name, {fetch, Filename, Options, CF}, ?TIMEOUT),

  ?assertEqual(ok, FetchAnswer),
  ?assertEqual([<<"first">>, <<"second">>, <<"third">>, <<"fourth">>], FetchHeader),
  lists:zipwith(fun
    ({_, [Value | _]}, {_, [FetchValue | _]}) -> ?assertEqual(Value, round(FetchValue))
  end, Data, FetchData).

should_not_fetch_data() ->
  should_create_rrd(),
  Filename = list_to_binary(?RRD_NAME),
  Options = <<"--start -5 --end -10">>,
  CF = <<"AVERAGE">>,
  {FetchAnswer, _} = gen_server:call(?RrdErlang_Name, {fetch, Filename, Options, CF}, ?TIMEOUT),
  ?assertEqual(error, FetchAnswer).

should_fetch_selected_data() ->
  should_create_rrd(),
  Data = update_rrd_ntimes(10, 1),
  [{StartTime, _} | _] = Data,
  BinaryStartTime = integer_to_binary(StartTime - 1),
  {EndTime, _} = lists:last(Data),
  BinaryEndTime = integer_to_binary(EndTime - 1),

  Filename = list_to_binary(?RRD_NAME),
  Options = <<"--start ", BinaryStartTime/binary, " --end ", BinaryEndTime/binary>>,
  CF = <<"AVERAGE">>,
  {FetchAnswer, {FetchHeader, FetchData}} = gen_server:call(?RrdErlang_Name, {fetch, Filename, Options, CF, {name, [<<"first">>]}}, ?TIMEOUT),

  ?assertEqual(ok, FetchAnswer),
  ?assertEqual([<<"first">>], FetchHeader),
  lists:zipwith(fun
    ({_, [Value | _]}, {_, [FetchValue | Rest]}) ->
      ?assertEqual(Value, round(FetchValue)),
      ?assertEqual([], Rest)
  end, Data, FetchData).

should_fetch_start_with_data() ->
  rrderlang:start_link(),
  Filename = list_to_binary(?RRD_NAME),
  Step = <<"--step 1">>,
  DSs = [
    <<"DS:Aa:GAUGE:20:-100:100">>,
    <<"DS:Bb:COUNTER:20:-100:100">>,
    <<"DS:Ac:DERIVE:20:-100:100">>,
    <<"DS:Cd:ABSOLUTE:20:-100:100">>
  ],
  RRAs = [
    <<"RRA:AVERAGE:0.5:1:100">>,
    <<"RRA:MIN:0.5:1:100">>,
    <<"RRA:MAX:0.5:1:100">>,
    <<"RRA:LAST:0.5:1:100">>
  ],
  {CreateAnswer, _} = gen_server:call(?RrdErlang_Name, {create, Filename, Step, DSs, RRAs}, ?TIMEOUT),
  ?assertEqual(ok, CreateAnswer),

  Data = update_rrd_ntimes(10, 1),
  [{StartTime, _} | _] = Data,
  BinaryStartTime = integer_to_binary(StartTime - 1),
  {EndTime, _} = lists:last(Data),
  BinaryEndTime = integer_to_binary(EndTime - 1),

  Options = <<"--start ", BinaryStartTime/binary, " --end ", BinaryEndTime/binary>>,
  CF = <<"AVERAGE">>,
  {FetchAnswer, {FetchHeader, _}} = gen_server:call(?RrdErlang_Name, {fetch, Filename, Options, CF, {starts_with, [<<"A">>]}}, ?TIMEOUT),

  ?assertEqual(ok, FetchAnswer),
  ?assertEqual([<<"Aa">>, <<"Ac">>], FetchHeader).

%% %===================================================================
%% % Internal functions
%% %===================================================================

update_rrd_ntimes(N, Step) ->
  {MegaSecs, Secs, MicroSecs} = erlang:now(),
  random:seed(MegaSecs, Secs, MicroSecs),
  Timestamp = 1000000 * MegaSecs + Secs,
  update_rrd_ntimes(N, Step, Timestamp, []).

update_rrd_ntimes(0, _, _, Acc) ->
  lists:reverse(Acc);
update_rrd_ntimes(N, Step, Timestamp, Acc) ->
  Filename = list_to_binary(?RRD_NAME),
  Options = <<>>,
  Values = lists:map(fun(_) -> random:uniform(100) end, lists:duplicate(4, 0)),
  {UpdateAnswer, _} = gen_server:call(?RrdErlang_Name, {update, Filename, Options, Values, integer_to_binary(Timestamp)}, ?TIMEOUT),
  ?assertEqual(ok, UpdateAnswer),
  timer:sleep(Step * 1000),
  update_rrd_ntimes(N - 1, Step, Timestamp + Step, [{Timestamp, Values} | Acc]).

-endif.
