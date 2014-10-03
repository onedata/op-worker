%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of worker_host.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

%% TODO przetestować synchroniczne zapytania
%% TODO przetestować zachowanie w przypadku błędu plug-inu

-module(worker_host_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

%% ====================================================================
%% Test functions
%% ====================================================================

%% This test checks if worker_host is resistant to incorrect requests.
wrong_request_test() ->
	Module = sample_plug_in,
  worker_host:start_link(sample_plug_in, [], 10),
	gen_server:cast(Module, abc),
	Reply = gen_server:call(Module, abc),
	?assert(Reply =:= wrong_request),
  worker_host:stop(Module).

%% This test checks if worker properly stores information about time used by
%% plug-in (oneprovider module) to process requests. The tests checks not only if this
%% information is stored but also verifies if old information is correctly deleted
%% (to provide only latest data to ccm).
load_info_storing_test() ->
	ClientsNum = 50,
	Module = sample_plug_in,
  worker_host:start_link(sample_plug_in, [], 2 * ClientsNum),
	
	startClients(ClientsNum, Module),
	timer:sleep(10 * ClientsNum),
	{Time, _Load} = gen_server:call(Module, getLoadInfo),
	{New, Old, NewListSize, Max} = gen_server:call(Module, getFullLoadInfo),
	?assert(NewListSize == ClientsNum),
	?assert(Max == 2* ClientsNum),
	?assert(length(New) == ClientsNum),
	?assert(Old =:= []),
	{ReqTime, _T} = lists:nth(ClientsNum, New),
	?assert(Time =:= ReqTime),

	startClients(ClientsNum, Module),
	timer:sleep(10 * ClientsNum),
	{Time2, _Load2} = gen_server:call(Module, getLoadInfo),
	{New2, Old2, NewListSize2, Max2} = gen_server:call(Module, getFullLoadInfo),
	?assert(NewListSize2 == 0),
	?assert(Max2 == 2* ClientsNum),
	?assert(length(New2) == 0),
	?assert(length(Old2) == 2* ClientsNum),
	{ReqTime2, _T2} = lists:last(Old2),
	?assert(Time2 =:= ReqTime2),
	?assert(Time2 =:= Time),

	startClients(3 * ClientsNum, Module),
	timer:sleep(30 * ClientsNum),
	{Time3, _Load3} = gen_server:call(Module, getLoadInfo),
	{New3, Old3, NewListSize3, Max3} = gen_server:call(Module, getFullLoadInfo),
	?assert(NewListSize3 == ClientsNum),
	?assert(Max3 == 2* ClientsNum),
	?assert(length(New3) == ClientsNum),
	?assert(length(Old3) == 2* ClientsNum),
	{ReqTime3, _T3} = lists:nth(ClientsNum, Old3),
	?assert(Time3 =:= ReqTime3),
	
	ok = gen_server:call(Module, clearLoadInfo),
	{_Time4, Load4} = gen_server:call(Module, getLoadInfo),
	?assert(Load4 == 0),
	{New4, Old4, NewListSize4, Max4} = gen_server:call(Module, getFullLoadInfo),
	?assert(NewListSize4 == 0),
	?assert(Max4 == 2* ClientsNum),
	?assert(New4 =:= []),
	?assert(Old4 =:= []),

  worker_host:stop(Module).

sequential_request_test() ->
    Module = sample_plug_in,
    worker_host:start_link(sample_plug_in, [], 10),

    gen_server:cast(Module, {sequential_asynch, 1, {long_request, 50, 1, self()}}),
    gen_server:cast(Module, {sequential_asynch, 1, {long_request, 20, 2, self()}}),
    gen_server:cast(Module, {sequential_asynch, 1, {long_request, 10, 3, self()}}),

    First =
        receive
        {1, T1} -> T1
        end,
    Second =
        receive
        {2, T2} -> T2
        end,
    Third =
        receive
        {3, T3} -> T3
        end,
    true = First < Second,
    true = Second < Third,

    worker_host:stop(Module).

synch_request_with_msg_id_test() ->
  Module = sample_plug_in,
  worker_host:start_link(sample_plug_in, [], 10),

  gen_server:cast(Module, {synch, 1, {ok_request, 1}, 11, {proc, self()}}),
  timer:sleep(50),
  gen_server:cast(Module, {synch, 1, {ok_request, 2}, 22, {proc, self()}}),
  timer:sleep(50),
  gen_server:cast(Module, {synch, 1, {ok_request, 3}, 33, {proc, self()}}),

  First =
    receive
      {worker_answer, 33, 3} -> ok
    after 1000 ->
      error
    end,
  Second =
    receive
      {worker_answer, 11, 1} -> ok
    after 1000 ->
      error
    end,
  Third =
    receive
      {worker_answer, 22, 2} -> ok
    after 1000 ->
      error
    end,
  ?assert(First =:= ok),
  ?assert(Second =:= ok),
  ?assert(Third  =:= ok),


  worker_host:stop(Module).

error_request_test() ->
  Module = sample_plug_in,
  worker_host:start_link(sample_plug_in, [], 10),

  gen_server:cast(Module, {synch, 1, error_request, {proc, self()}}),
  Ans = receive
    worker_plug_in_error -> ok
  after 1000 ->
    error
  end,
  ?assert(Ans =:= ok),

  worker_host:stop(Module).

%% This test checks if worker host delegates messages to sub procs correctly
sub_proc_simple_test() ->
  ProcFun = fun({AnsPid, _MapNum}) ->
    AnsPid ! self()
  end,
  MapFun = fun({_AnsPid, MapNum}) ->
    MapNum
  end,
  Pid = worker_host:start_sub_proc(sub_proc_simple, 1, 1, ProcFun, MapFun),

  Self = self(),
  Request1 = {Self, 1000},
  TestFun = fun() ->
    spawn(fun() ->
      Pid ! Request1
    end)
  end,

  TestRequestsNum = 10000,
  for(1, TestRequestsNum, TestFun),

  Ans = count_answers(TestRequestsNum),
  ?assertEqual(1, length(Ans)),
  [{AnsKey, AnsVal} | _] = Ans,
  ?assertEqual(Pid, AnsKey),
  ?assertEqual(TestRequestsNum, AnsVal).

%% This test checks if worker host delegates messages to sub procs correctly
sub_proc_depth1_test() ->
  ProcFun = fun({AnsPid, _MapNum}) ->
    AnsPid ! self()
  end,
  MapFun = fun({_AnsPid, MapNum}) ->
    MapNum
  end,
  Pid = worker_host:start_sub_proc(sub_proc_depth1, 2, 1, ProcFun, MapFun),

  Self = self(),
  Request1 = {Self, 1000},
  TestFun = fun() ->
    spawn(fun() ->
      Pid ! Request1
    end)
  end,

  TestRequestsNum = 10000,
  for(1, TestRequestsNum, TestFun),

  Ans = count_answers(TestRequestsNum),
  ?assertEqual(2, length(Ans)),
  Keys = proplists:get_keys(Ans),
  ?assert(lists:member(Pid, Keys)),
  ?assertEqual(TestRequestsNum, lists:foldl(fun(K, Sum) ->
    Sum + proplists:get_value(K, Ans, 0)
  end, 0, Keys)).

%% This test checks if worker host delegates messages to sub procs correctly
sub_proc_depth2_test() ->
  ProcFun = fun({AnsPid, _MapNum}) ->
    AnsPid ! self()
  end,
  MapFun = fun({_AnsPid, MapNum}) ->
    MapNum
  end,
  Pid = worker_host:start_sub_proc(sub_proc_depth2, 4, 1, ProcFun, MapFun),

  Self = self(),
  Request1 = {Self, 1000},
  TestFun = fun() ->
    spawn(fun() ->
      Pid ! Request1
    end)
  end,

  TestRequestsNum = 10000,
  for(1, TestRequestsNum, TestFun),

  Ans = count_answers(TestRequestsNum),
  ?assertEqual(4, length(Ans)),
  Keys = proplists:get_keys(Ans),
  ?assert(lists:member(Pid, Keys)),
  ?assertEqual(TestRequestsNum, lists:foldl(fun(K, Sum) ->
    Sum + proplists:get_value(K, Ans, 0)
  end, 0, Keys)).

%% This test checks if worker host delegates messages to sub procs correctly
sub_proc_width_test() ->
  ProcFun = fun({AnsPid, _MapNum}) ->
    AnsPid ! self()
  end,
  MapFun = fun({_AnsPid, MapNum}) ->
    MapNum
  end,
  Pid = worker_host:start_sub_proc(sub_proc_width, 2, 3, ProcFun, MapFun),

  Self = self(),
  TestFun = fun() ->
    spawn(fun() ->
      Pid ! {Self, 0},
      Pid ! {Self, 1},
      Pid ! {Self, 2},
      Pid ! {Self, 3},
      Pid ! {Self, 4}
    end)
  end,

  TestRequestsNum = 2000,
  for(1, TestRequestsNum, TestFun),

  Ans = count_answers(5* TestRequestsNum),
  ?assertEqual(4, length(Ans)),
  Keys = proplists:get_keys(Ans),
  ?assert(lists:member(Pid, Keys)),
  ?assertEqual(5* TestRequestsNum, lists:foldl(fun(K, Sum) ->
    Sum + proplists:get_value(K, Ans, 0)
  end, 0, Keys)).

%% ====================================================================
%% Helper functions
%% ====================================================================

startClients(ProcNum, Module) ->
	for(1, ProcNum, fun() -> spawn(fun() -> gen_server:cast(Module, {asynch, 1, sample_message}) end) end).

for(N, N, F) -> [F()];
for(I, N, F) -> [F()|for(I+1, N, F)].

count_answers(ExpectedNum) ->
  count_answers(ExpectedNum, []).

count_answers(0, TmpAns) ->
  TmpAns;

count_answers(ExpectedNum, TmpAns) ->
  receive
    {'EXIT', _, _} -> count_answers(ExpectedNum, TmpAns);
    Msg ->
      NewCounter = proplists:get_value(Msg, TmpAns, 0) + 1,
      NewAns = [{Msg, NewCounter} | proplists:delete(Msg, TmpAns)],
      count_answers(ExpectedNum - 1, NewAns)
  after 1000 ->
    TmpAns
  end.

-endif.
