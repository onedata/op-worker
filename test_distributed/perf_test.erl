%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This file contains definitions of annotations used during
%%% performnce tests.
%%% @end
%%%--------------------------------------------------------------------
-module(perf_test).
-author("Michal Wrzeszcz").

-annotation('function').
-include_lib("annotations/include/types.hrl").

-compile(export_all).

around_advice(#annotation{data = {perf_cases, Cases}}, M, F, Inputs) ->
  case os:getenv("perf_test") of
    "true" ->
      Cases;
    _ ->
      annotation:call_advised(M, F, Inputs)
  end;

around_advice(#annotation{data = {_, _} = SingleExt}, M, F, Inputs) ->
  around_advice(#annotation{data=[SingleExt]}, M, F, Inputs);

around_advice(#annotation{data = ConfExt}, M, F, Inputs) when is_list(ConfExt)->
  case os:getenv("perf_test") of
    "true" ->
      process_flag(trap_exit, true),
      Repeats = proplists:get_value(repeats, ConfExt, 1),
      case proplists:get_value(perf_configs, ConfExt, []) of
        [] ->
          Ext = proplists:get_value(perf_config, ConfExt, []),
          exec_perf_config(M, F, Inputs, Ext, Repeats);
        Exts ->
          lists:foreach(
            fun(Ext) -> exec_perf_config(M, F, Inputs, Ext, Repeats) end,
          Exts)
      end;
    _ ->
      Ext = proplists:get_value(ct_config, ConfExt, []),
      [I1] = Inputs,
      annotation:call_advised(M, F, [I1 ++ Ext])
  end;

around_advice(#annotation{}, M, F, Inputs) ->
  around_advice(#annotation{data=[]}, M, F, Inputs).

exec_perf_config(M, F, Inputs, Ext, Repeats) ->
  [I1] = Inputs,
  Ans = exec_multiple_tests(M, F, [I1 ++ Ext], Repeats),
  {ok, File} = file:open("perf_"++atom_to_list(M)++"_"++atom_to_list(F), [append]),
  io:fwrite(File, "Time: ~p, ok_counter ~p, errors: ~p, repeats ~p, conf_ext: ~p~n", Ans ++ [Repeats, Ext]),
  file:close(File).

exec_multiple_tests(M, F, Inputs, Count) ->
  exec_multiple_tests(M, F, Inputs, Count, 0, 0, []).

exec_multiple_tests(_M, _F, _Inputs, 0, Time, OkNum, Errors) ->
  [Time, OkNum, Errors];

exec_multiple_tests(M, F, Inputs, Count, Time, OkNum, Errors) ->
  case exec_test(M, F, Inputs) of
    {error, E} ->
      exec_multiple_tests(M, F, Inputs, Count - 1, Time, OkNum, [E | Errors]);
    T ->
      exec_multiple_tests(M, F, Inputs, Count - 1, Time + T, OkNum + 1, Errors)
  end.

exec_test(M, F, Inputs) ->
  try
    BeforeProcessing = os:timestamp(),
    annotation:call_advised(M, F, Inputs),
    AfterProcessing = os:timestamp(),
    check_links(AfterProcessing, BeforeProcessing)
  catch
    E1:E2 ->
      {error, {E1,E2}}
  end.

check_links(AfterProcessing, BeforeProcessing) ->
  receive
    {'EXIT', _, normal} ->
      check_links(AfterProcessing, BeforeProcessing);
    {'EXIT',_,_} ->
      {error, linked_proc_error}
  after 0 ->
    timer:now_diff(AfterProcessing, BeforeProcessing)
  end.