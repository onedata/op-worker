-module(basic_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0]).
-export([test1/1, test2/1, test3/1]).

all() -> [test1,test2,test3].

test1(_Config) ->
  [{Q,ok}] = ct_master:run("dist.spec").
  %%ok = check_progress(5000).

test2(_Config) ->
  1 = 1.

test3(_Config) ->
  1 = 1.

check_progress(Timeout) when Timeout < 0 ->
  timeout;

check_progress(Timeout) ->
  SleepTime = 500,

  Progress = check_progress_node(ct_master:progress()),
  case Progress of
    ok -> ok;
    ongoing ->
      timer:sleep(SleepTime),
      check_progress(Timeout - SleepTime);
    _Other -> Progress
  end.

check_progress_node([]) ->
  ok;

check_progress_node([{_Node, Status} | Progress]) ->
  case Status of
    finished_ok -> check_progress_node(Progress);
    ongoing -> ongoing;
    _Other -> error
  end.