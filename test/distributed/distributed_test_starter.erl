%% Copyright
-module(distributed_test_starter).
-author("michal").

%% API
-export([start/0]).

start() ->
  ct_master:run("dist.spec"),
  %%lAns = check_progress(5000),
  %%io:format("aaa ~s~n", [Ans]),
  init:stop().

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