%% Copyright
-module(distributed_test_starter).
-author("michal").

%% API
-export([start/0]).

start() ->
  ct_master:run("dist.spec"),
  init:stop().