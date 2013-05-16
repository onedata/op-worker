%% Copyright
-module(distributed_test_starter).
-author("michal").

%% API
-export([start/1]).

start(ConfigFiles) ->
  start_test(ConfigFiles),
  init:stop().

start_test([]) ->
  ok;

start_test([ConfigFile | Configs]) ->
  io:format("~n~nTest ~s~n~n~n", [ConfigFile]),
  ct_master:run(atom_to_list(ConfigFile)),
  start_test(Configs).
