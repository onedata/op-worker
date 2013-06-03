-module(crash_test_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("env_setter.hrl").
-include("registered_names.hrl").

-export([all/0]).
-export([ccm1_test/1, ccm2_test/1, worker_test/1, tester_test/1]).

all() -> [ccm1_test, ccm2_test, worker_test, tester_test].

ccm1_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:start_test(),
  env_setter:start_app([{node_type, ccm}, {dispatcher_port, 5555}]),
  timer:sleep(1000),
  _Workers = gen_server:call({global, ?CCM}, get_workers),
  env_setter:stop_app(),
  env_setter:stop_test().

ccm2_test(_Config) ->
  ok.

worker_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:start_test(),
  env_setter:start_app([{node_type, worker}, {dispatcher_port, 6666}]),
  timer:sleep(1000),
  env_setter:stop_app(),
  env_setter:stop_test().

tester_test(_Config) ->
  ?INIT_DIST_TEST,
  env_setter:start_test(),
  timer:sleep(500),
  %%_Workers = gen_server:call({global, ?CCM}, get_workers),
  env_setter:stop_test().