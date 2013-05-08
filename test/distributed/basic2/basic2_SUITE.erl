-module(basic2_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0]).
-export([test1/1, test2/1, init_vm/0]).

all() -> [test1,test2].

test1(_Config) ->
  pang = net_adm:ping('qqq@plgsl63.local'),
  pong = net_adm:ping('ccm1@plgsl63.local'),
  pong = net_adm:ping('ccm2@plgsl63.local').

test2(_Config) ->
  1 = 2.

init_vm() ->
  ok.