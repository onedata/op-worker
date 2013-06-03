-module(example_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0]).
-export([ccm1_test/1, ccm2_test/1]).

all() -> [ccm1_test,ccm2_test].

ccm1_test(_Config) ->
  pang = net_adm:ping('non_existing_node@localhost'),
  pong = net_adm:ping('ccm2@localhost').

ccm2_test(_Config) ->
  pang = net_adm:ping('non_existing_node@localhost'),
  pong = net_adm:ping('ccm1@localhost').