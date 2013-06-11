%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This test is an axample that shows how distributed test
%% should look like.
%% @end
%% ===================================================================

-module(example_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0]).
-export([ccm1_test/1, ccm2_test/1]).

all() -> [ccm1_test,ccm2_test].

%% ====================================================================
%% Test functions
%% ====================================================================

ccm1_test(_Config) ->
  pang = net_adm:ping('non_existing_node@localhost'),
  pong = net_adm:ping('ccm2@localhost').

ccm2_test(_Config) ->
  pang = net_adm:ping('non_existing_node@localhost'),
  pong = net_adm:ping('ccm1@localhost').