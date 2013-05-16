%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module starts distributed tests.
%% @end
%% ===================================================================
-module(distributed_test_starter).

%% ====================================================================
%% API
%% ====================================================================
-export([start/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% start/1
%% ====================================================================
%% @doc Starts all tests from the list.
-spec start(ConfigFiles :: list()) -> ok.
%% ====================================================================
start(ConfigFiles) ->
  start_test(ConfigFiles),
  init:stop().

%% ====================================================================
%% Internal functions
%% ====================================================================

%% start/1
%% ====================================================================
%% @doc Starts all tests from the list.
-spec start_test(ConfigFiles :: list()) -> ok.
%% ====================================================================
start_test([]) ->
  ok;

start_test([ConfigFile | Configs]) ->
  io:format("~n~nTest ~s~n~n~n", [ConfigFile]),
  ct_master:run(atom_to_list(ConfigFile)),
  start_test(Configs).
