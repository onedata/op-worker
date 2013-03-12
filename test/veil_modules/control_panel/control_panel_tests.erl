%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of node_manager.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(control_panel_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

type_test_() -> {spawn, test1()}.

test1() ->
  c:cd(".."),
  control_panel:init([]),
  timer:sleep(7000),
  ?_assert(true).

-endif.