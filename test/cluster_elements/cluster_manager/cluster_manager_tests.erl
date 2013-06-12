%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of cluster_manager.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(cluster_manager_tests).
-include("registered_names.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

%% ====================================================================
%% Test functions
%% ====================================================================

%% This test checks if ccm is resistant to incorrect requests.
wrong_request_test() ->
	application:set_env(?APP_Name, worker_load_memory_size, 1000),
  application:set_env(?APP_Name, hot_swapping_time, 10000),
  application:set_env(?APP_Name, initialization_time, 10),
  application:set_env(?APP_Name, cluster_clontrol_period, 300),

  cluster_manager:start_link(test),

	gen_server:cast({global, ?CCM}, abc),
	Reply = gen_server:call({global, ?CCM}, abc),
	?assert(Reply =:= wrong_request),

  cluster_manager:stop().

-endif.