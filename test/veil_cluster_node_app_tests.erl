%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of 
%% veil_cluster_node_app. It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(veil_cluster_node_app_tests).

-include("registered_names.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

env_test() ->
	ok = application:start(?APP_Name),
	{ok, _Type} = application:get_env(?APP_Name, node_type),
	ok = application:stop(?APP_Name).

worker_test() -> 
	application:set_env(?APP_Name, node_type, worker), 
	ok = application:start(?APP_Name),
    ?assertNot(undefined == whereis(?Supervisor_Name)),
	ok = application:stop(?APP_Name).

ccm_test() -> 
	application:set_env(?APP_Name, node_type, ccm), 
	ok = application:start(?APP_Name),
    ?assertNot(undefined == whereis(?Supervisor_Name)),
	ok = application:stop(?APP_Name).

-endif.