%% @author Michal Wrzeszcz

-module(veil_cluster_node_app_tests).

-define(APP_Name, veil_cluster_node).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

type_test() -> 
	ok = application:start(?APP_Name),
    ?assertNot(undefined == whereis(veil_cluster_node_sup)),
	ok = application:stop(?APP_Name).

-endif.