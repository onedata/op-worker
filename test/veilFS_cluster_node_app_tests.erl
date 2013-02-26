%% @author Michal Wrzeszcz

-module(veilFS_cluster_node_app_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

type_test() -> 
	ok = application:start(veilFS_cluster_node),
    ?assertNot(undefined == whereis(veilFS_cluster_node_sup)),
	ok = application:stop(veilFS_cluster_node).

-endif.