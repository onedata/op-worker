%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(ets_performance_test).
-author("Tomasz Lichon").

-include_lib("eunit/include/eunit.hrl").

-define(CASES, 10000000).

one_key_test_() ->
    {timeout, 60000,
        fun() ->
            catch ets:delete(workers_ets),
            worker_map:init(),
            ets:insert(workers_ets, {name, [node1, node2, node3, node4, node5, node6, node7, node8, node9]}),
            io:format(user, "one_key: ~p~n", [one_key(?CASES, 0)])
        end}.

multiple_key_test_() ->
    {timeout, 60000,
        fun() ->
            catch ets:delete(workers_ets),
            worker_map:init(),
            worker_map:update_workers([{node1, name}, {node2, name}, {node3, name}, {node4, name}, {node5, name}, {node6, name}, {node7, name}, {node8, name}, {node9, name}]),
            io:format(user, "multiple_key: ~p~n", [multiple_key(?CASES, 0)])
        end}.

one_key(0, Time) -> Time;
one_key(N, Time) ->
    ExecTime = exec_time(
        fun() ->
            case ets:lookup(workers_ets, name) of
                [] -> {error, not_found};
                Entries ->
                    RandomIndex = random:uniform(length(Entries)),
                    {_, Node} = lists:nth(RandomIndex, Entries),
                    {ok, Node}
            end
        end),
    one_key(N - 1, Time + ExecTime).

multiple_key(0, Time) -> Time;
multiple_key(N, Time) ->
    ExecTime = exec_time(
        fun() ->
            case ets:lookup(workers_ets, name) of
                [] -> {error, not_found};
                Entries ->
                    RandomIndex = random:uniform(length(Entries)),
                    {_, Node} = lists:nth(RandomIndex, Entries),
                    {ok, Node}
            end
        end),
    multiple_key(N - 1, Time + ExecTime).


exec_time(Fun) ->
    Timestamp1 = erlang:now(),
    Fun(),
    Timestamp2 = erlang:now(),
    timer:now_diff(Timestamp2, Timestamp1).