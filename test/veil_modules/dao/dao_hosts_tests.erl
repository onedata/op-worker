%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao_hosts module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dao_hosts_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

main_test_() ->
    {setup, fun start_link/0, fun cleanUp/1, []}.

start_link() ->
    {ok, Pid} = dao_hosts:start_link([]),
    Pid.

cleanUp(Pid) ->
    monitor(process, Pid),
    Pid ! {self(), shutdown},
    receive {'DOWN', _Ref, process, Pid, normal} -> ok after 1000 -> error(timeout) end.

-endif.