%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% fixme move to ctool
%%% @end
%%%-------------------------------------------------------------------
-module(onenv_test_utils).
-author("Michal Stanisz").

-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([
    kill_node/3,
    start_node/3
]).

kill_node(Config, Node, Service) ->
    OnenvScript = kv_utils:get(onenv_script, Config),
    Pod = kv_utils:get([pods, Node], Config),
    ServiceAsList = atom_to_list(Service),
    GetPidCommand = 
        ["ps", "-eo", "'%p,%a'", "|", "grep", "beam.*" ++ ServiceAsList, "|", "cut", "-d", "','", "-f" , "1"],
    PidToKill = ?assertMatch([_ | _], string:trim(utils:cmd([OnenvScript, "exec2", Pod] ++ GetPidCommand))),
    [] = utils:cmd([OnenvScript, "exec2", Pod, "kill", "-s", "SIGKILL", PidToKill]),
    ok.

start_node(Config, Node, Service) ->
    OnenvScript = kv_utils:get(onenv_script, Config),
    Pod = kv_utils:get([pods, Node], Config),
    [] = utils:cmd([OnenvScript, "exec2", Pod, atom_to_list(Service), "start"]),
    ok.
