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
    kill_opw_node/2,
    start_opw_node/2
]).

%%%===================================================================
%%% API
%%%===================================================================


kill_opw_node(Config, Node) ->
    % fixme find service type from Node prefix [op_worker, oz_worker, onepanel]@host
    OnenvScript = kv_utils:get(onenv_script, Config),
    Pod = kv_utils:get([pods, Node], Config),
    GetPidCommand = 
        ["ps", "-eo", "'%p,%a'", "|", "grep", "beam.*op_worker", "|", "cut", "-d", "','", "-f" , "1"],
    PidToKill = ?assertMatch([_ | _], string:trim(utils:cmd([OnenvScript, "exec2", Pod] ++ GetPidCommand))),
    [] = utils:cmd([OnenvScript, "exec2", Pod, "kill", "-s", "SIGKILL", PidToKill]),
    ok.

start_opw_node(Config, Node) ->
    OnenvScript = kv_utils:get(onenv_script, Config),
    OpScriptPath = kv_utils:get(opw_script, Config),
    Pod = kv_utils:get([pods, Node], Config),
    [] = utils:cmd([OnenvScript, "exec2", Pod, OpScriptPath, "start"]),
    ok.
