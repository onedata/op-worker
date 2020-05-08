%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% This module contains test utility functions useful in tests using onenv.
%%% fixme move to ctool
%%% @end
%%%-------------------------------------------------------------------
-module(onenv_test_utils).
-author("Michal Stanisz").

-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([
    kill_node/2,
    start_opw_node/2
]).

-type service() :: string(). % "op_worker" | "oz_worker" | "op_panel" | "oz_panel" | "cluster_manager"

%%%===================================================================
%%% API
%%%===================================================================

-spec kill_node(test_config:config(), node()) -> ok.
kill_node(Config, Node) ->
    OnenvScript = kv_utils:get(onenv_script, Config),
    Pod = kv_utils:get([pods, Node], Config),
    Service = get_service(Node),
    GetPidCommand = 
        ["ps", "-eo", "'%p,%a'", "|", "grep", "beam.*," ++ Service, "|", "cut", "-d", "','", "-f" , "1"],
    PidToKill = ?assertMatch([_ | _], string:trim(utils:cmd([OnenvScript, "exec2", Pod] ++ GetPidCommand))),
    [] = utils:cmd([OnenvScript, "exec2", Pod, "kill", "-s", "SIGKILL", PidToKill]),
    ok.

-spec start_opw_node(kv_utils:nested(), node()) -> ok.
start_opw_node(Config, Node) ->
    OnenvScript = kv_utils:get(onenv_script, Config),
    OpScriptPath = kv_utils:get(op_worker_script, Config),
    Pod = kv_utils:get([pods, Node], Config),
    [] = utils:cmd([OnenvScript, "exec2", Pod, OpScriptPath, "start"]),
    ok.

-spec get_service(node()) -> service().
get_service(Node) ->
    [Service, Host] = string:split(atom_to_list(Node), "@"),
    case Service of
        "onepanel" -> case string:find(Host, "onezone") of
            nomatch -> "op_panel";
            _ -> "oz_panel"
        end;
        _ -> Service
    end.
