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

-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/aai/caveats.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([
    prepare_base_test_config/1,
    kill_node/2,
    start_opw_node/2
]).

-type service() :: string(). % "op_worker" | "oz_worker" | "op_panel" | "oz_panel" | "cluster_manager"

%%%===================================================================
%%% API
%%%===================================================================

% fixme keep this in provider
-spec prepare_base_test_config(test_config:config()) -> test_config:config().
prepare_base_test_config(NodesConfig) ->
    ProvidersNodes = lists:foldl(fun(Node, Acc) ->
        ProviderId = rpc:call(Node, oneprovider, get_id, []),
        OtherNodes = maps:get(ProviderId, Acc, []),
        Acc#{ProviderId => [Node | OtherNodes]}
    end, #{}, proplists:get_value(op_worker_nodes, NodesConfig, [])),
    
    ProviderPanels = lists:foldl(fun(Node, Acc) ->
        [WorkerNode | _] = rpc:call(Node, service_op_worker, get_nodes, []),
        ProviderId = rpc:call(WorkerNode, oneprovider, get_id, []),
        OtherNodes = maps:get(ProviderId, Acc, []),
        Acc#{ProviderId => [Node | OtherNodes]}
    end, #{}, proplists:get_value(op_panel_nodes, NodesConfig, [])),
    
    PrimaryCm = maps:fold(fun(ProviderId, [PanelNode | _], Acc) ->
        {ok, Hostname} = rpc:call(PanelNode, service_cluster_manager,  get_main_host, []),
        Acc#{ProviderId => list_to_atom("cluster_manager@" ++ Hostname)}
    end, #{}, ProviderPanels),
    
    ProvidersList = maps:keys(ProvidersNodes),
    
    ProviderSpaces = lists:foldl(fun(ProviderId, Acc) ->
        [Node | _] = maps:get(ProviderId, ProvidersNodes),
        {ok, Spaces} = rpc:call(Node, provider_logic, get_spaces, []),
        Acc#{ProviderId => Spaces}
    end, #{}, ProvidersList),
    
    ProviderUsers = lists:foldl(fun(ProviderId, Acc) ->
        [Node | _] = maps:get(ProviderId, ProvidersNodes),
        {ok, Users} = rpc:call(Node, provider_logic, get_eff_users, []),
        Acc#{ProviderId => Users}
    end, #{}, ProvidersList),
    
    [OzNode | _ ] = kv_utils:get(oz_worker_nodes, NodesConfig),
    Sessions = maps:map(fun(ProviderId, Users) ->
        [Node | _] = maps:get(ProviderId, ProvidersNodes),
        lists:map(fun(UserId) ->
            {ok, SessId} = setup_user_session(UserId, OzNode, Node),
            {UserId, SessId}
        end, Users)
    end, ProviderUsers),
    
    test_config:set_many(NodesConfig, [
        [provider_nodes, ProvidersNodes],
        [providers, ProvidersList],
        [provider_panels, ProviderPanels],
        [primary_cm, PrimaryCm],
        [users, ProviderUsers],
        [sess_id, Sessions],
        [provider_spaces, ProviderSpaces]
    ]).


%% @private
-spec setup_user_session(UserId :: binary(), OzwNode :: node(), OpwNode :: node()) ->
    {ok, SessId :: binary()}.
setup_user_session(UserId, OzwNode, OpwNode) ->
    % fixme more time?
    TimeCaveat = #cv_time{valid_until = rpc:call(OzwNode, time_utils, cluster_time_seconds, []) + 100},
    {ok, AccessToken} =
        rpc:call(OzwNode, token_logic, create_user_temporary_token,
            [?ROOT, UserId, #{<<"caveats">> => [TimeCaveat]}]),
    {ok, SerializedAccessToken} = rpc:call(OzwNode, tokens, serialize, [AccessToken]),
    Nonce = base64:encode(crypto:strong_rand_bytes(8)),
    Identity = ?SUB(user, UserId),
    Credentials =
        rpc:call(OpwNode, auth_manager, build_token_credentials,
            [SerializedAccessToken, undefined, undefined, undefined, allow_data_access_caveats]),
    
    rpc:call(OpwNode, session_manager, reuse_or_create_fuse_session, [Nonce, Identity, Credentials]).


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
