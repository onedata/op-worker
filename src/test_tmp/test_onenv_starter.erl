%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Functions used by ct test to start nodes using one_env for testing
%%% fixme move to ctool
%%% @end
%%%-------------------------------------------------------------------
-module(test_onenv_starter).
-author("Michal Stanisz").

-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/aai/caveats.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([prepare_test_environment/2, clean_environment/1]).

-define(DEFAULT_COOKIE, cluster_node).

prepare_test_environment(Config, _Suite) ->
    application:start(yamerl),
    DataDir = ?config(data_dir, Config),
    ProjectRoot = filename:join(lists:takewhile(fun(Token) ->
        Token /= "test_distributed"
    end, filename:split(DataDir))),
    
    ScenarioName = kv_utils:get(scenario, Config, "1op"),
    
    CustomEnvs = kv_utils:get(custom_envs, Config, []),
    CustomConfigsPaths = add_custom_configs(ProjectRoot, CustomEnvs),

    OnenvScript = filename:join([ProjectRoot, "one-env", "onenv"]),
    ScenarioPath = filename:join([ProjectRoot, "test_distributed", "onenv_scenarios", ScenarioName ++ ".yaml"]),
    ct:pal("Starting onenv scenario ~p~n~n~p", [ScenarioName, ScenarioPath]),
    OnenvStartLogs = utils:cmd(["cd", "../../..", "&&", OnenvScript, "up", ScenarioPath]),
    ct:pal("~s", [OnenvStartLogs]),
    
    utils:cmd([OnenvScript, "wait", "--timeout", "600"]),
    
    Status = utils:cmd([OnenvScript, "status"]),
    [StatusProplist] = yamerl:decode(Status),
    ct:pal("~s", [Status]),
    case proplists:get_value("ready", StatusProplist) of
        false -> throw(environment_not_ready);
        true -> ok
    end,
    
    PodsProplist = proplists:get_value("pods", StatusProplist),
    NodesConfig = prepare_nodes_config(PodsProplist),

    add_entries_to_etc_hosts(StatusProplist),
    ping_nodes(NodesConfig),
    
    BaseConfig = prepare_base_test_config(NodesConfig),
    BaseConfig ++ [
        {onenv_script, OnenvScript},
        {opw_script, script_path(ProjectRoot, "op_worker")}, 
        {cm_script, script_path(ProjectRoot, "cluster_manager")},
        {custom_configs, CustomConfigsPaths}
    ].


clean_environment(Config) ->
    OnenvScript = ?config(onenv_script, Config),
    CustomConfigsPaths = kv_utils:get(custom_configs, Config),
    lists:foreach(fun(Path) ->
        file:delete(Path)
    end, CustomConfigsPaths),
    utils:cmd([OnenvScript, "clean", "--all", "--persistent-volumes"]),
    ok.


prepare_nodes_config(PodsProplist) ->
    {P, N} = lists:foldl(fun({PodName, X}, {Pods, Nodes}) -> 
        H = proplists:get_value("hostname", X), 
        case proplists:get_value("service-type", X) of 
            "oneprovider" -> 
                WorkerNodes = maps:get(op_worker_nodes, Nodes, []), 
                PanelNodes = maps:get(op_panel_nodes, Nodes, []),
                CmNodes = maps:get(cm_nodes, Nodes, []), 
                WorkerNode = list_to_atom("op_worker@" ++ H),
                NewNodes = Nodes#{
                    op_worker_nodes => [WorkerNode | WorkerNodes], 
                    op_panel_nodes => [list_to_atom("onepanel@" ++ H) | PanelNodes],
                    cm_nodes => [list_to_atom("cluster_manager@" ++ H) | CmNodes]
                },
                NewPods = Pods#{WorkerNode => PodName},
                {NewPods, NewNodes};
            "onezone" ->
                WorkerNodes = maps:get(oz_worker_nodes, Nodes, []),
                PanelNodes = maps:get(oz_panel_nodes, Nodes, []),
                CmNodes = maps:get(cm_nodes, Nodes, []), 
                NewNodes = Nodes#{
                    oz_worker_nodes => [list_to_atom("oz_worker@" ++ H) | WorkerNodes],
                    oz_panel_nodes => [list_to_atom("onepanel@" ++ H) | PanelNodes],
                    cm_nodes => [list_to_atom("cluster_manager@" ++ H) | CmNodes]
                },
                {Pods, NewNodes};
            _ -> {Pods, Nodes}
        end 
    end, {#{}, #{}}, PodsProplist),
    maps:to_list(N) ++ [{pods, maps:to_list(P)}].


ping_nodes(Config) ->
    NodesTypes = [cm_nodes, op_worker_nodes, op_panel_nodes, oz_worker_nodes, oz_panel_nodes],
    erlang:set_cookie(node(), ?DEFAULT_COOKIE),
    lists:foreach(fun(NodeType) ->
        Nodes = proplists:get_value(NodeType, Config, []),
        lists:foreach(fun(Node) ->
            true = net_kernel:connect_node(Node)
        end, Nodes)
    end, NodesTypes).


add_entries_to_etc_hosts(OnenvStatus) ->
    PodsConfig = proplists:get_value("pods", OnenvStatus),

    HostsEntries = lists:foldl(fun({_ServiceName, ServiceConfig}, Acc0) ->
        ServiceType = proplists:get_value("service-type", ServiceConfig),
        case lists:member(ServiceType, ["onezone", "oneprovider"]) of
            true ->
                Ip = proplists:get_value("ip", ServiceConfig),

                Acc1 = case proplists:get_value("domain", ServiceConfig, undefined) of
                    undefined -> Acc0;
                    Domain -> [{Domain, Ip} | Acc0]
                end,
                case proplists:get_value("hostname", ServiceConfig, undefined) of
                    undefined -> Acc1;
                    Hostname -> [{Hostname, Ip} | Acc1]
                end;
            false ->
                Acc0
        end
    end, [], PodsConfig),

    {ok, File} = file:open("/etc/hosts", [append]),

    lists:foreach(fun({DomainOrHostname, Ip}) ->
        io:fwrite(File, "~s ~s~n", [Ip, DomainOrHostname])
    end, HostsEntries),

    file:close(File).


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
    
    NodesConfig ++ 
        [{provider_nodes, maps:to_list(ProvidersNodes)}] ++ 
        [{providers, ProvidersList}] ++ 
        [{provider_panels, maps:to_list(ProviderPanels)}] ++
        [{primary_cm, maps:to_list(PrimaryCm)}] ++ 
        [{users, maps:to_list(ProviderUsers)}] ++ 
        [{sess_id, maps:to_list(Sessions)}] ++
        [{provider_spaces, maps:to_list(ProviderSpaces)}].


% fixme currently works only for op_worker (other sources not mounted in testmaster docker)
add_custom_configs(ProjectRoot, CustomEnvs) ->
    lists:foldl(fun({Component, Envs}, Acc) ->
        Path = test_custom_config_path(ProjectRoot, atom_to_list(Component)),
        file:write_file(Path, io_lib:format("~p.", [Envs])),
        [Path | Acc]
    end, [], CustomEnvs).


script_path(ProjectRoot, Service) ->
    SourcesRelPath = sources_rel_path(ProjectRoot, Service),
    filename:join([SourcesRelPath, Service, "bin", Service]).

test_custom_config_path(ProjectRoot, Service) ->
    SourcesRelPath = sources_rel_path(ProjectRoot, Service),
    filename:join([SourcesRelPath, Service, "etc", "config.d", "ct_test_custom.config"]).

sources_rel_path(ProjectRoot, Service) ->
    Service1 = re:replace(Service, "_", "-", [{return, list}]),
    filename:join([ProjectRoot, "..", Service1, "_build", "default", "rel"]).

setup_user_session(UserId, OzNode, OpwNode) ->
    TimeCaveat = #cv_time{valid_until = rpc:call(OzNode, time_utils, cluster_time_seconds, []) + 100},
    {ok, AccessToken} =
        rpc:call(OzNode, token_logic, create_user_temporary_token,
            [?ROOT, UserId, #{<<"caveats">> => [TimeCaveat]}]),
    {ok, SerializedAccessToken} = rpc:call(OzNode, tokens, serialize, [AccessToken]),
    Nonce = base64:encode(crypto:strong_rand_bytes(8)),
    Identity = ?SUB(user, UserId),
    Credentials = 
        rpc:call(OpwNode, auth_manager, build_token_credentials, 
            [SerializedAccessToken, undefined, undefined, undefined, allow_data_access_caveats]),
    
    rpc:call(OpwNode, session_manager, reuse_or_create_fuse_session, [Nonce, Identity, Credentials]).
