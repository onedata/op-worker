%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Functions used by ct test to start nodes using one_env for testing
%%% @end
%%%-------------------------------------------------------------------
-module(test_onenv_starter).
-author("Michal Stanisz").

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
    
    ScenarioName = ?config(scenario, Config),

    OnenvScript = filename:join([ProjectRoot, "one-env", "onenv"]),
    ScenarioPath = filename:join([ProjectRoot, "test_distributed", "onenv_scenarios", ScenarioName ++ ".yaml"]),
    ct:pal("Starting onenv scenario ~p~n~n~p", [ScenarioName, ScenarioPath]),
    OnenvStartLogs = utils:cmd(["cd", "../../..", "&&", OnenvScript, "up", ScenarioPath]),
    ct:pal("~s", [OnenvStartLogs]),
    
    utils:cmd([OnenvScript, "wait", "--timeout", "600"]),
    
    Status = utils:cmd([OnenvScript, "status"]),
    [StatusProplist] = yamerl:decode(Status),
    ct:pal("~s", [Status]),

    PodsProplist = proplists:get_value("pods", StatusProplist),
    NodesConfig = prepare_nodes_config(PodsProplist),

    add_entries_to_etc_hosts(StatusProplist),

    ping_nodes(NodesConfig),

    
    A = prepare_base_config(NodesConfig),
    B = A ++ [{onenv_script, OnenvScript}],
    lists:foreach(fun(Node) ->
        overwrite_sources(B, Node, ProjectRoot)
    end, ?config(op_worker_nodes, B)),
    B.

clean_environment(Config) ->
    OnenvScript = ?config(onenv_script, Config),
    % fixme
%%    utils:cmd([OnenvScript, "clean"]),
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


prepare_base_config(NodesConfig) ->
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
    
    Sessions = lists:foldl(fun(ProviderId, Acc) ->
        [Node | _] = maps:get(ProviderId, ProvidersNodes),
        {ok, L} = rpc:call(Node, session, list, []),
        ProviderSessions = lists:filtermap(fun({_, SessId, SessionDoc, _, _,_,_,_,_,_}) ->
            case element(4, SessionDoc) of
                fuse -> 
                    {_,_,_,UserId} = element(5, SessionDoc),
                    {true, {UserId, SessId}};
                _ -> false
            end
        end, L),
        Acc#{ProviderId => ProviderSessions}
    end, #{}, ProvidersList),
    
    ProviderUsers = maps:fold(fun(ProviderId, UserSessionMapping, Acc) ->
        Users = lists:usort(lists:map(fun({UserId, _SessionId}) -> UserId end, UserSessionMapping)),
        Acc#{ProviderId => Users}
    end, #{}, Sessions),
    
    NodesConfig ++ 
        [{provider_nodes, maps:to_list(ProvidersNodes)}] ++ 
        [{providers, ProvidersList}] ++ 
        [{provider_panels, maps:to_list(ProviderPanels)}] ++
        [{primary_cm, maps:to_list(PrimaryCm)}] ++ 
        [{users, maps:to_list(ProviderUsers)}] ++ 
        [{sess_id, maps:to_list(Sessions)}] ++
        [{provider_spaces, maps:to_list(ProviderSpaces)}].


% fixme temporary solution - fix onenv sources after restart
overwrite_sources(Config, Node, ProjectRoot) ->
    Pod = kv_utils:get([pods, Node], Config),
    
    VersionBin = rpc:call(Node, oneprovider, get_version, []),
    
    lists:foreach(fun(Name) ->
        TargetPrefix = filename:join(["/", "usr", "lib", Name, "lib"]),
        SourcePrefix = filename:join([ProjectRoot, "_build", "default", "lib"]),
        TargetDir = filename:join([TargetPrefix, Name ++ "-" ++ binary_to_list(VersionBin)]),
        SourceDir = filename:join([SourcePrefix, Name, "*"]),
        Deps = [{"cluster_worker", "3.0.0-beta3"}, {"ctool", "3.0.0-beta3"}, {"bp_tree", "1.0.0"}],
        
%%        ct:print("~p~n~p", [TargetDir, SourceDir]),
        OnenvScript = kv_utils:get(onenv_script, Config),
        utils:cmd([OnenvScript, "exec2", Pod, "rm", "-rf", TargetDir]),
        utils:cmd([OnenvScript, "exec2", Pod, "mkdir", TargetDir]),
        utils:cmd([OnenvScript, "exec2", Pod, "cp", "-Lrf", SourceDir, TargetDir]),
    
        lists:foreach(fun({Dep, Version}) ->
            T = filename:join([TargetPrefix, Dep ++ "-" ++ Version]),
            S = filename:join([SourcePrefix, Dep, "*"]),
%%            ct:print("~p~n~p", [S,T]),
            utils:cmd([OnenvScript, "exec2", Pod, "rm", "-rf", T]),
            utils:cmd([OnenvScript, "exec2", Pod, "mkdir", T]),
            utils:cmd([OnenvScript, "exec2", Pod, "cp", "-Lrf", S, T])
        end, Deps)
    
        end, ["op_worker", "cluster_manager"]),
    ok.
