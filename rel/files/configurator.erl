%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions that configure oneprovider_node release
%%% by replacing envs in vm.args and sys.config
%%% @end
%%%-------------------------------------------------------------------
-module(configurator).
-author("Tomasz Lichon").

%% API
-export([configure_release/10, get_env/3, replace_env/4, replace_vm_arg/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Configure release stored at ReleaseRootPath, according to given parameters
%% @end
%%--------------------------------------------------------------------
-spec configure_release(ReleaseRootPath :: string(), ApplicationName :: string(), NodeName :: string(),
    Cookie :: string(), NodeType :: atom(), CcmNodes :: [atom()], DbNodes :: [atom()],
    WorkersToTriggerInit :: integer() | infinity, DistributedAppFailoverTimeout :: integer(),
    SyncNodesTimeout :: integer()) -> ok | no_return().
configure_release(ReleaseRootPath, ApplicationName, NodeName, Cookie, NodeType, CcmNodes,
    DbNodes, WorkersToTriggerInit, DistributedAppFailoverTimeout, SyncNodesTimeout) ->
    {ok,[[{release, ApplicationName, AppVsn, _, _, _}]]} = file:consult(filename:join([ReleaseRootPath, "releases", "RELEASES"])),
    SysConfigPath = filename:join([ReleaseRootPath, "releases", AppVsn, "sys.config"]),
    VmArgsPath = filename:join([ReleaseRootPath, "releases", AppVsn, "vm.args"]),

    replace_vm_arg(VmArgsPath, "-name", NodeName),
    replace_vm_arg(VmArgsPath, "-setcookie", Cookie),
    replace_env(SysConfigPath, ApplicationName, node_type, NodeType),
    replace_env(SysConfigPath, ApplicationName, ccm_nodes, CcmNodes),
    replace_env(SysConfigPath, ApplicationName, db_nodes, DbNodes),
    replace_env(SysConfigPath, ApplicationName, workers_to_trigger_init, WorkersToTriggerInit),
    case NodeType =:= ccm andalso length(CcmNodes) > 1 of
        true ->
            OptCcms = CcmNodes -- [list_to_atom(NodeName)],
            replace_application_config(SysConfigPath, kernel,
                [
                    {distributed, [{list_to_atom(ApplicationName), DistributedAppFailoverTimeout, [list_to_atom(NodeName), list_to_tuple(OptCcms)]}]},
                    {sync_nodes_mandatory, OptCcms},
                    {sync_nodes_timeout, SyncNodesTimeout}
            ]);
        false -> ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get env from sys.config file
%% @end
%%--------------------------------------------------------------------
-spec get_env(string(), atom() | string, atom()) -> term() | no_return().
get_env(SysConfigPath, ApplicationName, EnvName) when is_list(ApplicationName) ->
    get_env(SysConfigPath, list_to_atom(ApplicationName), EnvName);
get_env(SysConfigPath, ApplicationName, EnvName) ->
    {ok, [SysConfig]} = file:consult(SysConfigPath),
    AppEnvs = proplists:get_value(ApplicationName, SysConfig),
    proplists:get_value(EnvName, AppEnvs).

%%--------------------------------------------------------------------
%% @doc
%% Replace env in sys.config file
%% @end
%%--------------------------------------------------------------------
-spec replace_env(string(), string() | atom(), atom(), term()) -> ok | no_return().
replace_env(SysConfigPath, ApplicationName, EnvName, EnvValue) when is_list(ApplicationName) ->
    replace_env(SysConfigPath, list_to_atom(ApplicationName), EnvName, EnvValue);
replace_env(SysConfigPath, ApplicationName, EnvName, EnvValue) ->
    {ok, [SysConfig]} = file:consult(SysConfigPath),
    ApplicationEnvs = proplists:get_value(ApplicationName, SysConfig),
    UpdatedApplicationEnvs = [{EnvName, EnvValue} | proplists:delete(EnvName, ApplicationEnvs)],
    replace_application_config(SysConfigPath, ApplicationName, UpdatedApplicationEnvs).

%%--------------------------------------------------------------------
%% @doc
%% Replace whole application config in sys.config file
%% @end
%%--------------------------------------------------------------------
-spec replace_application_config(string(), string() | atom(), list()) -> ok | no_return().
replace_application_config(SysConfigPath, ApplicationName, ApplicationEnvs) when is_list(ApplicationName) ->
    replace_application_config(SysConfigPath, list_to_atom(ApplicationName), ApplicationEnvs);
replace_application_config(SysConfigPath, ApplicationName, ApplicationEnvs) ->
    {ok, [SysConfig]} = file:consult(SysConfigPath),
    UpdatedSysConfig = [{ApplicationName, ApplicationEnvs} | proplists:delete(ApplicationName, SysConfig)],
    ok = file:write_file(SysConfigPath, term_to_string(UpdatedSysConfig) ++ ".").

%%--------------------------------------------------------------------
%% @doc
%% Replace env in vm.args file
%% @end
%%--------------------------------------------------------------------
-spec replace_vm_arg(string(), string(), string()) -> ok | no_return().
replace_vm_arg(VMArgsPath, FullArgName, ArgValue) ->
    [] = os:cmd("sed -i \"s#" ++ FullArgName ++ " .*#" ++ FullArgName ++ " " ++
        ArgValue ++ "#g\" \"" ++ VMArgsPath ++ "\"").

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Convert erlang term to string
%% @end
%%--------------------------------------------------------------------
-spec term_to_string(term()) -> string().
term_to_string(Term) ->
    io_lib:fwrite("~p",[Term]).
