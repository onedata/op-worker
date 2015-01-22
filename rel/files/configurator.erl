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
-export([configure_release/8, get_env/3, replace_env/4, replace_vm_arg/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Configure release stored at ReleaseRootPath, according to given parameters
%% @end
%%--------------------------------------------------------------------
-spec configure_release(ReleaseRootPath :: string(), ApplicationName :: atom(), NodeName :: string(), Cookie :: string(), NodeType :: atom(),
    CcmNodes :: [atom()], DbNodes :: [atom()], DistributedAppFailoverTimeout :: integer()) -> ok | no_return().
configure_release(ReleaseRootPath, ApplicationName, NodeName, Cookie, NodeType, CcmNodes, DbNodes, DistributedAppFailoverTimeout) ->
    {ok,[[{release, ApplicationName, AppVsn, _, _, _}]]} = file:consult(filename:join([ReleaseRootPath, "releases", "RELEASES"])),
    SysConfigPath = filename:join([ReleaseRootPath, "releases", AppVsn, "sys.config"]),
    VmArgsPath = filename:join([ReleaseRootPath, "releases", AppVsn, "vm.args"]),

    replace_vm_arg(VmArgsPath, "-name", NodeName),
    replace_vm_arg(VmArgsPath, "-setcookie", Cookie),
    replace_env(SysConfigPath, ApplicationName, node_type, NodeType),
    replace_env(SysConfigPath, ApplicationName, ccm_nodes, CcmNodes),
    replace_env(SysConfigPath, ApplicationName, db_nodes, DbNodes),
    replace_env(SysConfigPath, "kernel", distributed, [{list_to_atom(NodeName), DistributedAppFailoverTimeout, CcmNodes}]),
    replace_env(SysConfigPath, "kernel", sync_nodes_mandatory, CcmNodes -- [list_to_atom(NodeName)]).

%%--------------------------------------------------------------------
%% @doc
%% Get env from sys.config file
%% @end
%%--------------------------------------------------------------------
-spec get_env(string(), atom(), atom()) -> term() | no_return().
get_env(SysConfigPath, ApplicationName, EnvName) ->
    {ok, [SysConfig]} = file:consult(SysConfigPath),
    AppEnvs = proplists:get_value(ApplicationName, SysConfig),
    proplists:get_value(EnvName, AppEnvs).

%%--------------------------------------------------------------------
%% @doc
%% Replace env in sys.config file
%% @end
%%--------------------------------------------------------------------
-spec replace_env(string(), atom(), atom(), term()) -> ok | no_return().
replace_env(SysConfigPath, _ApplicationName, EnvName, EnvValue) ->
    [] = os:cmd("sed -i \"s#{" ++ atom_to_list(EnvName) ++ ",.*#{" ++ atom_to_list(EnvName) ++ ", " ++
        term_to_string(EnvValue) ++ "},#g\" \"" ++ SysConfigPath ++ "\""),
    ok.

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
