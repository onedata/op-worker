#!/usr/bin/env escript
%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% gen_dev script uses configurator.erl to configure release according to
%%% given gen_dev_args configuration. For each entry in gen_dev_args, the
%%% fresh release is copied from rel/?APP_NAME to 'target_dir', and
%%% vm.args/sys.config are configured properly.
%%% @end
%%%-------------------------------------------------------------------
-module(gen_dev).
-export([main/1]).

-define(ARGS_FILE, atom_to_list(?MODULE) ++ "_args.json").
-define(RELEASES_DIRECTORY, "rel").
-define(JSON_PARSER_DIR, "deps/mochiweb/ebin").

% oneprovider specific config
-define(ONEPROVIDER_APP_NAME, oneprovider_node).
-define(DIST_APP_FAILOVER_TIMEOUT, 5000).
-define(SYNC_NODES_TIMEOUT, 60000).

-define(PRINTING_WIDTH, 30).

main(Args) ->
    try
        ArgsFile = get_args_file(Args),
        {ok, FileContent} = file:read_file(ArgsFile),
        ParsedJson = parse_config_file(FileContent),
        configure_apps(ParsedJson)
    catch
        _Type:Error ->
            Stacktrace = erlang:get_stacktrace(),
            try print("Error: ~ts", [Error])
            catch _:_ -> print("Error: ~p", [Error])
            end,
            print("Stacktrace: ~p~n", [Stacktrace])
    end.

%%%===================================================================
%%% Application configuration
%%%===================================================================
configure_apps([]) ->
    [];
configure_apps([{AppName, ApplicationJson} | Rest]) ->
    Config = proplists:get_value(config, ApplicationJson),
    Nodes = proplists:get_value(nodes, ApplicationJson),
    [create_releases(AppName, Config, Nodes) | configure_apps(Rest)].

create_releases(_, _, []) ->
    ok;
create_releases(AppName, Config, [{Name, NodeConfig} | Rest]) ->
    {InputDir, TargetDir} = prepare_neccessary_paths(Config),
    ReleaseDir = prepare_fresh_release(InputDir, TargetDir, Name),
    {SysConfig, VmArgs} = prepare_and_print_configuration(InputDir, ReleaseDir, NodeConfig),
    configure_release(AppName, ReleaseDir, SysConfig, VmArgs),
    create_releases(AppName, Config, Rest).

prepare_neccessary_paths(Config) ->
    InputDir = proplists:get_value(input_dir, Config),
    TargetDir = proplists:get_value(target_dir, Config),
    make_dir(TargetDir),
    {InputDir, TargetDir}.

prepare_and_print_configuration(InputDir, ReleaseDir, NodeConfig) ->
    print("================ Configuring release ===================="),
    preety_print_entry({input_dir, InputDir}),
    preety_print_entry({release_dir, ReleaseDir}),
    print("====================== vm.args =========================="),
    VmArgs = proplists:get_value('vm.args', NodeConfig),
    lists:foreach(fun(X) -> preety_print_entry(X) end, VmArgs),
    print("===================== sys.config ========================"),
    SysConfig = proplists:get_value('sys.config', NodeConfig),
    lists:foreach(fun(X) -> preety_print_entry(X) end, SysConfig),
    print("========================================================="),
    print(""),
    {SysConfig, VmArgs}.

prepare_fresh_release(InputDir, TargetDir, Name) ->
    ReleaseDir = filename:join(TargetDir, atom_to_list(Name)),
    remove_dir(ReleaseDir),
    copy_dir(InputDir, ReleaseDir),
    ReleaseDir.

%%%===================================================================
%%% Release configuration
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Configure release stored at ReleaseRootPath, according to given parameters
%% @end
%%--------------------------------------------------------------------
-spec configure_release(ApplicationName :: atom(), ReleaseRootPath :: string(), SysConfig :: list(), VmArgs :: list()) ->
    ok | no_return().
configure_release(?ONEPROVIDER_APP_NAME, ReleaseRootPath, SysConfig, VmArgs) ->
    {SysConfigPath, VmArgsPath} = find_config_location(?ONEPROVIDER_APP_NAME, ReleaseRootPath),
    lists:foreach(fun({Key, Value}) -> replace_vm_arg(VmArgsPath, "-" ++ atom_to_list(Key), Value) end, VmArgs),
    lists:foreach(fun({Key, Value}) -> replace_env(SysConfigPath, ?ONEPROVIDER_APP_NAME, Key, Value) end, SysConfig),

    % configure kernel distributed erlang app
    NodeName = proplists:get_value(name, VmArgs),
    NodeType = proplists:get_value(type, SysConfig),
    CcmNodes = proplists:get_value(ccm_nodes, SysConfig),
    case NodeType =:= ccm andalso length(CcmNodes) > 1 of
        true ->
            OptCcms = CcmNodes -- [list_to_atom(NodeName)],
            replace_application_config(SysConfigPath, kernel,
                [
                    {distributed, [{?ONEPROVIDER_APP_NAME, ?DIST_APP_FAILOVER_TIMEOUT, [list_to_atom(NodeName), list_to_tuple(OptCcms)]}]},
                    {sync_nodes_mandatory, OptCcms},
                    {sync_nodes_timeout, ?SYNC_NODES_TIMEOUT}
                ]);
        false -> ok
    end;
configure_release(ApplicationName, ReleaseRootPath, SysConfig, VmArgs) ->
    {SysConfigPath, VmArgsPath} = find_config_location(ApplicationName, ReleaseRootPath),
    lists:foreach(fun({Key, Value}) -> replace_vm_arg(VmArgsPath, "-" ++ atom_to_list(Key), Value) end, VmArgs),
    lists:foreach(fun({Key, Value}) -> replace_env(SysConfigPath, ApplicationName, Key, Value) end, SysConfig).

%%--------------------------------------------------------------------
%% @doc
%% Reads erlang 'RELEASES' file in order to find where vm.args and
%% sys.config are located
%% @end
%%--------------------------------------------------------------------
-spec find_config_location(ApplicationName :: atom(), ReleaseRootPath :: atom()) ->
    {SysConfigPath :: string(), VmArgsPath :: string()}.
find_config_location(ApplicationName, ReleaseRootPath) ->
    ApplicationNameString = atom_to_list(ApplicationName),
    {ok,[[{release, ApplicationNameString, AppVsn, _, _, _}]]} = file:consult(filename:join([ReleaseRootPath, "releases", "RELEASES"])),
    SysConfigPath = filename:join([ReleaseRootPath, "releases", AppVsn, "sys.config"]),
    VmArgsPath = filename:join([ReleaseRootPath, "releases", AppVsn, "vm.args"]),
    {SysConfigPath, VmArgsPath}.

%%--------------------------------------------------------------------
%% @doc
%% Replace env in sys.config file
%% @end
%%--------------------------------------------------------------------
-spec replace_env(string(), atom(), atom(), term()) -> ok | no_return().
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
-spec replace_application_config(string(), atom(), list()) -> ok | no_return().
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

%%--------------------------------------------------------------------
%% @doc
%% Convert erlang term to string
%% @end
%%--------------------------------------------------------------------
-spec term_to_string(term()) -> string().
term_to_string(Term) ->
    io_lib:fwrite("~p",[Term]).

%%%===================================================================
%%% Filesystem operations
%%%===================================================================
make_dir(Path) ->
    case os:cmd("mkdir -p '" ++ Path ++ "'") of
        [] -> ok;
        Err -> throw(Err)
    end.
remove_dir(Path) ->
    case os:cmd("rm -rf '" ++ Path ++ "'") of
        [] -> ok;
        Err -> throw(Err)
    end.
copy_dir(From, To) ->
    case os:cmd("cp -R '" ++ From ++ "' '" ++ To ++ "'") of
        [] -> ok;
        Err -> throw(Err)
    end.

%%%===================================================================
%%% Args Parsing
%%%===================================================================
get_args_file([ConfigFilePath | _]) ->
    ConfigFilePath;
get_args_file(_) ->
    ?ARGS_FILE.

parse_config_file(FileContent) ->
    code:add_path(?JSON_PARSER_DIR),
    Json = mochijson2:decode(FileContent, [{format, proplist}]),
    json_proplist_to_term(Json).

json_proplist_to_term(Json) ->
    json_proplist_to_term(Json, atom).

json_proplist_to_term([], _) ->
    [];
json_proplist_to_term({<<"string">>, V}, _) when is_binary(V) ->
    binary_to_list(V);
json_proplist_to_term({<<"atom">>, V}, _) when is_binary(V) ->
    binary_to_atom(V, unicode);
json_proplist_to_term({<<"config">>, V}, _) ->
    {config, json_proplist_to_term(V, string)};
json_proplist_to_term({<<"sys.config">>, V}, _) ->
    {'sys.config', json_proplist_to_term(V, atom)};
json_proplist_to_term({<<"vm.args">>, V}, _) ->
    {'vm.args', json_proplist_to_term(V, string)};
json_proplist_to_term({K, V}, DefaultType) ->
    {binary_to_atom(K, unicode), json_proplist_to_term(V, DefaultType)};
json_proplist_to_term([Head | Tail], DefaultType) ->
    [json_proplist_to_term(Head, DefaultType) | json_proplist_to_term(Tail, DefaultType)];
json_proplist_to_term(Binary, atom) when is_binary(Binary) ->
    binary_to_atom(Binary, unicode);
json_proplist_to_term(Binary, string) when is_binary(Binary) ->
    binary_to_list(Binary);
json_proplist_to_term(Other, _) ->
    Other.


%%%===================================================================
%%% Logging
%%%===================================================================
preety_print_entry({Key, Value}) ->
    print("~*s~p", [-?PRINTING_WIDTH, atom_to_list(Key), Value]).

print(Msg) ->
    print(Msg, []).
print(Msg, Args) ->
    io:format(Msg ++ "~n", Args),
    Msg.