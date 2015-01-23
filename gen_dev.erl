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

-define(APP_NAME, "oneprovider_node").

-define(ARGS_FILE, atom_to_list(?MODULE) ++ "_args.json").
-define(RELEASES_DIRECTORY, "rel").
-define(JSON_PARSER_DIR, "deps/mochiweb/ebin").

-define(DIST_APP_FAILOVER_TIMEOUT, 5000).
-define(SYNC_NODES_TIMEOUT, 60000).

-define(PRINTING_WIDTH, 30).

main(Args) ->
    try
        ArgsFile = get_args_file(Args),
        {ok, FileContent} = file:read_file(ArgsFile),
        ParsedJson = parse_config_file(FileContent),
        Config = proplists:get_value(config, ParsedJson),
        Nodes = proplists:get_value(nodes, ParsedJson),
        create_releases(Nodes, Config)
    catch
        _Type:Error ->
            print("Stacktrace: ~p~n", [erlang:get_stacktrace()]),
            try print("Error: ~ts", [Error])
            catch _:_ -> print("Error: ~p", [Error])
            end
    end.

json_proplist_to_term(Json) ->
    json_proplist_to_term(Json, undefined).

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

parse_config_file(FileContent) ->
    code:add_path(?JSON_PARSER_DIR),
    Json = mochijson2:decode(FileContent, [{format, proplist}]),
    json_proplist_to_term(Json).


get_args_file([ConfigFilePath | _]) ->
    ConfigFilePath;
get_args_file(_) ->
    ?ARGS_FILE.

create_releases([], _) ->
    ok;
create_releases([{Name, NodeConfig} | Rest], Config) ->
    % prepare neccessary paths
    InputDir = proplists:get_value(input_dir, Config),
    TargetDir = proplists:get_value(target_dir, Config),
    ReleaseDirectory = filename:join(TargetDir, atom_to_list(Name)),

    % prepare and print configuration
    print("================ Configuring release ===================="),
    preety_print_entry({input_dir, InputDir}),
    preety_print_entry({output_dir, ReleaseDirectory}),
    print("====================== vm.args =========================="),
    VmArgs = proplists:get_value('vm.args', NodeConfig),
    lists:foreach(fun(X) -> preety_print_entry(X) end, VmArgs),
    print("===================== sys.config ========================"),
    SysConfig = proplists:get_value('sys.config', NodeConfig),
    lists:foreach(fun(X) -> preety_print_entry(X) end, SysConfig),
    print("========================================================="),
    print(""),

    % copy fresh release
    file:make_dir(TargetDir),
    remove_dir(ReleaseDirectory),
    copy_dir(InputDir, ReleaseDirectory),

    % configure
    prepare_helper_modules(TargetDir),
    configurator:configure_release(ReleaseDirectory, ?APP_NAME, SysConfig, VmArgs, ?DIST_APP_FAILOVER_TIMEOUT, ?SYNC_NODES_TIMEOUT),
    cleanup_helper_modules(TargetDir),

    create_releases(Rest, Config).

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

prepare_helper_modules(TargetDir) ->
    code:add_path(TargetDir),
    compile:file(filename:join([?RELEASES_DIRECTORY, "files", "configurator.erl"]), [{outdir, TargetDir}]).

preety_print_entry({Key, Value}) ->
    print("~*s~p", [-?PRINTING_WIDTH, atom_to_list(Key), Value]).

print(Msg) ->
    print(Msg, []).
print(Msg, Args) ->
    io:format(Msg ++ "~n", Args),
    Msg.

cleanup_helper_modules(TargetDir) ->
    file:delete(filename:join(TargetDir,"configurator.beam")).