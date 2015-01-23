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
%%% given gen_dev.args configuration. For each entry in gen_dev.args, the
%%% fresh release is copied from rel/?APP_NAME to 'target_dir', and
%%% vm.args/sys.config are configured properly.
%%% @end
%%%-------------------------------------------------------------------
-module(gen_dev).
-export([main/1]).

-define(APP_NAME, "oneprovider_node").

-define(ARGS_FILE, atom_to_list(?MODULE) ++ ".args").
-define(RELEASES_DIRECTORY, "rel").

%% Default input args
-define(DEFAULT_INPUT_DIR, filename:join(?RELEASES_DIRECTORY, ?APP_NAME)).
-define(DEFAULT_TARGET_DIR, filename:join(?RELEASES_DIRECTORY, "test_cluster")).
-define(DEFAULT_WORKERS_TO_TRIGGER_INIT, infinity).

-define(DIST_APP_FAILOVER_TIMEOUT, 5000).

main(Args) ->
    try
        ArgsFile = get_args_file(Args),
        prepare_helper_modules(),
        {ok, [NodesConfig]} = file:consult(ArgsFile),
        create_releases(NodesConfig),
        cleanup()
    catch
        _Type:Error ->
            cleanup(),
            try print("Error: ~ts", [Error])
            catch _:_ -> print("Error: ~p", [Error])
            end,
            print("Stacktrace: ~p", [erlang:get_stacktrace()])
    end.

get_args_file([_, ConfigFilePath | _]) ->
    ConfigFilePath;
get_args_file(_) ->
    ?ARGS_FILE.

create_releases([]) ->
    ok;
create_releases([Config | Rest]) ->
    % prepare configuration
    print("=================================="),
    print("Configuring new release"),

    FullName = proplists:get_value(name, Config),
    print("name - ~p", [FullName]),

    Type = proplists:get_value(type, Config),
    print("type - ~p", [Type]),

    CcmNodesList = proplists:get_value(ccm_nodes, Config),
    print("ccm_nodes - ~p", [CcmNodesList]),

    DbNodesList = proplists:get_value(db_nodes, Config),
    print("db_nodes - ~p", [DbNodesList]),

    Cookie = proplists:get_value(cookie, Config),
    print("cookie - ~p", [Cookie]),

    InputDir = proplists:get_value(input_dir, Config, ?DEFAULT_INPUT_DIR),
    print("input_dir - ~p", [InputDir]),

    TargetDir = proplists:get_value(target_dir, Config, ?DEFAULT_TARGET_DIR),
    ReleaseDirectory = filename:join(TargetDir, get_name(FullName)),
    print("release_dir - ~p", [ReleaseDirectory]),

    WorkersToTriggerInit = proplists:get_value(workers_to_trigger_init, Config, ?DEFAULT_WORKERS_TO_TRIGGER_INIT),
    case Type of
        ccm -> print("workers_to_trigger_init - ~p", [WorkersToTriggerInit]);
        _ -> ok
    end,

    file:make_dir(TargetDir),
    remove_dir(ReleaseDirectory),
    copy_dir(InputDir, ReleaseDirectory),
    print("Fresh release copied from ~p to ~p", [InputDir, ReleaseDirectory]),
    configurator:configure_release(ReleaseDirectory, ?APP_NAME, FullName, Cookie, Type, CcmNodesList, DbNodesList, WorkersToTriggerInit, ?DIST_APP_FAILOVER_TIMEOUT),
    print("Release configured sucessfully!"),
    print("==================================~n"),
    create_releases(Rest).

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

prepare_helper_modules() ->
    compile:file(filename:join([?RELEASES_DIRECTORY, "files", "configurator.erl"])).

get_name(Hostname) ->
    [Name, _] = string:tokens(Hostname, "@"),
    Name.

print(Msg) ->
    print(Msg, []).
print(Msg, Args) ->
    io:format(Msg ++ "~n", Args),
    Msg.

cleanup() ->
    file:delete("configurator.beam").
