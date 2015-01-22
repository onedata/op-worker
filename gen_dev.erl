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
-define(FRESH_RELEASE_DIRECTORY, filename:join(?RELEASES_DIRECTORY, ?APP_NAME)).

-define(DIST_APP_FAILOVER_TIMEOUT, 5000).

main(_) ->
    try
        prepare_helper_modules(),
        {ok, [NodesConfig]} = file:consult(?ARGS_FILE),
        create_releases(NodesConfig),
        cleanup()
    catch
        _Type:Error ->
            cleanup(),
            try print("Error: ~ts",[Error])
            catch _:_  -> print("Error: ~p",[Error])
            end,
            print("Stacktrace: ~p",[erlang:get_stacktrace()])
    end.

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
    TargetDir = proplists:get_value(target_dir, Config),
    ReleaseDirectory = filename:join(TargetDir, get_name(FullName)),
    print("target_dir - ~p", [ReleaseDirectory]),
    WorkersToTriggerInit = proplists:get_value(workers_to_trigger_init, Config, infinity),
    case Type of
        ccm -> print("workers_to_trigger_init - ~p", [ReleaseDirectory]);
        _ -> ok
    end,

    file:make_dir(TargetDir),
    remove_dir(ReleaseDirectory),
    copy_dir(?FRESH_RELEASE_DIRECTORY, ReleaseDirectory),
    print("Fresh release copied to ~p", [ReleaseDirectory]),
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
    print(Msg,[]).
print(Msg, Args) ->
    io:format(Msg ++ "~n",Args),
    Msg.

cleanup() ->
    file:delete("configurator.beam").
