#!/usr/bin/env escript

-module(gen_dev).
-export([main/1]).

-define(app_name, "oneprovider_node").

-define(args_file, atom_to_list(?MODULE) ++ ".args").
-define(releases_directory, "rel").
-define(fresh_release_directory, filename:join(?releases_directory, ?app_name)).

-define(worker_name_suffix, "_worker").
-define(dist_app_failover_timeout, 5000).

main(_) ->
    try
        prepare_helper_modules(),
        {ok, [NodesConfig]} = file:consult(?args_file),
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

    file:make_dir(TargetDir),
    remove_dir(ReleaseDirectory),
    copy_dir(?fresh_release_directory, ReleaseDirectory),
    print("Fresh release copied to ~p", [ReleaseDirectory]),
    configurator:configure_release(ReleaseDirectory, ?app_name, FullName, Cookie, Type, CcmNodesList, DbNodesList, ?dist_app_failover_timeout),
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
    compile:file(filename:join([?releases_directory, "files", "configurator.erl"])).

get_name(Hostname) ->
    [Name, _] = string:tokens(Hostname, "@"),
    Name.

extend_hostname_by_suffix(Hostname, Suffix) ->
    [Name, Host] = string:tokens(Hostname, "@"),
    Name ++ Suffix ++ "@" ++ Host.

get_host(Hostname) ->
    [_, Host] = string:tokens(Hostname, "@"),
    Host.

print(Msg) ->
    print(Msg,[]).
print(Msg, Args) ->
    io:format(Msg ++ "~n",Args),
    Msg.

cleanup() ->
    file:delete("configurator.beam").
