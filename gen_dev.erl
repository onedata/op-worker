#!/usr/bin/env escript

-module(gen_dev).

-define(args_file, atom_to_list(?MODULE) ++ ".args").
-define(releases_directory, "rel").
-define(app_name, "oneprovider_node").
-define(fresh_release_directory, filename:join(?releases_directory, ?app_name)).
-define(test_releases_directory, filename:join(?releases_directory, "test_cluster")).
-define(worker_name_suffix, "_worker").

main(_) ->
    try
        {ok, [Args]} = file:consult(?args_file),
        NodesConfig = expand_full_list_of_nodes(Args),
        file:make_dir(?test_releases_directory),
        create_releases(NodesConfig)
    catch
        _Type:Error ->
            print("Error: ~p",[Error]),
            print("Stacktrace: ~p",[erlang:get_stacktrace()])
    end.

create_releases([]) ->
    ok;
create_releases([Config | Rest]) ->
    % prepare configuration
    print("=================================="),
    print("Configuring new release"),
    Name = proplists:get_value(name, Config),
    print("name - ~p", [Name]),
    Type = proplists:get_value(type, Config),
    print("type - ~p", [Type]),
    CcmNodesList = proplists:get_value(ccm_nodes, Config),
    print("ccm_nodes - ~p", [CcmNodesList]),
    DbNodesList = proplists:get_value(db_nodes, Config),
    print("db_nodes - ~p", [DbNodesList]),
    ReleaseDirectory = get_release_location(Name),
    print("release_dir - ~p", [ReleaseDirectory]),

    remove_dir(ReleaseDirectory),
    copy_dir(?fresh_release_directory, ReleaseDirectory),
    print("Fresh release copied to ~p", [ReleaseDirectory]),
    %todo invoke configurator
    print("Release configured sucessfully!"),
    print("==================================~n"),
    create_releases(Rest).

expand_full_list_of_nodes([]) ->
    [];
expand_full_list_of_nodes([Config | Rest]) ->
    case proplists:get_value(type, Config) of
        ccm_and_worker ->
            %prepare ccm config
            CcmConfig = [{node_type, ccm} | proplists:delete(node_type, Config)],

            %prepare worker config
            CcmName = proplists:get_value(name, Config),
            WorkerName = extend_hostname_by_suffix(CcmName, ?worker_name_suffix),
            WorkerConfig = [{node_type, worker}, {name, WorkerName} | proplists:delete(name, proplists:delete(node_type, Config))],

            [CcmConfig, WorkerConfig | expand_full_list_of_nodes(Rest)];
        _ ->
            [Config | expand_full_list_of_nodes(Rest)]
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

get_release_location(Hostname) ->
    [Name, _] = string:tokens(Hostname, "@"),
    filename:join(?test_releases_directory, Name).

extend_hostname_by_suffix(Hostname, Suffix) ->
    [Name, Host] = string:tokens(Hostname, "@"),
    Name ++ Suffix ++ "@" ++ Host.

print(Msg) ->
    print(Msg,[]).
print(Msg, Args) ->
    io:format(Msg ++ "~n",Args),
    Msg.