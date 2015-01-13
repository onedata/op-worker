

% Default cookie used for communication with cluster 
-define(default_cookie, oneprovider_node).

% Installation directory of oneprovider RPM
-define(prefix, filename:join([filename:absname("/"), "opt", "oneprovider"])).

% Location of error_dump.txt
-define(error_dump_file, filename:join([?prefix, "error_dump.txt"])).

% Location of configured_nodes.cfg
-define(configured_nodes_path, filename:join([?prefix, "scripts", "configured_nodes.cfg"])).

% Location of erl_launcher
-define(erl_launcher_script_path, filename:join([?prefix, "scripts", "erl_launcher"])).

% Paths relative to oneprovider_node release
-define(config_args_path, filename:join(["bin", "config.args"])).
-define(oneprovider_script_path, filename:join(["bin", "oneprovider"])).
-define(start_command_suffix, filename:join(["bin", "oneprovider_node"]) ++ " start").

%Paths relative to database_node release
-define(db_start_command_suffix, filename:join(["bin", "bigcouch"])).
-define(nohup_output, filename:join(["var", "log", "nohup.out"])).

% System limit values
-define(ulimits_config_path, filename:join([?prefix, "scripts", "ulimits.cfg"])).

% Print error message to ?error_dump_file with formatting and halt
-define(error(Fmt, Args),
    file:write_file(?error_dump_file, io_lib:fwrite("Error: " ++ Fmt ++ "~n", Args), [append]),
    halt(1)).

% Print error message to ?error_dump_file and halt
-define(error(Msg), ?error("~s~n", [Msg])).

% Convinience macro to print to screen (debug etc.)
-define(dump(Term), io:format("~p~n", [Term])).

main(Args) ->
    {{Year, Month, Day}, {Hour, Min, Sec}} = erlang:localtime(),
    ScriptArg = try
        lists:nth(1, Args)
                catch _:_ -> "Unknown argument"
                end,
    file:write_file(?error_dump_file,
        io_lib:fwrite("\n\n==============================\n~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w  [~s]\n\n", [Year, Month, Day, Hour, Min, Sec, ScriptArg]), [append]),
    put(hostname, "@" ++ os:cmd("hostname -f") -- "\n"),
    set_up_net_kernel(),
    get_ulimits_from_config(),
    try
        try lists:nth(1, Args) of
            "start_oneprovider" -> start_oneprovider_nodes();

            "stop_oneprovider" -> stop_oneprovider_nodes();

            "start_db" -> start_db_node();

            "stop_db" -> stop_db_node();

            "status_oneprovider" -> halt(status(oneprovider));

            "status_db" -> halt(status(database));
            Unknown ->
                ?error("Unknown argument: " ++ Unknown),
                halt(1)
        catch _:_ ->
            ?error("Wrong script usage"),
            halt(1)
        end
    catch Type:Message ->
        ?error("The script terminated abnormally~n~p: ~p~nStack trace:~n~p", [Type, Message, erlang:get_stacktrace()]),
        halt(1)
    end.

% Returns (according to http://refspecs.linuxbase.org/LSB_3.1.1/LSB-Core-generic/LSB-Core-generic/iniscrptact.html):
% 0 - program is running or service is OK
% 3 - program is not running
% 4 - program or service status is unknown
status(NodeType) when is_atom(NodeType) ->
    case get_nodes_from_config(NodeType) of
        {none, []} ->
            4;
        {db, Db} ->
            status(Db);
        {worker, Worker} ->
            status(Worker);
        {ccm_plus_worker, {CCM, Worker}} ->
            case {status(CCM), status(Worker)} of
                {0, 0} -> 0;
                _ -> 3
            end
    end;
status({_NodeType, NodeName, _Path}) ->
    LongName = atom_to_list(NodeName) ++ get(hostname),
    case rpc:call(list_to_atom(LongName), init, get_status, []) of
        {started, _} -> 0;
        {starting, _} -> 0;
        _ -> 3
    end.

start_db_node() ->
    case get_nodes_from_config(database) of
        {none, []} ->
            nothing_to_start;

        {db, Db} ->
            start_db(Db)
    end.

stop_db_node() ->
    case get_nodes_from_config(database) of
        {none, []} ->
            nothing_to_stop;

        {db, Db} ->
            stop_db(Db)
    end.

start_db({db_node, _Name, Path}) ->
    BigcouchStartScript = filename:join([Path, ?db_start_command_suffix]),
    NohupOut = filename:join([Path, ?nohup_output]),
    open_port({spawn, "bash -c \"" ++ get(set_ulimits_cmd) ++ " ; " ++ "nohup " ++ BigcouchStartScript ++ " > " ++ NohupOut ++ " 2>&1 &" ++ "\" 2>&1 &"}, [out]).

stop_db({db_node, _Name, Path}) ->
    os:cmd("kill -TERM `ps aux | grep beam | grep " ++ Path ++ " | cut -d'\t' -f2 | awk '{print $2}'`").

start_oneprovider_nodes() ->
    case get_nodes_from_config(oneprovider) of
        {none, []} ->
            nothing_to_start;

        {worker, Worker} ->
            start_worker(Worker);

        {ccm_plus_worker, {CCM, Worker}} ->
            start_ccm_plus_worker(CCM, Worker)
    end.


stop_oneprovider_nodes() ->
    case get_nodes_from_config(oneprovider) of
        {none, []} ->
            nothing_to_stop;

        {worker, Worker} ->
            stop_worker(Worker);

        {ccm_plus_worker, {CCM, Worker}} ->
            stop_ccm_plus_worker(CCM, Worker)
    end.


% Connect to one of CCMs specified in config.args, get ccm and dbnode list from it, recofigure the release and start it.
start_worker({worker_node, Name, Path}) ->
    ConfigArgsPath = filename:join([Path, atom_to_list(Name), ?config_args_path]),
    OldMainCCM = read_config_args(ConfigArgsPath, "main_ccm", false),
    OldOptCCMs = read_config_args(ConfigArgsPath, "opt_ccms", true),
    {[MainCCM | OptCCMs], DBNodes, _WorkerList} = discover_cluster([OldMainCCM | OldOptCCMs]),

    reconfigure_node(Name, Path, MainCCM, OptCCMs, DBNodes),
    os:cmd(get(set_ulimits_cmd) ++ " ; " ++ filename:join([Path, atom_to_list(Name), ?start_command_suffix])).


% Stop a worker running on this machine, right after saving latest configuration
stop_worker({worker_node, Name, Path}) ->
    LongName = atom_to_list(Name) ++ get(hostname),
    {[MainCCM | OptCCMs], DBNodes, _WorkerList} = discover_node(LongName),

    reconfigure_node(Name, Path, MainCCM, OptCCMs, DBNodes),
    rpc:call(list_to_atom(LongName), init, stop, []).


% If this is an only CCM, assume cluster is empty - simply start CCM and worker.
% If not, connect to one of CCMs specified in config.args, get ccm and dbnode list from it, recofigure the releases and start them.
start_ccm_plus_worker({ccm_node, CCMName, CCMPath}, {worker_node, WorkerName, WorkerPath}) ->
    ConfigArgsPath = filename:join([CCMPath, atom_to_list(CCMName), ?config_args_path]),
    OldMainCCM = read_config_args(ConfigArgsPath, "main_ccm", false),
    OldOptCCMs = read_config_args(ConfigArgsPath, "opt_ccms", true),

    LongCCMName = atom_to_list(CCMName) ++ get(hostname),
    case OldMainCCM =:= LongCCMName andalso length(OldOptCCMs) =:= 0 of
        true ->
            os:cmd(get(set_ulimits_cmd) ++ " ; " ++ filename:join([CCMPath, atom_to_list(CCMName), ?start_command_suffix])),
            os:cmd(get(set_ulimits_cmd) ++ " ; " ++ filename:join([WorkerPath, atom_to_list(WorkerName), ?start_command_suffix]));

        false ->
            {[MainCCM | OptCCMs], DBNodes, WorkerList} = discover_cluster([OldMainCCM | OldOptCCMs]),
            NewOptCCMs = [list_to_atom(LongCCMName) | OptCCMs],

            lists:foreach(
                fun(Node) ->
                    reconfigure_remote_worker(Node, MainCCM, NewOptCCMs, DBNodes)
                end, WorkerList),

            lists:foreach(
                fun(Node) ->
                    reconfigure_and_restart_ccm(Node, MainCCM, NewOptCCMs, DBNodes)
                end, [MainCCM | OptCCMs]),


            reconfigure_node(CCMName, CCMPath, MainCCM, NewOptCCMs, DBNodes),
            os:cmd(get(set_ulimits_cmd) ++ " ; " ++ filename:join([CCMPath, atom_to_list(CCMName), ?start_command_suffix])),

            reconfigure_node(WorkerName, WorkerPath, MainCCM, NewOptCCMs, DBNodes),
            os:cmd(get(set_ulimits_cmd) ++ " ; " ++ filename:join([WorkerPath, atom_to_list(WorkerName), ?start_command_suffix]))
    end.


% Reconfigure cluster appropriately and stop ccm and worker
stop_ccm_plus_worker({ccm_node, CCMName, CCMPath}, {worker_node, WorkerName, WorkerPath}) ->
    stop_worker({worker_node, WorkerName, WorkerPath}),

    LongName = atom_to_list(CCMName) ++ get(hostname),
    {[MainCCM | OptCCMs], DBNodes, WorkerList} = discover_node(LongName),

    {NewMainCCM, NewOptCCMs} = case list_to_atom(LongName) of
                                   MainCCM ->
                                       case length(OptCCMs) of
                                           0 ->
                                               {MainCCM, OptCCMs};
                                           _ ->
                                               % Let the first opt ccm be the new main one
                                               [FirstCCM | Rest] = OptCCMs,
                                               {FirstCCM, Rest}
                                       end;
                                   _ ->
                                       {MainCCM, OptCCMs -- [list_to_atom(LongName)]}
                               end,

    lists:foreach(
        fun(Node) ->
            reconfigure_remote_worker(Node, NewMainCCM, NewOptCCMs, DBNodes)
        end, WorkerList),

    reconfigure_node(CCMName, CCMPath, NewMainCCM, NewOptCCMs, DBNodes),
    rpc:call(list_to_atom(LongName), init, stop, []),

    lists:foreach(
        fun(Node) ->
            reconfigure_and_restart_ccm(Node, NewMainCCM, NewOptCCMs, DBNodes)
        end, [NewMainCCM | NewOptCCMs] -- [list_to_atom(LongName)]).


% Change the configuration in config.args
reconfigure_node(Name, Path, MainCCM, OptCCMs, DBNodes) ->
    ConfigArgsPath = filename:join([Path, atom_to_list(Name), ?config_args_path]),
    overwrite_config_args(ConfigArgsPath, "main_ccm", atom_to_list(MainCCM)),
    overwrite_config_args(ConfigArgsPath, "opt_ccms", to_space_delimited_list(OptCCMs)),
    overwrite_config_args(ConfigArgsPath, "db_nodes", to_space_delimited_list(DBNodes)),
    os:cmd(filename:join([Path, atom_to_list(Name), ?oneprovider_script_path])).


% Reconfigure remote worker's ccm and/or db_node list
reconfigure_remote_worker(NodeName, MainCCM, OptCCMs, DBNodes) ->
    MainCCMString = " " ++ atom_to_list(MainCCM),
    % Empty list is a comma so that it is not skipped by bash script
    OptCCMsString = " " ++ lists:foldl(
        fun(Node, Acc) ->
            Acc ++ atom_to_list(Node) ++ ","
        end, ",", OptCCMs),
    DBNodesString = " " ++ lists:foldl(
        fun(Node, Acc) ->
            Acc ++ atom_to_list(Node) ++ ","
        end, ",", DBNodes),

    ReconfCmd = "./bin/node_reconf -reconfigure" ++ MainCCMString ++ OptCCMsString ++ DBNodesString ++ " &",
    rpc:call(NodeName, os, cmd, [ReconfCmd]),
    rpc:call(NodeName, application, set_env, [oneprovider_node, ccm_nodes, [MainCCM | OptCCMs]]),
    rpc:call(NodeName, application, set_env, [oneprovider_node, db_nodes, DBNodes]).


% Order a remote ccm node to change its configuration and restart itself
reconfigure_and_restart_ccm(NodeName, MainCCM, OptCCMs, DBNodes) ->
    MainCCMString = " " ++ atom_to_list(MainCCM),
    % Empty list is a comma so that it is not skipped by bash script
    OptCCMsString = " " ++ lists:foldl(
        fun(Node, Acc) ->
            Acc ++ atom_to_list(Node) ++ ","
        end, ",", OptCCMs),
    DBNodesString = " " ++ lists:foldl(
        fun(Node, Acc) ->
            Acc ++ atom_to_list(Node) ++ ","
        end, ",", DBNodes),

    % Get remote node's PID. If its responsive, order it to stop and restart
    case list_to_integer(rpc:call(NodeName, os, getpid, [])) of
        VmPid when is_integer(VmPid) ->
            ReconfCmd = "./bin/node_reconf -restart_ccm " ++ integer_to_list(VmPid) ++ MainCCMString ++ OptCCMsString ++ DBNodesString ++ " &",
            rpc:call(NodeName, os, cmd, [ReconfCmd]),
            rpc:call(NodeName, init, stop, []);

        _ -> skip
    end.


get_ulimits_from_config() ->
    case file:consult(?ulimits_config_path) of
        {ok, [{open_files, OpenFiles}, {process_limit, Processes}]} ->
            put(set_ulimits_cmd, "ulimit -n " ++ OpenFiles ++ " ; ulimit -u " ++ Processes);
        {ok, Terms} ->
            ?error("Wrong format of ~p file: ~p", [?ulimits_config_path, Terms]),
            halt(1);
        Err ->
            ?error("Cannot parse file ~p, error: ~p", [?ulimits_config_path, Err]),
            halt(1)
    end.

% Ensure EPMD is running and set up net kernel
set_up_net_kernel() ->
    os:cmd(?erl_launcher_script_path ++ " epmd"),
    {A, B, C} = erlang:now(),
    NodeName = "setup_node_" ++ integer_to_list(A, 32) ++ integer_to_list(B, 32) ++ integer_to_list(C, 32) ++ "@127.0.0.1",
    net_kernel:start([list_to_atom(NodeName), longnames]),
    erlang:set_cookie(node(), ?default_cookie).


% Try to connect to any CCM from config
discover_cluster(NodeList) ->
    Result = lists:foldl(
        fun(Node, no_connection) -> discover_node(Node);
            (_Node, Acc) -> Acc
        end, no_connection, NodeList),
    case Result of
        no_connection -> ?error("Could not connect to the cluster.");
        {CCMNodes, DBNodes, WorkerList} -> {CCMNodes, DBNodes, WorkerList}
    end.

% Try to connect to a node and retrieve CCM and DB nodes
discover_node(Node) ->
    try
        {ok, CCMNodes} = rpc:call(list_to_atom(Node), application, get_env, [oneprovider_node, ccm_nodes]),
        {ok, DBNodes} = rpc:call(list_to_atom(Node), application, get_env, [oneprovider_node, db_nodes]),
        WorkerList = rpc:call(list_to_atom(Node), gen_server, call, [{global, central_cluster_manager}, get_nodes]),

        {CCMNodes, DBNodes, WorkerList}

    catch _:_ ->
        no_connection
    end.


% Read contigured_nodes.cfg
% WhichCluster = oneprovider | database
get_nodes_from_config(WhichCluster) ->
    try
        {ok, Entries} = file:consult(?configured_nodes_path),
        case WhichCluster of
            database -> get_database_node_from_config(Entries);
            oneprovider -> get_oneprovider_nodes_from_config(Entries)
        end
    catch _:_ ->
        ?error("Error while reading ~s", [?configured_nodes_path])
    end.


% Do not use directly
get_database_node_from_config(Entries) ->
    case lists:keyfind(db_node, 1, Entries) of
        false -> {none, []};
        Node -> {db, Node}
    end.


% Do not use directly
get_oneprovider_nodes_from_config(Entries) ->
    case lists:keyfind(worker_node, 1, Entries) of
        false -> {none, []};
        Worker ->
            case lists:keyfind(ccm_node, 1, Entries) of
                false -> {worker, Worker};
                CCM -> {ccm_plus_worker, {CCM, Worker}}
            end
    end.


% Read config.args file for value of a specific parameter
read_config_args(Path, Parameter, ExpectingList) ->
    FileContent = case file:read_file(Path) of
                      {ok, DataRead} ->
                          binary_to_list(DataRead);
                      _ ->
                          ?error("Could not read config.args file")
                  end,

    {match, [{From, Through}]} = re:run(FileContent, Parameter ++ ":.*\n"),
    Result = string:substr(FileContent, From + length(Parameter) + 3, Through - length(Parameter) - 3),
    case ExpectingList of
        false -> Result;
        true -> string:tokens(Result, " ")
    end.


% Overwrite a parameter in config.args
overwrite_config_args(Path, Parameter, NewValue) ->
    FileContent = case file:read_file(Path) of
                      {ok, DataRead} ->
                          binary_to_list(DataRead);
                      _ ->
                          ?error("Could not read config.args file")
                  end,

    {match, [{From, Through}]} = re:run(FileContent, Parameter ++ ":.*\n"),
    Beginning = string:substr(FileContent, 1, From),
    End = string:substr(FileContent, From + Through, length(FileContent) - From - Through + 1),
    file:write_file(Path, list_to_binary(Beginning ++ Parameter ++ ": " ++ NewValue ++ End)).


% List to space-delimited-string list
to_space_delimited_list(List) ->
    lists:foldl(
        fun(Element, Acc) ->
            Acc ++ atom_to_list(Element) ++ " "
        end, [], List).