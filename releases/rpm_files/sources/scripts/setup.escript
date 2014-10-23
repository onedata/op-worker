% Default cookie used for communication with cluster
-define(default_cookie, oneprovider_node).

% Default bigcouch port
-define(default_port, "5986").

% Default storage paths
-define(default_main_directio_storage, filename:join([filename:absname("/"), "mnt", "vfs"])).
-define(default_group_name_prefix, "grp").
-define(default_group_storage_prefix, filename:join([filename:absname("/"), "mnt", ?default_group_name_prefix])).

%Ports that needs to be free
-define(ports_to_check, [53, 80, 443, 5555, 8443]).

% Curl options
-define(curl_opts, "-u admin:password --connect-timeout 5 -s").

% Installation directory of oneprovider RPM
-define(prefix, filename:join([filename:absname("/"), "opt", "oneprovider"])).

% Location of configured_nodes.cfg
-define(configured_nodes_path, filename:join([?prefix, "scripts", "configured_nodes.cfg"])).

% System limit values
-define(ulimits_config_path, filename:join([?prefix, "scripts", "ulimits.cfg"])).
-define(default_open_files, "65535").
-define(default_processes, "65535").

% Location of init.d script
-define(init_d_script_path, filename:join([filename:absname("/"), "etc", "init.d", "oneprovider"])).

% Location of release packages
-define(oneprovider_release, filename:join([?prefix, "files", "oneprovider_node"])).
-define(db_release, filename:join([?prefix, "files", "database_node"])).

% Location of erl_launcher
-define(erl_launcher_script_path, filename:join([?prefix, "scripts", "erl_launcher"])).

% Install path for nodes
-define(default_nodes_install_path, filename:join([?prefix, "nodes"])).
-define(default_bigcouch_install_path, filename:join([filename:absname("/"), "opt", "bigcouch"])). %should not be changed, unless you've configured bigcouch realease properly (the one from files/database_node)
-define(default_ccm_name, "ccm").
-define(default_worker_name, "worker").
-define(default_db_name, "db").

% Paths relative to oneprovider_node release
-define(config_args_path, filename:join(["bin", "config.args"])).
-define(oneprovider_script_path, filename:join(["bin", "oneprovider"])).
-define(storage_config_path, filename:join(["bin", "storage_info.cfg"])).

% Print error message with formatting and finish
-define(error(Fmt, Args),
    io:format("Error: " ++ Fmt ++ "~n", Args),
    halt(1)).

% Print error message and finish
-define(error(Msg), ?error("~s~n", [Msg])).

% Convinience macro to print to screen (debug etc.)
-define(dump(Term), io:format("~p~n", [Term])).

main(Args) ->
    case Args of
        ["-batch", BatchFile] -> put(batch_file, BatchFile);
        _ -> skip
    end,
    put(hostname, "@" ++ os:cmd("hostname -f") -- "\n"),
    set_up_net_kernel(),

    try
        setup_start()
    catch Type:Message ->
        ?error("The script terminated abnormally~n~p: ~p~nStack trace:~n~p", [Type, Message, erlang:get_stacktrace()])
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Functions defining the installation process 
% Each function represent one step in installation process

setup_start() ->
    h1("oneprovider SETUP"),
    info("Erlang nodes configured on this machine will use its hostname: " ++ get(hostname)),
    warn("Make sure it is resolvable by other hosts in the network (i. e. by adding adequate mapping to /etc/hosts)"),
    set_ulimits(),
    Option = interaction_choose_option(what_to_do, "What do you want to do?",
        [
            {manage_db, "Manage database nodes"},
            {manage_oneprovider, "Manage oneprovider nodes"},
            {exit, "Exit"}
        ]),
    case Option of
        manage_db -> setup_manage_db();
        manage_oneprovider -> setup_manage_oneprovider();
        exit -> halt(1)
    end.

% Set system limits
set_ulimits() ->
    case file:consult(?ulimits_config_path) of
        {ok, []} ->
            reset_ulimits();
        {ok, _} ->
            already_defined;
        {error, enoent} ->
            reset_ulimits();
        Err ->
            ?error("Cannot parse file ~p, error: ~p", [?ulimits_config_path, Err])
    end.

% Resets system limits
reset_ulimits() ->
    info("Set system limits for new nodes:"),
    OpenFiles = interaction_get_string(open_files_limit, "Open files: ", ?default_open_files),
    Processes = interaction_get_string(process_limit, "Processes: ", ?default_processes),
    file:write_file(?ulimits_config_path, io_lib:fwrite("~p.\n~p.\n", [{open_files, OpenFiles}, {process_limit, Processes}]), [append]).

% Manage oneprovider nodes
setup_manage_oneprovider() ->
    info("Each machine can only host a single worker or a ccm + worker pair."),

    OptionList = case get_nodes_from_config(oneprovider) of
                     {none, []} ->
                         [
                             {new_cluster, "Set up a new cluster"},
                             {extend_cluster, "Extend existing cluster"}
                         ];
                     _ ->
                         [{remove_nodes, "Remove node(s) configured on this machine"}]
                 end,

    Option = interaction_choose_option(what_to_do_oneprovider, "What do you want to do?", OptionList ++ [{go_back, "Go back"}]),
    case Option of
        new_cluster -> setup_confirm_new_cluster();
        extend_cluster -> setup_extend_cluster();
        remove_nodes -> setup_remove_oneprovider_nodes();
        go_back -> setup_start()
    end.


% Executed before the proper installation to inform about installation conditions
setup_confirm_new_cluster() ->
    check_open_ports(?ports_to_check),
    info("Installing a new cluster beside a running one may cause unpredictable behaviour."),
    info("It is required that all database nodes are installed prior to the cluster."),
    Option = interaction_choose_option(db_nodes_installed, "Do you wish to continue?",
        [
            {yes, "Yes"},
            {no, "No"}
        ]),
    case Option of
        no -> setup_start();
        yes -> setup_get_db_nodes()
    end.


% Ask the user for db nodes configured in the network and check the connection
setup_get_db_nodes() ->
    h2("List ALL running database nodes, delimiting them with commas (no spaces) [eg. db1@host.net,db2@host2.net,...]"),
    h2("The cluster will use ONLY the nodes specified now."),
    Input = interaction_get_string(define_db_nodes, "Running DB nodes: "),
    DBNodes = string:tokens(Input, ","),
    ConnectionTest = lists:foldl(
        fun(DBNode, Result) ->
            case rpc:call(list_to_atom(DBNode), lists, min, [[3, 2, 1]]) of
                1 -> Result;
                _ -> DBNode
            end
        end, ok, DBNodes),

    case ConnectionTest of
        ok ->
            h2("Connection to following database nodes has been confirmed:"),
            DBNodesAtoms = lists:map(
                fun(Node) ->
                    li(Node),
                    list_to_atom(Node)
                end, DBNodes),
            put(db_nodes, DBNodesAtoms),
            ConfiguredStorage = setup_create_storage(),
            setup_new_ccm_plus_worker(true, ConfiguredStorage);
        BadNode ->
            warn("Could not establish connection with " ++ BadNode),
            %do not ask again in batch mode
            case get(batch_file) of
                undefined ->
                    setup_get_db_nodes();
                _ ->
                    halt(1)
            end
    end,
    ok.

setup_create_storage() ->
    h2("Storage setup"),
    OneproviderRoot = interaction_get_string(oneprovider_storage_root, "Select path where oneprovider can store its files", ?default_main_directio_storage),
    OneproviderGroup = [{name, cluster_fuse_id}, {root, OneproviderRoot}],
    warn("IMPORTANT"),
    warn("Configuring direct storage (much faster than default proxy storage) for fuse client groups"),
    warn("If you don't create any storage now, all the data will go throught proxy\n and it will work really slow!"),
    UserDefinedGroups = get_fuse_groups_from_user([], 1),
    AllGroups = [OneproviderGroup | UserDefinedGroups],

    CreateDir = fun([{name, _}, {root, Root}]) -> os:cmd("mkdir -p " ++ Root) end,
    lists:foreach(CreateDir, AllGroups),
    AllGroups.


% Gets storage groups from user (I is used to determine different groups during -batch installation)
get_fuse_groups_from_user(CurrentGroups, I) ->
    %interacion IDs used by batch file
    ConfirmInteractionId = list_to_atom(atom_to_list(want_to_create_storage) ++ integer_to_list(I)),
    GroupNameInteractionId = list_to_atom(atom_to_list(storage_group_name) ++ integer_to_list(I)),
    GroupRootInteractionId = list_to_atom(atom_to_list(storage_group_directory) ++ integer_to_list(I)),
    DefaultGroupName = ?default_group_name_prefix ++ integer_to_list(I),
    DefaultGroupPath = ?default_group_storage_prefix ++ integer_to_list(I),
    CreateStorageQuestion = case I > 1 of
                                false ->
                                    "Do you wish to create new storage dedicated for fuse client group?";
                                true ->
                                    "Do you wish to create another storage dedicated for fuse client group?"
                            end,
    WantToCrate = interaction_choose_option(ConfirmInteractionId, CreateStorageQuestion,
        [
            {yes, "Yes"},
            {no, "No"}
        ]),
    case WantToCrate of
        yes ->
            h2("Type following attributes:"),
            Name = interaction_get_string(GroupNameInteractionId, "Fuse clients group name", DefaultGroupName),
            Root = interaction_get_string(GroupRootInteractionId, "DirectIO storage mount point", DefaultGroupPath),
            NewGroup = [{name, Name}, {root, Root}],
            get_fuse_groups_from_user(lists:append(CurrentGroups, [NewGroup]), I + 1);
        no ->
            AllGroupsString = case CurrentGroups =:= [] of
                                  true ->
                                      "!!! You should create at least one storage to achieve better performance !!!";
                                  false ->
                                      groups_to_string(CurrentGroups)
                              end,
            Option = interaction_choose_option(accept_created_storage,
                "Is this all?\n" ++ AllGroupsString,
                [
                    {yes, "Yes, continue instalation"},
                    {add_another, "Add another"},
                    {reconfigure, "Delete all and configure them again"}
                ]),
            case Option of
                yes ->
                    CurrentGroups;
                add_another ->
                    get_fuse_groups_from_user(CurrentGroups, I + 1);
                reconfigure ->
                    get_fuse_groups_from_user([], 1)
            end
    end.

groups_to_string([]) ->
    "";
groups_to_string([FirstGroup | Rest]) ->
    group_to_string(FirstGroup) ++ "\n" ++ groups_to_string(Rest).

group_to_string(Group) ->
    [{name, Name}, {root, Root}] = Group,
    "==> group_name: " ++ Name ++ ", root: " ++ Root.


% Install ccm along with a worker
setup_new_ccm_plus_worker(IsThisMainCCM, FuseGroups) ->
    CCMName = ?default_ccm_name,
    CCMPath = ?default_nodes_install_path,

    WorkerName = ?default_worker_name,
    WorkerPath = ?default_nodes_install_path,

    h2("Following nodes will be installed:"),
    li(CCMName ++ get(hostname)),
    li(WorkerName ++ get(hostname)),

    Option = interaction_choose_option(settings_ok, "Confirm:",
        [
            {ok, "Continue"},
            {back, "Go back"}
        ]),
    case Option of
        back ->
            setup_manage_oneprovider();
        ok ->
            LongName = CCMName ++ get(hostname),
            case IsThisMainCCM of
                true ->
                    put(main_ccm, list_to_atom(LongName)),
                    put(opt_ccms, []);
                false ->
                    put(opt_ccms, get(opt_ccms) ++ [list_to_atom(LongName)])
            end,
            install_oneprovider_node(ccm_node, CCMName, CCMPath),
            install_oneprovider_node(worker_node, WorkerName, WorkerPath),
            case IsThisMainCCM of
                true ->
                    save_storage_in_config(FuseGroups);
                false ->
                    ok_storage_defined_in_main
            end,
            info("Starting node(s)..."),
            os:cmd(?init_d_script_path ++ " start_oneprovider 1>/dev/null")
    end.


% Firstly check connection to an existing cluster, then ask to choose installation variant
setup_extend_cluster() ->
    check_open_ports(?ports_to_check),
    IPOrHostname = interaction_get_string(ip_or_hostname, "Specify IP/hostname of any host with running oneprovider node(s): "),
    case discover_cluster(IPOrHostname) of
        no_connection ->
            warn("No node at [" ++ IPOrHostname ++ "] is available. Make sure there is a viable connection between hosts."),
            %do not ask again in batch mode
            case get(batch_file) of
                undefined ->
                    WantRetry = interaction_choose_option(retry, "Do you want to try again?",
                        [
                            {yes, "Yes"},
                            {no, "No"}
                        ]),
                    case WantRetry of
                        yes -> setup_extend_cluster();
                        no -> setup_manage_oneprovider()
                    end;
                _ ->
                    halt(1)
            end;

        {[MainCCM | OptCCMS], DBNodes} ->
            put(main_ccm, MainCCM),
            put(opt_ccms, OptCCMS),
            put(db_nodes, DBNodes),
            info("Connection OK"),
            Option = interaction_choose_option(new_node_type, "What kind of configuration would you like to add?",
                [
                    {ccm_plus_worker, "Backup CCM node + worker node"},
                    {worker, "Worker node"}
                ]),
            case Option of
                ccm_plus_worker -> setup_new_ccm_plus_worker(false, []);
                worker -> setup_new_worker()
            end
    end.


% Install a single worker
setup_new_worker() ->
    WorkerName = ?default_worker_name,
    WorkerPath = ?default_nodes_install_path,

    h2("Following node will be installed:"),
    li(WorkerName ++ get(hostname)),

    Option = interaction_choose_option(settings_ok, "Confirm:",
        [
            {ok, "Continue"},
            {back, "Go back"}
        ]),
    case Option of
        back ->
            setup_manage_oneprovider();
        ok ->
            install_oneprovider_node(worker_node, WorkerName, WorkerPath),
            info("Starting node(s)..."),
            os:cmd(?init_d_script_path ++ " start_oneprovider 1>/dev/null")
    end.


% Install a generic oneprovider node
install_oneprovider_node(Type, Name, Path) ->
    LongName = Name ++ get(hostname),
    OneproviderNodePath = filename:join([Path, Name]),
    info("Installing " ++ LongName ++ "..."),
    os:cmd("mkdir -p " ++ OneproviderNodePath),
    os:cmd("cp -R " ++ filename:join([?oneprovider_release, "* "]) ++ OneproviderNodePath),

    MainCCM = get(main_ccm),
    OptCCMs = get(opt_ccms),
    DBNodes = get(db_nodes),
    StorageConfigPath = filename:join([Path, Name, ?storage_config_path]),
    ConfigArgsPath = filename:join([Path, Name, ?config_args_path]),

    overwrite_config_args(ConfigArgsPath, "name", LongName),
    overwrite_config_args(ConfigArgsPath, "main_ccm", atom_to_list(MainCCM)),
    overwrite_config_args(ConfigArgsPath, "opt_ccms", to_space_delimited_list(OptCCMs)),
    overwrite_config_args(ConfigArgsPath, "db_nodes", to_space_delimited_list(DBNodes)),
    overwrite_config_args(ConfigArgsPath, "storage_config_path", StorageConfigPath),

    os:cmd(filename:join([Path, Name, ?oneprovider_script_path])),
    add_node_to_config(Type, list_to_atom(Name), Path).


% List to space-delimited-string list
to_space_delimited_list(List) ->
    lists:foldl(
        fun(Element, Acc) ->
            Acc ++ atom_to_list(Element) ++ " "
        end, [], List).


% List currently installed nodes and ask for confirmation
setup_remove_oneprovider_nodes() ->
    case get_nodes_from_config(oneprovider) of
        {none, []} ->
            warn("There are no nodes configured on this machine"),
            setup_manage_oneprovider();
        {worker, {worker_node, WorkerName, _}} ->
            info("Nodes currently configured on this machine:"),
            li(atom_to_list(WorkerName) ++ get(hostname));
        {ccm_plus_worker, {{ccm_node, CCMName, _}, {worker_node, WorkerName, _}}} ->
            info("Currently configured on this machine:"),
            li(atom_to_list(CCMName) ++ get(hostname)),
            li(atom_to_list(WorkerName) ++ get(hostname))
    end,

    Option = interaction_choose_option(confirm_oneprovider_nodes_deletion, "Do you wish to remove current configuration?",
        [
            {yes, "Yes"},
            {no, "No"}
        ]),
    case Option of
        no -> setup_start();
        yes -> do_remove_oneprovider_nodes()
    end.


% Stop node(s) and remove them from the machine
do_remove_oneprovider_nodes() ->
    info("Stopping node(s)..."),
    os:cmd(?init_d_script_path ++ " stop_oneprovider"),
    case get_nodes_from_config(oneprovider) of
        {worker, {worker_node, Name, Path}} ->
            info("Removing " ++ atom_to_list(Name) ++ get(hostname)),
            os:cmd("rm -rf " ++ filename:join([Path, atom_to_list(Name)])),
            remove_node_from_config(Name);
        {ccm_plus_worker, {{ccm_node, CCMName, CCMPath}, {worker_node, WorkerName, WorkerPath}}} ->
            info("Removing " ++ atom_to_list(CCMName)),
            os:cmd("rm -rf " ++ filename:join([CCMPath, atom_to_list(CCMName)])),
            remove_node_from_config(CCMName),
            info("Removing " ++ atom_to_list(WorkerName)),
            os:cmd("rm -rf " ++ filename:join([WorkerPath, atom_to_list(WorkerName)])),
            remove_node_from_config(WorkerName)
    end.


setup_manage_db() ->
    OptionList = case get_nodes_from_config(database) of
                     {none, []} ->
                         [
                             {new_cluster, "Set up a new db custer"},
                             {extend_cluster, "Extend existing db cluster"},
                             {go_back, "Go back"}
                         ];
                     _ ->
                         [
                             {remove_node, "Remove node configured on this machine"},
                             {go_back, "Go back"}
                         ]
                 end,
    Option = interaction_choose_option(what_to_do_db, "What do you want to do?", OptionList),
    case Option of
        new_cluster -> setup_install_db();
        extend_cluster -> setup_extend_db();
        remove_node -> setup_remove_db();
        go_back -> setup_start()
    end.

setup_install_db() ->
    DbName = ?default_db_name,
    DbPath = ?default_bigcouch_install_path,
    h2("Following node will be installed:"),
    li(DbName ++ get(hostname)),
    Option = interaction_choose_option(settings_ok_db, "Confirm:",
        [
            {ok, "Continue"},
            {back, "Go back"}
        ]),
    case Option of
        back ->
            setup_manage_db();
        ok ->
            install_db_node(DbName, DbPath)
    end.

setup_extend_db() ->
    DbName = ?default_db_name,
    DbPath = ?default_bigcouch_install_path,
    %connect with some db node
    h2("Specify IP/hostname of any host with running database node:"),
    OtherNode = interaction_get_string(define_node_to_extend, "Running DB node: "),
    AllDocsAddress = "http://" ++ OtherNode ++ ":" ++ ?default_port ++ "/nodes/_all_docs",
    ConnectionTestResult = os:cmd("curl " ++ ?curl_opts ++ " -X GET " ++ AllDocsAddress),
    case ConnectionTestResult of
        "" ->
            warn("Could not establish connection with " ++ OtherNode),
            %do not ask again in batch mode
            case get(batch_file) of
                undefined ->
                    setup_extend_db();
                _ ->
                    halt(1)
            end;
        _ ->
            h2("Connection has been confirmed"),
            %install
            h2("Following node will be installed:"),
            li(DbName ++ get(hostname)),
            Option = interaction_choose_option(settings_ok_extend_db, "Confirm:",
                [
                    {ok, "Continue"},
                    {back, "Go back"}
                ]),
            case Option of
                back ->
                    setup_manage_db();
                ok ->
                    install_db_node(DbName, DbPath),
                    add_db_to_cluster(DbName, OtherNode)
            end
    end.

% Install and start db release
install_db_node(Name, Path) ->
    LongName = Name ++ get(hostname),

    info("Installing " ++ LongName ++ "..."),
    os:cmd("mkdir -p " ++ Path),
    os:cmd("cp -R " ++ filename:join([?db_release, "* "]) ++ Path),
    add_node_to_config(db_node, list_to_atom(Name), Path),
    info("installation complete"),

    %prepare db arguments
    set_db_cookie(Path, atom_to_list(?default_cookie)),
    actualize_db_hostname(Path, Name),

    info("Starting node..."),
    open_port({spawn, ?init_d_script_path ++ " start_db 1>/dev/null"}, [out]).

% add configured db node to existing cluster
add_db_to_cluster(DbName, OtherNodeHost) ->
    LocalNodeAddress = "http://" ++ OtherNodeHost ++ ":" ++ ?default_port ++ "/nodes/" ++ DbName ++ get(hostname),
    PutRequestResult = os:cmd("curl " ++ ?curl_opts ++ " -X PUT " ++ LocalNodeAddress ++ " -d '{}'"),
    case string:str(PutRequestResult, "\"ok\":true") of
        0 ->
            warn("Error, could not add node to cluster:"),
            warn(PutRequestResult);
        _ ->
            info("Sucessfully added node to cluster")
    end.

% stop and remove db node
setup_remove_db() ->
    case get_nodes_from_config(database) of
        {db_node, {db_node, Name, Path}} ->
            h2("Following node will be removed:"),
            li(atom_to_list(Name) ++ get(hostname)),
            h2(" (!) Remember that if you remove this node you will lost all data stored on it."),
            Option = interaction_choose_option(settings_ok_remove_db, "Confirm:",
                [
                    {ok, "Continue"},
                    {back, "Go back"}
                ]),
            case Option of
                ok ->
                    info("Deleting from cluster"),
                    remove_db_from_cluster(Name),
                    info("Stopping db..."),
                    os:cmd(?init_d_script_path ++ " stop_db"),
                    info("Removing " ++ atom_to_list(Name) ++ get(hostname)),
                    info("Deleting from disc"),
                    os:cmd("rm -rf " ++ Path),
                    info("Deleting from config"),
                    remove_node_from_config(Name);
                back ->
                    setup_manage_db()
            end;
        _ ->
            warn("Nothing to delete")
    end.

remove_db_from_cluster(Name) ->
    LocalHost = string:sub_string(get(hostname), 2),
    LongName = atom_to_list(Name) ++ get(hostname),
    LocalNodeAddress = "http://" ++ LocalHost ++ ":" ++ ?default_port ++ "/nodes/" ++ LongName,
    REV = os:cmd("curl  " ++ ?curl_opts ++ " -X GET " ++ LocalNodeAddress ++ " | grep -o '[0-9]*-[0-9a-f]*'"),
    case REV of
        "" ->
            warn("Already deleted!");
        _ ->
            LocalNodeAddressWithRev = LocalNodeAddress ++ "?rev=" ++ REV,
            Result = os:cmd("curl " ++ ?curl_opts ++ " -X DELETE " ++ LocalNodeAddressWithRev),
            case string:str(Result, "\"ok\":true") of
                0 ->
                    warn("Error, could not delete node from cluster:"),
                    warn(Result);
                _ ->
                    info("Sucessfully deleted node from cluster")
            end
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Functions used to establish connection with the cluster
%

% Ensure EPMD is running and set up net kernel
set_up_net_kernel() ->
    os:cmd(?erl_launcher_script_path ++ " epmd"),
    {A, B, C} = erlang:now(),
    NodeName = "setup_node_" ++ integer_to_list(A, 32) ++ integer_to_list(B, 32) ++ integer_to_list(C, 32) ++ "@127.0.0.1",
    net_kernel:start([list_to_atom(NodeName), longnames]),
    erlang:set_cookie(node(), ?default_cookie).


% Try to connect to the cluster (firstly workers, then ccms) and retrieve CCM and DB nodes
discover_cluster(IPOrHostname) ->
    try
        {ok, CCMNodes} = rpc:call(list_to_atom(?default_worker_name ++ "@" ++ IPOrHostname), application, get_env, [oneprovider_node, ccm_nodes]),
        {ok, DBNodes} = rpc:call(list_to_atom(?default_worker_name ++ "@" ++ IPOrHostname), application, get_env, [oneprovider_node, db_nodes]),
        {CCMNodes, DBNodes}
    catch _:_ ->
        try
            {ok, CCMNodes2} = rpc:call(list_to_atom(?default_ccm_name ++ "@" ++ IPOrHostname), application, get_env, [oneprovider_node, ccm_nodes]),
            {ok, DBNodes2} = rpc:call(list_to_atom(?default_ccm_name ++ "@" ++ IPOrHostname), application, get_env, [oneprovider_node, db_nodes]),
            {CCMNodes2, DBNodes2}
        catch _:_ ->
            no_connection
        end
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Functions used to manipulate bigcouch etc files

% Set cookie in etc/vm.args from given bigcouch installation path
set_db_cookie(BigcouchInstallationPath, Cookie) ->
    os:cmd("sed -i -e \"s/^\\-setcookie .*/\\-setcookie " ++ Cookie ++ "/g\" " ++ BigcouchInstallationPath ++ "/etc/vm.args").

% Set hostname in etc/vm.args for given bigcouch installation path
actualize_db_hostname(BigcouchInstallationPath, NodeName) ->
    os:cmd("sed -i -e \"s/^\\-name .*/\\-name " ++ NodeName ++ get(hostname) ++ "/g\" " ++ BigcouchInstallationPath ++ "/etc/vm.args").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Function used to process config file used by oneprovider script
%

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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Functions used to process config file containing currently configured nodes 


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
        Node -> {db_node, Node}
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


% Add a node to configured_nodes.cfg
add_node_to_config(Type, Name, Path) ->
    try
        {ok, Entries} = file:consult(?configured_nodes_path),
        save_nodes_in_config(Entries ++ [{Type, Name, Path}])
    catch _:_ ->
        ?error("Error while adding ~p to ~s", [Name, ?configured_nodes_path])
    end.


% Remove a node from configured_nodes.cfg
remove_node_from_config(Name) ->
    try
        {ok, Entries} = file:consult(?configured_nodes_path),
        ToDelete = case lists:keyfind(Name, 2, Entries) of
                       false -> ?error("Node ~p not found among configured nodes.", [Name]);
                       Term -> Term
                   end,

        save_nodes_in_config(Entries -- [ToDelete])
    catch _:_ ->
        ?error("Error while deleting ~p from ~s", [Name, ?configured_nodes_path])
    end.


% Save list of nodes in configured_nodes.cfg
save_nodes_in_config(NodeList) ->
    try
        file:write_file(?configured_nodes_path, ""),
        lists:foreach(
            fun(Node) ->
                file:write_file(?configured_nodes_path, io_lib:fwrite("~p.\n", [Node]), [append])
            end, NodeList)
    catch _:_ ->
        ?error("Error while writing to ~s", [?configured_nodes_path])
    end.

save_storage_in_config(Storage) ->
    StorageFilePath = filename:join([?default_nodes_install_path, ?default_worker_name, ?storage_config_path]),
    file:write_file(StorageFilePath, io_lib:fwrite("~p.\n", [Storage])).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Functions used to interact with user or read answers from batch file
%

% Consult the batch file specified in args (if any), looking for {Id, Value} entry
read_from_batch_file(Id) ->
    read_from_batch_file(Id, none).
read_from_batch_file(Id, Default) ->
    case get(batch_file) of
        undefined ->
            no_batch_mode;
        File ->
            try
                {ok, AnswerProplist} = file:consult(File),
                {Id, Value} = proplists:lookup(Id, AnswerProplist),
                {ok, Value}
            catch _:_ ->
                case Default of
                    none ->
                        ?error("Batch file error - could not find entry for '~p'", [Id]);
                    _ ->
                        warn("No entry for ~p, using default value: ~p", [Id, Default]),
                        {ok, Default}
                end
            end
    end.


% Prompt to choose one of the options or retrieve it from batch file
interaction_choose_option(Id, Prompt, OptionList) ->
    case read_from_batch_file(Id) of
        {ok, Answer} ->
            case proplists:lookup(Answer, OptionList) of
                {Answer, _} -> Answer;
                _ -> ?error("Batch file error - invalid entry for '~p'", [Id])
            end;

        _ ->
            h2(Prompt),
            OptionCount = lists:foldl(
                fun({_Name, Text}, Counter) ->
                    io:format(" [~p] ~s~n", [Counter, Text]),
                    Counter + 1
                end, 1, OptionList),
            Choice = try_reading_integer("Your choice: ", 1, OptionCount - 1),
            {Name, _Text} = lists:nth(Choice, OptionList),
            Name
    end.


% Will keep asking for integer unless its min <= answer <= max
try_reading_integer(Prompt, Min, Max) ->
    try
        {ok, [Result]} = io:fread("> " ++ Prompt, "~d"),
        true = is_integer(Result) andalso (Result >= Min) andalso (Result =< Max),
        Result
    catch _:_ ->
        io:format("~nEh?~n"),
        try_reading_integer(Prompt, Min, Max)
    end.


% Get a string from the console or retrieve it from batch file
interaction_get_string(Id, Prompt) ->
    case read_from_batch_file(Id) of
        {ok, Answer} ->
            Answer;
        no_batch_mode ->
            read_from_console(Prompt)
    end.
interaction_get_string(Id, Prompt, Default) ->
    NewPrompt = Prompt ++ " (default: " ++ Default ++ "): ",
    case read_from_batch_file(Id, Default) of
        {ok, Answer} ->
            Answer;
        no_batch_mode ->
            case read_from_console(NewPrompt) of
                "" ->
                    Default;
                Result ->
                    Result
            end
    end.

% Read user input from console
read_from_console(Prompt) ->
    try
        Line = io:get_line("> " ++ Prompt),
        true = is_list(Line),

        %deletes \n from end of the string
        [$\n | ReversedLine] = lists:reverse(Line),
        Result = lists:reverse(ReversedLine),

        % assert no whitespace
        0 = string:str(Result, " "),
        0 = string:str(Result, "\t"),
        0 = string:str(Result, "\n"),

        Result
    catch _:_ ->
        io:format("~nEh?~n"),
        read_from_console(Prompt)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Functions to check port usage
%

%if any of listed ports is in use - print warning ang exit
check_open_ports(Ports) ->
    case ports_are_free(Ports) of
        true ->
            ok;
        false ->
            warn("All following ports needs to be free: " ++ io_lib:fwrite("~p", [Ports])),
            warn("Terminating installation"),
            halt(1)
    end.

% Returns true if all listed ports are free
ports_are_free([]) ->
    true;
ports_are_free([FirstPort | Rest]) ->
    ports_are_free(FirstPort) and ports_are_free(Rest);
ports_are_free(Port) ->
    {Status, Socket} = gen_tcp:listen(Port, [{reuseaddr, true}]),
    case Status of
        ok ->
            gen_tcp:close(Socket),
            true;
        error ->
            false
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Convinience functions to print headers, prompts etc.
%

h1(Text) ->
    io:format("~n*** ~s ***~n", [Text]).

h2(Text) ->
    io:format("==> ~s~n", [Text]).

li(Text) ->
    io:format(" - ~s~n", [Text]).

warn(Text) ->
    warn(Text, []).

warn(Text, Args) ->
    io:format("(!) " ++ Text ++ "~n", Args).

info(Text) ->
    io:format("~~ ~s~n", [Text]).


% NOT USED

% Check if node named 'Name' existing on this machine
%check_name_availability(Name) ->
%	{ok, Entries} = file:consult(?configured_nodes_path),
%	case lists:keyfind(list_to_atom(Name), 2, Entries) of
%		false -> true;
%		_Found -> false
%	end.

% Get a valid (and unique) short node name from the console or retrieve it from batch file
%interaction_get_short_name(Id, Prompt) ->
%	NodeName = interaction_get_string(Id, Prompt),
%	case check_name_availability(NodeName) of 
%		false ->
%			warn("Node name '" ++ NodeName ++ "' is already used on this machine"),
%			interaction_get_short_name(Id, Prompt);
%		true ->
%			case lists:member($/, NodeName) of
%				true -> 
%					warn("Node name cannot contain a slash"),
%					interaction_get_short_name(Id, Prompt);
%				false -> NodeName
%			end
%	end.

%% Get a path to directory and check if its a valid path or retrieve it from batch file
%interaction_get_dir(Id, Prompt) ->
%	case read_from_batch_file(Id) of
%		{ok, Answer} -> 
%			case is_list(Answer) andalso lists:nth(1, Answer) =:= $/ andalso filelib:is_dir(Answer) of
%				false -> ?error("Batch file error - invalid entry for '~p'", [Id]);
%				true -> maybe_add_slash(Answer)
%			end;
%
%		_ ->
%			try 
%				{ok, [Result]} = io:fread("> " ++ Prompt, "~s"),
%				true = is_list(Result),
%				true = filelib:is_dir(Result),
%				maybe_add_slash(Result)
%			catch _:_ ->
%				io:format("~nEnter valid path to an existing directory.~n"),
%				interaction_get_dir(Id, Prompt)
%			end
%	end.
%
%% Add slash on the end if not existent
%maybe_add_slash(Path) ->
%	case lists:last(Path) of
%		$/ -> Path;
%		_ -> Path ++ "/"
%	end.
