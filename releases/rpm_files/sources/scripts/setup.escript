

% Default cookie used for communication with cluster 
-define(default_cookie, veil_cluster_node).

% Installation directory of veil RPM
-define(prefix, "/opt/veil/").

% Location of configured_nodes.cfg
-define(configured_nodes_path, ?prefix ++ "scripts/configured_nodes.cfg").

% Location of init.d script
-define(init_d_script_path, "/etc/init.d/veil").

% Location of release packages
-define(veil_release, ?prefix ++ "files/veil_cluster_node").                        
-define(db_release, ?prefix ++ "files/database_node").

% Location of erl_launcher
-define(erl_launcher_script_path, ?prefix ++ "scripts/erl_launcher").

% Install path for nodes
-define(default_nodes_install_path, ?prefix ++ "nodes/").
-define(default_ccm_name, "ccm").
-define(default_worker_name, "worker").

% Paths relative to veil_cluster_node release
-define(config_args_path, "bin/config.args").
-define(veil_cluster_script_path, "bin/veil_cluster").

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
	h1("Veil SETUP"),
	info("Nodes configured on this machine will use its hostname: " ++ get(hostname)),
	warn("Make sure it is resolvable by other hosts in the network"),
	Option = interaction_choose_option(what_to_do, "What do you want to do?", 
		[
			{manage_db, "Manage database nodes"},
			{manage_veil, "Manage veil nodes"},
			{exit, "Exit"}
		]),
	case Option of 
		manage_db -> setup_manage_db();
		manage_veil -> setup_manage_veil();
		exit -> halt(1)
	end.


% Manage veil cluster nodes
setup_manage_veil() ->	
	info("Each machine can only host a single worker or a ccm + worker pair."),

	OptionList = case get_nodes_from_config(veil) of
		{none, []} -> 
			[
				{new_cluster, "Set up a new cluster"}, 
				{extend_cluster, "Extend existing cluster"}
			];
		_ -> 
			[{remove_nodes, "Remove node(s) configured on this machine"}]
	end, 

	Option = interaction_choose_option(what_to_do_veil, "What do you want to do?", OptionList ++ [{go_back, "Go back"}]),
	case Option of 
		new_cluster -> setup_confirm_new_cluster();
		extend_cluster -> setup_extend_cluster();
		remove_nodes -> setup_remove_veil_nodes();
		go_back -> setup_start()
	end.


% Executed before the proper installation to inform about installation conditions
setup_confirm_new_cluster() ->
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
			setup_new_ccm_plus_worker(true);
		BadNode ->
			warn("Could not establish connection with " ++ BadNode),
			setup_get_db_nodes()
	end,
	ok.


% Install ccm along with a worker
setup_new_ccm_plus_worker(IsThisMainCCM) ->	
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
			setup_manage_veil();
		ok -> 
			LongName = CCMName ++ get(hostname),
			case IsThisMainCCM of
				true ->
					put(main_ccm, list_to_atom(LongName)),
					put(opt_ccms, []);
				false ->
					put(opt_ccms, get(opt_ccms) ++ [list_to_atom(LongName)])
			end,
			install_veil_node(ccm, CCMName, CCMPath),
			install_veil_node(worker, WorkerName, WorkerPath),
			info("Starting node(s)..."),
			os:cmd(?init_d_script_path ++ " start_veil")
	end.


% Firstly check connection to an existing cluster, then ask to choose installation variant
setup_extend_cluster() ->
	IPOrHostname = interaction_get_string(ip_or_hostname, "Specify IP/hostname of any host with running veil node(s): "),
	case discover_cluster(IPOrHostname) of
		no_connection -> 
			warn("No node at [" ++ IPOrHostname ++ "] is available. Make sure there is a viable connection between hosts."),
			WantRetry = interaction_choose_option(retry, "Do you want to try again?",
				[
					{yes, "Yes"},
					{no, "No"}
				]),
			case WantRetry of 
				yes -> setup_extend_cluster();
				no -> setup_manage_veil()
			end;
			
		{[MainCCM|OptCCMS], DBNodes} ->
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
				ccm_plus_worker -> setup_new_ccm_plus_worker(false);
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
			setup_manage_veil();
		ok ->  
			install_veil_node(worker, WorkerName, WorkerPath),
			info("Starting node(s)..."),
			os:cmd(?init_d_script_path ++ " start_veil")
	end.


% Install a generic veil node
install_veil_node(Type, Name, Path) ->
	LongName = Name ++ get(hostname),
	info("Installing " ++ LongName ++ "..."),
	os:cmd("mkdir -p " ++ Path ++ Name),
	os:cmd("cp -R " ++ ?veil_release ++ "/* " ++ Path ++ Name),

	MainCCM = get(main_ccm),
	OptCCMs = get(opt_ccms),
	DBNodes = get(db_nodes),

	overwrite_config_args(Path ++ Name ++ "/" ++ ?config_args_path, "name", LongName),
	overwrite_config_args(Path ++ Name ++ "/" ++ ?config_args_path, "main_ccm", atom_to_list(MainCCM)),
	overwrite_config_args(Path ++ Name ++ "/" ++ ?config_args_path, "opt_ccms", to_space_delimited_list(OptCCMs)),
	overwrite_config_args(Path ++ Name ++ "/" ++ ?config_args_path, "db_nodes", to_space_delimited_list(DBNodes)),	

	os:cmd(Path ++ Name ++ "/" ++ ?veil_cluster_script_path),
	add_node_to_config(Type, list_to_atom(Name), Path).


% List to space-delimited-string list
to_space_delimited_list(List) ->
	lists:foldl(
		fun(Element, Acc) ->
			Acc ++ atom_to_list(Element) ++ " "
		end, [], List).


% List currently installed nodes and ask for confirmation
setup_remove_veil_nodes() ->
	case get_nodes_from_config(veil) of
		{none, []} -> 
			warn("There are no nodes configured on this machine"),
			setup_manage_veil();
		{worker, {worker, WorkerName, _}} ->	
			info("Nodes currently configured on this machine:"),
			li(atom_to_list(WorkerName) ++ get(hostname));
		{ccm_plus_worker, {{ccm, CCMName,  _}, {worker, WorkerName, _}}} ->	
			info("Currently configured on this machine:"),
			li(atom_to_list(CCMName) ++ get(hostname)),
			li(atom_to_list(WorkerName) ++ get(hostname))
	end,

	Option = interaction_choose_option(confirm_veil_nodes_deletion, "Do you wish to remove current configuration?", 
		[
			{yes, "Yes"},
			{no, "No"}
		]),
	case Option of 
		no -> setup_start();
		yes -> do_remove_veil_nodes()
	end.


% Stop node(s) and remove them from the machine
do_remove_veil_nodes() ->
	info("Stopping node(s)..."),
	os:cmd(?init_d_script_path ++ " stop_veil"),
	case get_nodes_from_config(veil) of
		{worker, {worker, Name, Path}} ->	
			info("Removing " ++ atom_to_list(Name) ++ get(hostname)),
			os:cmd("rm -rf " ++ Path ++ atom_to_list(Name)),
			remove_node_from_config(Name);
		{ccm_plus_worker, {{ccm, CCMName,  CCMPath}, {worker, WorkerName, WorkerPath}}} ->	
			info("Removing " ++ atom_to_list(CCMName)),
			os:cmd("rm -rf " ++ CCMPath ++ atom_to_list(CCMName)),
			remove_node_from_config(CCMName),
			info("Removing " ++ atom_to_list(WorkerName)),
			os:cmd("rm -rf " ++ WorkerPath ++ atom_to_list(WorkerName)),
			remove_node_from_config(WorkerName)
	end.


% TODO
setup_manage_db() ->
	info("Not yet implemented").



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
		{ok, CCMNodes} = rpc:call(list_to_atom(?default_worker_name ++ "@" ++ IPOrHostname), application, get_env, [veil_cluster_node, ccm_nodes]),
		{ok, DBNodes} = rpc:call(list_to_atom(?default_worker_name ++ "@" ++ IPOrHostname), application, get_env, [veil_cluster_node, db_nodes]),
		{CCMNodes, DBNodes}
	catch _:_ ->
		try
			{ok, CCMNodes2} = rpc:call(list_to_atom(?default_ccm_name ++ "@" ++ IPOrHostname), application, get_env, [veil_cluster_node, ccm_nodes]),
			{ok, DBNodes2} = rpc:call(list_to_atom(?default_ccm_name ++ "@" ++ IPOrHostname), application, get_env, [veil_cluster_node, db_nodes]),
			{CCMNodes2, DBNodes2}
		catch _:_ ->
			no_connection
		end
	end.

    

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Function used to process config file used by veil_cluster script
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
% WhichCluster = veil | database
get_nodes_from_config(WhichCluster) ->
	try
		{ok, Entries} = file:consult(?configured_nodes_path),
		case WhichCluster of
			database ->	get_database_node_from_config(Entries);
			veil -> get_veil_nodes_from_config(Entries)
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
get_veil_nodes_from_config(Entries) ->
	case lists:keyfind(worker, 1, Entries) of
		false -> {none, []};
		Worker -> 
			case lists:keyfind(ccm, 1, Entries) of
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



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Functions used to interact with user or read answers from batch file
%

% Consult the batch file specified in args (if any), looking for {Id, Value} entry
read_from_batch_file(Id) ->
	case get(batch_file) of 
		undefined -> 
			no_batch_mode;

		File ->
			try
				{ok, AnswerProplist} = file:consult(File),
				{Id, Value} = proplists:lookup(Id, AnswerProplist),
				{ok, Value}
			catch _:_ ->
				?error("Batch file error - could not find entry for '~p'", [Id])
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

		_ ->
			try 
				{ok, [Result]} = io:fread("> " ++ Prompt, "~s"),
				true = is_list(Result),
				Result
			catch _:_ ->
				io:format("~nEh?~n"),
				interaction_get_string(Id, Prompt)
			end
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
	io:format("(!) ~s~n", [Text]).

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
