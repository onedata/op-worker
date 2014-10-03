% Installation directory of oneprovider RPM
-define(prefix, "/opt/oneprovider/").

% Args file
-define(args_file, ?prefix ++"scripts/addusers.cfg").

% Args
-define(all_args,[?hosts,?key_pool,?users,?uid_range,?gid_range,?debug]).
-define(hosts, "hosts").
-define(key_pool, "keys").
-define(users,"users").
-define(uid_range,"uid_range").
-define(gid_range,"gid_range").
-define(debug, "d").

main(Args) ->
	try
		%parse all args
		info("Parsing arguments from ~s and commandline...",[?args_file]),
		parse_args(Args),
		info("Arguments:"),
		[info("~p: ~p",[Name, get(Name)]) || Name <- ?all_args],

		%check connections
		info("Checking connection to hosts..."),
		[assert_connection_ok(Host,get(?key_pool)) || Host <- get(?hosts)],
		info("ok"),

		%prepare and add all groups
		info("Creating groups..."),
		AllGroups = [Group || [_Username,UserGroups] <- get(?users),Group <- UserGroups] ++
					[Username || [Username,_UserGroups] <- get(?users)],
		UniqueGroups = sets:to_list(sets:from_list(AllGroups)),
		grp_add_all(get(?hosts),UniqueGroups),

		%add all users
		info("Creating users..."),
		usr_add_all(get(?hosts),get(?users))

	catch
	    _Type:Error ->
		    print_error("Error: ~p",[Error]),
		    print_error("Stacktrace: ~p",[erlang:get_stacktrace()])
	end.



%% ====================================================================
%% Group management
%% ====================================================================

% Find minimal free gid for selected group name, on all hosts
find_free_gid(_Hosts,_Name,Min,Max) when Min>Max ->
	print_error("Cannot find free group id, terminating"),
	halt(1);
find_free_gid(Hosts,Name,Min,Max) ->
	case lists:all(fun(Host) -> (not grp_exists(Host,Min)) orelse (grp_get_name(Host,Min) == Name)  end,Hosts) of
		true ->
			Min;
		false ->
			find_free_gid(Hosts,Name,Min+1,Max)
	end.

%Check if group with given groupname or gid exists on host
grp_exists(Host,Gid) when is_integer(Gid) ->
	grp_exists(Host,integer_to_list(Gid)); %when gid given, we also check existence by calling grp_get_gid function (it works ok with gid too)
grp_exists(Host,GroupNameOrGid) ->
	case grp_get_gid(Host,GroupNameOrGid) of
		"" ->
			false;
		_ ->
			true
	end.

% Get gid of group (by groupname or gid)
grp_get_gid(Host,GroupName) ->
	{0,Ans} = call_command_on_host(Host,"getent group "++GroupName++" | cut -d: -f3"),
	case Ans of
		[] -> [];
		_ ->
			[$\n | Reversed]=lists:reverse(Ans),
			lists:reverse(Reversed)
	end.

% Get name of group (by gid)
grp_get_name(Host,Gid) when is_integer(Gid) ->
	grp_get_name(Host,integer_to_list(Gid));
grp_get_name(Host,Gid) ->
	{0,Ans} = call_command_on_host(Host,"getent group "++Gid++" | cut -d: -f1"),
	case Ans of
		[] -> [];
		_ ->
			[$\n | Reversed]=lists:reverse(Ans),
			lists:reverse(Reversed)
	end.

% Add group on host, fails if group exists
grp_add(Host,GroupName,Gid) ->
	{0,Ans} = call_command_on_host(Host,"groupadd -g "++Gid++" "++GroupName),
	Ans.

% Add group on host, prints warning if group exists
grp_add_with_warning(Host,Group,Gid) ->
	case grp_exists(Host,Group) of
		true ->
			warn("Group ~p already exists on host: ~p, with gid: ~p",[Group,Host,grp_get_gid(Host,Group)]);
		false ->
			grp_add(Host,Group,Gid),
			info("Group ~p added on host: ~p, with gid: ~p",[Group,Host,Gid])
	end.

% Add all groups on all hosts
grp_add_all(_Hosts,[]) ->
	ok;
grp_add_all(Hosts,[Group]) ->
	[MinGid,MaxGid] = get(?gid_range),
	Gid = integer_to_list(find_free_gid(Hosts,Group,list_to_integer(MinGid),list_to_integer(MaxGid))),
	[grp_add_with_warning(Host,Group,Gid) || Host <- Hosts];
grp_add_all(Hosts,Groups) ->
	[ grp_add_all(Hosts,[Group]) ||Group <- Groups].


%% ====================================================================
%% User management
%% ====================================================================

% Find minimal free uid for selected username, on all hosts
find_free_uid(_Hosts,_Name,Min,Max) when Min>Max ->
	print_error("Cannot find free user id, terminating"),
	halt(1);
find_free_uid(Hosts,Name,Min,Max) ->
	case lists:all(fun(Host) -> (not usr_exists(Host,Min)) orelse (usr_get_name(Host,Min) == Name) end,Hosts) of
		true ->
			Min;
		false ->
			find_free_uid(Hosts,Name,Min+1,Max)
	end.

%Check if user with given username or uid exists on host
usr_exists(Host,Uid) when is_integer(Uid) ->
	usr_exists(Host,integer_to_list(Uid));
usr_exists(Host,UserNameOrUid) ->
	case usr_get_uid(Host,UserNameOrUid) of
		"" ->
			false;
		_ ->
			true
	end.

% Get uid of user (by username or uid)
usr_get_uid(Host,UserName) ->
	{0,Ans} = call_command_on_host(Host,"getent passwd "++UserName++" | cut -d: -f3"),
	case Ans of
		[] -> [];
		_ ->
			[$\n | Reversed]=lists:reverse(Ans),
			lists:reverse(Reversed)
	end.

% Get name of user (by uid)
usr_get_name(Host,Uid) when is_integer(Uid) ->
	usr_get_name(Host,integer_to_list(Uid));
usr_get_name(Host,Uid) ->
	{0,Ans} = call_command_on_host(Host,"getent passwd "++Uid++" | cut -d: -f1"),
	case Ans of
		[] -> [];
		_ ->
			[$\n | Reversed]=lists:reverse(Ans),
			lists:reverse(Reversed)
	end.

% Add user to group
usr_add_to_grp(Host,User,Group) ->
	{0,Ans} = call_command_on_host(Host,"usermod -a -G "++Group++" "++User),
	Ans.

% Add user on host, fails if user exists
usr_add(Host,UserName,Uid) ->
	{0,Ans} = call_command_on_host(Host,"useradd -u "++Uid++" -g "++UserName++" "++UserName),
	Ans.

% Add user on host, prints warning if user exists
usr_add_with_warning(Host,User,Uid) when is_list(User) and is_list(Host) ->
	case usr_exists(Host,User) of
		true ->
			warn("user ~p already exists on host: ~p, with uid: ~p",[User,Host,usr_get_uid(Host,User)]);
		false ->
			usr_add(Host,User,Uid),
			info("user ~p added on host: ~p, with uid: ~p",[User,Host,Uid])
	end.

% Add all users on all hosts
usr_add_all(_Hosts,[]) ->
	ok;
usr_add_all(Hosts,[ [Username,Groups] ]) ->
	[MinUid,MaxUid] = get(?uid_range),
	Uid = integer_to_list(find_free_uid(Hosts,Username,list_to_integer(MinUid),list_to_integer(MaxUid))),
	[usr_add_with_warning(Host,Username,Uid) || Host <- Hosts],
	[usr_add_to_grp(Host,Username,Group) || Group <- Groups, Host <- Hosts];
usr_add_all(Hosts,Users) ->
	[ usr_add_all(Hosts,[User]) ||User <- Users].


%% ====================================================================
%% Arg parsing
%% ====================================================================

% Parse default and commandline args and put them into process dictionary
parse_args(Args) ->
	%set args to default
	{Status, DefaultArgs} = file:consult(?args_file),
	case Status of
		ok -> ok;
		error ->
			print_error("Error during parsing ~p: ~p",[?args_file,DefaultArgs]),
			halt(1)
	end,
	[put(atom_to_list(Name),Value) || {Name,Value} <- DefaultArgs],
	[assert_arg_ok(Name) || {Name,_Value} <- DefaultArgs],

	%override default args by those obtained from commandline
	ArgNames = lists:filter(fun(Arg) -> hd(Arg)==$- end,Args),
	[put(Name,get_arg(Args,Name)) || [$-|Name] <- ArgNames],
	[assert_arg_ok(Name) || [$-|Name] <- ArgNames],
	ok.

% Check if given arg is globally defined and properly formatted
assert_arg_ok(Arg) when is_atom(Arg) ->
	assert_arg_ok(atom_to_list(Arg));
assert_arg_ok(Arg) ->
	case lists:member(Arg,?all_args) andalso arg_format_ok(Arg)  of
		true ->
			ok;
		false ->
			print_error("Bad argument: ~p",[Arg]),
			print_error("For command line args usage info, please read ~s config file.",[?args_file]),
			halt(1)
	end.

%checks format of given argument
arg_format_ok(StringListArg) when StringListArg==?hosts orelse StringListArg==?key_pool ->
	StringList = get(StringListArg),
	is_list(StringList) andalso lists:all(fun(X) -> is_list(X) end,StringList);
arg_format_ok(RangeArg) when RangeArg==?gid_range orelse RangeArg==?uid_range ->
	try
		[Min,Max] = get(RangeArg),
		list_to_integer(Min),
		list_to_integer(Max),
		true
	catch
	    _:_  -> false
	end;
arg_format_ok(?users) ->
	Users = get(?users),
	is_list(Users) andalso lists:all(fun([Name,Groups]) -> is_list(Name) andalso is_list(Groups); (_) -> false end,Users);
arg_format_ok(_) ->
	true.

% Parses arguments associated with given Argname to erlang term
get_arg(Args,Argname) when is_atom(Argname) ->
	get_arg(Args,atom_to_list(Argname));
get_arg(Args,Argname) ->
	ArgsAsString = arglist_to_erlang_string(args_filter(Args,Argname)),
	{ok,Tokens,_} = erl_scan:string(ArgsAsString),
	{ok,Term} = erl_parse:parse_term(Tokens),
	Term.

% Filter list of arguments to contain only args associated with given Argname
args_filter(Args,Argname) ->
	args_filter(Args,"-"++Argname,false).
args_filter([],_,_) ->
	[];
args_filter([Argname|Rest],Argname,_) ->
	args_filter(Rest,Argname,true);
args_filter([[$-|_Arg]|Rest],Argname,_) ->
	args_filter(Rest,Argname,false);
args_filter([_OtherArg|Rest],Argname,false) ->
	args_filter(Rest,Argname,false);
args_filter([HostArg|Rest],Argname,true) ->
	[HostArg | args_filter(Rest,Argname,true)].

% Convert arglist to string representation
% example: ["a","[","[","b","c","]","d","]"]  ->  "[a,[[b,c],d]]."
arglist_to_erlang_string(Args) ->
	arglist_to_erlang_string("[",Args) ++ "].".
arglist_to_erlang_string(Prefix,[]) ->
	Prefix;
arglist_to_erlang_string(Prefix,[ "]" | Rest]) ->
	arglist_to_erlang_string(Prefix++"]",Rest);
arglist_to_erlang_string(Prefix,[ "[" | Rest]) ->
	case lists:last(Prefix) of
		$[ ->
			arglist_to_erlang_string(Prefix++"[",Rest);
		_ ->
			arglist_to_erlang_string(Prefix++","++"[",Rest)
	end;
arglist_to_erlang_string(Prefix,[ Other | Rest]) ->
	case lists:last(Prefix) of
		$[ ->
			arglist_to_erlang_string(Prefix++"\""++Other++"\"",Rest);
		_ ->
			arglist_to_erlang_string(Prefix++","++"\""++Other++"\"",Rest)
	end.

%% ====================================================================
%% SSH calls
%% ====================================================================

% checks if ssh connection can be established and simple command can be executed
assert_connection_ok(Host,KeyPool) ->
	case call_command_on_host(Host,KeyPool,"test 0",true) of
		{0,""} -> ok;
		{_,_} ->
			{_,Error} = call_command_on_host(Host,KeyPool,"test 0",false),
			print_error("Could not connect to: ~p, error: ~p ",[Host,Error]),
			halt(1)
	end.

% tries to connect with given list of keys and executes command on host
call_command_on_host(Host,Command) ->
	call_command_on_host(Host,get(?key_pool),Command).
call_command_on_host(Host,KeyPool,Command) ->
	call_command_on_host(Host,KeyPool,Command,true).
call_command_on_host(Host,KeyPool,Command,Quiet) ->
	debug("executing command on "++Host++": "++Command),
	QuietArg = case Quiet of
				   true -> "-q";
				   false -> ""
	           end,
	Result =
		case Host of
			"localhost" ->
				os:cmd(Command ++ ";echo *$?"); % command ; echo *$?
			_ ->
				os:cmd("ssh "++QuietArg++" "++parse_key_pool(KeyPool)++" root@"++Host++" "++"'"++Command++"'" ++ ";echo *$?") % ssh -i key1 -i key2.. user@host 'command' ; echo *$?
		end,
	Ans = get_answer(Result),
	RCode = get_return_code(Result),
	debug("result: "++Ans),
	{RCode,Ans}.

%parse key_pool to ssh format (-i key1 -i key2...)
parse_key_pool([]) ->
	"";
parse_key_pool([Key1|Rest]) ->
	" -i "++Key1++parse_key_pool(Rest).

%get ansver from os command result
get_answer(Result) ->
	SeparatorPosition = string:rchr(Result,$*),
	string:substr(Result,1,SeparatorPosition-1).

%get return code from os command result
get_return_code(Result) ->
	Code = lists:last(string:tokens(Result,"*")),
	[_ | RevertedCode] =lists:reverse(Code),
	list_to_integer(lists:reverse(RevertedCode)).

%% ====================================================================
%% Logging
%% ====================================================================

debug(Text) ->
	debug(Text,[]).
debug(Text,Args) ->
	case get(?debug) of
		undefined ->
			none;
		_ ->
			io:format("[debug] "++Text++"~n",Args)
	end.

info(Text) ->
	info(Text, []).
info(Text,Args) ->
	io:format("[info] "++Text++"~n",Args).

warn(Text,Args) ->
	io:format("[warning] "++ Text++"~n",Args).

print_error(Text) ->
	print_error(Text,[]).
print_error(Text,Args) ->
	io:format("[error] "++Text++"~n",Args).
%% -------------------------------------