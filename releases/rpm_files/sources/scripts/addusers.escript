% Installation directory of veil RPM
-define(prefix, "/opt/veil/").

% Args
-define(all_args,[?hosts,?key_pool,?users,?uid_range,?gid_range,?debug]).
-define(hosts, "hosts").
-define(key_pool, "keys").
-define(users,"users").
-define(uid_range,"uid_range").
-define(gid_range,"gid_range").
-define(debug, "d").

% Args file
-define(args_file, ?prefix ++"scripts/addusers.cfg").

main(Args) ->
	try
		parse_args(Args),
		%print all args
		[debug("~p: ~p",[Name, get(Name)]) || Name <- ?all_args],
		%check connections
		[assert_connection_ok(Host,get(?key_pool)) || Host <- get(?hosts)],

		%find minimmal gid
		[MinGid,MaxGid] = get(?gid_range),
		FreeGid = find_free_gid(get(?hosts),list_to_integer(MinGid),list_to_integer(MaxGid)),
	    debug("~p",[FreeGid])

	catch
	    _Type:Error ->
		    print_error("Error: ~p",[Error]),
		    print_error("Stacktrace: ~p",[erlang:get_stacktrace()])
	end.



%% --------- Group management ----------
find_free_gid(_Hosts,Min,Max) when Min>Max ->
	print_error("cannot find free group id"),
	halt(1);
find_free_gid(Hosts,Min,Max) ->
	case lists:all(fun(Host) -> not grp_exists(Host,Min) end,Hosts) of
		true ->
			Min;
		false ->
			find_free_gid(Hosts,Min+1,Max)
	end.

grp_exists(Host,Gid) when is_integer(Gid) ->
	grp_exists(Host,integer_to_list(Gid));
grp_exists(Host,GroupNameOrGid) ->
	case grp_get_gid(Host,GroupNameOrGid) of
		"" ->
			false;
		_ ->
			true
	end.

grp_get_gid(Host,GroupName) ->
	{0,Ans} = call_command_on_host(Host,"getent group "++GroupName++" | cut -d: -f3"),
	case Ans of
		[] -> [];
		_ ->
			[$\n | Reversed]=lists:reverse(Ans),
			lists:reverse(Reversed)
	end.

grp_add(Host,GroupName,Gid) ->
	{0,Ans} = call_command_on_host(Host,"groupadd -g "++Gid++" "++GroupName),
	Ans.
%% -------------------------------------

%% --------- Users management ----------
find_free_uid(_Hosts,Min,Max) when Min>Max ->
	print_error("cannot find free user id"),
	halt(1);
find_free_uid(Hosts,Min,Max) ->
	case lists:all(fun(Host) -> not usr_exists(Host,Min) end,Hosts) of
		true ->
			Min;
		false ->
			find_free_uid(Hosts,Min+1,Max)
	end.

usr_exists(Host,Uid) when is_integer(Uid) ->
	usr_exists(Host,integer_to_list(Uid));
usr_exists(Host,UserNameOrUid) ->
	case usr_get_uid(Host,UserNameOrUid) of
		"" ->
			false;
		_ ->
			true
	end.

usr_get_uid(Host,UserName) ->
	{0,Ans} = call_command_on_host(Host,"getent passwd "++UserName++" | cut -d: -f3"),
	case Ans of
		[] -> [];
		_ ->
			[$\n | Reversed]=lists:reverse(Ans),
			lists:reverse(Reversed)
	end.

usr_add(Host,UserName,Uid) ->
	{0,Ans} = call_command_on_host(Host,"useradd -u "++Uid++" "++UserName),
	Ans.
%% -------------------------------------

%% ------------ Arg parsing ------------
% Parse default and commandline args and put them into process dictionary
parse_args(Args) ->
	%set args to default
	{Status, DefaultArgs} = file:consult(?args_file),
	case Status of
		ok -> ok;
		error ->
			print_error("error during parsing ~p: ~p",[?args_file,DefaultArgs]),
			halt(1)
	end,
	[assert_arg_ok(Name) || {Name,_Value} <- DefaultArgs],
	[put(atom_to_list(Name),Value) || {Name,Value} <- DefaultArgs],

	%override default args by those obtained from commandline
	ArgNames = lists:filter(fun(Arg) -> hd(Arg)==$- end,Args),
	[assert_arg_ok(Name) || [$-|Name] <- ArgNames],
	[put(Name,get_arg(Args,Name)) || [$-|Name] <- ArgNames].

% Check if given arg is globally defined

assert_arg_ok(Arg) when is_atom(Arg) ->
	assert_arg_ok(atom_to_list(Arg));
assert_arg_ok(Arg) ->
	case lists:member(Arg,?all_args) of
		true ->
			ok;
		false ->
			print_error("Unknown argument: ~p",[Arg]),
			halt(1)
	end.

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
%% -------------------------------------

%% ------------- SSH calls -------------
% checks if ssh connection can be established and simple command can be executed
assert_connection_ok(Host,KeyPool) ->
	case call_command_on_host(Host,KeyPool,"test 0") of
		{0,_} -> ok;
		_ ->
			print_error("Could not connect to: ~p",[Host]),
			halt(1)
	end.

% tries to connect with given list of keys and executes command on host
call_command_on_host(Host,Command) ->
	call_command_on_host(Host,get(?key_pool),Command).
call_command_on_host(Host,KeyPool,Command) ->
	debug(Host++": "++Command),
	Result =
		case Host of
			"localhost" ->
				os:cmd(Command ++ ";echo *$?"); % command ; echo *$?
			_ ->
				os:cmd("ssh "++parse_key_pool(KeyPool)++" "++Host++" "++"'"++Command++"'" ++ ";echo *$?") % ssh -i key1 -i key2.. user@host 'command' ; echo *$?
		end,
	Ans = get_answer(Result),
	RCode = get_return_code(Result),
	debug(Ans),
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
%% -------------------------------------


%% -------- Internal functions ---------
debug(Text) ->
	debug(Text,[]).
debug(Text,Args) ->
	case get(?debug) of
		undefined ->
			none;
		_ ->
			print("[debug] "++Text,Args)
	end.

print(Text) ->
	print(Text, []).
print(Text,Args) ->
	io:format(Text++"~n",Args).

print_error(Text) ->
	print_error(Text,[]).
print_error(Text,Args) ->
	io:format("(!) "++Text++"~n",Args).
%% -------------------------------------