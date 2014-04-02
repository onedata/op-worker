% Args
-define(hosts, "-h").
-define(key_pool, "-k").
-define(commands, "-c").
-define(debug, "-d").

main(Args) ->
	[Host,KeyPool,Command] = parse_args(Args),
	debug("args: ~p" ,[[Host,KeyPool,Command]]),
	connection_ok(Host,KeyPool),
	debug("~p",[call_command_on_host(Host,KeyPool,Command)]).

%% ------------ Arg parsing ------------
parse_args(Args) ->
	try
		[Host] = args_get(Args,?hosts),
		KeyPool = args_get(Args,?key_pool),
		[Command] = args_get(Args,?commands),
		put(debug,lists:member(?debug,Args)),
		[Host,KeyPool,Command]
	catch
	    _Type:_Error  ->
%% 			print_error("usage: veil_addusers [-k {key}] -h {user@host}  -c {command} "),
%% 			print_error("Script will execute all comands on each host, using key pool to authenticate"),
			erlang:halt(1)
	end.

% get list of arguments of some type
args_get(Args,Type) ->
	args_get(Args,Type,false).
args_get([],_,_) ->
	[];
args_get([Type|Rest],Type,_) ->
	args_get(Rest,Type,true);
args_get([[$-|_Arg]|Rest],Type,_) ->
	args_get(Rest,Type,false);
args_get([_OtherArg|Rest],Type,false) ->
	args_get(Rest,Type,false);
args_get([HostArg|Rest],Type,true) ->
	[HostArg | args_get(Rest,Type,true)].
%% -------------------------------------

%% ------------- SSH calls -------------
% checks if ssh connection can be established and simple command can be executed
connection_ok(Host,KeyPool) ->
	case call_command_on_host(Host,KeyPool,"test 0") of
		{0,_} -> true;
		_ -> false
	end.

% tries to connect with given list of keys and executes command on host
call_command_on_host(Host,KeyPool,Command) ->
	debug(Host++": "++Command),
	Result = os:cmd("ssh "++parse_key_pool(KeyPool)++" "++Host++" "++"'"++Command++"'" ++ ";echo *$?"), % ssh -i key1 -i key2.. user@host 'command' ; echo #$?
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
	case get(debug) of
		true ->
			print("[debug] "++Text,Args);
		false ->
			none
	end.
print(Text) ->
	print(Text, []).
print(Text,Args) ->
	io:format(Text++"~n",Args).
print_error(Text) ->
	io:format(Text++"~n").
%% -------------------------------------