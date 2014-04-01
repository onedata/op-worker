% Args
-define(hosts, "-h").
-define(key_pool, "-k").
-define(commands, "-c").
-define(debug, "-d").
-define(silent, "-s").

main(Args) ->
	case args_correct(Args) of
		true ->
			ok;
		false ->
			print_error("usage: veil_remotecall [-k {key}] -h {user@host}  -c {command} "),
			print_error("Script will execute all comands on each host, using key pool to authenticate"),
			erlang:halt(1)
	end,
	put(debug,lists:member(?debug,Args)),
	put(silent,lists:member(?silent,Args)),
	Hosts = args_get(Args,?hosts),
	debug("hosts:~p",[Hosts]),
	KeyPool = args_get(Args,?key_pool),
	debug("keys:~p",[KeyPool]),
	Commands = args_get(Args,?commands),
	debug("commands:~p",[Commands]),

	[call_commands_on_host(Host,KeyPool,Commands) || Host <-Hosts].

%% ------------ Arg parsing ------------
% check if all necessary args are present
args_correct(Args) ->
	lists:member(?hosts,Args)  andalso
	lists:member(?commands,Args).

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
call_commands_on_host(_Host,_KeyPool,[]) ->
	ok;
call_commands_on_host(Host,KeyPool,[Command|Rest]) ->
	print(Host++": "++Command),
	Ans = os:cmd("ssh "++parse_key_pool(KeyPool)++" "++Host++" "++Command),
	print(Ans),
	call_commands_on_host(Host,KeyPool,Rest).

%parse key_pool to ssh format (-i key1 -i key2...)
parse_key_pool([]) ->
	"";
parse_key_pool([Key1|Rest]) ->
	" -i "++Key1++parse_key_pool(Rest).


%% -------------- Printing -------------
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
	case get(silent) of
		false ->
			io:format(Text++"~n",Args);
		true ->
			none
	end.
print_error(Text) ->
	io:format(Text++"~n").
%% -------------------------------------
