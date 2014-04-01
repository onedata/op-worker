% Args
-define(hosts, "-h").
-define(keys, "-k").
-define(commands, "-c").

% Debug
-define(debug,true).

main(Args) ->
	case args_correct(Args) of
		true ->
			ok;
		false ->
			print("usage: veil_remotecall [-h {user@host}] [-k {key}] [-c {command}] "),
			print("Script will execute all comands on each (host,key) pair."),
			print("Number of hosts must be equal to number of keys."),
			erlang:halt(1)
	end,
	Hosts = args_get(Args,?hosts),
	debug("hosts:~p",[Hosts]),
	Keys = args_get(Args,?keys),
	debug("keys:~p~n",[Keys]),
	Commands = args_get(Args,?commands),
	debug("commands:~p",[Commands]),

	[call_commands_on_host(Host,Key,Commands) || {Host,Key} <- lists:zip(Hosts,Keys)].

%% ------------ Arg parsing ------------
% check if all necessary args are present
args_correct(Args) ->
	lists:member(?hosts,Args)  andalso
	lists:member(?keys,Args)  andalso
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
call_commands_on_host(_Host,_Key,[]) ->
	ok;
call_commands_on_host(Host,Key,[Command|Rest]) ->
	print(Host++": "++Command),
	Ans = os:cmd("ssh -i "++Key++" "++Host++" "++Command),
	print(Ans),
	call_commands_on_host(Host,Key,Rest).


%% -------------- Printing -------------
print(Text) ->
	print(Text, []).
print(Text,Args) ->
	io:format(Text++"~n",Args).
debug(Text,Args) ->
	case ?debug of
		true ->
			print("[debug] "++Text,Args);
		false ->
			none
	end.
%% -------------------------------------
