%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: It is the main module of application. It lunches
%% supervisor which then initializes appropriate components of node.
%% @end
%% ===================================================================

-module(veil_cluster_node_app).
-behaviour(application).

-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% Application callbacks
-export([start/2, stop/1]).

-ifdef(TEST).
-export([ports_ok/0]).
-endif.

%% ===================================================================
%% Application callbacks
%% ===================================================================

%% start/2
%% ====================================================================
%% @doc Starts application by supervisor initialization.
-spec start(_StartType :: any(), _StartArgs :: any()) -> Result when
	Result ::  {ok, pid()}
                | ignore
                | {error, Error},
	Error :: {already_started, pid()}
                | {shutdown, term()}
                | term().
%% ====================================================================
start(_StartType, _StartArgs) ->
	Ans = case its_ccm() orelse ports_ok() of
		true ->
			{ok, NodeType} = application:get_env(?APP_Name, node_type),
			fprof:start(), %% Start fprof server. It doesnt do enything unless it's used.
			veil_cluster_node_sup:start_link(NodeType);
		false ->
			{error, "Not all ports are free"}
	end,
	?info("Application start at node: ~p. Start result: ~p.", [node(), Ans]),
	Ans.


%% stop/1
%% ====================================================================
%% @doc Stops application.
-spec stop(_State :: any()) -> Result when
	Result ::  ok.
%% ====================================================================
stop(_State) ->
  ok.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% get_env/1
%% ====================================================================
%% @doc Gets environment variable, prints error logs if it's udefined
-spec get_env(Name :: atom()) -> Result when
	Result :: {Status, Value :: term()},
	Status :: ok | error.
%% ====================================================================
get_env(Name) ->
	GetEnvResult = application:get_env(?APP_Name,Name),
	case GetEnvResult of
		{ok,Value} ->
			{ok,Value};
		undefined ->
			?error("Could not get '~p' environment variable.",[Name]),
			io:format(standard_error, "Could not get '~p' environment variable.~n",[Name]),
			{error,undefined}
	end.

%% its_ccm/0
%% ====================================================================
%% @doc Returns true if actual node is ccm
-spec its_ccm() -> Result when
	Result :: true | false.
%% ====================================================================
its_ccm() ->
	GetEnvResult = get_env(node_type),
	case GetEnvResult of
		{ok, ccm} ->
			true;
		_ ->
			false
	end.

%% ports_ok/0
%% ====================================================================
%% @doc Get port list from environment and check if they're free
-spec ports_ok() -> Result when
	Result :: true | false.
%% ====================================================================
ports_ok() ->
	GetEnvResult = get_env(ports_in_use),
	case GetEnvResult of
		{ok,PortEnvNames} ->
			GetEnvResultList = lists:map(fun(X) -> get_env(X) end, PortEnvNames),
			CorrectPortsNumbers = [ Number || {ok,Number} <- GetEnvResultList],
			Errors = [ Error || {error,Error} <- GetEnvResultList],
			case Errors of
				[] ->
					ports_are_free(CorrectPortsNumbers);
				_ ->
					false
			end;
		_ ->
			false
	end.

%% ports_are_free/1
%% ====================================================================
%% @doc Returns true if all listed ports are free, if not - log error
%% and print to stderr
-spec ports_are_free(Ports :: list() | integer()) -> Result when
	Result :: true | false.
%% ====================================================================
ports_are_free([]) ->
	true;
ports_are_free([FirstPort | Rest])->
	ports_are_free(FirstPort) and ports_are_free(Rest);
ports_are_free(Port)->
	{Status, Socket} = gen_tcp:listen(Port, [{reuseaddr, true}]),
	case Status of
		ok ->
			gen_tcp:close(Socket),
			true;
		error ->
			?error("Port ~p is in use, error: ~p. Starting aborted. ~n", [Port,Socket]),
			io:format(standard_error, "Port ~w is in use. Starting aborted.~n", [Port]),
			false
	end.
