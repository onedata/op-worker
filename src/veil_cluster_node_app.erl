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

%% Application callbacks
-export([start/2, stop/1]).


%% ===================================================================
%% Application callbacks
%% ===================================================================

%% start/1
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
	case ports_ok() of
		true ->
			{ok, NodeType} = application:get_env(?APP_Name, node_type),
			fprof:start(), %% Start fprof server. It doesnt do enything unless it's used.
			veil_cluster_node_sup:start_link(NodeType);
		false ->
			{error, "Not all ports are free"}
	end.


%% stop/1
%% ====================================================================
%% @doc Stops application.
-spec stop(_State :: any()) -> Result when
	Result ::  ok.
%% ====================================================================
stop(_State) ->
  ok.

%% ports_ok/0
%% ====================================================================
%% @doc Get port list from environment and check if they're free
-spec ports_ok() -> Result when
	Result :: true | false.
%% ====================================================================
ports_ok() ->
	GetEnvResult = application:get_env(veil_cluster_node,ports_in_use),
	case GetEnvResult of
		{ok,PortList} ->
			ports_are_free(PortList);
		undefined ->
			lager:error("Could not get 'ports_in_use' environment variable."),
			io:format(standard_error, "Could not get 'ports_in_use' environment variable.~n"),
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
	{Status, Socket} = gen_tcp:listen(Port, []),
	case Status of
		ok ->
			gen_tcp:close(Socket),
			true;
		error ->
			lager:error("Port ~w is in use. Starting aborted.~n", [Port]),
			io:format(standard_error, "Port ~w is in use. Starting aborted.~n", [Port]),
			false
	end.
