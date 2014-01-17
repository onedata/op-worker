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
  Free_ports_info = are_ports_free([55,443,5555]),
  case Free_ports_info of
    {ok, free} ->
        {ok, NodeType} = application:get_env(?APP_Name, node_type),
        fprof:start(), %% Start fprof server. It doesnt do enything unless it's used.
        veil_cluster_node_sup:start_link(NodeType);
    {_,_} ->
        {error, "Some ports are in use."}
  end.

%% are_ports_free/1
%% ====================================================================
%% @doc Checks if ports are free. If some ports are in use, logs and writes to stderr.
-spec are_ports_free(Ports :: list()) -> Result when
    Result :: {ok, free}
                | {error, occupied}.
%% ====================================================================
are_ports_free([]) ->
    {ok, free};
are_ports_free([Port|Ports])->
    Port_result = is_port_free(Port),
    case Port_result of
        {ok, free} -> are_ports_free(Ports);
        {error,_} ->
            lager:error("Port ~w is in use. Starting aborted.~n", [Port]),
            io:fwrite(standard_error, "Port ~w is in use.~n", [Port]),
            {error, occupied}
    end.

%% is_port_free/1
%% ====================================================================
%% @doc Checks if port is free.
-spec is_port_free(Ports :: integer()) -> Result when
    Result :: {ok, free}
                | {error, occupied}.
%% ====================================================================
is_port_free(Port) ->
    Cmd_result = os:cmd("netstat -tlun | awk '{print $4}' | tail -n +3 | grep -E -o ':[0-9]+' | cut -d: -f 2 | grep ^" ++ integer_to_list(Port) ++ "$"),
    case Cmd_result of
        "" ->
            {ok, free};
        _ ->
            {error, occupied}
    end.

%% stop/1
%% ====================================================================
%% @doc Stops application.
-spec stop(_State :: any()) -> Result when
	Result ::  ok.
%% ====================================================================
stop(_State) ->
  ok.
