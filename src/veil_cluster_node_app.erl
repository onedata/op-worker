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
  {ok, NodeType} = application:get_env(?APP_Name, node_type),
  fprof:start(), %% Start fprof server. It doesnt do enything unless it's used.
  veil_cluster_node_sup:start_link(NodeType).


%% stop/1
%% ====================================================================
%% @doc Stops application.
-spec stop(_State :: any()) -> Result when
	Result ::  ok.
%% ====================================================================
stop(_State) ->
  ok.
