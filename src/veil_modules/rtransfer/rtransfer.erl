%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour to provide
%% remote transfer manager functionality (gateways' management).
%% @end
%% ===================================================================

-module(rtransfer).
-behaviour(worker_plugin_behaviour).

-include_lib("ctool/include/logging.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

init(_Args) ->
	[].

handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, healthcheck) ->
	ok;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(_ProtocolVersion, _Msg) ->
  ?warning("Wrong request: ~p", [_Msg]),
	ok.

cleanup() ->
	ok.
