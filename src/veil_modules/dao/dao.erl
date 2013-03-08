%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module gives high level API for VeilFS database
%% @end
%% ===================================================================
-module(dao).
-behaviour(worker_plugin_behaviour).

%% API
-export([]).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/2, cleanUp/0]).

%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc worker_plugin_behaviour callback init/1
-spec init(Args :: term()) -> Result when
      Result :: ok | {error, Error},
      Error :: term().
%% ====================================================================
init(_Args) ->
    ok.

%% init/1
%% ====================================================================
%% @doc worker_plugin_behaviour callback init/1
-spec handle(ProtocolVersion :: term(), Request :: term()) -> Result when
      Result :: {ok, Response} | {error, Error},
      Response :: term(),
      Error :: term().
%% ====================================================================
handle(_ProtocolVersion, _Request) ->
    {ok, done}.

%% cleanUp/0
%% ====================================================================
%% @doc worker_plugin_behaviour callback cleanUp/0
-spec cleanUp() -> Result when
      Result :: ok | {error, Error},
      Error :: term().
%% ====================================================================
cleanUp() ->
    ok.

%% ===================================================================
%% API functions
%% ===================================================================


%% ===================================================================
%% Internal functions
%% ===================================================================

