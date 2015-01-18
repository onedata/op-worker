%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc It is the main module of application. It lunches
%% supervisor which then initializes appropriate components of node.
%%% @end
%%%--------------------------------------------------------------------
-module(oneprovider_node_app).
-author("Michal Wrzeszcz").

-behaviour(application).

-include("registered_names.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Starts application by supervisor initialization.
%%--------------------------------------------------------------------
-spec start(_StartType :: any(), _StartArgs :: any()) -> Result when
    Result :: {ok, pid()}
    | ignore
    | {error, Error},
    Error :: {already_started, pid()}
    | {shutdown, term()}
    | term().
start(_StartType, _StartArgs) ->
    {ok, NodeType} = application:get_env(?APP_NAME, node_type),
    oneprovider_node_sup:start_link(NodeType).

%%--------------------------------------------------------------------
%% @doc Stops application.
%%--------------------------------------------------------------------
-spec stop(_State :: any()) -> Result when
    Result :: ok.
stop(_State) ->
    ok.