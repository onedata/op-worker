%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc It is the main module of application. It lunches
%%% supervisor which then initializes appropriate components of node.
%%% @end
%%%--------------------------------------------------------------------
-module(op_worker_app).
-author("Michal Wrzeszcz").

-behaviour(application).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts application by supervisor initialization.
%% @end
%%--------------------------------------------------------------------
-spec start(StartType :: application:start_type(), StartArgs :: term()) ->
    {ok, Pid :: pid()} | {ok, Pid :: pid(), State :: term()} |
    {error, Reason ::term()}.
start(_StartType, _StartArgs) ->
    {error, not_supported}.

%%--------------------------------------------------------------------
%% @doc
%% Stops application.
%% @end
%%--------------------------------------------------------------------
-spec stop(State :: term()) -> ok.
stop(_State) ->
    ok.
