%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% It is the behaviour of each application component. All
%%% components defined in subdirectories of oneprovider_modules must implement it.
%%% @end
%%%-------------------------------------------------------------------
-module(worker_plugin_behaviour).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").

%%--------------------------------------------------------------------
%% @doc
%% Initialize module
%% @end
%%--------------------------------------------------------------------
-callback init(Args :: term()) -> {ok, State :: term()} | {error, Error :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% Do your work.
%% @end
%%--------------------------------------------------------------------
-callback handle(Request :: term(), State :: term()) ->
    healthcheck_reponse() | ok | pong | {ok, Answer :: term()} | 
    {error, Error :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% The module will not be used anymore. Clean up!
%% @end
%%--------------------------------------------------------------------
-callback cleanup() -> ok | {error, Error :: term()}.
