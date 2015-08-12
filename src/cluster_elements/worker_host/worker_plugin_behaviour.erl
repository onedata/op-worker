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

%%--------------------------------------------------------------------
%% @doc
%% Initialize module
%% @end
%%--------------------------------------------------------------------
-callback init(Args :: term()) ->
    {ok, State :: worker_host:plugin_state()} | {error, Reason :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% Do your work.
%% @end
%%--------------------------------------------------------------------
-callback handle(Request :: term()) ->
    nagios_handler:healthcheck_response() | ok | pong | {ok, Answer :: term()} |
    {error, Reason :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% The module will not be used anymore. Clean up!
%% @end
%%--------------------------------------------------------------------
-callback cleanup() -> ok | {error, Reason :: term()}.
