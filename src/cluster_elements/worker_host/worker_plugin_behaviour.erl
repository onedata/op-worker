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
-callback init(Args :: term()) -> {ok, State :: term()} | {error, Error :: any()}.

%%--------------------------------------------------------------------
%% @doc
%% Do your work.
%% @end
%%--------------------------------------------------------------------
-callback handle(Request :: term(), State :: term()) ->
    ok | {ok, Ans :: term()} | {error, Error :: any()}.

%%--------------------------------------------------------------------
%% @doc
%% The module will not be used anymore. Clean up!
%% @end
%%--------------------------------------------------------------------
-callback cleanup() -> ok | {error, Error :: any()}.
