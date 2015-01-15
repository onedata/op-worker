%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: It is the behaviour of each application component. All
%% components defined in subdirectories of oneprovider_modules must implement it.
%% @end
%% ===================================================================
-module(worker_plugin_behaviour).

%% init/1
%% ====================================================================
%% Description: Initialize module
%% ====================================================================
-callback init(Args :: term()) -> ok | {error, Error :: any()}.

%% handle/2
%% ====================================================================
%% Description: Do your work.
%% ====================================================================
-callback handle(ProtocolVersion :: term(), Request :: term()) ->
    {ok, Ans :: term()} | {error, Error :: any()}.

%% cleanup/0
%% ====================================================================
%% Description: The module will not be used anymore. Clean up!
%% ====================================================================
-callback cleanup() -> ok | {error, Error :: any()}.
