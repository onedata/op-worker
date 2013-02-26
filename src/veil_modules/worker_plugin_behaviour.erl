%% @author Michal Wrzeszcz

-module(worker_plugin_behaviour).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{init,1},
     {handle, 2},
     {cleanUp, 0}];
behaviour_info(_Other) ->
    undefined.

%% ====================================================================
%% Dunctions description
%% ====================================================================

%% init/1
%% ====================================================================
%% Function: init(Args) -> ok | {error,Error}
%% Description: Initialize module
%% ====================================================================

%% handle/2
%% ====================================================================
%% Function: handle(ProtocolVersion, Request) -> {ok, Ans} | {error,Error}
%% Description: Do your work.
%% ====================================================================

%% cleanUp/0
%% ====================================================================
%% Function: cleanUp() -> ok | {error,Error}
%% Description: The module will not be used anymore. Clean up!
%% ====================================================================