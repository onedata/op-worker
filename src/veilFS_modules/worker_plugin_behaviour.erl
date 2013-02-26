%% @author Michal Wrzeszcz

-module(worker_plugin_behaviour).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{init,1},
     {handle, 3},
     {cleanUp, 1}];
behaviour_info(_Other) ->
    undefined.

%% ====================================================================
%% Dunctions description
%% ====================================================================

%% init/1
%% ====================================================================
%% Function: init(Args) -> {ok, State} | {error,Error}
%% Description: Initialize module
%% ====================================================================

%% handle/1
%% ====================================================================
%% Function: handle(ProtocolVersion, Request, State) -> {ok, NewState} | {error,Error}
%% Description: Do your work.
%% ====================================================================

%% cleanUp/1
%% ====================================================================
%% Function: cleanUp(State) -> ok | {error,Error}
%% Description: The module will not be used anymore. Clean up!
%% ====================================================================