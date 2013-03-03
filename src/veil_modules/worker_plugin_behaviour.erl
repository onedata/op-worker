%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: It is the behaviour of each application component. All 
%% components defined in subdirectories of veil_modules must implement it.
%% @end
%% ===================================================================

-module(worker_plugin_behaviour).

-export([behaviour_info/1]).

%% behaviour_info/1
%% ====================================================================
%% @doc Defines behaviour
-spec behaviour_info(Arg) -> Result when
	Arg :: callbacks | Other,
	Result ::  [Fun_def] 
			| undefined,
	Fun_def :: tuple(),
	Other :: any().
%% ====================================================================
behaviour_info(callbacks) ->
    [{init,1},
     {handle, 2},
     {cleanUp, 0}];
behaviour_info(_Other) ->
    undefined.

%% ====================================================================
%% Functions description
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