%% @author Michal
%% @doc @todo Add description to sample_plug_in.

-module(sample_plug_in).
-behaviour(worker_plugin_behaviour).

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanUp/0]).

init(_Args) ->
	[].

handle(_ProtocolVersion, _Msg) ->
	ok.

cleanUp() ->
	ok.

%% ====================================================================
%% Internal functions
%% ====================================================================


