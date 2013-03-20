%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of worker_host.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(worker_host_tests).
-include("registered_names.hrl").
-include("supervision_macros.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

%% ====================================================================
%% Test functions
%% ====================================================================

%% This test checks if worker_host is resistant to incorrect requests.
wrong_request_test() ->
	application:set_env(?APP_Name, node_type, worker), 
	ok = application:start(?APP_Name),

	Module = sample_plug_in,
	{ok, _ChildPid} = supervisor:start_child(?Supervisor_Name, ?Sup_Child(Module, worker_host, transient, [sample_plug_in, [], 10])),
	gen_server:cast(Module, abc),
	Reply = gen_server:call(Module, abc),
	?assert(Reply =:= wrong_request),

	ok = application:stop(?APP_Name).

%% This test checks if worker properly stores information about time used by
%% plug-in (veil module) to process requests. The tests checks not only if this
%% information is stored but also verifies if old information is correctly deleted
%% (to provide only latest data to ccm).
load_info_storing_test() ->
	ClientsNum = 50,
	application:set_env(?APP_Name, node_type, worker), 
	ok = application:start(?APP_Name),

	Module = sample_plug_in,
	{ok, _ChildPid} = supervisor:start_child(?Supervisor_Name, ?Sup_Child(Module, worker_host, transient, [sample_plug_in, [], 2 * ClientsNum])),
	
	startClients(ClientsNum, Module),
	timer:sleep(10 * ClientsNum),
	{Time, _Load} = gen_server:call(Module, getLoadInfo),
	{New, Old, NewListSize, Max} = gen_server:call(Module, getFullLoadInfo),
	?assert(NewListSize == ClientsNum),
	?assert(Max == 2* ClientsNum),
	?assert(length(New) == ClientsNum),
	?assert(Old =:= []),
	{ReqTime, _T} = lists:nth(ClientsNum, New),
	?assert(Time =:= ReqTime),

	startClients(ClientsNum, Module),
	timer:sleep(10 * ClientsNum),
	{Time2, _Load2} = gen_server:call(Module, getLoadInfo),
	{New2, Old2, NewListSize2, Max2} = gen_server:call(Module, getFullLoadInfo),
	?assert(NewListSize2 == 0),
	?assert(Max2 == 2* ClientsNum),
	?assert(length(New2) == 0),
	?assert(length(Old2) == 2* ClientsNum),
	{ReqTime2, _T2} = lists:last(Old2),
	?assert(Time2 =:= ReqTime2),
	?assert(Time2 =:= Time),

	startClients(3 * ClientsNum, Module),
	timer:sleep(30 * ClientsNum),
	{Time3, _Load3} = gen_server:call(Module, getLoadInfo),
	{New3, Old3, NewListSize3, Max3} = gen_server:call(Module, getFullLoadInfo),
	?assert(NewListSize3 == ClientsNum),
	?assert(Max3 == 2* ClientsNum),
	?assert(length(New3) == ClientsNum),
	?assert(length(Old3) == 2* ClientsNum),
	{ReqTime3, _T3} = lists:nth(ClientsNum, Old3),
	?assert(Time3 =:= ReqTime3),
	
	ok = gen_server:call(Module, clearLoadInfo),
	{_Time4, Load4} = gen_server:call(Module, getLoadInfo),
	?assert(Load4 == 0),
	{New4, Old4, NewListSize4, Max4} = gen_server:call(Module, getFullLoadInfo),
	?assert(NewListSize4 == 0),
	?assert(Max4 == 2* ClientsNum),
	?assert(New4 =:= []),
	?assert(Old4 =:= []),

	ok = application:stop(?APP_Name).

%% ====================================================================
%% Helper functions
%% ====================================================================

startClients(ProcNum, Module) ->
	for(1, ProcNum, fun() -> spawn(fun() -> gen_server:cast(Module, {asynch, 1, sample_message}) end) end).

for(N, N, F) -> [F()];
for(I, N, F) -> [F()|for(I+1, N, F)].

-endif.
