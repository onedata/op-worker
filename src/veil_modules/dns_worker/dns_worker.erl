%% ===================================================================
%% @author Bartosz Polnik
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements {@link worker_plugin_behaviour} to provide
%% functionality of node to ip resolution.
%% @end
%% ===================================================================

-module(dns_worker).
-behaviour(worker_plugin_behaviour).
-include("records.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).


%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1
-spec init(Args :: term()) -> Result when
	Result :: #dns_worker_state{} | {error, Error},
	Error :: term().
%% ====================================================================
init([]) ->
	#dns_worker_state{workers_list = []};

init(InitialState) when is_record(InitialState, dns_worker_state) ->
	InitialState;

init(_) ->
	{error, unknown_initial_argument}.


%% handle/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1
-spec handle(ProtocolVersion :: term(), Request) -> Result when
	Request :: ping | get_version,
	Result :: ok | {ok, Response} | {error, Error} | pong | Version,
	Response :: term(),
	Version :: term(),
	Error :: term().
%% ====================================================================

handle(_ProtocolVersion, ping) ->
	pong;

handle(_ProtocolVersion, get_version) ->
	node_manager:check_vsn();

handle(_ProtocolVersion, {update_state, ModulesToNodes}) ->
	New_DNS_State = #dns_worker_state{workers_list = ModulesToNodes},
	ok = gen_server:call(?MODULE, {updatePlugInState, New_DNS_State});

handle(_ProtocolVersion, {get_worker, Module}) ->
	DNS_State = gen_server:call(?MODULE, getPlugInState),
	WorkerList = DNS_State#dns_worker_state.workers_list,
	Result = proplists:get_value(Module, WorkerList, []),
	{ok, Result};

handle(_ProtocolVersion, _Msg) ->
	ok.

%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0
-spec cleanup() -> Result when
	Result :: ok | {error, Error},
	Error :: timeout | term().
%% ====================================================================
cleanup() ->
	ok.
