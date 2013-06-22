%% ===================================================================
%% @author Bartosz Polnik
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module is used only for mocking purposes.
%% @end
%% ===================================================================
-module(dns_internal_sample_plugin).
-behaviour(worker_plugin_behaviour).



%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).


%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1.
%% @end
%% ====================================================================
-spec init(Args :: term()) -> Result when
	Result :: ok | {error, Error},
	Error :: term().
%% ====================================================================
init(ok) ->
	ok.

%% handle/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1.
%% @end
%% ====================================================================
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
	node_manager:check_vsn().

%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0.
%% @end
%% ====================================================================
-spec cleanup() -> Result when
	Result :: ok | {error, Error},
	Error :: timeout | term().
%% ====================================================================
cleanup() ->
	ok.
