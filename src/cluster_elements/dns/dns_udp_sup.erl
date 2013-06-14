%% ===================================================================
%% @author Bartosz Polnik
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: The main responsibility of this module is to sustain udp listener for dns.
%% @end
%% ===================================================================

-module(dns_udp_sup).
-include("registered_names.hrl").
-include("supervision_macros.hrl").
-behaviour(supervisor).

%% API
-export([start_link/0, env_dependencies/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

env_dependencies() ->
	[{dns_port, integer},
     {dns_response_ttl, integer},
	 {dispatcher_timeout, integer}].

%% start_link/1
%% ====================================================================
%% @doc Starts udp listener supervisor.
-spec start_link() -> Result when
	Result ::  {ok, pid()}
	| ignore
	| {error, Error},
	Error :: {already_started, pid()}
	| {shutdown, term()}
	| term().
%% ====================================================================
start_link() ->
	supervisor:start_link(?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc Reads udp listener variables and returns child specification based on those data.
-spec init([]) -> Result when
	Result :: {ok, {SupervisionPolicy, [ChildSpec]}},
	SupervisionPolicy :: {RestartStrategy, MaxR :: non_neg_integer(), MaxT :: pos_integer()},
	RestartStrategy :: one_for_all
	| one_for_one
	| rest_for_one
	| simple_one_for_one,
	ChildSpec :: {Id :: term(), StartFunc, RestartPolicy, Type :: worker | supervisor, Modules},
	StartFunc :: {M :: module(), F :: atom(), A :: [term()] | undefined},
	RestartPolicy :: permanent
	| transient
	| temporary,
	Modules :: [module()] | dynamic.
%% ====================================================================
init([]) ->
	{ok, DNSPort} = application:get_env(?APP_Name, dns_port),
	{ok, DNSResponseTTL} = application:get_env(?APP_Name, dns_response_ttl),
	{ok, DispatcherTimeout} = application:get_env(?APP_Name, dispatcher_timeout),

	Childs = [?Sup_Child(dns_udp_acceptor, dns_udp_handler, start_link, permanent,
						[DNSPort, DNSResponseTTL, DispatcherTimeout])],

	{ok, {{one_for_one, 10, 5}, Childs}}.