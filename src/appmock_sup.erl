%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements OTP supervisor behaviour.
%%% @end
%%%-------------------------------------------------------------------
-module(appmock_sup).
-author("Lukasz Opiola").
-behaviour(supervisor).

-include("appmock_internal.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, {{one_for_one, 5, 10}, [
        {mock_resp_server, {mock_resp_server, start_link, []}, permanent, 5000, worker, [mock_resp_server]},
        {remote_control_server, {remote_control_server, start_link, []}, permanent, 5000, worker, [remote_control_server]}
    ]}}.

