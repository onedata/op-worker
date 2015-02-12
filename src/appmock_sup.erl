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

-behaviour(supervisor).

-include("appmock_internal.hrl").

%% API
-export([start_link/0, clean_up/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

clean_up() ->
    appmock_logic:terminate().

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, AppDescriptionFile} = application:get_env(?APP_NAME, app_description_file),
    ok = appmock_logic:initialize(AppDescriptionFile),
    {ok, { {one_for_one, 5, 10}, []} }.

