%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% CT hook responsible for starting test environment.
% fixme move to ctool
%%% @end
%%%-------------------------------------------------------------------
-module(cth_onenv_up).
-author("Michal Stanisz").

%% API
%% CTH callback
%% initialization
-export([init/2]).
%% posthooks
-export([post_init_per_suite/4, post_end_per_suite/4]).

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/logging.hrl").

-record(state, {disabled=false}).
-type state() :: #state{}.

%%--------------------------------------------------------------------
%% @doc
%% CTH callback called when hook is being installed.
%% @end
%%--------------------------------------------------------------------
-spec init(_Id :: term(), _Opts :: term()) -> {ok, state(), non_neg_integer()}.
init(_Id, _Opts) ->
    {ok, #state{}, ?CTH_ENV_UP_PRIORITY}.


%%--------------------------------------------------------------------
%% @doc
%% CTH callback called after init_per_suite. Starts test environment.
%% Name of test environment yaml file should be provided in Config 
%% by calling test_config:set_onenv_scenario/2 in init_per_suite. 
%% Given file must exist in test_distributed/onenv_scenarios.
%% If you intend to perform some initialization after environment is up,
%% pass fun(Config) -> ...end under key ?ENV_UP_POSTHOOK to Config.
%% @end
%%--------------------------------------------------------------------
-spec post_init_per_suite(Suite :: atom(), _Config :: [term()], Return :: [term()],
    State :: state()) -> {[term()], state()}.
post_init_per_suite(Suite, _Config, Return, State) ->
    ct:pal("Environment initialization in ~p", [Suite]),
    NewConfig = test_onenv_starter:prepare_test_environment(Return, Suite),
    {NewConfig, State}.


%%--------------------------------------------------------------------
%% @doc
%% CTH callback called after end_per_suite.
%% Cleans environment used in given test suite.
%% @end
%%--------------------------------------------------------------------
-spec post_end_per_suite(Suite :: atom(), Config :: [term()], Return :: term(),
    State :: state()) -> {[term()], state()}.
post_end_per_suite(Suite, Config, Return, State) ->
    ct:pal("Environment cleaning in ~p", [Suite]),
    test_onenv_starter:clean_environment(Config),
    {Return, State}.

