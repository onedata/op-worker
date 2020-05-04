%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% fixme move to ctool
%%% @end
%%%-------------------------------------------------------------------
-module(test_config_utils).
-author("Michal Stanisz").

%% API
-export([
    set_onenv_scenario/2,
    add_envs/4
]).

-export([
    get_providers/1, get_all_op_worker_nodes/1,
    get_provider_nodes/2, get_provider_spaces/2, get_provider_users/2,
    get_user_session_id_on_provider/3
]).

% Init per suite functions

set_onenv_scenario(Config, ScenarioName) ->
    kv_utils:put(scenario, ScenarioName, Config).

add_envs(Config, Component, Application, Envs) ->
    kv_utils:put([custom_envs, Component, Application], Envs, Config).


% Test functions

get_providers(Config) ->
    kv_utils:get(providers, Config).

get_all_op_worker_nodes(Config) ->
    kv_utils:get(op_worker_nodes, Config).

get_provider_nodes(Config, ProviderId) ->
    kv_utils:get([provider_nodes, ProviderId], Config).

get_provider_spaces(Config, ProviderId) ->
    kv_utils:get([provider_spaces, ProviderId], Config).

get_provider_users(Config, ProviderId) ->
    kv_utils:get([users, ProviderId], Config).

get_user_session_id_on_provider(Config, User, ProviderId) ->
    kv_utils:get([sess_id, ProviderId, User], Config).
