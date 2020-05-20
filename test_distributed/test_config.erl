%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% This module is responsible for manipulation of config property in tests. 
%%% fixme move to ctool
%%% @end
%%%-------------------------------------------------------------------
-module(test_config).
-author("Michal Stanisz").

-include_lib("ctool/include/test/test_utils.hrl").

% init per suite config setters
-export([
    set_onenv_scenario/2,
    add_envs/4,
    set_posthook/2
]).

% test config getters
-export([
    get_providers/1, get_all_op_worker_nodes/1,
    get_provider_nodes/2, get_provider_spaces/2, get_provider_users/2,
    get_user_session_id_on_provider/3,
    get_custom/2, get_custom/3
]).

% config setters
-export([
    set_many/2,
    set_custom/3
]).

% environment setup API
-export([
    set_project_root_path/2,
    get_project_root_path/1,
    set_onenv_script_path/2,
    get_onenv_script_path/1,
    get_scenario/1,
    get_custom_envs/1
]).

-type key() :: atom() | binary() | any().
-type service_as_list() :: string(). % "op_worker" | "oz_worker" | "op_panel" | "oz_panel" | "cluster_manager"
-type service() :: onedata:service() | cluster_manager.
-opaque config() :: kv_utils:nested(key(), any()).

-export_type([config/0, service_as_list/0]).

%%%===================================================================
%%% Init per suite config setters
%%%===================================================================

-spec set_onenv_scenario(config(), string()) -> config().
set_onenv_scenario(Config, ScenarioName) ->
    set_custom(Config, scenario, ScenarioName).

-spec add_envs(config(), service(), atom(), proplists:proplist()) -> config().
add_envs(Config, Service, Application, Envs) ->
    set_custom(Config, [custom_envs, Service, Application], Envs).

-spec set_posthook(test_config:config(), fun((test_config:config()) -> test_config:config())) -> 
    test_config:config().
set_posthook(Config, Posthook) ->
    set_custom(Config, [?ENV_UP_POSTHOOK], Posthook).


%%%===================================================================
%%% Test config getters
%%%===================================================================

-spec get_providers(config()) -> [od_provider:id()].
get_providers(Config) ->
    get_custom(Config, providers).

-spec get_all_op_worker_nodes(config()) -> [node()].
get_all_op_worker_nodes(Config) ->
    get_custom(Config, op_worker_nodes, []).

-spec get_provider_nodes(config(), od_provider:id()) -> [node()].
get_provider_nodes(Config, ProviderId) ->
    get_custom(Config, [provider_nodes, ProviderId]).

-spec get_provider_spaces(config(), od_provider:id()) -> [od_space:id()].
get_provider_spaces(Config, ProviderId) ->
    get_custom(Config, [provider_spaces, ProviderId]).

-spec get_provider_users(config(), od_provider:id()) -> [od_user:id()].
get_provider_users(Config, ProviderId) ->
    get_custom(Config, [users, ProviderId]).

-spec get_user_session_id_on_provider(config(), od_user:id(), od_provider:id()) -> session:id().
get_user_session_id_on_provider(Config, User, ProviderId) ->
    get_custom(Config, [sess_id, ProviderId, User]).

-spec get_custom(config(), key() | [key()]) -> any().
get_custom(Config, Key) ->
    kv_utils:get(Key, Config).

-spec get_custom(config(), key() | [key()], any()) -> any().
get_custom(Config, Key, Default) ->
    kv_utils:get(Key, Config, Default).


%%%===================================================================
%%% Config setters
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Allows for executing multiple setter functions in one call.
%% When FunctionName is omitted `set_custom` is assumed.
%% @end
%%--------------------------------------------------------------------
-spec set_many(config(), [{FunctionName :: atom(), Args :: [any()]}]) -> config().
set_many(Config, FunsAndArgs) ->
    lists:foldl(fun
        ({FunName, Args}, TmpConfig) when is_list(Args)-> erlang:apply(?MODULE, FunName, [TmpConfig | Args]);
        ({FunName, Arg}, TmpConfig) when not is_list(Arg)-> erlang:apply(?MODULE, FunName, [TmpConfig, Arg]);
        ([Key, Value], TmpConfig) -> erlang:apply(?MODULE, set_custom, [TmpConfig, Key, Value]) 
    end, Config, FunsAndArgs).

-spec set_custom(config(), key() | [key()], any()) -> config().
set_custom(Config, Key, Value) ->
    kv_utils:put(Key, Value, Config).

%%%===================================================================
%%% Environment setup API
%%%===================================================================

-spec set_project_root_path(test_config:config(), string()) -> test_config:config().
set_project_root_path(Config, ProjectRoot) ->
    set_custom(Config, project_root, ProjectRoot).

-spec get_project_root_path(test_config:config()) -> string().
get_project_root_path(Config) ->
    get_custom(Config, project_root).

-spec set_onenv_script_path(test_config:config(), string()) -> test_config:config().
set_onenv_script_path(Config, OnenvScript) ->
    set_custom(Config, onenv_script, OnenvScript).

-spec get_onenv_script_path(test_config:config()) -> string().
get_onenv_script_path(Config) ->
    get_custom(Config, onenv_script).

-spec get_scenario(config()) -> any().
get_scenario(Config) ->
    get_custom(Config, scenario).

-spec get_custom_envs(test_config:config()) -> proplists:proplist().
get_custom_envs(Config) ->
    get_custom(Config, custom_envs, []).
