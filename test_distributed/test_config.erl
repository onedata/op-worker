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

% init per suite functions
-export([
    set_onenv_scenario/2,
    add_envs/4
]).

% config getters
-export([
    get_providers/1, get_all_op_worker_nodes/1,
    get_provider_nodes/2, get_provider_spaces/2, get_provider_users/2,
    get_user_session_id_on_provider/3,
    get_custom/2, get_custom/3
]).

-export([
    set_many/2,
    set_custom/3
]).

-export([
    get_scenario/2,
    get_envs/1,
    set_onenv_script_path/2,
    get_onenv_script_path/1
]).

-type key() :: atom() | binary() | any().
-opaque config() :: kv_utils:nested(key(), any()).
-export_type([config/0]).

%%%===================================================================
%%% Init per suite config setters
%%%===================================================================

-spec set_onenv_scenario(config(), string()) -> config().
set_onenv_scenario(Config, ScenarioName) ->
    set_custom(Config, scenario, ScenarioName).

% fixme better types
-spec add_envs(config(), atom(), atom(), proplists:proplist()) -> config().
add_envs(Config, Component, Application, Envs) ->
    set_custom(Config, [custom_envs, Component, Application], Envs).


%%%===================================================================
%%% Config getters
%%%===================================================================

-spec get_providers(config()) -> [od_provider:id()].
get_providers(Config) ->
    get_custom(Config, providers).

-spec get_all_op_worker_nodes(config()) -> [node()].
get_all_op_worker_nodes(Config) ->
    get_custom(Config, op_worker_nodes).

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


%%%===================================================================
%%% Custom % fixme
%%%===================================================================

-spec get_custom(config(), key() | [key()]) -> any().
get_custom(Config, Key) ->
    kv_utils:get(Key, Config).

-spec get_custom(config(), key() | [key()], any()) -> any().
get_custom(Config, Key, Default) ->
    kv_utils:get(Key, Config, Default).


-spec set_custom(config(), key() | [key()], any()) -> config().
set_custom(Config, Key, Value) ->
    kv_utils:put(Key, Value, Config).


%%%===================================================================
%%% Config setters
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% fixme
% fixme bad spec
% Args without specified function is equivalent to {set_custom, Args}
% When FunctionName is omitted `set_custom` is assumed.
%% @end
%%--------------------------------------------------------------------
-spec set_many(config(), [{FunctionName :: atom(), Args :: [any()]}]) -> config().
set_many(Config, FunsAndArgs) ->
    lists:foldl(fun
        ({FunName, Args}, TmpConfig) when is_list(Args)-> erlang:apply(?MODULE, FunName, [TmpConfig | Args]);
        ({FunName, Arg}, TmpConfig) when not is_list(Arg)-> erlang:apply(?MODULE, FunName, [TmpConfig, Arg]);
        (Args, TmpConfig) when is_list(Args)-> erlang:apply(?MODULE, set_custom, [TmpConfig | Args])
    end, Config, FunsAndArgs).

%fixme docs, specs and reorganize

-spec get_scenario(config(), any()) -> any().
get_scenario(Config, Default) ->
    get_custom(Config, scenario, Default).

get_envs(Config) ->
    get_custom(Config, custom_envs, []).

get_onenv_script_path(Config) ->
    get_custom(Config, onenv_script).

set_onenv_script_path(Config, OnenvScript) ->
    set_custom(Config, onenv_script, OnenvScript).