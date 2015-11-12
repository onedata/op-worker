%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines config of the datastore. It contains default
%%% (required by infrastructure) config, which is complemented by
%%% information from datastore_config_plugin.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_config).
-author("Michal Zmuda").

-behaviour(datastore_config_behaviour).

-define(DATASTORE_CONFIG_PLUGIN, datastore_config_plugin).

%% datastore_config_behaviour callbacks
-export([models/0, global_caches/0, local_caches/0]).

%%--------------------------------------------------------------------
%% @doc
%% List of models used.
%% @end
%%--------------------------------------------------------------------
-spec models() -> Models :: [model_behaviour:model_type()].
models() -> [
  some_record,
  cache_controller,
  task_pool
] ++ ?DATASTORE_CONFIG_PLUGIN:models().

%%--------------------------------------------------------------------
%% @doc
%% List of models cached globally.
%% @end
%%--------------------------------------------------------------------
-spec global_caches() -> Models :: [model_behaviour:model_type()].
global_caches() -> [
  some_record
] ++ ?DATASTORE_CONFIG_PLUGIN:global_caches().

%%--------------------------------------------------------------------
%% @doc
%% List of models cached locally.
%% @end
%%--------------------------------------------------------------------
-spec local_caches() -> Models :: [model_behaviour:model_type()].
local_caches() ->
  [] ++ ?DATASTORE_CONFIG_PLUGIN:local_caches().