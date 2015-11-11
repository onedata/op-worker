%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% todo
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_config_plugin).
-author("Michal Zmuda").

-behaviour(datastore_config_behaviour).

%% datastore_config_behaviour callbacks
-export([models/0, global_caches/0, local_caches/0]).

%%--------------------------------------------------------------------
%% @doc
%% todo
%% @end
%%--------------------------------------------------------------------
-spec models() -> Models :: [model_behaviour:model_type()].
models() -> [
  subscription,
  session,
  onedata_user,
  identity,
  file_meta,
  storage,
  file_location,
  file_watcher
].

%%--------------------------------------------------------------------
%% @doc
%% todo
%% @end
%%--------------------------------------------------------------------
-spec global_caches() -> Models :: [model_behaviour:model_type()].
global_caches() -> [
  file_meta,
  storage,
  file_location,
  file_watcher
].

%%--------------------------------------------------------------------
%% @doc
%% todo
%% @end
%%--------------------------------------------------------------------
-spec local_caches() -> Models :: [model_behaviour:model_type()].
local_caches() ->
  [].