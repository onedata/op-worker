%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines datastore config related to op_worker.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_config_plugin).
-author("Michal Zmuda").

-behaviour(datastore_config_behaviour).

-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").

%% datastore_config_behaviour callbacks
-export([models/0, throttled_models/0, get_mutator/0]).

%%--------------------------------------------------------------------
%% @doc
%% {@link datastore_config_behaviour} callback models/0.
%% @end
%%--------------------------------------------------------------------
-spec models() -> Models :: [model_behaviour:model_type()].
models() -> [
    od_user,
    od_group,
    od_space,
    od_share,
    od_provider,
    od_handle_service,
    od_handle,
    subscriptions_state,
    subscription,
    file_subscription,
    session,
    user_identity,
    file_meta,
    storage,
    file_location,
    space_storage,
    dbsync_state2,
    files_to_chown,
    space_quota,
    monitoring_state,
    file_handles,
    sfm_handle,
    dbsync_batches,
    custom_metadata,
    indexes,
    permissions_cache,
    permissions_cache_helper,
    times,
    helper_handle,
    space_strategies,
    file_force_proxy,
    reverse_luma,
    luma,
    file_popularity
].

%%--------------------------------------------------------------------
%% @doc
%% {@link datastore_config_behaviour} callback throttled_models/0.
%% @end
%%--------------------------------------------------------------------
-spec throttled_models() -> Models :: [model_behaviour:model_type()].
throttled_models() -> [file_meta].

%%--------------------------------------------------------------------
%% @doc
%% {@link datastore_config_behaviour} callback get_mutator/0.
%% @end
%%--------------------------------------------------------------------
-spec get_mutator() -> datastore:mutator() | undefined.
get_mutator() ->
    oneprovider:get_provider_id().