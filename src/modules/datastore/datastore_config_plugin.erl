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

%% datastore_config_behaviour callbacks
-export([models/0, throttled_models/0]).

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
    dbsync_state,
    files_to_chown,
    message_id,
    space_quota,
    monitoring_state,
    file_handles,
    sfm_handle,
    dbsync_batches,
    custom_metadata,
    indexes,
    file_consistency,
    permissions_cache,
    permissions_cache_helper,
    change_propagation_controller,
    times,
    helper_handle,
    space_strategies,
    file_force_proxy
].

%%--------------------------------------------------------------------
%% @doc
%% {@link datastore_config_behaviour} callback throttled_models/0.
%% @end
%%--------------------------------------------------------------------
-spec throttled_models() -> Models :: [model_behaviour:model_type()].
throttled_models() -> [file_meta].
