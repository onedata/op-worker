%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides datastore config.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_config_plugin).
-author("Michal Zmuda").

%% datastore_config callbacks
-export([get_models/0, get_throttled_models/0]).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of datastore custom models.
%% @end
%%--------------------------------------------------------------------
-spec get_models() -> [datastore_model:model()].
get_models() -> [
    od_user,
    od_group,
    od_space,
    od_share,
    od_provider,
    od_handle_service,
    od_handle,
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
    space_quota,
    monitoring_state,
    file_handles,
    sfm_handle,
    custom_metadata,
    indexes,
    permissions_cache,
    permissions_cache_helper,
    times,
    helper_handle,
    space_strategies,
    file_force_proxy,
    luma_cache,
    file_popularity,
    transfer,
    storage_sync_histogram
].

%%--------------------------------------------------------------------
%% @doc
%% Returns list of throttled datastore models.
%% @end
%%--------------------------------------------------------------------
-spec get_throttled_models() -> [datastore_model:model()].
get_throttled_models() ->
    [file_meta].