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
    od_harvester,
    od_storage,
    provider_auth,
    file_download_code,
    subscription,
    file_subscription,
    session,
    user_identity,
    file_meta,
    storage_config,
    file_location,
    file_local_blocks,
    dbsync_state,
    files_to_chown,
    space_quota,
    monitoring_state,
    file_handles,
    sd_handle,
    custom_metadata,
    permissions_cache,
    permissions_cache_helper,
    permissions_cache_helper2,
    times,
    helper_handle,
    space_strategies,
    luma_cache,
    file_popularity,
    space_transfer_stats,
    space_transfer_stats_cache,
    transfer,
    transferred_file,
    autocleaning,
    dir_location,
    storage_sync_monitoring,
    storage_sync_info,
    replica_deletion,
    replica_deletion_lock,
    index,
    autocleaning_run,
    file_popularity_config,
    harvesting_state,
    idp_access_token,
    tree_traverse_job,
    file_qos,
    qos_entry,
    file_meta_posthooks,
    storage_sync_links,
    storage_traverse_job
].

%%--------------------------------------------------------------------
%% @doc
%% Returns list of throttled datastore models.
%% @end
%%--------------------------------------------------------------------
-spec get_throttled_models() -> [datastore_model:model()].
get_throttled_models() ->
    [file_meta].