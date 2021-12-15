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
    od_token,
    od_atm_inventory,
    od_atm_lambda,
    od_atm_workflow_schema,
    temporary_token_secret,
    provider_auth,
    offline_access_credentials,
    file_download_code,
    subscription,
    file_subscription,
    file_subscription_counter,
    session,
    session_local_links,
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
    file_popularity,
    space_transfer_stats,
    space_transfer_stats_cache,
    transfer,
    transferred_file,
    autocleaning,
    dir_location,
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
    qos_entry_audit_log,
    qos_status,
    qos_transfer_stats,
    file_meta_posthooks,
    storage_sync_links,
    storage_traverse_job,
    space_unsupport_job,
    luma_db,
    storage_import_config,
    storage_import_monitoring,
    process_handles,
    deletion_marker,
    tree_traverse_progress,
    files_counter,
    dataset,
    bulk_download_task,
    archive,
    workflow_engine_state,
    workflow_execution_state,
    workflow_cached_item,
    workflow_cached_async_result,
    workflow_iterator_snapshot,
    workflow_async_call_pool,

    % automation related models
    atm_store,
    atm_task_execution,
    atm_workflow_schema_snapshot,
    atm_lambda_snapshot,
    atm_workflow_execution,
    atm_tree_forest_iterator_queue,
    atm_openfaas_function_activity_registry,

    %% @TODO VFS-6767 deprecated, included for upgrade procedure. Remove in next major release after 21.02.*.
    space_strategies,
    storage_sync_monitoring
].

%%--------------------------------------------------------------------
%% @doc
%% Returns list of throttled datastore models.
%% @end
%%--------------------------------------------------------------------
-spec get_throttled_models() -> [datastore_model:model()].
get_throttled_models() ->
    [file_meta].
