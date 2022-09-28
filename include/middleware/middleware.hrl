%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO VFS-5621
%%% Common definitions concerning middleware.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(MIDDLEWARE_HRL).
-define(MIDDLEWARE_HRL, 1).

-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").


% Record expressing middleware request
-record(op_req, {
    auth = ?GUEST :: aai:auth(),
    gri :: gri:gri(),
    operation = create :: middleware:operation(),
    data = #{} :: middleware:data(),
    auth_hint = undefined :: undefined | middleware:auth_hint(),
    % applicable for create/get requests - returns the revision of resource
    return_revision = false :: boolean()
}).


-define(throw_on_error(__EXPR), middleware_utils:throw_if_error(__EXPR)).
-define(check(__EXPR), middleware_utils:check_result(__EXPR)).


%%%===================================================================
%%% Archives related operations available in middleware_worker
%%%===================================================================


-record(archives_list_request, {
    dataset_id :: dataset:id(),
    opts :: archives_list:opts(),
    mode = ?BASIC_INFO :: archive_api:listing_mode()
}).

-record(dataset_archive_request, {
    id :: dataset:id(),
    config :: archive:config(),
    preserved_callback :: archive:callback(),
    deleted_callback :: archive:callback(),
    description :: archive:description()
}).

-record(archivisation_cancel_request, {
    id :: archive:id(),
    preservation_policy :: archive:cancel_preservation_policy()
}).

-record(archive_info_get_request, {
    id :: archive:id()
}).

-record(archive_update_request, {
    id :: archive:id(),
    description :: archive:description() | undefined,
    diff :: archive:diff()
}).

-record(archive_delete_request, {
    id :: archive:id(),
    callback :: archive:callback()
}).

-record(archive_recall_request, {
    archive_id :: archive:id(),
    parent_directory_guid :: file_id:file_guid(),
    target_filename = default :: file_meta:name() | default
}).

-record(archive_recall_cancel_request, {
    id :: archive_recall:id()
}).

-record(archive_recall_details_get_request, {
    id :: archive_recall:id()
}).

-record(archive_recall_progress_get_request, {
    id :: archive_recall:id()
}).

-record(archive_recall_log_browse_request, {
    id :: archive_recall:id(),
    options :: audit_log_browse_opts:opts()
}).


%%%===================================================================
%%% Automation related operations available in middleware_worker
%%%===================================================================


-record(atm_workflow_execution_schedule_request, {
    atm_workflow_schema_id :: od_atm_workflow_schema:id(),
    atm_workflow_schema_revision_num :: atm_workflow_schema_revision:revision_number(),
    store_initial_content_overlay :: atm_workflow_execution_api:store_initial_content_overlay(),
    callback_url :: undefined | http_client:url()
}).

-record(atm_workflow_execution_cancel_request, {
    atm_workflow_execution_id :: atm_workflow_execution:id()
}).

-record(atm_workflow_execution_repeat_request, {
    type :: atm_workflow_execution:repeat_type(),
    atm_workflow_execution_id :: atm_workflow_execution:id(),
    atm_lane_run_selector :: atm_lane_execution:lane_run_selector()
}).


%%%===================================================================
%%% CDMI related operations available in middleware_worker
%%%===================================================================


-record(transfer_encoding_get_request, {}).

-record(transfer_encoding_set_request, {
    value :: binary()
}).

-record(cdmi_completion_status_get_request, {}).

-record(cdmi_completion_status_set_request, {
    value :: binary()
}).

-record(mimetype_get_request, {}).

-record(mimetype_set_request, {
    value :: binary()
}).


%%%===================================================================
%%% Datasets related operations available in middleware_worker
%%%===================================================================


-record(top_datasets_list_request, {
    state :: dataset:state(),
    opts :: dataset_api:listing_opts(),
    mode = ?BASIC_INFO :: dataset_api:listing_mode()
}).

-record(children_datasets_list_request, {
    id :: dataset:id(),
    opts :: dataset_api:listing_opts(),
    mode = ?BASIC_INFO :: dataset_api:listing_mode()
}).

-record(dataset_establish_request, {
    protection_flags = ?no_flags_mask :: data_access_control:bitmask()
}).

-record(dataset_info_get_request, {
    id :: dataset:id()
}).

-record(dataset_update_request, {
    id :: dataset:id(),
    state :: undefined | dataset:state(),
    flags_to_set = ?no_flags_mask :: data_access_control:bitmask(),
    flags_to_unset = ?no_flags_mask :: data_access_control:bitmask()
}).

-record(dataset_remove_request, {
    id :: dataset:id()
}).

-record(file_eff_dataset_summary_get_request, {}).


%%%===================================================================
%%% QoS related operations available in middleware_worker
%%%===================================================================


-record(qos_entry_add_request, {
    expression :: qos_expression:expression(),
    replicas_num :: qos_entry:replicas_num(),
    entry_type = user_defined :: qos_entry:type()
}).

-record(qos_entry_get_request, {
    id :: qos_entry:id()
}).

-record(qos_entry_remove_request, {
    id :: qos_entry:id()
}).

-record(effective_file_qos_get_request, {}).

-record(qos_status_check_request, {
    qos_id :: qos_entry:id()
}).


%%%===================================================================
%%% Shares related operations available in middleware_worker
%%%===================================================================


-record(share_create_request, {
    name :: od_share:name(),
    description :: od_share:description()
}).

-record(share_remove_request, {
    share_id :: od_share:id()
}).


%%%===================================================================
%%% Transfers related operations available in middleware_worker
%%%===================================================================


-record(file_transfer_schedule_request, {
    % meaning of fields in this record is explained in datastore_models.hrl
    % in definition of transfer record
    replicating_provider_id :: undefined | oneprovider:id(),
    evicting_provider_id :: undefined | oneprovider:id(),
    callback :: transfer:callback()
}).

-record(view_transfer_schedule_request, {
    % meaning of fields in this record is explained in datastore_models.hrl
    % in definition of transfer record
    replicating_provider_id :: undefined | oneprovider:id(),
    evicting_provider_id :: undefined | oneprovider:id(),
    view_name :: transfer:view_name(),
    query_view_params :: transfer:query_view_params(),
    callback :: transfer:callback()
}).


%%%===================================================================
%%% File metadata related operations available in middleware_worker
%%%===================================================================


-record(custom_metadata_get_request, {
    type :: custom_metadata:type(),
    query = [] :: custom_metadata:query(),
    inherited = false :: boolean()
}).

-record(custom_metadata_set_request, {
    type :: custom_metadata:type(),
    query = [] :: custom_metadata:query(),
    value :: term()
}).

-record(custom_metadata_remove_request, {
    type :: custom_metadata:type()
}).

-record(file_distribution_gather_request, {}).

-record(historical_dir_size_stats_gather_request, {
    request :: ts_browse_request:record()
}).

-record(file_storage_locations_get_request, {}).

-endif.
