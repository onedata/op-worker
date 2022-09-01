%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module routes middleware operations to corresponding handler modules.
%%% @end
%%%-------------------------------------------------------------------
-module(middleware_worker_handlers).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([execute/3]).


%%%===================================================================
%%% API
%%%===================================================================

%% Archives

-spec execute(
    user_ctx:ctx(), 
    file_ctx:ctx(), 
    middleware_worker:operation()
) ->
    ok | {ok, term()} | no_return().
execute(UserCtx, SpaceDirCtx, #archives_list_request{
    dataset_id = DatasetId,
    opts = Opts,
    mode = ListingMode
}) ->
    dataset_req:list_archives(SpaceDirCtx, DatasetId, Opts, ListingMode, UserCtx);

execute(UserCtx, SpaceDirCtx, #dataset_archive_request{
    id = DatasetId,
    config = Config,
    preserved_callback = PreservedCallback,
    deleted_callback = DeletedCallback,
    description = Description
}) ->
    dataset_req:create_archive(
        SpaceDirCtx, DatasetId, Config, PreservedCallback, DeletedCallback, Description, UserCtx
    );

execute(UserCtx, SpaceDirCtx, #archivisation_cancel_request{id = ArchiveId}) ->
    dataset_req:cancel_archivisation(SpaceDirCtx, ArchiveId, UserCtx);

execute(UserCtx, SpaceDirCtx, #archive_info_get_request{id = ArchiveId}) ->
    dataset_req:get_archive_info(SpaceDirCtx, ArchiveId, UserCtx);

execute(UserCtx, SpaceDirCtx, #archive_update_request{id = ArchiveId, diff = Diff}) ->
    dataset_req:update_archive(SpaceDirCtx, ArchiveId, Diff, UserCtx);

execute(UserCtx, SpaceDirCtx, #archive_delete_request{id = ArchiveId, callback = CallbackUrl}) ->
    dataset_req:init_archive_delete(SpaceDirCtx, ArchiveId, CallbackUrl, UserCtx);

execute(UserCtx, SpaceDirCtx, #archive_recall_request{
    archive_id = ArchiveId, parent_directory_guid = ParentDirectoryGuid, target_filename = TargetName}
) ->
    dataset_req:init_archive_recall(SpaceDirCtx, ArchiveId, ParentDirectoryGuid, TargetName, UserCtx);

execute(UserCtx, FileCtx, #archive_recall_cancel_request{id = Id}) ->
    dataset_req:cancel_archive_recall(FileCtx, Id, UserCtx);

execute(UserCtx, FileCtx, #archive_recall_details_get_request{id = Id}) ->
    dataset_req:get_archive_recall_details(FileCtx, Id, UserCtx);

execute(UserCtx, FileCtx, #archive_recall_progress_get_request{id = Id}) ->
    dataset_req:get_archive_recall_progress(FileCtx, Id, UserCtx);

execute(UserCtx, FileCtx, #archive_recall_log_browse_request{id = Id, options = Options}) ->
    dataset_req:browse_archive_recall_log(FileCtx, Id, UserCtx, Options);


%% Automation

execute(UserCtx, SpaceDirCtx, #atm_workflow_execution_schedule_request{
    atm_workflow_schema_id = AtmWorkflowSchemaId,
    atm_workflow_schema_revision_num = AtmWorkflowSchemaRevisionNum,
    store_initial_content_overlay = AtmStoreInitialContentOverlay,
    callback_url = CallbackUrl
}) ->
    {ok, atm_workflow_execution_api:schedule(
        UserCtx, file_ctx:get_space_id_const(SpaceDirCtx),
        AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum,
        AtmStoreInitialContentOverlay, CallbackUrl
    )};

execute(_UserCtx, _SpaceDirCtx, #atm_workflow_execution_cancel_request{
    atm_workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ok = atm_workflow_execution_api:cancel(AtmWorkflowExecutionId);

execute(UserCtx, _SpaceDirCtx, #atm_workflow_execution_repeat_request{
    type = Type,
    atm_workflow_execution_id = AtmWorkflowExecutionId,
    atm_lane_run_selector = AtmLaneRunSelector
}) ->
    ok = atm_workflow_execution_api:repeat(
        UserCtx, Type, AtmLaneRunSelector, AtmWorkflowExecutionId
    );


%% CDMI

execute(UserCtx, FileCtx, #transfer_encoding_get_request{}) ->
    cdmi_metadata_req:get_transfer_encoding(UserCtx, FileCtx);

execute(UserCtx, FileCtx, #transfer_encoding_set_request{value = Encoding}) ->
    cdmi_metadata_req:set_transfer_encoding(UserCtx, FileCtx, Encoding, false, false);

execute(UserCtx, FileCtx, #cdmi_completion_status_get_request{}) ->
    cdmi_metadata_req:get_cdmi_completion_status(UserCtx, FileCtx);

execute(UserCtx, FileCtx, #cdmi_completion_status_set_request{value = CompletionStatus}) ->
    cdmi_metadata_req:set_cdmi_completion_status(UserCtx, FileCtx, CompletionStatus, false, false);

execute(UserCtx, FileCtx, #mimetype_get_request{}) ->
    cdmi_metadata_req:get_mimetype(UserCtx, FileCtx);

execute(UserCtx, FileCtx, #mimetype_set_request{value = CompletionStatus}) ->
    cdmi_metadata_req:set_mimetype(UserCtx, FileCtx, CompletionStatus, false, false);


%% Datasets

execute(UserCtx, SpaceDirCtx, #top_datasets_list_request{state = State, opts = Opts, mode = ListingMode}) ->
    SpaceId = file_ctx:get_space_id_const(SpaceDirCtx),
    dataset_req:list_top_datasets(SpaceId, State, Opts, ListingMode, UserCtx);

execute(UserCtx, SpaceDirCtx, #children_datasets_list_request{
    id = DatasetId,
    opts = Opts,
    mode = ListingMode
}) ->
    dataset_req:list_children_datasets(SpaceDirCtx, DatasetId, Opts, ListingMode, UserCtx);

execute(UserCtx, FileCtx, #dataset_establish_request{protection_flags = ProtectionFlags}) ->
    dataset_req:establish(FileCtx, ProtectionFlags, UserCtx);

execute(UserCtx, SpaceDirCtx, #dataset_info_get_request{id = DatasetId}) ->
    dataset_req:get_info(SpaceDirCtx, DatasetId, UserCtx);

execute(UserCtx, SpaceDirCtx, #dataset_update_request{
    id = DatasetId,
    state = NewState,
    flags_to_set = FlagsToSet,
    flags_to_unset = FlagsToUnset
}) ->
    dataset_req:update(SpaceDirCtx, DatasetId, NewState, FlagsToSet, FlagsToUnset, UserCtx);

execute(UserCtx, SpaceDirCtx, #dataset_remove_request{id = DatasetId}) ->
    dataset_req:remove(SpaceDirCtx, DatasetId, UserCtx);

execute(UserCtx, FileCtx, #file_eff_dataset_summary_get_request{}) ->
    dataset_req:get_file_eff_summary(FileCtx, UserCtx);


%% File metadata

execute(UserCtx, FileCtx, #custom_metadata_get_request{
    type = Type,
    query = Query,
    inherited = Inherited
}) ->
    metadata_req:get_metadata(UserCtx, FileCtx, Type, Query, Inherited);

execute(UserCtx, FileCtx, #custom_metadata_set_request{
    type = Type,
    query = Query,
    value = Value
}) ->
    metadata_req:set_metadata(UserCtx, FileCtx, Type, Value, Query, false, false);

execute(UserCtx, FileCtx, #custom_metadata_remove_request{type = Type}) ->
    metadata_req:remove_metadata(UserCtx, FileCtx, Type);

execute(UserCtx, FileCtx, #file_distribution_gather_request{}) ->
    file_distribution:gather(UserCtx, FileCtx);

execute(UserCtx, FileCtx, #historical_dir_size_stats_gather_request{request = Request}) ->
    {ok, dir_size_stats_req:gather_historical(UserCtx, FileCtx, Request)};

execute(UserCtx, FileCtx, #file_storage_locations_get_request{}) ->
    file_distribution:get_storage_locations(UserCtx, FileCtx);


%% QoS

execute(UserCtx, FileCtx, #qos_entry_add_request{
    expression = Expression,
    replicas_num = ReplicasNum,
    entry_type = EntryType
}) ->
    qos_req:add_qos_entry(UserCtx, FileCtx, Expression, ReplicasNum, EntryType);

execute(UserCtx, FileCtx, #effective_file_qos_get_request{}) ->
    qos_req:get_effective_file_qos(UserCtx, FileCtx);

execute(UserCtx, FileCtx, #qos_entry_get_request{id = QosEntryId}) ->
    qos_req:get_qos_entry(UserCtx, FileCtx, QosEntryId);

execute(UserCtx, FileCtx, #qos_entry_remove_request{id = QosEntryId}) ->
    qos_req:remove_qos_entry(UserCtx, FileCtx, QosEntryId);

execute(UserCtx, FileCtx, #qos_status_check_request{qos_id = QosEntryId}) ->
    qos_req:check_status(UserCtx, FileCtx, QosEntryId);

%% Shares

execute(UserCtx, FileCtx, #share_create_request{name = Name, description = Description}) ->
    share_req:create_share(UserCtx, FileCtx, Name, Description);

execute(UserCtx, FileCtx, #share_remove_request{share_id = ShareId}) ->
    share_req:remove_share(UserCtx, FileCtx, ShareId);

%% Transfers

execute(UserCtx, FileCtx, #file_transfer_schedule_request{
    replicating_provider_id = ReplicatingProviderId,
    evicting_provider_id = EvictingProviderId,
    callback = Callback
}) ->
    transfer_req:schedule_file_transfer(
        UserCtx, FileCtx,
        ReplicatingProviderId, EvictingProviderId,
        Callback
    );

execute(UserCtx, FileCtx, #view_transfer_schedule_request{
    replicating_provider_id = ReplicatingProviderId,
    evicting_provider_id = EvictingProviderId,
    view_name = ViewName,
    query_view_params = QueryViewParams,
    callback = Callback
}) ->
    transfer_req:schedule_view_transfer(
        UserCtx, FileCtx,
        ReplicatingProviderId, EvictingProviderId,
        ViewName, QueryViewParams,
        Callback
    ).
