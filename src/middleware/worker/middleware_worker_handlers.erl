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
-include("proto/oneprovider/provider_rpc_messages.hrl").

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
execute(UserCtx, SpaceDirCtx, #list_archives{
    dataset_id = DatasetId,
    opts = Opts,
    mode = ListingMode
}) ->
    dataset_req:list_archives(SpaceDirCtx, DatasetId, Opts, ListingMode, UserCtx);

execute(UserCtx, SpaceDirCtx, #archive_dataset{
    id = DatasetId,
    config = Config,
    preserved_callback = PreservedCallback,
    deleted_callback = DeletedCallback,
    description = Description
}) ->
    dataset_req:create_archive(
        SpaceDirCtx, DatasetId, Config, PreservedCallback, DeletedCallback, Description, UserCtx
    );

execute(UserCtx, SpaceDirCtx, #get_archive_info{id = ArchiveId}) ->
    dataset_req:get_archive_info(SpaceDirCtx, ArchiveId, UserCtx);

execute(UserCtx, SpaceDirCtx, #update_archive{id = ArchiveId, diff = Diff}) ->
    dataset_req:update_archive(SpaceDirCtx, ArchiveId, Diff, UserCtx);

execute(UserCtx, SpaceDirCtx, #delete_archive{id = ArchiveId, callback = CallbackUrl}) ->
    dataset_req:init_archive_delete(SpaceDirCtx, ArchiveId, CallbackUrl, UserCtx);

execute(UserCtx, SpaceDirCtx, #recall_archive{
    archive_id = ArchiveId, parent_directory_guid = ParentDirectoryGuid, target_filename = TargetName}
) ->
    dataset_req:init_archive_recall(SpaceDirCtx, ArchiveId, ParentDirectoryGuid, TargetName, UserCtx);

execute(UserCtx, FileCtx, #cancel_archive_recall{id = Id}) ->
    dataset_req:cancel_archive_recall(FileCtx, Id, UserCtx);

execute(UserCtx, FileCtx, #get_recall_details{id = Id}) ->
    dataset_req:get_archive_recall_details(FileCtx, Id, UserCtx);

execute(UserCtx, FileCtx, #get_recall_progress{id = Id}) ->
    dataset_req:get_archive_recall_progress(FileCtx, Id, UserCtx);

execute(UserCtx, FileCtx, #browse_recall_log{id = Id, options = Options}) ->
    dataset_req:browse_archive_recall_log(FileCtx, Id, UserCtx, Options);


%% Automation

execute(UserCtx, SpaceDirCtx, #schedule_atm_workflow_execution{
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

execute(_UserCtx, _SpaceDirCtx, #cancel_atm_workflow_execution{
    atm_workflow_execution_id = AtmWorkflowExecutionId
}) ->
    ok = atm_workflow_execution_api:cancel(AtmWorkflowExecutionId);

execute(UserCtx, _SpaceDirCtx, #repeat_atm_workflow_execution{
    type = Type,
    atm_workflow_execution_id = AtmWorkflowExecutionId,
    atm_lane_run_selector = AtmLaneRunSelector
}) ->
    ok = atm_workflow_execution_api:repeat(
        UserCtx, Type, AtmLaneRunSelector, AtmWorkflowExecutionId
    );


%% Datasets

execute(UserCtx, SpaceDirCtx, #list_top_datasets{state = State, opts = Opts, mode = ListingMode}) ->
    SpaceId = file_ctx:get_space_id_const(SpaceDirCtx),
    dataset_req:list_top_datasets(SpaceId, State, Opts, ListingMode, UserCtx);

execute(UserCtx, SpaceDirCtx, #list_children_datasets{
    id = DatasetId,
    opts = Opts,
    mode = ListingMode
}) ->
    dataset_req:list_children_datasets(SpaceDirCtx, DatasetId, Opts, ListingMode, UserCtx);

execute(UserCtx, FileCtx, #establish_dataset{protection_flags = ProtectionFlags}) ->
    dataset_req:establish(FileCtx, ProtectionFlags, UserCtx);

execute(UserCtx, SpaceDirCtx, #get_dataset_info{id = DatasetId}) ->
    dataset_req:get_info(SpaceDirCtx, DatasetId, UserCtx);

execute(UserCtx, SpaceDirCtx, #update_dataset{
    id = DatasetId,
    state = NewState,
    flags_to_set = FlagsToSet,
    flags_to_unset = FlagsToUnset
}) ->
    dataset_req:update(SpaceDirCtx, DatasetId, NewState, FlagsToSet, FlagsToUnset, UserCtx);

execute(UserCtx, SpaceDirCtx, #remove_dataset{id = DatasetId}) ->
    dataset_req:remove(SpaceDirCtx, DatasetId, UserCtx);

execute(UserCtx, FileCtx, #get_file_eff_dataset_summary{}) ->
    dataset_req:get_file_eff_summary(FileCtx, UserCtx);


%% File metadata

execute(UserCtx, FileCtx, #file_distribution_gather_request{}) ->
    file_distribution:gather(UserCtx, FileCtx);

execute(UserCtx, FileCtx, #historical_dir_size_stats_gather_request{request = Request}) ->
    {ok, dir_size_stats_req:gather_historical(UserCtx, FileCtx, Request)};

%% QoS

execute(UserCtx, FileCtx, #add_qos_entry{
    expression = Expression,
    replicas_num = ReplicasNum,
    entry_type = EntryType
}) ->
    qos_req:add_qos_entry(UserCtx, FileCtx, Expression, ReplicasNum, EntryType);

execute(UserCtx, FileCtx, #get_effective_file_qos{}) ->
    qos_req:get_effective_file_qos(UserCtx, FileCtx);

execute(UserCtx, FileCtx, #get_qos_entry{id = QosEntryId}) ->
    qos_req:get_qos_entry(UserCtx, FileCtx, QosEntryId);

execute(UserCtx, FileCtx, #remove_qos_entry{id = QosEntryId}) ->
    qos_req:remove_qos_entry(UserCtx, FileCtx, QosEntryId);

execute(UserCtx, FileCtx, #check_qos_status{qos_id = QosEntryId}) ->
    qos_req:check_status(UserCtx, FileCtx, QosEntryId);

%% Shares

execute(UserCtx, FileCtx, #create_share{name = Name, description = Description}) ->
    share_req:create_share(UserCtx, FileCtx, Name, Description);

execute(UserCtx, FileCtx, #remove_share{share_id = ShareId}) ->
    share_req:remove_share(UserCtx, FileCtx, ShareId);

%% Transfers

execute(UserCtx, FileCtx, #schedule_file_transfer{
    replicating_provider_id = ReplicatingProviderId,
    evicting_provider_id = EvictingProviderId,
    callback = Callback
}) ->
    transfer_req:schedule_file_transfer(
        UserCtx, FileCtx,
        ReplicatingProviderId, EvictingProviderId,
        Callback
    );

execute(UserCtx, FileCtx, #schedule_view_transfer{
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
