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


-spec execute(user_ctx:ctx(), file_ctx:ctx(), middleware_worker:operation()) ->
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
    purged_callback = PurgedCallback,
    description = Description
}) ->
    dataset_req:create_archive(
        SpaceDirCtx, DatasetId, Config, PreservedCallback, PurgedCallback, Description, UserCtx
    );

execute(UserCtx, SpaceDirCtx, #get_archive_info{id = ArchiveId}) ->
    dataset_req:get_archive_info(SpaceDirCtx, ArchiveId, UserCtx);

execute(UserCtx, SpaceDirCtx, #update_archive{id = ArchiveId, diff = Diff}) ->
    dataset_req:update_archive(SpaceDirCtx, ArchiveId, Diff, UserCtx);

execute(UserCtx, SpaceDirCtx, #init_archive_purge{id = ArchiveId, callback = CallbackUrl}) ->
    dataset_req:init_archive_purge(SpaceDirCtx, ArchiveId, CallbackUrl, UserCtx);

execute(UserCtx, SpaceDirCtx, #schedule_atm_workflow_execution{
    atm_workflow_schema_id = AtmWorkflowSchemaId,
    atm_workflow_schema_revision_num = AtmWorkflowSchemaRevisionNum,
    store_initial_values = AtmStoreInitialValues,
    callback_url = CallbackUrl
}) ->
    {ok, atm_workflow_execution_api:schedule(
        UserCtx, file_ctx:get_space_id_const(SpaceDirCtx),
        AtmWorkflowSchemaId, AtmWorkflowSchemaRevisionNum,
        AtmStoreInitialValues, CallbackUrl
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
    dataset_req:get_file_eff_summary(FileCtx, UserCtx).