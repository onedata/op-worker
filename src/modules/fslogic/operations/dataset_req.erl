%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on datasets.
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_req).
-author("Jakub Kudzia").

-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/privileges.hrl").


%% Datasets API
-export([
    establish/3,
    update/6,
    remove/3,
    get_info/3,
    get_file_eff_summary/2,
    list_top_datasets/5,
    list_children_datasets/5
]).

%% Archives API
-export([
    create_archive/7,
    update_archive/4,
    get_archive_info/3,
    list_archives/5,
    init_archive_purge/4,
    init_archive_recall/5,
    cancel_archive_recall/3,
    get_archive_recall_details/3,
    get_archive_recall_progress/3
]).

-type error() :: {error, term()}.


%%%===================================================================
%%% API functions
%%%===================================================================


-spec establish(file_ctx:ctx(), data_access_control:bitmask(), user_ctx:ctx()) ->
    {ok, dataset:id()} | error().
establish(FileCtx0, ProtectionFlags, UserCtx) ->
    assert_has_eff_privilege(FileCtx0, UserCtx, ?SPACE_MANAGE_DATASETS),

    FileCtx1 = fslogic_authz:ensure_authorized(UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS]),
    dataset_api:establish(FileCtx1, ProtectionFlags).


-spec update(
    file_ctx:ctx(),
    dataset:id(),
    dataset:state() | undefined,
    data_access_control:bitmask(),
    data_access_control:bitmask(),
    user_ctx:ctx()
) ->
    ok | error().
update(SpaceDirCtx, DatasetId, NewDatasetState, FlagsToSet, FlagsToUnset, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_MANAGE_DATASETS),

    {ok, DatasetDoc} = dataset:get(DatasetId),
    FileCtx0 = dataset_api:get_associated_file_ctx(DatasetDoc),
    fslogic_authz:ensure_authorized(UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS]),

    ok = dataset_api:update(DatasetDoc, NewDatasetState, FlagsToSet, FlagsToUnset).


-spec remove(file_ctx:ctx(), dataset:id(), user_ctx:ctx()) -> ok | error().
remove(SpaceDirCtx, DatasetId, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_MANAGE_DATASETS),

    {ok, DatasetDoc} = dataset:get(DatasetId),
    {ok, CurrentState} = dataset:get_state(DatasetDoc),

    case CurrentState =:= ?ATTACHED_DATASET of
        true ->
            FileCtx0 = dataset_api:get_associated_file_ctx(DatasetDoc),
            fslogic_authz:ensure_authorized(UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS]);
        false ->
            ok
    end,

    ok = dataset_api:remove(DatasetDoc).


-spec get_info(file_ctx:ctx(), dataset:id(), user_ctx:ctx()) ->
    {ok, dataset_api:info()} | error().
get_info(SpaceDirCtx, DatasetId, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_VIEW),

    dataset_api:get_info(DatasetId).


-spec get_file_eff_summary(file_ctx:ctx(), user_ctx:ctx()) ->
    {ok, dataset_api:file_eff_summary()} | error().
get_file_eff_summary(FileCtx0, UserCtx) ->
    assert_has_eff_privilege(FileCtx0, UserCtx, ?SPACE_VIEW),
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS]
    ),

    dataset_api:get_effective_summary(FileCtx1).


-spec list_top_datasets(
    od_space:id(),
    dataset:state(),
    dataset_api:listing_opts(),
    dataset_api:listing_mode(),
    user_ctx:ctx()
) ->
    {ok, {dataset_api:entries(), boolean()}} | error().
list_top_datasets(SpaceId, State, Opts, ListingMode, UserCtx) ->
    UserId = user_ctx:get_user_id(UserCtx),
    space_logic:assert_has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW),

    dataset_api:list_top_datasets(SpaceId, State, Opts, ListingMode).


-spec list_children_datasets(
    file_ctx:ctx(),
    dataset:id(),
    dataset_api:listing_opts(),
    dataset_api:listing_mode(),
    user_ctx:ctx()
) ->
    {ok, {dataset_api:entries(), boolean()}} | error().
list_children_datasets(SpaceDirCtx, Dataset, Opts, ListingMode, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_VIEW),

    dataset_api:list_children_datasets(Dataset, Opts, ListingMode).


%%%===================================================================
%%% Archives API functions
%%%===================================================================


-spec create_archive(
    file_ctx:ctx(),
    dataset:id(),
    archive:config(),
    archive:callback(),
    archive:callback(),
    archive:description(),
    user_ctx:ctx()
) ->
    {ok, archive:id()} | error().
create_archive(SpaceDirCtx, DatasetId, Config, PreservedCallback, PurgedCallback, Description, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_MANAGE_DATASETS),
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_CREATE_ARCHIVES),

    archive_api:start_archivisation(
        DatasetId, Config, PreservedCallback, PurgedCallback, Description, UserCtx
    ).


-spec update_archive(file_ctx:ctx(), archive:id(), archive:diff(), user_ctx:ctx()) ->
    ok | error().
update_archive(SpaceDirCtx, ArchiveId, Diff, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_MANAGE_DATASETS),
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_CREATE_ARCHIVES),

    archive_api:update_archive(ArchiveId, Diff).


-spec get_archive_info(file_ctx:ctx(), archive:id(), user_ctx:ctx()) ->
    {ok, archive_api:info()} | error().
get_archive_info(SpaceDirCtx, ArchiveId, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_VIEW_ARCHIVES),
    archive_api:get_archive_info(ArchiveId).


-spec list_archives(
    file_ctx:ctx(),
    dataset:id(),
    archives_list:opts(),
    dataset_api:listing_mode(),
    user_ctx:ctx()
) ->
    {ok, {archive_api:entries(), boolean()}} | error().
list_archives(SpaceDirCtx, DatasetId, Opts, ListingMode, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_VIEW_ARCHIVES),
    archive_api:list_archives(DatasetId, Opts, ListingMode).


-spec init_archive_purge(file_ctx:ctx(), archive:id(), archive:callback(), user_ctx:ctx()) ->
    ok | error().
init_archive_purge(SpaceDirCtx, ArchiveId, CallbackUrl, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_MANAGE_DATASETS),
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_REMOVE_ARCHIVES),

    archive_api:purge(ArchiveId, CallbackUrl).


-spec init_archive_recall(file_ctx:ctx(), archive:id(), file_id:file_guid(), file_meta:name() | undefined, 
    user_ctx:ctx()) -> {ok, file_id:file_guid()} | error().
init_archive_recall(SpaceDirCtx, ArchiveId, ParentDirectoryGuid, TargetName, UserCtx) ->
    assert_has_eff_privilege(SpaceDirCtx, UserCtx, ?SPACE_RECALL_ARCHIVES),
    
    archive_api:recall(ArchiveId, UserCtx, ParentDirectoryGuid, TargetName).


-spec cancel_archive_recall(file_ctx:ctx(), archive_recall:id(), user_ctx:ctx()) -> ok | error().
cancel_archive_recall(FileCtx, RecallId, UserCtx) ->
    assert_has_eff_privilege(FileCtx, UserCtx, ?SPACE_RECALL_ARCHIVES),
    fslogic_authz:ensure_authorized(UserCtx, FileCtx, [?TRAVERSE_ANCESTORS]),
    
    archive_api:cancel_recall(RecallId).


-spec get_archive_recall_details(file_ctx:ctx(), archive_recall:id(), user_ctx:ctx()) -> 
    {ok, archive_recall:record()} | error().
get_archive_recall_details(FileCtx, RecallId, UserCtx) ->
    fslogic_authz:ensure_authorized(UserCtx, FileCtx, [?TRAVERSE_ANCESTORS]),
    
    archive_recall:get_details(RecallId).


-spec get_archive_recall_progress(file_ctx:ctx(), archive_recall:id(), user_ctx:ctx()) ->
    {ok, archive_recall:recall_progress_map()} | error().
get_archive_recall_progress(FileCtx, RecallId, UserCtx) ->
    fslogic_authz:ensure_authorized(UserCtx, FileCtx, [?TRAVERSE_ANCESTORS]),
    
    archive_recall:get_progress(RecallId).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_has_eff_privilege(file_ctx:ctx(), user_ctx:ctx(), privileges:space_privilege()) -> ok.
assert_has_eff_privilege(FileCtx, UserCtx, Privilege) ->
    UserId = user_ctx:get_user_id(UserCtx),
    case UserId =:= ?ROOT_USER_ID of
        true ->
            ok;
        false ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            space_logic:assert_has_eff_privilege(SpaceId, UserId, Privilege)
    end.
