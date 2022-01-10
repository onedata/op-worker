%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for managing archives (requests are delegated to middleware_worker).
%%% TODO - VFS-8382 investigate low performance of archives functions
%%% @end
%%%-------------------------------------------------------------------
-module(mi_archives).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([
    list/4,
    archive_dataset/6,
    get_info/2,
    update/3,
    init_purge/3,
    recall/4
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec list(
    session:id(),
    dataset:id(),
    dataset_api:listing_opts(),
    undefined | dataset_api:listing_mode()
) ->
    {archive_api:entries(), boolean()} | no_return().
list(SessionId, DatasetId, Opts, ListingMode) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #list_archives{
        dataset_id = DatasetId,
        opts = Opts,
        mode = utils:ensure_defined(ListingMode, ?BASIC_INFO)
    }).


-spec archive_dataset(
    session:id(),
    dataset:id(),
    archive:config(),
    archive:callback(),
    archive:callback(),
    archive:description()
) ->
    archive:id() | no_return().
archive_dataset(SessionId, DatasetId, Config, PreservedCallback, PurgedCallback, Description) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #archive_dataset{
        id = DatasetId,
        config = Config,
        description = Description,
        preserved_callback = PreservedCallback,
        purged_callback = PurgedCallback
    }).


-spec get_info(session:id(), archive:id()) ->
    archive_api:info() | no_return().
get_info(SessionId, ArchiveId) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #get_archive_info{id = ArchiveId}).


-spec update(session:id(), archive:id(), archive:diff()) ->
    ok | no_return().
update(SessionId, ArchiveId, Diff) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #update_archive{
        id = ArchiveId,
        diff = Diff
    }).


-spec init_purge(session:id(), archive:id(), archive:callback()) ->
    ok | no_return().
init_purge(SessionId, ArchiveId, CallbackUrl) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #init_archive_purge{
        id = ArchiveId,
        callback = CallbackUrl
    }).


-spec recall(session:id(), archive:id(), file_id:file_guid(), file_meta:name() | default) ->
    file_id:file_guid() | no_return().
recall(SessionId, ArchiveId, TargetParentGuid, TargetFilename) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),
    
    middleware_worker:check_exec(SessionId, SpaceGuid, #recall_archive{
        id = ArchiveId,
        target_parent_guid = TargetParentGuid,
        target_filename = TargetFilename
    }).
    

%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec dataset_id_to_space_guid(dataset:id()) -> file_id:file_guid() | no_return().
dataset_id_to_space_guid(DatasetId) ->
    fslogic_uuid:spaceid_to_space_dir_guid(?check(dataset:get_space_id(DatasetId))).


%% @private
-spec archive_id_to_space_guid(archive:id()) -> fslogic_worker:file_guid() | no_return().
archive_id_to_space_guid(ArchiveId) ->
    fslogic_uuid:spaceid_to_space_dir_guid(?check(archive:get_space_id(ArchiveId))).
