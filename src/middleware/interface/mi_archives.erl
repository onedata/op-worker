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
    cancel_archivisation/3,
    get_info/2,
    update/3,
    delete/3,
    recall/4,
    cancel_recall/2,
    get_recall_details/2,
    get_recall_progress/2,
    browse_recall_log/3
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

    middleware_worker:check_exec(SessionId, SpaceGuid, #archives_list_request{
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
    archive_api:info() | no_return().
archive_dataset(SessionId, DatasetId, Config, PreservedCallback, DeletedCallback, Description) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #dataset_archive_request{
        id = DatasetId,
        config = Config,
        description = Description,
        preserved_callback = PreservedCallback,
        deleted_callback = DeletedCallback
    }).


-spec cancel_archivisation(session:id(), archive:id(), archive:cancel_preservation_policy()) ->
    ok | no_return().
cancel_archivisation(SessionId, ArchiveId, PreservationPolicy) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),
    
    middleware_worker:check_exec(SessionId, SpaceGuid, #archivisation_cancel_request{
        id = ArchiveId,
        preservation_policy = PreservationPolicy
    }).


-spec get_info(session:id(), archive:id()) ->
    archive_api:info() | no_return().
get_info(SessionId, ArchiveId) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #archive_info_get_request{id = ArchiveId}).


-spec update(session:id(), archive:id(), archive:diff()) ->
    ok | no_return().
update(SessionId, ArchiveId, Diff) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #archive_update_request{
        id = ArchiveId,
        diff = Diff
    }).


-spec delete(session:id(), archive:id(), archive:callback()) ->
    ok | no_return().
delete(SessionId, ArchiveId, CallbackUrl) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #archive_delete_request{
        id = ArchiveId,
        callback = CallbackUrl
    }).


-spec recall(session:id(), archive:id(), file_id:file_guid(), file_meta:name() | default) ->
    file_id:file_guid() | no_return().
recall(SessionId, ArchiveId, ParentDirectoryGuid, TargetFilename) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),
    
    middleware_worker:check_exec(SessionId, SpaceGuid, #archive_recall_request{
        archive_id = ArchiveId,
        parent_directory_guid = ParentDirectoryGuid,
        target_filename = TargetFilename
    }).


-spec cancel_recall(session:id(), file_id:file_guid()) -> ok | no_return().
cancel_recall(SessionId, FileGuid) ->
    middleware_worker:check_exec(SessionId, FileGuid, #archive_recall_cancel_request{
        id = file_id:guid_to_uuid(FileGuid)
    }).


-spec get_recall_details(session:id(), file_id:file_guid()) -> 
    archive_recall:record() | no_return().
get_recall_details(SessionId, FileGuid) ->
    middleware_worker:check_exec(SessionId, FileGuid, #archive_recall_details_get_request{
        id = file_id:guid_to_uuid(FileGuid)
    }).


-spec get_recall_progress(session:id(), file_id:file_guid()) ->
    archive_recall:recall_progress_map() | no_return().
get_recall_progress(SessionId, FileGuid) ->
    middleware_worker:check_exec(SessionId, FileGuid, #archive_recall_progress_get_request{
        id = file_id:guid_to_uuid(FileGuid)
    }).


-spec browse_recall_log(session:id(), file_id:file_guid(), audit_log_browse_opts:opts()) ->
    audit_log:browse_result() | no_return().
browse_recall_log(SessionId, FileGuid, BrowseOpts) ->
    middleware_worker:check_exec(SessionId, FileGuid, #archive_recall_log_browse_request{
        id = file_id:guid_to_uuid(FileGuid),
        options = BrowseOpts
    }).
    

%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec dataset_id_to_space_guid(dataset:id()) -> file_id:file_guid() | no_return().
dataset_id_to_space_guid(DatasetId) ->
    fslogic_file_id:spaceid_to_space_dir_guid(?check(dataset:get_space_id(DatasetId))).


%% @private
-spec archive_id_to_space_guid(archive:id()) -> fslogic_worker:file_guid() | no_return().
archive_id_to_space_guid(ArchiveId) ->
    fslogic_file_id:spaceid_to_space_dir_guid(?check(archive:get_space_id(ArchiveId))).
