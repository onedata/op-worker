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
archive_dataset(SessionId, DatasetId, Config, PreservedCallback, DeletedCallback, Description) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #archive_dataset{
        id = DatasetId,
        config = Config,
        description = Description,
        preserved_callback = PreservedCallback,
        deleted_callback = DeletedCallback
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


-spec delete(session:id(), archive:id(), archive:callback()) ->
    ok | no_return().
delete(SessionId, ArchiveId, CallbackUrl) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #delete_archive{
        id = ArchiveId,
        callback = CallbackUrl
    }).


-spec recall(session:id(), archive:id(), file_id:file_guid(), file_meta:name() | default) ->
    file_id:file_guid() | no_return().
recall(SessionId, ArchiveId, ParentDirectoryGuid, TargetFilename) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),
    
    middleware_worker:check_exec(SessionId, SpaceGuid, #recall_archive{
        archive_id = ArchiveId,
        parent_directory_guid = ParentDirectoryGuid,
        target_filename = TargetFilename
    }).


-spec cancel_recall(session:id(), file_id:file_guid()) -> ok | no_return().
cancel_recall(SessionId, FileGuid) ->
    middleware_worker:check_exec(SessionId, FileGuid, #cancel_archive_recall{
        id = file_id:guid_to_uuid(FileGuid)
    }).


-spec get_recall_details(session:id(), file_id:file_guid()) -> 
    archive_recall:record() | no_return().
get_recall_details(SessionId, FileGuid) ->
    middleware_worker:check_exec(SessionId, FileGuid, #get_recall_details{
        id = file_id:guid_to_uuid(FileGuid)
    }).


-spec get_recall_progress(session:id(), file_id:file_guid()) ->
    archive_recall:recall_progress_map() | no_return().
get_recall_progress(SessionId, FileGuid) ->
    middleware_worker:check_exec(SessionId, FileGuid, #get_recall_progress{
        id = file_id:guid_to_uuid(FileGuid)
    }).


-spec browse_recall_log(session:id(), file_id:file_guid(), json_infinite_log_model:listing_opts()) ->
    json_infinite_log_model:browse_result() | no_return().
browse_recall_log(SessionId, FileGuid, BrowseOpts) ->
    middleware_worker:check_exec(SessionId, FileGuid, #browse_recall_log{
        id = file_id:guid_to_uuid(FileGuid),
        options = BrowseOpts
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
