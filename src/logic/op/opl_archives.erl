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
-module(opl_archives).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([
    list/4,
    archive_dataset/6,
    get_info/2,
    update/3,
    init_purge/3
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
    {ok, {archive_api:entries(), boolean()}} | errors:error().
list(SessionId, DatasetId, Opts, ListingMode) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),

    middleware_worker:exec(SessionId, SpaceGuid, #list_archives{
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
    {ok, archive:id()} | errors:error().
archive_dataset(SessionId, DatasetId, Config, PreservedCallback, PurgedCallback, Description) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),

    middleware_worker:exec(SessionId, SpaceGuid, #archive_dataset{
        id = DatasetId,
        config = Config,
        description = Description,
        preserved_callback = PreservedCallback,
        purged_callback = PurgedCallback
    }).


-spec get_info(session:id(), archive:id()) ->
    {ok, archive_api:info()} | errors:error().
get_info(SessionId, ArchiveId) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),

    middleware_worker:exec(SessionId, SpaceGuid, #get_archive_info{id = ArchiveId}).


-spec update(session:id(), archive:id(), archive:diff()) ->
    ok | errors:error().
update(SessionId, ArchiveId, Diff) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),

    middleware_worker:exec(SessionId, SpaceGuid, #update_archive{
        id = ArchiveId,
        diff = Diff
    }).


-spec init_purge(session:id(), archive:id(), archive:callback()) ->
    ok | errors:error().
init_purge(SessionId, ArchiveId, CallbackUrl) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),

    middleware_worker:exec(SessionId, SpaceGuid, #init_archive_purge{
        id = ArchiveId,
        callback = CallbackUrl
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec dataset_id_to_space_guid(dataset:id()) -> fslogic_worker:file_guid().
dataset_id_to_space_guid(DatasetId) ->
    {ok, SpaceId} = dataset:get_space_id(DatasetId),
    fslogic_uuid:spaceid_to_space_dir_guid(SpaceId).


%% @private
-spec archive_id_to_space_guid(archive:id()) -> fslogic_worker:file_guid().
archive_id_to_space_guid(ArchiveId) ->
    {ok, SpaceId} = archive:get_space_id(ArchiveId),
    fslogic_uuid:spaceid_to_space_dir_guid(SpaceId).
