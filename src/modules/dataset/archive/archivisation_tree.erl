%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper module for archivisation mechanism.
%%% It contains functions used to create directories in
%%% which archive files will be stored.
%%%
%%% The subtree will be rooted in .__onedata_archives directory so
%%% that it won't be visible in the space.
%%%
%%% The subtree will have the following structure:
%%% /<SPACE DIRECTORY>
%%%     /.__onedata_archives, uuid: ?ARCHIVES_ROOT_DIR_UUID(SpaceId)
%%%         /dataset_archives_<DatasetId>, uuid: ?DATASET_ARCHIVES_DIR_UUID(DatasetId)
%%%             /archive_<ArchiveId>, uuid: ?ARCHIVE_DIR_UUID(ArchiveId)
%%%                 /DatasetRootFile
%%%                     /... (Dataset files and directories)
%%% @end
%%%-------------------------------------------------------------------
-module(archivisation_tree).
-author("Jakub Kudzia").

-include("modules/dataset/archivisation_tree.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([create_archive_dir/4, is_special_uuid/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec create_archive_dir(archive:id(), dataset:id(), od_space:id(), archive:creator()) ->
    {ok, file_meta:uuid()}.
create_archive_dir(ArchiveId, DatasetId, SpaceId, ArchiveCreator) ->
    ensure_archives_root_dir_created(SpaceId),
    ensure_dataset_archives_dir_created(DatasetId, SpaceId),
    DatasetArchivesDirUuid = ?DATASET_ARCHIVES_DIR_UUID(DatasetId),
    ArchiveDirUuid = ?ARCHIVE_DIR_UUID(ArchiveId),
    ArchiveDirDoc = file_meta:new_doc(
        ArchiveDirUuid, ArchiveDirUuid,
        ?DIRECTORY_TYPE, ?DEFAULT_DIR_PERMS, ArchiveCreator,
        DatasetArchivesDirUuid, SpaceId
    ),
    create_file_meta(DatasetArchivesDirUuid, ArchiveDirDoc),
    {ok, ArchiveDirUuid}.


-spec is_special_uuid(file_meta:uuid()) -> boolean().
is_special_uuid(<<?ARCHIVES_ROOT_DIR_UUID_PREFIX, _/binary>>) ->
    true;
is_special_uuid(<<?DATASET_ARCHIVES_DIR_UUID_PREFIX, _/binary>>) ->
    true;
is_special_uuid(<<?ARCHIVE_DIR_UUID_PREFIX, _/binary>>) ->
    true;
is_special_uuid(_) ->
    false.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec ensure_archives_root_dir_created(od_space:id()) -> ok.
ensure_archives_root_dir_created(SpaceId) ->
    case file_meta:exists(?ARCHIVES_ROOT_DIR_UUID(SpaceId)) of
        true -> ok;
        false -> create_archives_root_dir(SpaceId)
    end.


-spec create_archives_root_dir(od_space:id()) -> ok.
create_archives_root_dir(SpaceId) ->
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    ArchivesRootDirDoc = file_meta:new_doc(
        ?ARCHIVES_ROOT_DIR_UUID(SpaceId), ?ARCHIVES_ROOT_DIR_NAME,
        ?DIRECTORY_TYPE, ?ARCHIVES_ROOT_DIR_PERMS, ?SPACE_OWNER_ID(SpaceId),
        SpaceUuid, SpaceId
    ),
    create_file_meta(SpaceUuid, ArchivesRootDirDoc).


-spec ensure_dataset_archives_dir_created(dataset:id(), od_space:id()) -> ok.
ensure_dataset_archives_dir_created(DatasetId, SpaceId) ->
    case file_meta:exists(?DATASET_ARCHIVES_DIR_UUID(DatasetId)) of
        true -> ok;
        false -> create_dataset_archives_dir(DatasetId, SpaceId)
    end.


-spec create_dataset_archives_dir(dataset:id(), od_space:id()) -> ok.
create_dataset_archives_dir(DatasetId, SpaceId) ->
    ArchivesRootDirUuid = ?ARCHIVES_ROOT_DIR_UUID(SpaceId),
    DatasetArchivesDirUuid = ?DATASET_ARCHIVES_DIR_UUID(DatasetId),
    DatasetArchivedDirDoc = file_meta:new_doc(
        DatasetArchivesDirUuid, DatasetArchivesDirUuid,
        ?DIRECTORY_TYPE, ?DATASET_ARCHIVES_DIR_PERMS, ?SPACE_OWNER_ID(SpaceId),
        ArchivesRootDirUuid, SpaceId
    ),
    create_file_meta(ArchivesRootDirUuid, DatasetArchivedDirDoc).


-spec create_file_meta(file_meta:uuid(), file_meta:doc()) -> ok.
create_file_meta(ParentUuid, FileDoc) ->
    ?extract_ok(?ok_if_exists(file_meta:create({uuid, ParentUuid}, FileDoc))).