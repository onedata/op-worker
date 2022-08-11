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
%%% The subtree will be rooted in .__onedata_archive directory so
%%% that it won't be visible in the space.
%%%
%%% The subtree will have the following structure:
%%% <SPACE DIRECTORY>
%%% |--- .__onedata_archive, uuid: ?ARCHIVES_ROOT_DIR_UUID(SpaceId)
%%%      |--- dataset_archives_<DatasetId>, uuid: ?DATASET_ARCHIVES_DIR_UUID(DatasetId)
%%%           |--- archive_<ArchiveId>, uuid: ?ARCHIVE_DIR_UUID(ArchiveId)
%%%                |--- ...
%%%                |--- ... (Dataset files and directories)
%%%                |--- ...
%%%
%%%
%%% NOTE !!!
%%% If createNestedArchives option is enabled, archivisation_traverse
%%% (see archivisation_traverse.erl module) will create archives
%%% for nested datasets.
%%% Such archives are called nested archives.
%%% Nested archives will be created in their own archive_<ArchiveId>
%%% directories and symlinked from parent archives. 
%%% 
%%% When includeDip option is enabled, alongside with archival information 
%%% package (AIP), additional archive representing dissemination information 
%%% package (DIP) is created in its own archive_<ArchiveId> directory.
%%%
%%% Please see docs in plain_archive.erl and bagit_archive.erl to
%%% learn about structure of nested archives in case of corresponding
%%% layouts.
%%% @end
%%%-------------------------------------------------------------------
-module(archivisation_tree).
-author("Jakub Kudzia").

-include("modules/dataset/archivisation_tree.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create_archive_dir/4, is_special_uuid/1, is_in_archive/1,
    uuid_to_archive_id/1, extract_archive_id/1, get_filename_for_download/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec create_archive_dir(archive:id(), dataset:id(), od_space:id(), od_user:id()) ->
    {ok, file_meta:uuid()}.
create_archive_dir(ArchiveId, DatasetId, SpaceId, ArchiveCreatorId) ->
    DatasetArchivesDirUuid = ?DATASET_ARCHIVES_DIR_UUID(DatasetId),
    ArchiveDirUuid = ?ARCHIVE_DIR_UUID(ArchiveId),
    ArchiveDirDoc = file_meta:new_doc(
        ArchiveDirUuid, ?ARCHIVE_DIR_NAME(ArchiveId),
        ?DIRECTORY_TYPE, ?DEFAULT_DIR_PERMS, ArchiveCreatorId,
        DatasetArchivesDirUuid, SpaceId
    ),
    case create_file_meta(DatasetArchivesDirUuid, ArchiveDirDoc) of
        ok ->
            ok;
        ?ERROR_NOT_FOUND ->
            {ok, _} = create_dataset_archives_dir(DatasetId, SpaceId),
            ok = create_file_meta(DatasetArchivesDirUuid, ArchiveDirDoc)
    end,
    {ok, ArchiveDirUuid}.


-spec is_special_uuid(file_meta:uuid()) -> boolean().
is_special_uuid(<<?ARCHIVES_ROOT_DIR_UUID_PREFIX, _/binary>>) ->
    true;
is_special_uuid(<<?DATASET_ARCHIVES_DIR_UUID_PREFIX, _/binary>>) ->
    true;
is_special_uuid(_) ->
    false.


-spec uuid_to_archive_id(file_meta:uuid()) -> archive:id() | undefined.
uuid_to_archive_id(?ARCHIVE_DIR_UUID(ArchiveId)) ->
    ArchiveId;
uuid_to_archive_id(_) ->
    undefined.


-spec is_in_archive(file_meta:path()) -> boolean().
is_in_archive(CanonicalPath) ->
    ArchivesRootDirName = ?ARCHIVES_ROOT_DIR_NAME,
    case filepath_utils:split(CanonicalPath) of
        [_Sep, _SpaceId, ArchivesRootDirName | _Rest] -> true;
        _ -> false
    end.


-spec extract_archive_id(file_meta:path()) -> {ok, archive:id()} | {error, term()}.
extract_archive_id(CanonicalPath) ->
    ArchivesRootDirName = ?ARCHIVES_ROOT_DIR_NAME,
    case filename:split(CanonicalPath) of
        [
            ?DIRECTORY_SEPARATOR_BIN,
            _SpaceId, ArchivesRootDirName,
            ?DATASET_ARCHIVES_DIR_NAME(_),
            ?ARCHIVE_DIR_NAME(ArchiveId)
            | _Rest
        ] ->
            {ok, ArchiveId};
        _ ->
            ?ERROR_NOT_FOUND
    end.


-spec get_filename_for_download(archive:id()) -> binary() | no_return().
get_filename_for_download(ArchiveId) ->
    {ok, Description} = archive:get_description(ArchiveId),
    case re:run(Description, <<"_USE_FILENAME: *([a-zA-Z0-9+_-]+)">>, [{capture, all_but_first, binary}]) of
        {match, [Filename]} ->
            Filename;
        nomatch ->
            ?ARCHIVE_DIR_NAME(ArchiveId)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_dataset_archives_dir(dataset:id(), od_space:id()) -> {ok, file_meta:uuid()}.
create_dataset_archives_dir(DatasetId, SpaceId) ->
    ArchivesRootDirUuid = ?ARCHIVES_ROOT_DIR_UUID(SpaceId),
    DatasetArchivesDirUuid = ?DATASET_ARCHIVES_DIR_UUID(DatasetId),
    DatasetArchivedDirDoc = file_meta:new_doc(
        DatasetArchivesDirUuid, ?DATASET_ARCHIVES_DIR_NAME(DatasetId),
        ?DIRECTORY_TYPE, ?DEFAULT_DIR_PERMS, ?SPACE_OWNER_ID(SpaceId),
        ArchivesRootDirUuid, SpaceId
    ),
    case create_file_meta(ArchivesRootDirUuid, DatasetArchivedDirDoc) of
        ok -> ok;
        ?ERROR_NOT_FOUND ->
            {ok, _} = create_archives_root_dir(SpaceId),
            ok = create_file_meta(ArchivesRootDirUuid, DatasetArchivedDirDoc)
    end,
    {ok, DatasetArchivesDirUuid}.


-spec create_archives_root_dir(od_space:id()) -> {ok, file_meta:uuid()}.
create_archives_root_dir(SpaceId) ->
    SpaceUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),
    ArchivesRootDirUuid = ?ARCHIVES_ROOT_DIR_UUID(SpaceId),
    ArchivesRootDirDoc = file_meta:new_doc(
        ArchivesRootDirUuid, ?ARCHIVES_ROOT_DIR_NAME,
        ?DIRECTORY_TYPE, ?ARCHIVES_ROOT_DIR_PERMS, ?SPACE_OWNER_ID(SpaceId),
        SpaceUuid, SpaceId
    ),
    ok = create_file_meta(SpaceUuid, ArchivesRootDirDoc),
    {ok, ArchivesRootDirUuid}.


-spec create_file_meta(file_meta:uuid(), file_meta:doc()) -> ok | {error, term()}.
create_file_meta(ParentUuid, FileDoc) ->
    ?extract_ok(?ok_if_exists(file_meta:create({uuid, ParentUuid}, FileDoc))).