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
-export([ensure_archives_root_dir_exists/1,
    ensure_dataset_archives_dir_exists/2, ensure_dataset_root_link_exists/2,
    create_archive_dir/4, is_special_uuid/1, is_archives_root_dir_uuid/1, is_archive_dir_uuid/1, is_in_archive/1,
    uuid_to_archive_id/1, extract_archive_id/1, get_filename_for_download/1, get_root_dir_uuid/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec ensure_archives_root_dir_exists(od_space:id()) -> {ok, file_meta:uuid()}.
ensure_archives_root_dir_exists(SpaceId) ->
    SpaceUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),
    ArchivesRootDirUuid = ?ARCHIVES_ROOT_DIR_UUID(SpaceId),
    %% @TODO VFS-11644 - Untangle special dirs and place their logic in one, well-explained place
    ArchivesRootDirDoc = file_meta:new_special_dir_doc(
        ArchivesRootDirUuid, ?ARCHIVES_ROOT_DIR_NAME, ?ARCHIVES_ROOT_DIR_PERMS, ?SPACE_OWNER_ID(SpaceId),
        SpaceUuid, SpaceId
    ),
    ok = ?ok_if_exists(create_file_meta(SpaceUuid, ArchivesRootDirDoc)),
    ok = ?ok_if_exists(?extract_ok(file_meta_forest:add(fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId), SpaceId,
        ?ARCHIVES_ROOT_DIR_NAME, ?ARCHIVES_ROOT_DIR_UUID(SpaceId)))),
    {ok, ArchivesRootDirUuid}.


-spec ensure_dataset_archives_dir_exists(dataset:id(), od_space:id()) -> {ok, file_meta:uuid()}.
ensure_dataset_archives_dir_exists(DatasetId, SpaceId) ->
    ArchivesRootDirUuid = ?ARCHIVES_ROOT_DIR_UUID(SpaceId),
    DatasetArchivesDirUuid = ?DATASET_ARCHIVES_DIR_UUID(DatasetId),
    %% @TODO VFS-11644 - Untangle special dirs and place their logic in one, well-explained place
    DatasetArchivesDirDoc = file_meta:new_special_dir_doc(
        DatasetArchivesDirUuid, ?DATASET_ARCHIVES_DIR_NAME(DatasetId),
        ?DEFAULT_DIR_PERMS, ?SPACE_OWNER_ID(SpaceId), ArchivesRootDirUuid, SpaceId
    ),
    case create_file_meta(ArchivesRootDirUuid, DatasetArchivesDirDoc) of
        ok -> ok;
        ?ERROR_NOT_FOUND ->
            {ok, _} = ensure_archives_root_dir_exists(SpaceId),
            ok = create_file_meta(ArchivesRootDirUuid, DatasetArchivesDirDoc)
    end,
    ensure_dataset_root_link_exists(DatasetId, SpaceId),
    {ok, DatasetArchivesDirUuid}.


-spec ensure_dataset_root_link_exists(dataset:id(), od_space:id()) -> ok.
ensure_dataset_root_link_exists(DatasetId, SpaceId) ->
    ok = ?ok_if_exists(?extract_ok(file_meta_forest:add(?ARCHIVES_ROOT_DIR_UUID(SpaceId), SpaceId,
        ?DATASET_ARCHIVES_DIR_NAME(DatasetId), ?DATASET_ARCHIVES_DIR_UUID(DatasetId)))).


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
            {ok, _} = ensure_dataset_archives_dir_exists(DatasetId, SpaceId),
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


-spec is_archives_root_dir_uuid(file_meta:uuid()) -> boolean().
is_archives_root_dir_uuid(<<?ARCHIVES_ROOT_DIR_UUID_PREFIX, _/binary>>) ->
    true;
is_archives_root_dir_uuid(_) ->
    false.


-spec is_archive_dir_uuid(file_meta:uuid()) -> boolean().
is_archive_dir_uuid(<<?ARCHIVE_DIR_UUID_PREFIX, _/binary>>) ->
    true;
is_archive_dir_uuid(_) ->
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


-spec get_root_dir_uuid(od_space:id()) -> file_meta:uuid().
get_root_dir_uuid(SpaceId) ->
    ?ARCHIVES_ROOT_DIR_UUID(SpaceId).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_file_meta(file_meta:uuid(), file_meta:doc()) -> ok | {error, term()}.
create_file_meta(ParentUuid, FileDoc) ->
    ?extract_ok(?ok_if_exists(file_meta:create({uuid, ParentUuid}, FileDoc))).