%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Root directory of an archive - for more details @see archivisation_tree.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_dir).
-author("Michal Stanisz").

-behaviour(special_dir_behaviour).

-include("modules/dataset/archivisation_tree.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").

% API
-export([uuid/1, guid/2, ensure_exists/4]).
% special_dir_behaviour
-export([is_special/2, is_operation_allowed/1, exists/1]).

-define(ALLOWED_OPERATIONS, [
    resolve_guid,
    get_file_path,
    resolve_guid_by_relative_path,

    create_dir,
    create_file,
    make_file,
    make_link,
    make_symlink,

    get_file_attr,
    get_file_children,
    get_child_attr,
    get_file_children_attrs,
    get_recursive_file_list,

    custom_metadata_get_request,
    historical_dir_size_stats_get_request,
    file_eff_dataset_summary_get_request
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec uuid(archive:id()) -> file_meta:uuid().
uuid(ArchiveId) -> ?ARCHIVE_DIR_UUID(ArchiveId).


-spec guid(od_space:id(), archive:id()) -> file_meta:uuid().
guid(SpaceId, ArchiveId) -> file_id:pack_guid(uuid(ArchiveId), SpaceId).


-spec ensure_exists(archive:id(), dataset:id(), od_space:id(), od_user:id()) -> ok.
ensure_exists(ArchiveId, DatasetId, SpaceId, ArchiveCreatorId) ->
    DatasetArchivesDirUuid = ?DATASET_ARCHIVES_DIR_UUID(DatasetId),
    ArchiveDirUuid = ?ARCHIVE_DIR_UUID(ArchiveId),
    ArchiveDirDoc = file_meta:new_dir_doc(
        ArchiveDirUuid, ?ARCHIVE_DIR_NAME(ArchiveId),
        ?DEFAULT_DIR_PERMS, ArchiveCreatorId, DatasetArchivesDirUuid, SpaceId
    ),
    dataset_archives_root_dir:ensure_exists(DatasetId, SpaceId),
    special_dir_docs:create(SpaceId, ArchiveDirDoc, add_link),
    ok.


%%%===================================================================
%%% special_dir_behaviour callbacks
%%%===================================================================

-spec is_special(uuid | guid, file_meta:uuid()) -> boolean().
is_special(uuid, ?ARCHIVE_DIR_UUID(_ArchiveId)) -> true;
is_special(guid, Guid) -> is_special(uuid, file_id:guid_to_uuid(Guid));
is_special(_, _) -> false.


-spec is_operation_allowed(atom()) -> boolean().
is_operation_allowed(Operation) ->
    lists:member(Operation, ?ALLOWED_OPERATIONS).


-spec exists(file_meta:uuid()) -> boolean().
exists(Uuid) ->
    file_meta:exists(Uuid).
