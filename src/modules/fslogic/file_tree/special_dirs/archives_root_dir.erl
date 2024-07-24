%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Directory containing all archives of a space - for more details @see archivisation_tree.
%%% @end
%%%-------------------------------------------------------------------
-module(archives_root_dir).
-author("Michal Stanisz").

-behaviour(special_dir_behaviour).

-include("modules/dataset/archivisation_tree.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").

% API
-export([uuid/1, guid/1, ensure_exists/1]).
% special_dir_behaviour
-export([is_special/2, is_operation_allowed/1, exists/1]).

-define(ALLOWED_OPERATIONS, [
    resolve_guid,
    resolve_guid_by_relative_path,

    get_file_attr,
    get_file_children,
    get_child_attr,
    get_file_children_attrs,
    get_recursive_file_list
]).

%%%===================================================================
%%% special_dir_behaviour callbacks
%%%===================================================================

-spec uuid(od_space:id()) -> file_meta:uuid().
uuid(SpaceId) -> ?ARCHIVES_ROOT_DIR_UUID(SpaceId).


-spec guid(od_space:id()) -> file_meta:uuid().
guid(SpaceId) -> file_id:pack_guid(uuid(SpaceId), SpaceId).


-spec is_special(uuid | guid, file_meta:uuid() | file_id:file_guid()) -> boolean().
is_special(uuid, ?ARCHIVES_ROOT_DIR_UUID(_SpaceId)) -> true;
is_special(guid, Guid) -> is_special(uuid, file_id:guid_to_uuid(Guid));
is_special(_, _) -> false.


-spec ensure_exists(binary()) -> ok.
ensure_exists(SpaceId) ->
    SpaceDirUuid = space_dir:uuid(SpaceId),
    ArchivesRootDirUuid = ?ARCHIVES_ROOT_DIR_UUID(SpaceId),
    ArchivesRootDirDoc = file_meta:new_dir_doc(
        ArchivesRootDirUuid, ?ARCHIVES_ROOT_DIR_NAME, ?ARCHIVES_ROOT_DIR_PERMS, ?SPACE_OWNER_ID(SpaceId),
        SpaceDirUuid, SpaceId
    ),
    special_dir_docs:create(SpaceId, ArchivesRootDirDoc, add_link),
    ok.


-spec is_operation_allowed(atom()) -> boolean().
is_operation_allowed(Operation) ->
    lists:member(Operation, ?ALLOWED_OPERATIONS).


-spec exists(file_meta:uuid()) -> boolean().
exists(Uuid) ->
    file_meta:exists(Uuid).