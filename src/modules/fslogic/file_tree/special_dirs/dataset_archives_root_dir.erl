%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Directory containing all archives of a dataset - for more details @see archivisation_tree.
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_archives_root_dir).
-author("Michal Stanisz").

-behaviour(special_dir_behaviour).

-include("modules/dataset/archivisation_tree.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").

% API
-export([uuid/1, ensure_exists/2, ensure_parent_link_exists/2, delete_parent_link/2]).
% special_dir_behaviour
-export([
    is_special/2,
    is_operation_allowed/1,
    is_scope_root_dir/0,
    is_restricted_for_datasets/0,
    is_harvested/0,
    is_ignored_in_dir_stats/0,
    is_ignored_in_events/0,
    is_without_parent/0,
    exists/1
]).

-define(ALLOWED_OPERATIONS, [
    resolve_guid,
    resolve_guid_by_relative_path,

    get_file_attr,
    get_file_children,
    get_child_attr,
    get_file_children_attrs,
    get_recursive_file_list,

    historical_dir_size_stats_get_request
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec uuid(od_space:id()) -> file_meta:uuid().
uuid(DatasetId) -> ?DATASET_ARCHIVES_DIR_UUID(DatasetId).


-spec ensure_exists(binary(), binary()) -> ok.
ensure_exists(DatasetId, SpaceId) ->
    ParentUuid = space_archives_root_dir:uuid(SpaceId),
    FMDoc = file_meta:new_dir_doc(uuid(DatasetId), ?DATASET_ARCHIVES_DIR_NAME(DatasetId),
        ?DEFAULT_DIR_PERMS, ?SPACE_OWNER_ID(SpaceId), ParentUuid, SpaceId
    ),
    space_archives_root_dir:ensure_exists(SpaceId),
    special_dir_docs:create(SpaceId, FMDoc, add_link),
    ok.


-spec ensure_parent_link_exists(dataset:id(), od_space:id()) -> ok.
ensure_parent_link_exists(DatasetId, SpaceId) ->
    special_dir_docs:ensure_parent_link(space_archives_root_dir:uuid(SpaceId), SpaceId,
        ?DATASET_ARCHIVES_DIR_NAME(DatasetId), uuid(DatasetId)).


-spec delete_parent_link(dataset:id(), od_space:id()) -> ok.
delete_parent_link(DatasetId, SpaceId) ->
    special_dir_docs:delete_parent_link(space_archives_root_dir:uuid(SpaceId), SpaceId,
        ?DATASET_ARCHIVES_DIR_NAME(DatasetId), uuid(DatasetId)).

%%%===================================================================
%%% special_dir_behaviour callbacks
%%%===================================================================

-spec is_special(uuid | guid, file_meta:uuid() | file_id:file_guid()) -> boolean().
is_special(uuid, ?DATASET_ARCHIVES_DIR_UUID(_DatasetId)) -> true;
is_special(guid, Guid) -> is_special(uuid, file_id:guid_to_uuid(Guid));
is_special(_, _) -> false.


-spec is_operation_allowed(atom()) -> boolean().
is_operation_allowed(Operation) ->
    lists:member(Operation, ?ALLOWED_OPERATIONS).


-spec is_scope_root_dir() -> boolean().
is_scope_root_dir() -> false.


-spec is_restricted_for_datasets() -> boolean().
is_restricted_for_datasets() -> true.


-spec is_harvested() -> boolean().
is_harvested() -> false.


-spec is_ignored_in_dir_stats() -> boolean().
is_ignored_in_dir_stats() -> true.


-spec is_ignored_in_events() -> boolean().
is_ignored_in_events() -> false.


-spec is_without_parent() -> boolean().
is_without_parent() -> false.


-spec exists(file_meta:uuid()) -> boolean().
exists(Uuid) ->
    file_meta:exists(Uuid).
