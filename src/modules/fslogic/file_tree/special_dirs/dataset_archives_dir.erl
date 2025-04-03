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
-module(dataset_archives_dir).
-author("Michal Stanisz").

-behaviour(special_dir_behaviour).

% ignore this function as it uses record definitions without setting fields values
-dialyzer({nowarn_function, allowed_operations/0}).

-include("middleware/middleware.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/dataset/archivisation_tree.hrl").

% API
-export([uuid/1, ensure_exists/2, ensure_parent_link_exists/2, delete_parent_link/2]).
% special_dir_behaviour
-export([
    is_special/2,
    allowed_operations/0,
    is_filesystem_root_dir/0,
    can_be_shared/0,
    is_affected_by_protection_flags/0,
    is_included_in_harvesting/0,
    is_included_in_dir_stats/0,
    is_included_in_events/0,
    is_logically_detached/0,
    exists/1
]).

-define(ALLOWED_OPERATIONS, [
    #resolve_guid{},
    #resolve_guid_by_relative_path{},

    #get_file_attr{},
    #get_file_children{},
    #get_child_attr{},
    #get_file_children_attrs{},
    #get_recursive_file_list{},

    #historical_dir_size_stats_get_request{}
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec uuid(od_space:id()) -> file_meta:uuid().
uuid(DatasetId) -> ?DATASET_ARCHIVES_DIR_UUID(DatasetId).


-spec ensure_exists(binary(), binary()) -> ok.
ensure_exists(DatasetId, SpaceId) ->
    ParentUuid = space_archives_dir:uuid(SpaceId),
    FMDoc = file_meta:new_dir_doc(uuid(DatasetId), ?DATASET_ARCHIVES_DIR_NAME(DatasetId),
        ?DEFAULT_DIR_PERMS, ?SPACE_OWNER_ID(SpaceId), ParentUuid, SpaceId
    ),
    space_archives_dir:ensure_exists(SpaceId),
    case special_dir_docs:create(SpaceId, FMDoc, add_link) of
        created -> ?info("Created dataset archives directory for dataset '~ts'.", [DatasetId]);
        exists -> ok
    end.


-spec ensure_parent_link_exists(dataset:id(), od_space:id()) -> ok.
ensure_parent_link_exists(DatasetId, SpaceId) ->
    special_dir_docs:ensure_parent_link(space_archives_dir:uuid(SpaceId), SpaceId,
        ?DATASET_ARCHIVES_DIR_NAME(DatasetId), uuid(DatasetId)).


-spec delete_parent_link(dataset:id(), od_space:id()) -> ok.
delete_parent_link(DatasetId, SpaceId) ->
    special_dir_docs:delete_parent_link(space_archives_dir:uuid(SpaceId), SpaceId,
        ?DATASET_ARCHIVES_DIR_NAME(DatasetId), uuid(DatasetId)).

%%%===================================================================
%%% special_dir_behaviour callbacks
%%%===================================================================

-spec is_special(uuid | guid, file_meta:uuid() | file_id:file_guid()) -> boolean().
is_special(uuid, ?DATASET_ARCHIVES_DIR_UUID(_DatasetId)) -> true;
is_special(guid, Guid) -> is_special(uuid, file_id:guid_to_uuid(Guid));
is_special(_, _) -> false.


-spec allowed_operations() -> [middleware_worker:operation() | fslogic_worker:operation()].
allowed_operations() -> ?ALLOWED_OPERATIONS.


-spec is_filesystem_root_dir() -> boolean().
is_filesystem_root_dir() -> false.


-spec can_be_shared() -> boolean().
can_be_shared() -> false.


-spec is_affected_by_protection_flags() -> boolean().
is_affected_by_protection_flags() -> false.


-spec is_included_in_harvesting() -> boolean().
is_included_in_harvesting() -> false.


-spec is_included_in_dir_stats() -> boolean().
is_included_in_dir_stats() -> false.


-spec is_included_in_events() -> boolean().
is_included_in_events() -> true.


-spec is_logically_detached() -> boolean().
is_logically_detached() -> false.


-spec exists(file_meta:uuid()) -> boolean().
exists(Uuid) ->
    file_meta:exists(Uuid).
