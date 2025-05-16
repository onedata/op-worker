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
-module(space_archives_dir).
-author("Michal Stanisz").

-behaviour(special_dir_behaviour).

% ignore this function as it uses record definitions without setting fields values
-dialyzer({nowarn_function, supported_operations/0}).

-include("middleware/middleware.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include("proto/oneclient/fuse_messages.hrl").

% API
-export([uuid/1, guid/1, ensure_exists/1]).
% special_dir_behaviour
-export([
    is_special/2,
    supported_operations/0,
    is_filesystem_root_dir/0,
    can_be_shared/0,
    is_affected_by_protection_flags/0,
    is_included_in_harvesting/0,
    is_included_in_dir_stats/0,
    is_included_in_events/0,
    is_logically_detached/0,
    exists/1
]).

-define(SUPPORTED_OPERATIONS, [
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
%%% special_dir_behaviour callbacks
%%%===================================================================

-spec uuid(od_space:id()) -> file_meta:uuid().
uuid(SpaceId) -> ?SPACE_ARCHIVES_DIR_UUID(SpaceId).


-spec guid(od_space:id()) -> file_meta:uuid().
guid(SpaceId) -> file_id:pack_guid(uuid(SpaceId), SpaceId).


-spec ensure_exists(binary()) -> ok.
ensure_exists(SpaceId) ->
    ParentUuid = space_dir:uuid(SpaceId),
    FMDoc = file_meta:new_dir_doc(uuid(SpaceId), ?SPACE_ARCHIVES_DIR_NAME, ?SPACE_ARCHIVES_DIR_PERMS,
        ?SPACE_OWNER_ID(SpaceId), ParentUuid, SpaceId
    ),
    case special_dir_docs:create(SpaceId, FMDoc, add_link) of
        created -> ?info("Created space archives directory for space '~ts'", [SpaceId]);
        exists -> ok
    end.


-spec is_special(uuid | guid, file_meta:uuid() | file_id:file_guid()) -> boolean().
is_special(uuid, ?SPACE_ARCHIVES_DIR_UUID(_SpaceId)) -> true;
is_special(guid, Guid) -> is_special(uuid, file_id:guid_to_uuid(Guid));
is_special(_, _) -> false.


-spec supported_operations() -> [middleware_worker:operation() | fslogic_worker:operation()].
supported_operations() -> ?SUPPORTED_OPERATIONS.


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
is_logically_detached() -> true.


-spec exists(file_meta:uuid()) -> boolean().
exists(Uuid) ->
    file_meta:exists(Uuid).