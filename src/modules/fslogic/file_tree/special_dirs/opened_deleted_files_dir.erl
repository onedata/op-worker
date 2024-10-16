%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Directory containing files that are deleted but not all handles are yet released.
%%% Such files have hardlinks created in this directory (@see hardlink_registry_utils).
%%% Because it is child of the tmp dir it is not synchronized between providers.
%%% @end
%%%-------------------------------------------------------------------
-module(opened_deleted_files_dir).
-author("Michal Stanisz").

-behaviour(special_dir_behaviour).

-include("proto/oneclient/fuse_messages.hrl").

% API
-export([uuid/1, guid/1, ensure_exists/1]).
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

    #get_file_attr{},
    #get_file_children{},
    #get_child_attr{},
    #get_file_children_attrs{},
    #get_recursive_file_list{}
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec uuid(od_space:id()) -> file_meta:uuid().
uuid(SpaceId) ->
    ?OPENED_DELETED_FILES_DIR_UUID(SpaceId).


-spec guid(od_space:id()) -> file_id:file_guid().
guid(SpaceId) ->
    file_id:pack_guid(uuid(SpaceId), SpaceId).


-spec ensure_exists(binary()) -> ok.
ensure_exists(SpaceId) ->
    Doc = file_meta:new_doc(
        ?OPENED_DELETED_FILES_DIR_UUID(SpaceId), ?OPENED_DELETED_FILES_DIR_DIR_NAME, ?DIRECTORY_TYPE,
        ?DEFAULT_DIR_MODE, ?SPACE_OWNER_ID(SpaceId), tmp_dir:uuid(SpaceId), SpaceId, true
    ),
    case special_dir_docs:create(SpaceId, Doc, add_link) of
        created ->
            dir_size_stats:report_file_created(?DIRECTORY_TYPE, tmp_dir:guid(SpaceId));
        exists ->
            ok
    end.


%%%===================================================================
%%% special_dir_behaviour callbacks
%%%===================================================================

-spec is_special(uuid | guid, file_meta:uuid() | file_id:file_guid()) -> boolean().
is_special(uuid, ?OPENED_DELETED_FILES_DIR_UUID(_SpaceId)) -> true;
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
is_included_in_dir_stats() -> true.


-spec is_included_in_events() -> boolean().
is_included_in_events() -> false.


-spec is_logically_detached() -> boolean().
is_logically_detached() -> false.


-spec exists(file_meta:uuid()) -> boolean().
exists(Uuid) ->
    file_meta:exists(Uuid).
