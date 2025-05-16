%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Virtual root directory containing all supported spaces.
%%% NOTE: spaces are also linked to virtual root directories of each user belonging to space (@see user_root_dir)
%%% @end
%%%-------------------------------------------------------------------
-module(global_root_dir).
-author("Michal Stanisz").

-behaviour(special_dir_behaviour).

-include("modules/fslogic/fslogic_common.hrl").

% API
-export([uuid/0, guid/0]).
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
    exists/1,
    get_file_meta/1
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec uuid() -> file_meta:uuid().
uuid() ->
    ?GLOBAL_ROOT_DIR_UUID.


-spec guid() -> file_id:file_guid().
guid() ->
    file_id:pack_guid(uuid(), ?ROOT_DIR_VIRTUAL_SPACE_ID).


%%%===================================================================
%%% special_dir_behaviour callbacks
%%%===================================================================

-spec is_special(uuid | guid, file_meta:uuid()) -> boolean().
is_special(uuid, ?GLOBAL_ROOT_DIR_UUID) -> true;
is_special(guid, Guid) -> is_special(uuid, file_id:guid_to_uuid(Guid));
is_special(_, _) -> false.


-spec supported_operations() -> [middleware_worker:operation() | fslogic_worker:operation()].
supported_operations() -> [].


-spec is_filesystem_root_dir() -> boolean().
is_filesystem_root_dir() -> true.


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
exists(_) ->
    true.


-spec get_file_meta(file_meta:uuid()) -> file_meta:doc().
get_file_meta(_) ->
    #document{
        key = ?GLOBAL_ROOT_DIR_UUID,
        value = #file_meta{
            name = ?GLOBAL_ROOT_DIR_NAME,
            is_scope = true,
            mode = ?DEFAULT_DIR_PERMS,
            owner = ?ROOT_USER_ID,
            parent_uuid = ?GLOBAL_ROOT_DIR_UUID
        },
        scope = ?ROOT_DIR_SCOPE
    }.
