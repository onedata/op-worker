%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Directory of a space. It is treated as normal directory and as such
%%% it's modification is controlled by access rights. All documents associated
%%% with it (and files/dirs inside of it) are synchronized among providers
%%% supporting this space.
%%% @end
%%%-------------------------------------------------------------------
-module(space_dir).
-author("Michal Stanisz").

-behaviour(special_dir_behaviour).

-include("middleware/middleware.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/fslogic_operations.hrl").
-include("modules/datastore/datastore_runner.hrl").


% API
-export([uuid/1, guid/1, ensure_exists/1, extract_space_id/1]).
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

-define(SPACE_ROOT_PREFIX, "space_").

-define(FILE_META_DOC(SpaceId), #document{
    key = uuid(SpaceId),
    value = #file_meta{
        name = SpaceId,
        type = ?DIRECTORY_TYPE,
        mode = ?DEFAULT_DIR_MODE,
        owner = ?SPACE_OWNER_ID(SpaceId),
        is_scope = true,
        parent_uuid = ?GLOBAL_ROOT_DIR_UUID
    },
    scope = SpaceId
}).

-define(DISALLOWED_OPERATIONS, [
    #move_to_trash{}
    #delete_file{}
    #change_mode{}
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec uuid(od_space:id()) -> file_meta:uuid().
uuid(SpaceId) ->
    <<?SPACE_ROOT_PREFIX, SpaceId/binary>>.


-spec guid(od_space:id()) -> file_id:file_guid().
guid(SpaceId) ->
    file_id:pack_guid(uuid(SpaceId), SpaceId).


-spec ensure_exists(binary()) -> ok.
ensure_exists(SpaceId) ->
    special_dir_docs:create(SpaceId, ?FILE_META_DOC(SpaceId), add_link),
    ok.


-spec extract_space_id(file_meta:uuid()) -> od_space:id().
extract_space_id(<<?SPACE_ROOT_PREFIX, SpaceId/binary>>) -> SpaceId.


%%%===================================================================
%%% special_dir_behaviour callbacks
%%%===================================================================

-spec is_special(uuid | guid, file_meta:uuid()) -> boolean().
is_special(uuid, <<?SPACE_ROOT_PREFIX, _SpaceId/binary>>) -> true;
is_special(guid, Guid) -> is_special(uuid, file_id:guid_to_uuid(Guid));
is_special(_, _) -> false.


-spec allowed_operations() -> [middleware_worker:operation() | fslogic_worker:operation()].
allowed_operations() ->
    (?MIDDLEWARE_ALL_OPERATIONS ++ ?FSLOGIC_ALL_OPERATIONS) -- ?DISALLOWED_OPERATIONS.


-spec is_filesystem_root_dir() -> boolean().
is_filesystem_root_dir() -> false.


-spec can_be_shared() -> boolean().
can_be_shared() -> true.


-spec is_affected_by_protection_flags() -> boolean().
is_affected_by_protection_flags() -> true.


-spec is_included_in_harvesting() -> boolean().
is_included_in_harvesting() -> true.


-spec is_included_in_dir_stats() -> boolean().
is_included_in_dir_stats() -> true.


-spec is_included_in_events() -> boolean().
is_included_in_events() -> true.


-spec is_logically_detached() -> boolean().
is_logically_detached() -> false.


-spec exists(file_meta:uuid()) -> boolean().
exists(Uuid) ->
    file_meta:exists(Uuid).
