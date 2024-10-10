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

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_runner.hrl").


% API
-export([uuid/1, guid/1, ensure_exists/1, extract_space_id/1]).
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
    move_to_trash,
    delete_file,
    change_mode
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


-spec is_operation_allowed(atom()) -> boolean().
is_operation_allowed(Operation) ->
    not lists:member(Operation, ?DISALLOWED_OPERATIONS).


-spec is_scope_root_dir() -> boolean().
is_scope_root_dir() -> false.


-spec is_restricted_for_datasets() -> boolean().
is_restricted_for_datasets() -> false.


-spec is_harvested() -> boolean().
is_harvested() -> true.


-spec is_ignored_in_dir_stats() -> boolean().
is_ignored_in_dir_stats() -> false.


-spec is_ignored_in_events() -> boolean().
is_ignored_in_events() -> false.


-spec is_without_parent() -> boolean().
is_without_parent() -> false.


-spec exists(file_meta:uuid()) -> boolean().
exists(Uuid) ->
    file_meta:exists(Uuid).
