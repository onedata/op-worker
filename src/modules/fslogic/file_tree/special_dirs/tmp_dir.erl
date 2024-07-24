%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Directory which content is NOT synchronized between providers.
%%% @end
%%%-------------------------------------------------------------------
-module(tmp_dir).
-author("Michal Stanisz").

-behaviour(special_dir_behaviour).

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_runner.hrl").

% API
-export([uuid/1, guid/1, ensure_exists/1, ensure_tmp_dir_link_exists/1]).
% special_dir_behaviour
-export([is_special/2, is_operation_allowed/1, exists/1]).


-define(ALLOWED_OPERATIONS, [
    resolve_guid,
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
    get_recursive_file_list
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec uuid(od_space:id()) -> file_meta:uuid().
uuid(SpaceId) ->
    ?TMP_DIR_UUID(SpaceId).


-spec guid(od_space:id()) -> file_id:file_guid().
guid(SpaceId) ->
    file_id:pack_guid(uuid(SpaceId), SpaceId).


-spec ensure_exists(binary()) -> ok.
ensure_exists(SpaceId) ->
    TmpDirDoc = file_meta:new_doc(
        tmp_dir:uuid(SpaceId), ?TMP_DIR_NAME, ?DIRECTORY_TYPE, ?DEFAULT_DIR_MODE,
        ?SPACE_OWNER_ID(SpaceId), space_dir:uuid(SpaceId), SpaceId, true
    ),
    special_dir_docs:create(SpaceId, TmpDirDoc, add_link),
    ok.


-spec ensure_tmp_dir_link_exists(od_space:id()) -> ok.
ensure_tmp_dir_link_exists(SpaceId) ->
    special_dir_docs:add_parent_link(space_dir:uuid(SpaceId), SpaceId, ?TMP_DIR_NAME, tmp_dir:uuid(SpaceId)).


%%%===================================================================
%%% special_dir_behaviour callbacks
%%%===================================================================

-spec is_special(uuid | guid, file_meta:uuid()) -> boolean().
is_special(uuid, <<?TMP_DIR_UUID_PREFIX, _SpaceId/binary>>) -> true;
is_special(guid, Guid) -> is_special(uuid, file_id:guid_to_uuid(Guid));
is_special(_, _) -> false.

-spec is_operation_allowed(atom()) -> boolean().
is_operation_allowed(Operation) ->
    lists:member(Operation, ?ALLOWED_OPERATIONS).


-spec exists(file_meta:uuid()) -> boolean().
exists(Uuid) ->
    file_meta:exists(Uuid).
