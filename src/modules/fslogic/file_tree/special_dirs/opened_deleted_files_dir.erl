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

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_runner.hrl").

-export([
    ensure_exists/1
]).

% special_dir_behaviour
-export([
    is_special/2,
    is_operation_allowed/1,
    exists/1
]).

-define(ALLOWED_OPERATIONS, [
    resolve_guid,

    get_file_attr,
    get_file_children,
    get_child_attr,
    get_file_children_attrs,
    get_recursive_file_list
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec ensure_exists(binary()) -> ok.
ensure_exists(SpaceId) ->
    Doc = file_meta:new_doc(
        ?OPENED_DELETED_FILES_DIR_UUID(SpaceId), ?OPENED_DELETED_FILES_DIR_DIR_NAME, ?DIRECTORY_TYPE,
        ?DEFAULT_DIR_MODE, ?SPACE_OWNER_ID(SpaceId), tmp_dir:uuid(SpaceId), SpaceId, true
    ),
    case special_dir_docs:create(SpaceId, Doc, add_link) of
        created ->
            dir_size_stats:report_file_created(?DIRECTORY_TYPE, file_id:pack_guid(tmp_dir:uuid(SpaceId), SpaceId));
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


-spec is_operation_allowed(atom()) -> boolean().
is_operation_allowed(Operation) ->
    lists:member(Operation, ?ALLOWED_OPERATIONS).


-spec exists(file_meta:uuid()) -> boolean().
exists(Uuid) ->
    file_meta:exists(Uuid).
